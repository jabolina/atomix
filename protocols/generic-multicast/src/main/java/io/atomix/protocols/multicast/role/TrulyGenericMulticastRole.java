/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.multicast.role;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.multicast.exception.GenericMulticastError;
import io.atomix.protocols.multicast.exception.GenericMulticastException;
import io.atomix.protocols.multicast.impl.GenericMulticastSession;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.protocols.multicast.protocol.message.response.GenericMulticastResponse;
import io.atomix.protocols.multicast.role.conflict.UniversalHashConflictHandler;
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;
import io.atomix.utils.concurrent.Futures;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public final class TrulyGenericMulticastRole extends AbstractGenericMulticastRole {
  private final Set<OperationRequest> previousSet = Sets.newConcurrentHashSet();
  private final AtomicLong localTimestamp = new AtomicLong(0);

  public TrulyGenericMulticastRole(GenericMulticastServiceContext serviceContext) {
    super(serviceContext, new UniversalHashConflictHandler());
  }

  @Override
  public CompletableFuture<ExecuteResponse> onExecute(ExecuteRequest request) {
    if (request.operation().id().type().equals(OperationType.QUERY)) {
      return query(request);
    }

    if (request.operation().id().type().equals(OperationType.COMMAND)) {
      return command(request);
    }

    return Futures.exceptionalFuture(new IllegalArgumentException("Unknown operation type"));
  }

  @Override
  public CompletableFuture<ComputeResponse> onCompute(ComputeRequest request) {
    Session session = serviceContext.sessionManager().getOrCreate(request.session(), request.memberId());
    if (OperationRequest.State.S0.equals(request.state())) {
      if (conflictHandler.conflict(request, Lists.newArrayList(previousSet))) {
        // Since all processes executing this are all inside the same data structure primitive
        // we have that the timestamp is shared amongst all participants of the protocol already
        // This will be the same as executing for a single group (?)
        localTimestamp.incrementAndGet();
        previousSet.clear();
      }
      request.sequence(serviceContext.timestampProvider().get());
      previousSet.add(request);
    }

    // We have a situation here. When asking for participants that are not local participants.
    // One of the key features for scalability in Atomix is the option to create a data structure
    // primitive across many partitions as configured and for each partition will exist
    // an instance of the desired protocol being executed (e.g. Raft), using this group
    // partition more parallelism can be achieved for replication, because this will allow
    // for the Raft leaders to replicated concurrently across multiple partitions instead of a
    // single Raft leader replicating multiple partitions.
    // So this means that when retrieved the participants for the protocol is every process
    // or is it only the one inside the same partition as where the request is being executed?
    // This was supposed to count across how many partitions the primitive is spread, and then
    // send the request to all others members on another partitions.
    if (serviceContext.participants().size() > 1) {
      if (OperationRequest.State.S0.equals(request.state())) {
        request.state(OperationRequest.State.S1);
        request.sequence(localTimestamp.get());
      } else if (OperationRequest.State.S2.equals(request.state())) {
        request.state(OperationRequest.State.S3);
        if (request.sequence() > localTimestamp.get()) {
          localTimestamp.set(request.sequence());
          previousSet.clear();
        }
      }
    } else {
      request.sequence(localTimestamp.get());
      request.state(OperationRequest.State.S3);
    }
    return issuer.issue(build(GatherRequest.builder(), request, session))
        .thenApply(res -> build(ComputeResponse.builder(), res));
  }

  @Override
  public CompletableFuture<GatherResponse> onGather(GatherRequest request) {
    Session session = serviceContext.sessionManager().getOrCreate(request.session(), request.memberId());
    long ts = Math.max(request.sequence(), serviceContext.timestampProvider().get());
    if (request.sequence() >= ts) {
      if (OperationRequest.State.S3.equals(request.state())) {
        return Futures.completedFuture(GatherResponse.builder()
            .withIdentifier(request.identifier())
            .withStatus(GenericMulticastResponse.Status.OK)
            .withResult(request.operation().value())
            .withTs(request.sequence())
            .build());
      }
      request.state(OperationRequest.State.S3);
      return issuer.issue(build(GatherRequest.builder(), request, session))
          .thenApply(res -> build(GatherResponse.builder(), res));
    }

    request.sequence(ts);
    request.state(OperationRequest.State.S2);
    return issuer.issue(build(ComputeRequest.builder(), request, session))
        .thenApply(res -> build(GatherResponse.builder(), res));
  }

  @Override
  public CompletableFuture<CloseResponse> onClose(CloseRequest request) {
    GenericMulticastSession session = serviceContext.sessionManager().get(request.session());
    if (session == null) {
      return CompletableFuture.completedFuture(CloseResponse.builder()
          .withStatus(GenericMulticastResponse.Status.ERROR)
          .withError(GenericMulticastError.Error.ILLEGAL_MEMBER_STATE)
          .withIdentifier(request.identifier())
          .build());
    }

    serviceContext.nextIndex();
    long timestamp = System.currentTimeMillis();
    return issuer.issue(build(CloseRequest.builder(), request, session))
        .thenApply(response -> {
          this.close(session);
          serviceContext.setTimestamp(timestamp);
          stop();
          return build(CloseResponse.builder(), response);
        });
  }

  @Override
  public CompletableFuture<Void> close(GenericMulticastSession session) {
    serviceContext.sessionManager().close(session.sessionId().id());
    return CompletableFuture.completedFuture(null);
  }

  /**
   * This method will work as both `GM-Cast` and `EnqueueMessage` procedures on the
   * algorithm.
   *
   * Working as:
   *
   * <pre>
   * {@code
   *  procedure COMMAND
   *  When: RM-Deliver<m>
   *    m.state <- S0
   *    for all q in serviceContext.participants():
   *      RM-Cast<m> to q
   * }
   * </pre>
   *
   * @param request: command that changes the data structure
   * @return a promise to be completed with the change applied
   */
  private CompletableFuture<ExecuteResponse> command(ExecuteRequest request) {
    if (request.state() == null || OperationRequest.State.S0.equals(request.state())) {
      Session session = serviceContext.sessionManager().getOrCreate(request.session(), request.memberId());
      request.state(OperationRequest.State.S0);
      // The issuer will enqueue the the request and sent it to all others participants
      return issuer.issue(build(ComputeRequest.builder(), request, session))
          .thenApply(res -> build(ExecuteResponse.builder(), res));
    }

    throw new GenericMulticastException.IllegalMemberState("Execute message state differs from S0 cannot be applied");
  }

  private CompletableFuture<ExecuteResponse> query(ExecuteRequest request) {
    return CompletableFuture.completedFuture(queryAndCommit(request));
  }
}

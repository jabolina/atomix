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
package io.atomix.protocols.multicast.service;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.multicast.impl.GenericMulticastSession;
import io.atomix.protocols.multicast.protocol.GenericMulticastServerProtocol;
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.protocols.multicast.role.GenericMulticastRole;
import io.atomix.protocols.multicast.role.TrulyGenericMulticastRole;
import io.atomix.protocols.multicast.session.GenericMulticastSessionManager;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

public class GenericMulticastServiceContext implements ServiceContext {
  private final Logger log;
  private final String serverName;
  private final PrimitiveId primitiveId;
  private final PrimitiveType primitiveType;
  private final PrimitiveDescriptor descriptor;
  private final ThreadContext threadContext;
  private final ClusterMembershipService clusterMembershipService;
  private final GenericMulticastServerProtocol protocol;
  private final PrimitiveService service;
  private final ServiceConfig serviceConfig;
  private final MemberId localMemberId;
  private final GenericMulticastSessionManager sessionManager;
  private final AtomicCounter timestampProvider;
  private final PrimaryElection primaryElection;
  private long currentTimestamp;
  private final WallClock wallClock = new WallClock() {
    @Override
    public WallClockTimestamp getTime() {
      return new WallClockTimestamp(currentTimestamp);
    }
  };
  private final LogicalClock logicalClock = new LogicalClock() {
    @Override
    public LogicalTimestamp getTime() {
      return new LogicalTimestamp(operationIndex);
    }
  };
  private final PrimaryElectionEventListener primaryElectionListener = ev -> changeParticipants(ev.term());
  private final ClusterMembershipEventListener membershipEventListener = this::handleClusterEvent;

  private OperationType currentOperation = OperationType.COMMAND;
  private Session currentSession;
  private long operationIndex;
  private long currentIndex;
  private long currentTerm;
  private GenericMulticastRole localParticipant;
  private List<MemberId> members;

  public GenericMulticastServiceContext(
      String serverName,
      PrimitiveId primitiveId,
      PrimitiveType primitiveType,
      PrimitiveDescriptor descriptor,
      ThreadContext threadContext,
      ClusterMembershipService clusterMembershipService,
      GenericMulticastServerProtocol protocol,
      AtomicCounter timestampProvider,
      PrimaryElection primaryElection) {
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(getClass())
        .addValue(serverName)
        .build());
    this.serverName = serverName;
    this.primitiveId = primitiveId;
    this.primitiveType = primitiveType;
    this.descriptor = descriptor;
    this.threadContext = threadContext;
    this.primaryElection = primaryElection;
    this.clusterMembershipService = clusterMembershipService;
    this.protocol = protocol;
    this.serviceConfig = Serializer.using(primitiveType.namespace()).decode(descriptor.config());
    this.service = primitiveType.newService(serviceConfig);
    this.localMemberId = clusterMembershipService.getLocalMember().id();
    this.sessionManager = new GenericMulticastSessionManager(this, this.service);
    this.timestampProvider = timestampProvider;
    this.members = new ArrayList<>();
    this.localParticipant = new TrulyGenericMulticastRole(this);
    clusterMembershipService.addListener(membershipEventListener);
    primaryElection.addListener(primaryElectionListener);
  }

  @Override
  public PrimitiveId serviceId() {
    return primitiveId;
  }

  @Override
  public String serviceName() {
    return serverName;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  public MemberId localMemberId() {
    return localMemberId;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends ServiceConfig> C serviceConfig() {
    return (C) serviceConfig;
  }

  @Override
  public long currentIndex() {
    return currentIndex;
  }

  @Override
  public Session currentSession() {
    return currentSession;
  }

  @Override
  public OperationType currentOperation() {
    return currentOperation;
  }

  @Override
  public LogicalClock logicalClock() {
    return logicalClock;
  }

  @Override
  public WallClock wallClock() {
    return wallClock;
  }

  public PrimitiveService primitiveService() {
    return this.service;
  }

  /**
   * Retrieve the server timestamp provider
   *
   * @return the timestamp provider
   */
  public AtomicCounter timestampProvider() {
    return timestampProvider;
  }

  public Session setSession(Session session) {
    this.currentSession = session;
    return session;
  }

  /**
   * Return service thread context
   *
   * @return service thread context
   */
  public ThreadContext threadContext() {
    return threadContext;
  }

  /**
   * Returns the server protocol
   *
   * @return the server protocol
   */
  public GenericMulticastServerProtocol protocol() {
    return protocol;
  }

  /**
   * Returns the session manager
   *
   * @return session manager
   */
  public GenericMulticastSessionManager sessionManager() {
    return sessionManager;
  }

  /**
   * Returns the participants nodes member ids
   *
   * @return participants member ids
   */
  public List<MemberId> participants() {
    return members;
  }

  /**
   * Returns the primitive descriptor
   *
   * @return the primitive descriptor
   */
  public PrimitiveDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Increments and returns the next service index
   *
   * @return the next index
   */
  public long nextIndex() {
    currentOperation = OperationType.COMMAND;
    return ++operationIndex;
  }

  /**
   * Returns the current index
   *
   * @return the current index
   */
  public long getIndex() {
    currentOperation = OperationType.QUERY;
    return currentIndex;
  }

  /**
   * Update the last acked index
   *
   * @param index: acked index to update
   * @return current index
   */
  public long setIndex(long index) {
    currentOperation = OperationType.COMMAND;
    currentIndex = index;
    return currentIndex;
  }

  /**
   * Returns the current wall clock timestamp.
   *
   * @return the current wall clock timestamp
   */
  public long currentTimestamp() {
    return currentTimestamp;
  }

  /**
   * Sets the current timestamp.
   *
   * @param timestamp the updated timestamp
   * @return the current timestamp
   */
  public long setTimestamp(long timestamp) {
    this.currentTimestamp = timestamp;
    service.tick(WallClockTimestamp.from(timestamp));
    return currentTimestamp;
  }

  /**
   * Closes the service.
   *
   * @return future to be completed when everything is stopped
   */
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      try {
        clusterMembershipService.removeListener(membershipEventListener);
        primaryElection.removeListener(primaryElectionListener);
        localParticipant.stop();
      } finally {
        future.complete(null);
      }
    });
    return future.thenRunAsync(threadContext::close);
  }

  /**
   * Opens the service context.
   *
   * @return a future to be completed once the service context has been opened
   */
  public CompletableFuture<Void> open() {
    service.init(this);
    return primaryElection.getTerm()
        .thenAccept(this::changeParticipants)
        .thenRun(() -> service.init(this));
  }

  /**
   * Handles a execute request.
   *
   * @param request the execute request
   * @return future to be completed with a execute response
   */
  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    ComposableFuture<ExecuteResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> localParticipant.onExecute(request).whenComplete(future));
    return future;
  }

  /**
   * Handles a compute request.
   *
   * @param request the compute request
   * @return future to be completed with a compute response
   */
  public CompletableFuture<ComputeResponse> compute(ComputeRequest request) {
    ComposableFuture<ComputeResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> localParticipant.onCompute(request).whenComplete(future));
    return future;
  }

  /**
   * Handles a gather request.
   *
   * @param request the gather request
   * @return future to be completed with a gather response
   */
  public CompletableFuture<GatherResponse> gather(GatherRequest request) {
    ComposableFuture<GatherResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> localParticipant.onGather(request).whenComplete(future));
    return future;
  }

  /**
   * Handles a close request.
   *
   * @param request the close request
   * @return future to be completed with a close response
   */
  public CompletableFuture<CloseResponse> close(CloseRequest request) {
    ComposableFuture<CloseResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> localParticipant.onClose(request));
    return future;
  }

  public GenericMulticastRole getLocalParticipant() {
    return localParticipant;
  }

  /**
   * Handle the election participants
   *
   * @param term: election term
   */
  private void changeParticipants(PrimaryTerm term) {
    threadContext.execute(() -> {
      if (term.term() > currentTerm) {
        log.debug("Term changed {}", term);
        currentTerm = term.term();
        MemberId primary = term.primary().memberId();
        members = term.candidates()
            .stream()
            .map(GroupMember::memberId)
            .collect(Collectors.toList());
        if (!members.contains(primary)) {
          members.add(primary);
        }
        log.info("participants {}", toStringHelper(members));
      }
    });
  }

  /**
   * handle cluster events
   *
   * @param event: cluster event
   */
  private void handleClusterEvent(ClusterMembershipEvent event) {
    threadContext.execute(() -> {
      log.info("cluster ev {}", event);
      if (ClusterMembershipEvent.Type.MEMBER_REMOVED.equals(event.type())) {
        members.remove(event.subject().id());
        for (GenericMulticastSession session: sessionManager.values()) {
          if (session.memberId().equals(event.subject().id())) {
            localParticipant.close(session);
          }
        }
      }

      if (ClusterMembershipEvent.Type.MEMBER_ADDED.equals(event.type()) && !members.contains(event.subject().id())) {
        members.add(event.subject().id());
      }
    });
  }
}

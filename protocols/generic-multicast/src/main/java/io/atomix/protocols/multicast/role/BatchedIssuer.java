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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.MemberId;
import io.atomix.protocols.multicast.exception.GenericMulticastException;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@SuppressWarnings("unchecked")
public class BatchedIssuer implements Issuer {
  private final Logger log;
  private final GenericMulticastServiceContext serviceContext;
  private final GenericMulticastDeliverSequence deliver;
  private final Map<MemberId, BatchQueue> queues = Maps.newConcurrentMap();
  private final Map<UUID, CompletableFuture<OperationResponse>> futures = Maps.newConcurrentMap();
  private final BiConsumer<OperationResponse, Throwable> removeOnFinish = (response, throwable) -> futures.remove(response.identifier());

  public BatchedIssuer(GenericMulticastServiceContext serviceContext, GenericMulticastDeliverSequence deliver, Logger log) {
    this.log = log;
    this.serviceContext = serviceContext;
    this.deliver = deliver;
  }

  @Override
  public <T extends OperationRequest> CompletableFuture<OperationResponse> issue(T operation, CompletableFuture<OperationResponse> future) {
    if (serviceContext.participants().isEmpty()) {
      return Futures.exceptionalFuture(new GenericMulticastException.ClosedSession("Server dont exists"));
    }

    if (operation instanceof ComputeRequest) {
      serviceContext.protocol().compute((ComputeRequest) operation, serviceContext.participants());
    } else {
      for (MemberId memberId: serviceContext.participants()) {
        log.trace("Issuing for [{}:{}] {}", memberId, operation.identifier(), operation);
        queues.computeIfAbsent(memberId, BatchQueue::new).add(operation);
      }
    }

    deliver.request(operation);
    return future;
  }

  @Override
  public <T extends OperationRequest> CompletableFuture<OperationResponse> issue(T operation) {
    return futures.compute(operation.identifier(), (uuid, future) -> {
      if (future == null || future.isDone()) {
        log.trace("Creating future for [{}]", operation);
        future = new CompletableFuture<>();
        future.whenCompleteAsync(removeOnFinish.andThen(deliver), serviceContext.threadContext());
      }

      return issue(operation, future);
    });
  }

  @Override
  public void close() {
    queues.values().forEach(BatchQueue::close);
  }

  private final class BatchQueue<T extends OperationRequest, U extends OperationResponse> {
    private static final long MAX_BATCH_TIME = 100;
    private static final long MAX_BATCH_SIZE = 100;
    private final Set<T> operations = Sets.newConcurrentHashSet();
    private final Scheduled issueTimer;
    private final MemberId memberId;
    private long lastSent;

    private BatchQueue(MemberId memberId) {
      this.memberId = memberId;
      this.issueTimer = serviceContext.threadContext()
          .schedule(Duration.ofMillis(MAX_BATCH_TIME / 2), Duration.ofMillis(MAX_BATCH_TIME / 2), this::tryIssue);
    }

    void add(T operation) {
      operations.add(operation);
      if (operations.size() >= MAX_BATCH_SIZE) {
        issue();
      }
    }

    private void tryIssue() {
      if (System.currentTimeMillis() - lastSent > MAX_BATCH_TIME && !operations.isEmpty()) {
        issue();
      }
    }

    private void issue() {
      List<T> ready = ImmutableList.copyOf(operations);
      operations.clear();
      issue(ready);
    }

    private void issue(List<T> operations) {
      for (T operation: operations) {
        log.trace("Batching [{}] [{}]", memberId, operation);
        deliver.request(operation);
        if (operation instanceof GatherRequest) {
          serviceContext.protocol().gather((GatherRequest) operation, memberId)
              .whenCompleteAsync((res, err) -> response(
                  operation,
                  (U) res,
                  err
              ), serviceContext.threadContext());
        } else if (operation instanceof CloseRequest) {
          serviceContext.protocol().close((CloseRequest) operation, memberId)
              .whenCompleteAsync((res, err) -> response(
                  operation,
                  (U) res,
                  err
              ), serviceContext.threadContext());
        } else {
          log.error("Unknown request type {}", operation);
        }
      }

      lastSent = System.currentTimeMillis();
    }

    private void response(T req, U res, Throwable err) {
      if (err == null) {
        if (req instanceof GatherRequest && OperationRequest.State.S3.equals(req.state())) {
          deliver.response(req, res, futures.get(req.identifier()));
        }
      } else {
        log.error("Error response {}", res, err);
      }
    }

    void close() {
      issueTimer.cancel();
    }
  }
}

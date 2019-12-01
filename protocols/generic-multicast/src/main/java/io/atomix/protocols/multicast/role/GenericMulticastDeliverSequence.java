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
import io.atomix.protocols.multicast.exception.GenericMulticastException;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.protocols.multicast.role.conflict.GenericMulticastConflictHandler;
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;
import io.atomix.utils.concurrent.ThreadContext;
import org.slf4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Client response sequencer
 * <p>
 * This will works with similar intentions as RaftSessionSequencer.
 * When the generic multicast protocol is finished, the result will be delivered ordered by the
 * final timestamp present in the message.
 * <p>
 * Each {@link OperationRequest m} with {@link OperationRequest.State} equals to {@link OperationRequest.State#S3} already has its
 * final timestamp and is ready to be delivered on the right order. Firstly, for each message m that do
 * not conflict with any other message with states {@link OperationRequest.State#S0}, {@link OperationRequest.State#S1}
 * and {@link OperationRequest.State#S2}, m can be GM-Delivered.
 * <p>
 * Secondly, the algorithm search for the message m with the smaller timestamp {@link OperationRequest#sequence()}
 * between all other messages, if m is on state {@link OperationRequest.State#S3} and do not conflict with any other
 * message on the same timestamp, m can be delivered. At last, when exist more than one message with state
 * {@link OperationRequest.State#S3} and the same timestamp conflicting, the algorithm has to choose in a deterministic
 * way the messages to be delivered first (e.g. choosing the messages with the smaller m.id).
 * <p>
 * Since it is used the Java {@link java.util.UUID} for the id generation, when the need to choose deterministic between
 * two messages, will be used {@link java.util.UUID#compareTo(UUID)}.
 */
public final class GenericMulticastDeliverSequence<T extends OperationRequest, U extends OperationResponse> implements BiConsumer<OperationResponse, Throwable> {
  private static final int DELIVER_TRY_TIME = 100;
  private static final Predicate<OperationRequest.State> STATE_PREDICATE = state -> OperationRequest.State.S0.equals(state)
      || OperationRequest.State.S1.equals(state)
      || OperationRequest.State.S2.equals(state);
  private final Map<UUID, Holder> requests = Maps.newConcurrentMap();
  private final GenericMulticastConflictHandler conflictHandler;
  private final GenericMulticastServiceContext serviceContext;
  private final ThreadContext threadContext;
  private final Logger log;

  public GenericMulticastDeliverSequence(GenericMulticastConflictHandler conflictHandler, Logger log, GenericMulticastServiceContext context) {
    this.conflictHandler = conflictHandler;
    this.log = log;
    this.threadContext = context.threadContext();
    this.serviceContext = context;
  }

  /**
   * Verify if the future for the given request already exists and save it into memory. If the future already exists
   * always use the same, otherwise create a new one.
   *
   * @param request: request to verify
   */
  void request(T request) {
    requests.compute(request.identifier(), (uuid, holder) -> new Holder(uuid, request));
  }

  /**
   * Save the new received response and try to deliver
   *
   * @param response: request final response
   */
  void response(T request, U response, CompletableFuture<OperationResponse> future) {
    requests.compute(response.identifier(), (uuid, holder) -> new Holder(uuid, request, response, future));
    if (threadContext.isCurrentContext()) {
      deliverResponses();
    } else {
      threadContext.execute(this::deliverResponses);
    }
  }

  /**
   * For all requests on state {@link OperationRequest.State#S3}, try to deliver
   */
  private void deliverResponses() {
    List<Holder> finals = this.get(r -> OperationRequest.State.S3.equals(r.request.state()));
    finals.sort((a, b) -> (int) (a.request.sequence() - b.request.sequence()));

    for (Holder holder : finals) {
      if (!complete(holder)) {
        forceComplete(holder);
      }
    }
  }

  /**
   * When none of the previous tries was successful, this will order the requests with the same timestamp as the
   * given request, sort using the {@link java.util.UUID#compareTo(UUID)} and force deliver the requests
   *
   * @param holder: Holder containing the request metadata
   */
  private void forceComplete(Holder holder) {
    List<Holder> toDeliver = get(r -> OperationRequest.State.S3.equals(r.request.state())
        && holder.request.sequence() == r.request.sequence());
    toDeliver.sort(Comparator.comparing(h -> h.identifier));
    toDeliver.forEach(this::deliver);
  }

  /**
   * Try to complete the request, verifying if conflict exists. If this fails, the requests will be sorted
   * and delivered.
   *
   * @param holder: Holder containing the request metadata
   * @return true if delivered, false otherwise
   */
  private boolean complete(Holder holder) {
    if (holder.future == null || holder.response == null) {
      return false;
    }

    if (firstRequirement(holder.request)) {
      return deliver(holder);
    }

    if (secondRequirement(holder.request)) {
      return deliver(holder);
    }

    return false;
  }

  /**
   * Complete the future with the given response.
   *
   * @param holder: Holder containing the request metadata
   * @return true if completed, false otherwise
   */
  private boolean deliver(Holder holder) {
    try {
      if (holder.future == null || holder.response == null) {
        return false;
      }

      log.trace("GM-cast {}", holder);
      if (holder.response instanceof GatherResponse) {
        serviceContext.getLocalParticipant().commit(holder.request, holder.response, select(holder.response), holder.timestamp, holder.index)
            .whenComplete((res, err) -> {
              if (err == null) {
                holder.future.complete((U) res);
              } else {
                holder.future.completeExceptionally((Throwable) err);
              }
            });
      } else {
        holder.future.complete(holder.response);
      }
      requests.remove(holder.identifier);
      return true;
    } catch (Exception e) {
      log.error("Error delivering [{}]", holder, e);
    }
    return false;
  }

  private OperationResponse.Builder select(U operation) {
    if (operation instanceof GatherResponse) {
      return GatherResponse.builder();
    }

    if (operation instanceof ComputeResponse) {
      return ComputeResponse.builder();
    }

    if (operation instanceof ExecuteResponse) {
      return ExecuteResponse.builder();
    }

    if (operation instanceof CloseResponse) {
      return CloseResponse.builder();
    }
    log.info("Failed!");
    throw new GenericMulticastException.CommandFailure("Unknown response type");
  }

  /**
   * Verifying the first arguments if the request can be delivered.
   * <p>
   * Firstly, for each message {@link OperationRequest} m that do not conflict with any other message with states
   * {@link OperationRequest.State#S0}, {@link OperationRequest.State#S1} and {@link OperationRequest.State#S2},
   * m can be GM-Delivered.
   *
   * @param request: request to be verified
   * @return true if can be delivered, false otherwise
   */
  private boolean firstRequirement(OperationRequest request) {
    List<Holder> holders = get(r -> STATE_PREDICATE.test(r.request.state()) && !r.identifier.equals(request.identifier()));
    return !conflictHandler.conflict(request, holders.stream().map(h -> h.request).collect(Collectors.toList()));
  }

  /**
   * Verifying the second argument if the request can be delivered.
   * <p>
   * Secondly, the algorithm search for the message m with the smaller timestamp {@link OperationRequest#sequence()}
   * between all other messages, if m is on state {@link OperationRequest.State#S3} and do not conflict with any other
   * message on the same timestamp, m can be delivered.
   *
   * @param request: request to be verified
   * @return true if can be delivered, false otherwise
   */
  private boolean secondRequirement(OperationRequest request) {
    List<Holder> holders = get(r -> OperationRequest.State.S3.equals(r.request.state())
        && !r.identifier.equals(request.identifier())
        && r.request.sequence() == request.sequence());
    return !conflictHandler.conflict(request, holders.stream().map(h -> h.request).collect(Collectors.toList()));
  }

  /**
   * Apply a filter into the requests
   *
   * @param filter: filter to be applied into requests
   * @return filtered requests
   */
  private List<Holder> get(Predicate<Holder> filter) {
    return ImmutableList.copyOf(requests.values()).parallelStream()
        .filter(Objects::nonNull)
        .filter(filter)
        .collect(Collectors.toList());
  }

  @Override
  public void accept(OperationResponse response, Throwable throwable) {
    log.trace("Removing [{}]", response);
    requests.remove(response.identifier());
  }

  private final class Holder {
    private final UUID identifier;
    private final long timestamp;
    private final long index;
    private T request;
    private U response;
    private CompletableFuture<OperationResponse> future;

    private Holder(UUID identifier, T request) {
      this.identifier = identifier;
      this.request = request;
      this.timestamp = System.currentTimeMillis();
      this.index = serviceContext.nextIndex();
    }

    private Holder(UUID identifier, T request, U response, CompletableFuture<OperationResponse> future) {
      this(identifier, request);
      this.response = response;
      this.future = future;
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("identifier", identifier)
          .add("request", request)
          .add("response", response)
          .add("future", future != null)
          .toString();
    }
  }
}

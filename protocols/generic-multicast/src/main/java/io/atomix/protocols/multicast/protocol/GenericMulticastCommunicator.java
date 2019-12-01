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
package io.atomix.protocols.multicast.protocol;

import com.google.common.base.Throwables;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessagingException;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.protocol.message.request.GenericMulticastRequest;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.protocols.multicast.protocol.message.response.GenericMulticastResponse;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class GenericMulticastCommunicator implements GenericMulticastClientProtocol {
  private final GenericMulticastMessageContext context;
  private final Serializer serializer;
  private final ClusterCommunicationService clusterCommunicator;

  public GenericMulticastCommunicator(
      String prefix,
      Serializer serializer,
      ClusterCommunicationService clusterCommunicator) {
    this.context = new GenericMulticastMessageContext(prefix);
    this.serializer = serializer;
    this.clusterCommunicator = clusterCommunicator;
  }

  private <T extends GenericMulticastRequest, U extends GenericMulticastResponse> CompletableFuture<U> sendAndReceive(
      String subject,
      T request,
      MemberId memberId) {
    CompletableFuture<U> future = new CompletableFuture<>();
    clusterCommunicator.<T, U>send(subject, request, serializer::encode, serializer::decode, memberId)
        .whenComplete((res, err) -> {
          if (err == null) {
            future.complete(res);
          } else {
            Throwable cause = Throwables.getRootCause(err);
            if (cause instanceof MessagingException.NoRemoteHandler) {
              future.completeExceptionally(new PrimitiveException.Unavailable());
            } else {
              future.completeExceptionally(err);
            }
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(MemberId memberId, ExecuteRequest request) {
    return sendAndReceive(context.execute, request, memberId);
  }

  @Override
  public CompletableFuture<ComputeResponse> compute(MemberId memberId, ComputeRequest request) {
    return sendAndReceive(context.compute, request, memberId);
  }

  @Override
  public CompletableFuture<GatherResponse> gather(MemberId memberId, GatherRequest request) {
    return sendAndReceive(context.gather, request, memberId);
  }

  @Override
  public CompletableFuture<CloseResponse> close(MemberId memberId, CloseRequest request) {
    return sendAndReceive(context.close, request, memberId);
  }

  @Override
  public void register(SessionId sessionId, Consumer<PrimitiveEvent> listener, Executor executor) {
    clusterCommunicator.subscribe(context.eventSubject(sessionId.id()), serializer::decode, listener, executor);
  }

  @Override
  public void unregister(SessionId sessionId) {
    clusterCommunicator.unsubscribe(context.eventSubject(sessionId.id()));
  }
}

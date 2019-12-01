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

import com.google.common.collect.Sets;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Generic multicast server protocol using {@link ClusterCommunicationService}
 */
public class GenericMulticastServerCommunicator implements GenericMulticastServerProtocol {
  private final GenericMulticastMessageContext context;
  private final Serializer serializer;
  private final ClusterCommunicationService clusterCommunicator;

  public GenericMulticastServerCommunicator(
      String prefix,
      Serializer serializer,
      ClusterCommunicationService communicator) {
    this.context = new GenericMulticastMessageContext(prefix);
    this.serializer = checkNotNull(serializer, "Serializer cannot be null!");
    this.clusterCommunicator = checkNotNull(communicator, "Cluster communicator cannot be null!");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(String subject, T req, MemberId memberId) {
    return clusterCommunicator.send(subject, req, serializer::encode, serializer::decode, MemberId.from(memberId.id()));
  }

  private <T> void reliableMulticast(String subject, T req, Collection<MemberId> memberIds) {
    clusterCommunicator.multicast(subject, req, serializer::encode, Sets.newHashSet(memberIds), true);
  }

  @Override
  public void registerCloseHandler(Function<CloseRequest, CompletableFuture<CloseResponse>> handler) {
    clusterCommunicator.subscribe(context.close, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterCloseHandler() {
    clusterCommunicator.unsubscribe(context.close);
  }

  @Override
  public void registerExecuteHandler(Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> handler) {
    clusterCommunicator.subscribe(context.execute, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterExecuteHandler() {
    clusterCommunicator.unsubscribe(context.execute);
  }

  @Override
  public void registerComputeHandler(Function<ComputeRequest, CompletableFuture<ComputeResponse>> handler) {
    clusterCommunicator.subscribe(context.compute, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterComputeHandler() {
    clusterCommunicator.unsubscribe(context.compute);
  }

  @Override
  public void registerGatherHandler(Function<GatherRequest, CompletableFuture<GatherResponse>> handler) {
    clusterCommunicator.subscribe(context.gather, serializer::decode, handler, serializer::encode);
  }

  @Override
  public void unregisterGatherHandler() {
    clusterCommunicator.unsubscribe(context.gather);
  }

  @Override
  public void event(MemberId memberId, SessionId sessionId, PrimitiveEvent event) {
    clusterCommunicator.unicast(context.eventSubject(sessionId.id()), event, serializer::encode, memberId);
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request, MemberId memberId) {
    return sendAndReceive(context.execute, request, memberId);
  }

  @Override
  public void compute(ComputeRequest request, Collection<MemberId> memberIds) {
    reliableMulticast(context.compute, request, memberIds);
  }

  @Override
  public CompletableFuture<GatherResponse> gather(GatherRequest request, MemberId memberId) {
    return sendAndReceive(context.gather, request, memberId);
  }

  @Override
  public CompletableFuture<CloseResponse> close(CloseRequest request, MemberId memberId) {
    return sendAndReceive(context.close, request, memberId);
  }
}

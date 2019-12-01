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

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
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
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class GenericMulticastClientProtocolTest extends GenericMulticastProtocolTest implements GenericMulticastClientProtocol {
  private final Map<SessionId, Consumer<PrimitiveEvent>> listeners = Maps.newConcurrentMap();

  protected GenericMulticastClientProtocolTest(MemberId memberId, Map<MemberId, GenericMulticastServerProtocolTest> servers, Map<MemberId, GenericMulticastClientProtocolTest> clients) {
    super(servers, clients);
    clients.put(memberId, this);
  }

  private CompletableFuture<GenericMulticastServerProtocolTest> getServer(MemberId memberId) {
    GenericMulticastServerProtocolTest server = server(memberId);

    if (server == null) {
      return Futures.exceptionalFuture(new ConnectException("Server not found"));
    }

    return Futures.completedFuture(server);
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(MemberId memberId, ExecuteRequest request) {
    return getServer(memberId).thenCompose(server -> server.execute(request));
  }

  @Override
  public CompletableFuture<ComputeResponse> compute(MemberId memberId, ComputeRequest request) {
    return getServer(memberId).thenCompose(s -> s.compute(request));
  }

  @Override
  public CompletableFuture<GatherResponse> gather(MemberId memberId, GatherRequest request) {
    return getServer(memberId).thenCompose(s -> s.gather(request));
  }

  @Override
  public CompletableFuture<CloseResponse> close(MemberId memberId, CloseRequest request) {
    return getServer(memberId).thenCompose(s -> s.close(request, memberId));
  }

  @Override
  public void register(SessionId sessionId, Consumer<PrimitiveEvent> listener, Executor executor) {
    listeners.put(sessionId, event -> executor.execute(() -> listener.accept(event)));
  }

  @Override
  public void unregister(SessionId sessionId) {
    listeners.remove(sessionId);
  }

  void event(SessionId sessionId, PrimitiveEvent event) {
    Consumer<PrimitiveEvent> listener = listeners.get(sessionId);
    if (listener != null) {
      listener.accept(event);
    }
  }
}

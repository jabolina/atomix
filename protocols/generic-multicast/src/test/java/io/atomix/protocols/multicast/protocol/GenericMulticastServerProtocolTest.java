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

import io.atomix.cluster.MemberId;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.protocol.message.request.RestoreRequest;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.protocols.multicast.protocol.message.response.RestoreResponse;
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class GenericMulticastServerProtocolTest extends GenericMulticastProtocolTest implements GenericMulticastServerProtocol {
  private Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> executeHandler;
  private Function<ComputeRequest, CompletableFuture<ComputeResponse>> computeHandler;
  private Function<GatherRequest, CompletableFuture<GatherResponse>> gatherHandler;
  private Function<CloseRequest, CompletableFuture<CloseResponse>> closeHandler;
  private Function<RestoreRequest, CompletableFuture<RestoreResponse>> restoreHandler;

  protected GenericMulticastServerProtocolTest(MemberId memberId, Map<MemberId, GenericMulticastServerProtocolTest> servers, Map<MemberId, GenericMulticastClientProtocolTest> clients) {
    super(servers, clients);
    servers.put(memberId, this);
  }

  private CompletableFuture<GenericMulticastServerProtocolTest> getServer(MemberId memberId) {
    GenericMulticastServerProtocolTest server = server(memberId);

    if (server == null) {
      return Futures.exceptionalFuture(new ConnectException());
    }

    return Futures.completedFuture(server);
  }

  private CompletableFuture<GenericMulticastClientProtocolTest> getClient(MemberId memberId) {
    GenericMulticastClientProtocolTest client = client(memberId);

    if (client == null) {
      return Futures.exceptionalFuture(new ConnectException("Client not found!"));
    }

    return Futures.completedFuture(client);
  }

  @Override
  public void registerCloseHandler(Function<CloseRequest, CompletableFuture<CloseResponse>> handler) {
    this.closeHandler = handler;
  }

  @Override
  public void unregisterCloseHandler() {
    this.closeHandler = null;
  }

  @Override
  public void registerExecuteHandler(Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> handler) {
    this.executeHandler = handler;
  }

  @Override
  public void unregisterExecuteHandler() {
    this.executeHandler = null;
  }

  @Override
  public void registerComputeHandler(Function<ComputeRequest, CompletableFuture<ComputeResponse>> handler) {
    this.computeHandler = handler;
  }

  @Override
  public void unregisterComputeHandler() {
    this.computeHandler = null;
  }

  @Override
  public void registerGatherHandler(Function<GatherRequest, CompletableFuture<GatherResponse>> handler) {
    this.gatherHandler = handler;
  }

  @Override
  public void unregisterGatherHandler() {
    this.gatherHandler = null;
  }

  @Override
  public void registerRestoreHandler(Function<RestoreRequest, CompletableFuture<RestoreResponse>> handler) {
    this.restoreHandler = handler;
  }

  @Override
  public void unregisterRestoreHandler() {
    this.restoreHandler = null;
  }

  @Override
  public void event(MemberId memberId, SessionId sessionId, PrimitiveEvent event) {
    getClient(memberId).thenAccept(client -> client.event(sessionId, event));
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request, MemberId memberId) {
    return getServer(memberId).thenCompose(server -> server.execute(request));
  }

  @Override
  public void compute(ComputeRequest request, Collection<MemberId> memberIds) {
    for (MemberId memberId: memberIds) {
      getServer(memberId).whenComplete((server, err) -> server.compute(request));
    }
  }

  @Override
  public CompletableFuture<GatherResponse> gather(GatherRequest request, MemberId memberId) {
    return getServer(memberId).thenCompose(server -> server.gather(request));
  }

  @Override
  public CompletableFuture<CloseResponse> close(CloseRequest request, MemberId memberId) {
    if (closeHandler == null) {
      return Futures.exceptionalFuture(new ConnectException("Close handler not exists!"));
    }

    return closeHandler.apply(request);
  }

  @Override
  public CompletableFuture<RestoreResponse> restore(RestoreRequest request, MemberId memberId) {
    if (restoreHandler == null) {
      return Futures.exceptionalFuture(new ConnectException("Restore handler not exists!"));
    }
    return restoreHandler.apply(request);
  }

  CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    if (executeHandler == null) {
      return Futures.exceptionalFuture(new ConnectException("Execute handler not exists!"));
    }

    return executeHandler.apply(request);
  }

  CompletableFuture<ComputeResponse> compute(ComputeRequest request) {
    if (computeHandler == null) {
      return Futures.exceptionalFuture(new ConnectException("Compute handler not exists!"));
    }

    return computeHandler.apply(request);
  }

  CompletableFuture<GatherResponse> gather(GatherRequest request) {
    if (gatherHandler == null) {
      return Futures.exceptionalFuture(new ConnectException("Gather handler not exists!"));
    }

    return gatherHandler.apply(request);
  }
}

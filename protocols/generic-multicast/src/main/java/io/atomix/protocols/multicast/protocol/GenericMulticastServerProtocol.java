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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Generic multicast protocol implementation for the server application
 */
public interface GenericMulticastServerProtocol {

  /**
   * Register a handler to act as a callback for the close request.
   *
   * @param handler: close callback
   */
  void registerCloseHandler(Function<CloseRequest, CompletableFuture<CloseResponse>> handler);

  /**
   * Unregister the close request callback.
   */
  void unregisterCloseHandler();

  /**
   * Register an execute request callback
   * @param handler: callback
   */
  void registerExecuteHandler(Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> handler);

  /**
   * Unregister the execute callback
   */
  void unregisterExecuteHandler();

  /**
   * Register an compute request callback
   *
   * @param handler: callback
   */
  void registerComputeHandler(Function<ComputeRequest, CompletableFuture<ComputeResponse>> handler);

  /**
   * Unregister compute callback
   */
  void unregisterComputeHandler();

  /**
   * Register an gather request callback
   *
   * @param handler: callback
   */
  void registerGatherHandler(Function<GatherRequest, CompletableFuture<GatherResponse>> handler);

  /**
   * Unregister gather callback
   */
  void unregisterGatherHandler();

  /**
   * Register an restore request callback
   *
   * @param handler: callback
   */
  void registerRestoreHandler(Function<RestoreRequest, CompletableFuture<RestoreResponse>> handler);

  /**
   * Unregister restore callback
   */
  void unregisterRestoreHandler();

  /**
   * Sends a primitive event to a generic multicast node
   *
   * @param memberId who will publish the event
   * @param sessionId the session which will publish the event
   * @param event the event to be published
   */
  void event(MemberId memberId, SessionId sessionId, PrimitiveEvent event);

  /**
   * Handler for execute request
   *
   * @param request: execute request
   * @param memberId: who sent the message
   * @return future with execute response
   */
  CompletableFuture<ExecuteResponse> execute(ExecuteRequest request, MemberId memberId);

  /**
   * Handler for compute request
   *
   * @param request: compute request
   * @param memberIds: who sent the message
   */
  void compute(ComputeRequest request, Collection<MemberId> memberIds);

  /**
   * Handler for gather request
   *
   * @param request: gather request
   * @param memberId: who sent the message
   * @return future with gather request response
   */
  CompletableFuture<GatherResponse> gather(GatherRequest request, MemberId memberId);

  /**
   * Handler for close request
   *
   * @param request: close request
   * @param memberId: who sent the message
   * @return future with close request response
   */
  CompletableFuture<CloseResponse> close(CloseRequest request, MemberId memberId);

  /**
   * Handler for restore request
   *
   * @param request: restore request
   * @param memberId: who sent the message
   * @return future with restore request response
   */
  CompletableFuture<RestoreResponse> restore(RestoreRequest request, MemberId memberId);
}

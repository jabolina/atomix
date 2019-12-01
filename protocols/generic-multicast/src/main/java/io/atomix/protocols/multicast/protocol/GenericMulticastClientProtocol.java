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
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Executes the generic multicast protocol. Being able to communicate to the processes inside the session.
 */
public interface GenericMulticastClientProtocol {

  /**
   * Sends a request
   *
   * @param memberId: the node to send the request
   * @param request: request to be sent
   * @return a future to be complete with the response
   */
  CompletableFuture<ExecuteResponse> execute(MemberId memberId, ExecuteRequest request);

  /**
   * Sends a compute request
   *
   * @param memberId: the node to send the request
   * @param request: request to be sent
   * @return future to be completed with the response
   */
  CompletableFuture<ComputeResponse> compute(MemberId memberId, ComputeRequest request);

  /**
   * Sends the gather request
   *
   * @param memberId: the node to send the request
   * @param request: request to be sent
   * @return a future to be completed with the response
   */
  CompletableFuture<GatherResponse> gather(MemberId memberId, GatherRequest request);

  /**
   * Sends a close request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CloseResponse> close(MemberId memberId, CloseRequest request);

  /**
   * Register a listener into session
   *
   * @param sessionId: the session to listen
   * @param listener: the listener to register
   * @param executor: executor to execute the callbacks
   */
  void register(SessionId sessionId, Consumer<PrimitiveEvent> listener, Executor executor);

  /**
   * Unregister listeners from session
   *
   * @param sessionId: session to unregister listeners
   */
  void unregister(SessionId sessionId);
}

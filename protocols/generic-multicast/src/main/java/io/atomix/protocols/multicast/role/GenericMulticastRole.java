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

import io.atomix.protocols.multicast.impl.GenericMulticastSession;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;
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
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;
import io.atomix.utils.Managed;

import java.util.concurrent.CompletableFuture;

/**
 * Each generic multicast instance will be node. Each node available must
 * implement some methods.
 * <p>
 * This is the implementation of the Generic Multicast 1 by Douglas Antunes.
 * The details of the protocol can be found in master thesis with name
 * `Fault-Tolerant Generic Multicast Algorithms for Wide Area Networks`
 */
public interface GenericMulticastRole extends Managed<GenericMulticastRole> {

  /**
   * Handles a request command.
   * <p>
   * A message {@link ExecuteRequest} m, set the state of m to {@link OperationRequest.State#S0} indicating that
   * m don’t have a timestamp associated yet and add m to the queue of messages.
   *
   * @param request: request to be executed
   * @return A completable future with the request response
   */
  CompletableFuture<ExecuteResponse> onExecute(ExecuteRequest request);

  /**
   * Handles a compute request
   * <p>
   * After the process GB-Deliver {@link OperationRequest} m, if {@link OperationRequest.State} is equals to
   * {@link OperationRequest.State#S0}, firstly the algorithm check if m conflict with any other message on
   * {@link TrulyGenericMulticastRole#previousSet}, if so, the process p increment its local clock and empty the
   * previousSet.
   * At last, the message m receives its group timestamp and is added to previousSet maintaining
   * the information about conflict relations to future messages. On the second part of this procedure,
   * the process p checks if m.dest ({@link GenericMulticastServiceContext#participants()}) has only one destination,
   * if so, message m can jump to state {@link OperationRequest.State#S3}, since its not necessary to exchange
   * timestamp information between others destination groups and a second consensus can be avoided due to
   * group timestamp is now a final timestamp. Otherwise, for messages on state {@link OperationRequest.State#S0},
   * we set the group timestamp (m.ts) to {@link GenericMulticastServiceContext#timestampProvider()},
   * update m.state to {@link OperationRequest.State#S1} and send m to all others groups in m.dest.
   *
   * On the other hand, to messages on state {@link OperationRequest.State#S2}, the message decided has the
   * final timestamp of m, thus m.state can be updated to the final state {@link OperationRequest.State#S3} and
   * if {@link OperationRequest#sequence} is greater than {@link GenericMulticastServiceContext#timestampProvider()} K,
   * the K is updated to m.ts and the {@link TrulyGenericMulticastRole#previousSet} can be cleaned.
   *
   * @param request: request to be computed
   * @return A completable future with the compute response
   */
  CompletableFuture<ComputeResponse> onCompute(ComputeRequest request);

  /**
   * Handles a gather request.
   * <p>
   * When a message {@link GatherRequest} m has more than one destination group, the destination groups have to
   * exchange its timestamps to decide the final timestamp to m. Thus, after receiving all other timestamp values,
   * a temporary variable ts is agree upon the maximum timestamp value received.
   * Once the algorithm have select the ts value, the process checks if {@link OperationRequest#sequence} is greater or
   * equal to ts, in positive case, a second consensus instance can be avoided and the state of m can jump directly to
   * state {@link OperationRequest.State#S3} since the group local clock {@link GenericMulticastServiceContext#timestampProvider}
   * is already bigger then ts.
   *
   * @param request: a request to be gathered
   * @return A completable future with the gather response
   */
  CompletableFuture<GatherResponse> onGather(GatherRequest request);

  /**
   * Handles a close request.
   *
   * @param request the close request
   * @return future to be completed with a close response
   */
  CompletableFuture<CloseResponse> onClose(CloseRequest request);

  /**
   * Used when a member of the protocol needs to restore the state machine.
   *
   * @param request: request sent
   * @return a future with the request response
   */
  CompletableFuture<RestoreResponse> onRestore(RestoreRequest request);

  <T extends OperationResponse> CompletableFuture<T> commit(OperationRequest req, T res, OperationResponse.Builder<?, T> builder, long timestamp, long index);

  /**
   * Closes the given session
   *
   * @param session to close
   * @return a future to be completed when the session is closed
   */
  CompletableFuture<Void> close(GenericMulticastSession session);
}

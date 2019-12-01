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

import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Generic multicast base issuer type
 */
public interface Issuer {

  /**
   * Issue the given operation
   *
   * @param operation operation to be issued
   * @param future: future to be completed with the operation response
   * @param <T>: type of operation
   * @return a future to be completed
   */
  <T extends OperationRequest> CompletableFuture<OperationResponse> issue(T operation, CompletableFuture<OperationResponse> future);

  /**
   * Issue the given operation
   *
   * @param operation operation to be issued
   * @param <T>: type of operation
   * @return a future to be completed
   */
  <T extends OperationRequest> CompletableFuture<OperationResponse> issue(T operation);

  /**
   * Close the issuer
   */
  void close();
}

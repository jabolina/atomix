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
package io.atomix.protocols.multicast;

/**
 * Constants for specifying Generic Multicast {@link io.atomix.primitive.operation.OperationType#QUERY} consistency
 * levels.
 * <p>
 * Using this enum is possible to configure the consistency level for {@link io.atomix.primitive.operation.OperationType#QUERY}
 * queries submitted to the cluster. The consistency level will dictate how the queries will be executed through the
 * cluster.
 */
public enum ReadConsistency {
  /**
   * Enforces the read request to be sent through the protocol and be ordered amongst all requests.
   * <p>
   * Using this consistency level will ensure that the read value be applied to all clients after a
   * sequence of commands and that all clients retrieve the most recent value. This will enforce the consistency
   * level for the use case, base the response will be more slow, since the request will be executed as a regular
   * request by the protocol.
   */
  PROTOCOL(true),

  /**
   * Avoid a request submission to the protocol.
   * <p>
   * Using this consistency level, is not guaranteed that the retrieved value is the most up-to-date value, since
   * can exist a more recent value. Using this consistency level will be more fast and will retrieve the most
   * recent commit for all clients, so the {@link io.atomix.primitive.operation.OperationType#QUERY} does not need
   * to be issued to the protocol.
   */
  COMMIT(false);

  private final boolean status;

  ReadConsistency(boolean status) {
    this.status = status;
  }

  /**
   * Get the consistency level status
   *
   * @return true if protocol, false if commit
   */
  public boolean status() {
    return status;
  }
}

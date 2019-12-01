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
package io.atomix.protocols.multicast.protocol.message.response;

import io.atomix.protocols.multicast.exception.GenericMulticastError;
import io.atomix.protocols.multicast.protocol.message.GenericMulticastMessage;

/**
 * Base interface for responses.
 * <p>
 * Each response has a non-null {@link GenericMulticastResponse.Status} of either {@link GenericMulticastResponse.Status#OK} or
 * {@link GenericMulticastResponse.Status#ERROR}. Responses where {@link #status()} is {@link GenericMulticastResponse.Status#ERROR}
 * may provide an optional {@link #error()} code.
 */
public interface GenericMulticastResponse extends GenericMulticastMessage {

  enum Status {
    /**
     * Generic multicast executed successfully
     */
    OK(1),

    /**
     * An error occurred
     */
    ERROR(0);

    /**
     * Returns the status for the given identifier.
     *
     * @param id The status identifier.
     * @return The status for the given identifier.
     * @throws IllegalArgumentException if {@code id} is not 0 or 1
     */
    public static Status forId(int id) {
      switch (id) {
        case 1:
          return OK;
        case 0:
          return ERROR;
        default: break;
      }

      throw new IllegalArgumentException("invalid response status " + id);
    }

    private final byte id;

    Status(int id) {
      this.id = (byte) id;
    }

    /**
     * Returns the status identifier.
     *
     * @return The status identifier.
     */
    public byte id() {
      return id;
    }
  }

  /**
   * Returns the response status.
   *
   * @return The response status.
   */
  Status status();

  /**
   * Returns the response error if the response status is {@code Status.ERROR}
   *
   * @return The response error.
   */
  GenericMulticastError error();

  /**
   * Returns the response result
   * @return The response result
   */
  byte[] result();

  interface Builder<T extends Builder<T, U>, U extends GenericMulticastResponse> extends io.atomix.utils.Builder<U> {
    /**
     * Sets the response status.
     *
     * @param status The response status.
     * @return The response builder.
     * @throws NullPointerException if {@code status} is null
     */
    T withStatus(Status status);

    /**
     * Sets response result
     *
     * @param result: The response result
     * @return The response builder.
     * @throws NullPointerException if {@code status} is null
     */
    T withResult(byte[] result);

    /**
     * Sets the response error.
     *
     * @param error The response error.
     * @return The response builder.
     * @throws NullPointerException if {@code error} is null
     */
    T withError(GenericMulticastError error);

    /**
     * Sets the response error.
     *
     * @param error The response error type.
     * @return The response builder.
     * @throws NullPointerException if {@code type} is null
     */
    default T withError(GenericMulticastError.Error error) {
      return withError(new GenericMulticastError(error, null));
    }

    /**
     * Sets the response error.
     *
     * @param error The response error type.
     * @param message The error message
     * @return The response builder.
     * @throws NullPointerException if {@code type} is null
     */
    default T withError(GenericMulticastError.Error error, String message) {
      return withError(new GenericMulticastError(error, message));
    }
  }
}

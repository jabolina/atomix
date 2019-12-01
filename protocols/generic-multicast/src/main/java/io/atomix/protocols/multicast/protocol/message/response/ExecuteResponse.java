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
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;

import java.util.UUID;

/**
 * Client execute response.
 * <p>
 * Command responses are sent by servers to clients upon the completion of a
 * {@link ExecuteRequest}.
 */
public class ExecuteResponse extends OperationResponse {

  protected ExecuteResponse(UUID uuid, long ts, Status status, byte[] result, GenericMulticastError error) {
    super(uuid, ts, status, result, error);
  }

  /**
   * Returns a new submit response builder.
   *
   * @return A new submit response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Command response builder.
   */
  public static class Builder extends OperationResponse.Builder<Builder, ExecuteResponse> {
    @Override
    public ExecuteResponse build() {
      validate();
      return new ExecuteResponse(identifier, ts, status, result, error);
    }
  }
}

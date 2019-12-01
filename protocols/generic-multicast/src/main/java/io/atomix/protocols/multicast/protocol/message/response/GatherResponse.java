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

import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

public class GatherResponse extends OperationResponse {
  protected GatherResponse(UUID uuid, long ts, Status status, byte[] result, GenericMulticastError error) {
    super(uuid, ts, status, result, error);
  }

  /**
   * Returns a new gather response builder
   * @return gather response builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Gather response builder
   */
  public static class Builder extends OperationResponse.Builder<Builder, GatherResponse> {

    @Override
    protected void validate() {
      super.validate();
      checkArgument(ts >= 0, "Final timestamp not valid");
    }

    @Override
    public GatherResponse build() {
      validate();
      return new GatherResponse(identifier, ts, status, result, error);
    }
  }
}

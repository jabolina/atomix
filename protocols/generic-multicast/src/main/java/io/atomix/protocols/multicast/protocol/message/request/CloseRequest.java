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
package io.atomix.protocols.multicast.protocol.message.request;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;

import java.util.UUID;

public class CloseRequest extends OperationRequest {

  protected CloseRequest(UUID identifier, MemberId memberId, long session, long sequence, PrimitiveOperation operation, PrimitiveDescriptor descriptor) {
    super(identifier, memberId, session, sequence, operation, descriptor, State.S0);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends OperationRequest.Builder<Builder, CloseRequest> {

    @Override
    public CloseRequest build() {
      return new CloseRequest(identifier, memberId, session, sequence, operation, descriptor);
    }
  }
}

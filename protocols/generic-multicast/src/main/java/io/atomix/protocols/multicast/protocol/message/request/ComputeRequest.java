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

import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ComputeRequest extends OperationRequest {
  protected ComputeRequest(UUID uuid, MemberId memberId, long session, long sequence, PrimitiveOperation operation, PrimitiveDescriptor descriptor, State state) {
    super(uuid, memberId, session, sequence, operation, descriptor, state);
  }

  /**
   * Returns compute request builder
   *
   * @return compute request builder
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ComputeRequest) {
      ComputeRequest request = (ComputeRequest) object;
      return request.session == session
          && request.sequence == sequence
          && request.operation.equals(operation);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("identifier", identifier)
        .add("sequence", sequence)
        .add("operation", operation)
        .add("state", state)
        .toString();
  }

  /**
   * Compute request builder
   */
  public static class Builder extends OperationRequest.Builder<Builder, ComputeRequest> {
    @Override
    public ComputeRequest build() {
      validate();
      return new ComputeRequest(identifier, memberId, session, sequence, operation, descriptor, state);
    }
  }
}

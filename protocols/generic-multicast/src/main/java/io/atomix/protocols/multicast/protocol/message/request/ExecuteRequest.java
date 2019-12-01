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

/**
 * Client command request.
 * <p>
 * Command requests are submitted by clients to the Raft cluster to commit commands to
 * the replicated state machine. Each command request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequence()} number within that session. Commands will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a command request, the request should be resent
 * with the same sequence number. Commands are guaranteed to be applied in sequence order, sequence given by the
 * conflict relationship.
 * <p>
 * Command requests should always be submitted to the server to which the client is connected and will
 * be delivered to all members of the cluster using multicast.
 */
public class ExecuteRequest extends OperationRequest {
  public ExecuteRequest(MemberId node, UUID uuid, long session, long sequence, PrimitiveOperation operation, PrimitiveDescriptor descriptor, State state) {
    super(uuid, node, session, sequence, operation, descriptor, state);
  }

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
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
    if (object instanceof ExecuteRequest) {
      ExecuteRequest request = (ExecuteRequest) object;
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
        .add("sequence", sequence)
        .add("operation", operation)
        .add("state", state)
        .toString();
  }

  /**
   * Execute request builder.
   */
  public static class Builder extends OperationRequest.Builder<Builder, ExecuteRequest> {

    @Override
    public ExecuteRequest build() {
      validate();
      return new ExecuteRequest(memberId, identifier, session, sequence, operation, descriptor, state);
    }
  }
}

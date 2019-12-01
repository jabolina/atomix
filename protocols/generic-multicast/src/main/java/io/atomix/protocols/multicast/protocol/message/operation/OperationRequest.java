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
package io.atomix.protocols.multicast.protocol.message.operation;

import com.google.common.base.Objects;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;
import io.atomix.protocols.multicast.protocol.message.request.Request;

import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client operation request.
 * <p>
 * Operation requests are sent by clients to servers to execute operations on the replicated state
 * machine. Each operation request must be sequenced with a {@link #sequence()} number. All operations
 * will be applied to replicated state machines in the sequence in which the conflict relationship says.
 * Sequence numbers must always be sequential, and in the event that an operation request fails, it must
 * be resent by the client.
 */
public abstract class OperationRequest extends Request {
  protected long sequence;
  protected final PrimitiveOperation operation;
  protected final MemberId memberId;
  protected State state;
  protected UUID identifier;

  protected OperationRequest(UUID identifier, MemberId memberId, long session, long sequence, PrimitiveOperation operation, PrimitiveDescriptor descriptor, State state) {
    super(session, descriptor);
    this.memberId = memberId;
    this.sequence = sequence;
    this.operation = operation;
    this.state = state;
    this.identifier = identifier;
  }

  public enum State {
    /**
     * The message does not have a timestamp defined yet
     */
    S0,

    /**
     * The message receives a group timestamp
     */
    S1,

    /**
     * The message receives a final timestamp
     */
    S2,

    /**
     * The message is ready to be delivered
     */
    S3
  }

  /**
   * Returns the request identifier
   *
   * @return the request identifier
   */
  public UUID identifier() {
    return identifier;
  }

  /**
   * Returns the request sequence number.
   *
   * @return The request sequence number.
   */
  public long sequence() {
    return sequence;
  }

  /**
   * Increase the sequence number, this occur in some occasions.
   */
  public void pump() {
    this.sequence++;
  }

  /**
   * Defines a new sequence number
   *
   * @param sequence: the new sequence number
   */
  public void sequence(long sequence) {
    this.sequence = sequence;
  }

  /**
   * Define the new request state
   *
   * @param state: state to be used in request
   */
  public void state(State state) {
    this.state = state;
  }

  /**
   * Returns the request state.
   *
   * @return the request state.
   */
  public State state() {
    return state;
  }

  public MemberId memberId() {
    return memberId;
  }

  /**
   * Returns the operation.
   *
   * @return The operation.
   */
  public PrimitiveOperation operation() {
    return operation;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), identifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof OperationRequest)) {
      return false;
    }

    return ((OperationRequest) obj).identifier == this.identifier;
  }

  @SuppressWarnings("unchecked")
  public abstract static class Builder<T extends Builder<T, U>, U extends OperationRequest> extends Request.Builder<T, U> {
    protected long sequence;
    protected PrimitiveOperation operation;
    protected State state = State.S0;
    protected UUID identifier;
    protected MemberId memberId;

    public T withMemberId(MemberId memberId) {
      this.memberId = memberId;
      return (T) this;
    }

    /**
     * Sets the request sequence number.
     *
     * @param sequence The request sequence number.
     * @return The request builder.
     * @throws IllegalArgumentException If the request sequence number is not positive.
     */
    public T withSequence(long sequence) {
      this.sequence = sequence;
      return (T) this;
    }

    /**
     * Sets the request operation.
     *
     * @param operation The operation.
     * @return The request builder.
     * @throws NullPointerException if the request {@code operation} is {@code null}
     */
    public T withOperation(PrimitiveOperation operation) {
      this.operation = operation;
      return (T) this;
    }

    /**
     * Sets the request state.
     *
     * @param state: the current state of the request
     * @return The request builder
     */
    public T withState(State state) {
      this.state = state;
      return (T) this;
    }

    /**
     * Sets the request identifier
     *
     * @param uuid: uuid generated to identify the request
     * @return The request builder
     */
    public T withIdentifier(UUID uuid) {
      this.identifier = uuid;
      return (T) this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(sequence >= 0, "sequence must be positive");
      checkNotNull(operation, "operation cannot be null");
      checkNotNull(state, "Request state cannot be null");
      checkNotNull(identifier, "Request identifier cannot be null");
      checkNotNull(memberId, "Member id cannot be null!");
    }
  }
}

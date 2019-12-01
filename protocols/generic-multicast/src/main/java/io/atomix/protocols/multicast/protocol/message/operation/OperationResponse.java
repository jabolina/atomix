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

import io.atomix.protocols.multicast.exception.GenericMulticastError;
import io.atomix.protocols.multicast.protocol.message.response.AbstractGenericMulticastResponse;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;

public abstract class OperationResponse extends AbstractGenericMulticastResponse {
  protected final UUID identifier;
  protected final long ts;

  protected OperationResponse(UUID uuid, long ts, Status status, byte[] result, GenericMulticastError error) {
    super(status, result, error);
    this.identifier = uuid;
    this.ts = ts;
  }

  protected OperationResponse(UUID uuid, Status status, byte[] result, GenericMulticastError error) {
    super(status, result, error);
    this.identifier = uuid;
    this.ts = 0L;
  }

  /**
   * Returns the identifier
   *
   * @return response identifier
   */
  public UUID identifier() {
    return identifier;
  }

  /**
   * Returns the protocol final timestamp when request finished
   *
   * @return the protocol final ts
   */
  public long ts() {
    return ts;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || !getClass().isAssignableFrom(object.getClass())) {
      return false;
    }

    OperationResponse response = (OperationResponse) object;
    return response.status == status
        && Objects.equals(response.error, error)
        && Arrays.equals(response.result, result);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("identifier", identifier)
        .add("status", status)
        .add("error", error)
        .add("result", result)
        .toString();
  }

  @SuppressWarnings("unchecked")
  public abstract static class Builder<T extends Builder<T, U>, U extends OperationResponse> extends AbstractGenericMulticastResponse.Builder<T, U> {
    protected byte[] result;
    protected UUID identifier;
    protected long ts;

    @Override
    public T withResult(byte[] result) {
      this.result = result;
      return (T) this;
    }

    /**
     * Sets the response identifier
     *
     * @param identifier: response identifier
     * @return The command response builder
     */
    public T withIdentifier(UUID identifier) {
      this.identifier = Objects.requireNonNull(identifier, "Response identifier cannot be null!");
      return (T) this;
    }

    /**
     * Sets the response final timestamp
     *
     * @param ts: response final timestamp
     * @return The command response builder
     */
    public T withTs(long ts) {
      this.ts = ts;
      return (T) this;
    }
  }
}

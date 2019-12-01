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

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base response for generic multicast algorithm
 */
public abstract class AbstractGenericMulticastResponse implements GenericMulticastResponse {
  protected final Status status;
  protected final byte[] result;
  protected final GenericMulticastError error;

  protected AbstractGenericMulticastResponse(Status status, GenericMulticastError error) {
    this(status, null, error);
  }

  protected AbstractGenericMulticastResponse(Status status, byte[] result) {
    this(status, result, null);
  }

  protected AbstractGenericMulticastResponse(Status status, byte[] result, GenericMulticastError error) {
    this.status = status;
    this.result = result;
    this.error = error;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public byte[] result() {
    return result;
  }

  @Override
  public GenericMulticastError error() {
    return error;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .toString();
    }

    return toStringHelper(this)
        .add("status", status)
        .add("error", "failed")
        .toString();
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  @SuppressWarnings("unchecked")
  protected abstract static class Builder<T extends Builder<T, U>, U extends AbstractGenericMulticastResponse>
      implements GenericMulticastResponse.Builder<T, U> {

    protected Status status;
    protected byte[] result;
    protected GenericMulticastError error;

    @Override
    public T withStatus(Status status) {
      this.status = checkNotNull(status, "status cannot be null");
      return (T) this;
    }

    @Override
    public T withResult(byte[] result) {
      this.result = result;
      return (T) this;
    }

    @Override
    public T withError(GenericMulticastError error) {
      this.error = error;
      return (T) this;
    }

    protected void validate() {
      checkNotNull(status, "status cannot be null");
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("status", status)
          .add("result", new String(result, StandardCharsets.UTF_8))
          .toString();
    }
  }
}

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

import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Request extends AbstractGenericMulticastRequest {
  protected final long session;
  protected final PrimitiveDescriptor descriptor;

  protected Request(long session, PrimitiveDescriptor descriptor) {
    this.session = session;
    this.descriptor = checkNotNull(descriptor, "request primitive descriptor cannot be null");
  }

  /**
   * Returns the session ID
   *
   * @return The session ID
   */
  public long session() {
    return session;
  }

  public PrimitiveDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || !getClass().isAssignableFrom(obj.getClass())) {
      return false;
    }

    Request request = (Request) obj;
    return request.session == session
        && request.descriptor.name().equals(descriptor.name());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .toString();
  }

  @SuppressWarnings("unchecked")
  public abstract static class Builder<T extends Builder<T, U>, U extends Request> extends AbstractGenericMulticastRequest.Builder<T, U> {
    protected long session;
    protected PrimitiveDescriptor descriptor;

    /**
     * Sets the session ID.
     *
     * @param session The session ID.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code session} is less than 0
     */
    public T withSession(long session) {
      this.session = session;
      return (T) this;
    }

    public T withDescriptor(PrimitiveDescriptor descriptor) {
      this.descriptor = descriptor;
      return (T) this;
    }

    @Override
    protected void validate() {
      checkArgument(session > 0, "session must be positive");
      checkNotNull(descriptor, "primitive descriptor cannot be null");
    }
  }
}

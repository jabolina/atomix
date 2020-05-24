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

import static com.google.common.base.MoreObjects.toStringHelper;

public class RestoreRequest extends Request {
  private final long term;

  private RestoreRequest(long session, PrimitiveDescriptor descriptor, long term) {
    super(session, descriptor);
    this.term = term;
  }

  public static RestoreRequest request(PrimitiveDescriptor descriptor, long term, long session) {
    return new RestoreRequest(session, descriptor, term);
  }

  public long term() {
    return this.term;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitive", descriptor)
        .add("term", term)
        .toString();
  }
}

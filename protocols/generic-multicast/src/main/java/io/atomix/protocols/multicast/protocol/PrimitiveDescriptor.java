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
package io.atomix.protocols.multicast.protocol;

import io.atomix.protocols.multicast.ReadConsistency;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primitive descriptor.
 */
public class PrimitiveDescriptor {
  private final String name;
  private final String type;
  private final byte[] config;
  private final ReadConsistency readConsistency;

  public PrimitiveDescriptor(String name, String type, byte[] config, ReadConsistency readConsistency) {
    this.name = name;
    this.type = type;
    this.config = config;
    this.readConsistency = readConsistency;
  }

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the primitive type name.
   *
   * @return the primitive type name
   */
  public String type() {
    return type;
  }

  /**
   * Returns the primitive service configuration.
   *
   * @return the primitive service configuration
   */
  public byte[] config() {
    return config;
  }

  /**
   * Returns the configured read consistency
   *
   * @return the read consistency level
   */
  public ReadConsistency readConsistency() {
    return readConsistency;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .toString();
  }
}

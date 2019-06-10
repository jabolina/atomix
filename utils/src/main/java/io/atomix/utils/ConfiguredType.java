/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

/**
 * Configured type.
 */
public interface ConfiguredType<C extends Message> extends NamedType {

  /**
   * Returns the configuration class.
   *
   * @return the configuration class
   */
  Class<C> getConfigClass();

  /**
   * Returns the descriptor for the type's configuration.
   *
   * @return the descriptor for the type's configuration
   */
  Descriptors.Descriptor getConfigDescriptor();

}

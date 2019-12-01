/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.multicast;

import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

import java.time.Duration;

/**
 * Generic multicast protocol builder
 */
public class GenericMulticastProtocolBuilder extends PrimitiveProtocolBuilder<GenericMulticastProtocolBuilder,
    GenericMulticastProtocolConfig,
    GenericMulticastProtocol> {

  protected GenericMulticastProtocolBuilder(GenericMulticastProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol builder
   */
  public GenericMulticastProtocolBuilder withPartitioner(Partitioner<String> partitioner) {
    config.setPartitioner(partitioner);
    return this;
  }

  /**
   * Sets the protocol recovery strategy.
   *
   * @param recovery the protocol recovery strategy
   * @return the protocol builder
   */
  public GenericMulticastProtocolBuilder withRecovery(Recovery recovery) {
    config.setRecovery(recovery);
    return this;
  }

  /**
   * Sets the number of clients.
   *
   * @param numClients the number of clients
   * @return the protocol builder
   */
  public GenericMulticastProtocolBuilder withClients(int numClients) {
    config.setNumClients(numClients);
    return this;
  }

  /**
   * Sets the maximum number of retries before an operation can be failed.
   *
   * @param maxRetries the maximum number of retries before an operation can be failed
   * @return the proxy builder
   */
  public GenericMulticastProtocolBuilder withMaxRetries(int maxRetries) {
    config.setMaxRetries(maxRetries);
    return this;
  }

  /**
   * Sets the operation retry delay.
   *
   * @param retryDelay the delay between operation retries
   * @return the proxy builder
   * @throws NullPointerException if the delay is null
   */
  public GenericMulticastProtocolBuilder withRetryDelay(Duration retryDelay) {
    config.setRetryDelay(retryDelay);
    return this;
  }

  @Override
  public GenericMulticastProtocol build() {
    return new GenericMulticastProtocol(config);
  }
}

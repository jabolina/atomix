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
package io.atomix.protocols.multicast;

import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;

import java.time.Duration;

/**
 * Generic multicast protocol configuration
 */
public class GenericMulticastProtocolConfig extends PrimitiveProtocolConfig<GenericMulticastProtocolConfig> {
  private String group;
  private Partitioner<String> partitioner = Partitioner.MURMUR3;
  private Recovery recovery = Recovery.RECOVER;
  private int maxRetries = 3;
  private int numClients = 5;
  private Duration minTimeout = Duration.ofMillis(250);
  private Duration maxTimeout = Duration.ofSeconds(5);
  private Duration retryDelay = Duration.ofMillis(100);
  private AtomicCounter timestamp = null;

  @Override
  public PrimitiveProtocol.Type getType() {
    return GenericMulticastProtocol.TYPE;
  }

  /**
   * Returns the partition group
   *
   * @return the partition group
   */
  public String getGroup() {
    return group;
  }

  /**
   * Sets the partition group
   *
   * @param group the partition group
   * @return the protocol configuration
   */
  public GenericMulticastProtocolConfig setGroup(String group) {
    this.group = group;
    return this;
  }

  /**
   * Returns the protocol partitioner.
   *
   * @return the protocol partitioner
   */
  public Partitioner<String> getPartitioner() {
    return partitioner;
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol configuration
   */
  public GenericMulticastProtocolConfig setPartitioner(Partitioner<String> partitioner) {
    this.partitioner = partitioner;
    return this;
  }

  /**
   * Returns the recovery strategy.
   *
   * @return the recovery strategy
   */
  public Recovery getRecovery() {
    return recovery;
  }

  /**
   * Sets the recovery strategy.
   *
   * @param recovery the recovery strategy
   * @return the protocol configuration
   */
  public GenericMulticastProtocolConfig setRecovery(Recovery recovery) {
    this.recovery = recovery;
    return this;
  }

  public AtomicCounter getTimestamp() {
    return timestamp;
  }

  public GenericMulticastProtocolConfig setTimestamp(AtomicCounter timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public GenericMulticastProtocolConfig setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public int getNumClients() {
    return numClients;
  }

  public GenericMulticastProtocolConfig setNumClients(int numClients) {
    this.numClients = numClients;
    return this;
  }

  public Duration getMinTimeout() {
    return minTimeout;
  }

  public GenericMulticastProtocolConfig setMinTimeout(Duration minTimeout) {
    this.minTimeout = minTimeout;
    return this;
  }

  public Duration getMaxTimeout() {
    return maxTimeout;
  }

  public GenericMulticastProtocolConfig setMaxTimeout(Duration maxTimeout) {
    this.maxTimeout = maxTimeout;
    return this;
  }

  public Duration getRetryDelay() {
    return retryDelay;
  }

  public GenericMulticastProtocolConfig setRetryDelay(Duration retryDelay) {
    this.retryDelay = retryDelay;
    return this;
  }
}

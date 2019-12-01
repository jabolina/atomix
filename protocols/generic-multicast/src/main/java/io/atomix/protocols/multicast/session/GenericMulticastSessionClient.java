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
package io.atomix.protocols.multicast.session;

import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.session.SessionClient;
import io.atomix.protocols.multicast.ReadConsistency;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public interface GenericMulticastSessionClient extends SessionClient {

  abstract class Builder extends SessionClient.Builder {
    protected Duration minTimeout = Duration.ofMillis(250);
    protected Duration maxTimeout = Duration.ofSeconds(5);
    protected Recovery recovery = Recovery.RECOVER;
    protected int maxRetries = 0;
    protected Duration retryDelay = Duration.ofMillis(100);
    protected ReadConsistency readConsistency = ReadConsistency.PROTOCOL;
    protected AtomicCounter timestampProvider;

    public Builder withMinTimeout(Duration minTimeout) {
      this.minTimeout = checkNotNull(minTimeout, "Minimum timeout cannot be null");
      return this;
    }

    /**
     * Sets the maximum session timeout.
     *
     * @param maxTimeout the maximum session timeout
     * @return the Raft protocol builder
     */
    public Builder withMaxTimeout(Duration maxTimeout) {
      this.maxTimeout = checkNotNull(maxTimeout, "Maximum timeout cannot be null");
      return this;
    }

    /**
     * Sets the recovery strategy.
     *
     * @param recoveryStrategy the recovery strategy
     * @return the Raft protocol builder
     */
    public Builder withRecoveryStrategy(Recovery recoveryStrategy) {
      this.recovery = checkNotNull(recoveryStrategy, "recoveryStrategy cannot be null");
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      checkArgument(maxRetries >= 0, "maxRetries must be positive");
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @return the proxy builder
     * @throws NullPointerException if the delay is null
     */
    public Builder withRetryDelay(Duration retryDelay) {
      this.retryDelay = checkNotNull(retryDelay, "retryDelay cannot be null");
      return this;
    }

    /**
     * Sets the read consistency for the protocol
     *
     * @param readConsistency: the read consistency level
     * @return the proxy builder
     * @throws NullPointerException if the read consistency level is null
     */
    public Builder withReadConsistency(ReadConsistency readConsistency) {
      this.readConsistency = checkNotNull(readConsistency, "Read consistency cannot be null");
      return this;
    }
  }
}

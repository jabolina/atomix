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
package io.atomix.core.barrier;

import io.atomix.primitive.SyncPrimitive;

import java.time.Duration;

/**
 * Distributed cyclic barrier.
 */
public interface DistributedCyclicBarrier extends SyncPrimitive {

  /**
   * Waits until all parties have invoked await on this barrier.
   *
   * @return the arrival index of the current thread, where index {@link #getParties()} - 1 indicates the first to
   * arrive and zero indicates the last to arrive
   */
  int await();

  /**
   * Waits until all parties have invoked await on this barrier.
   *
   * @param timeout the time to wait for the barrier
   * @return the arrival index of the current thread, where index {@link #getParties()} - 1 indicates the first to
   * arrive and zero indicates the last to arrive
   */
  int await(Duration timeout);

  /**
   * Returns the number of parties currently waiting at the barrier.
   *
   * @return the number of parties currently waiting at the barrier
   */
  int getNumberWaiting();

  /**
   * Returns the number of parties required to trip this barrier.
   *
   * @return the number of parties required to trip this barrier
   */
  int getParties();

  /**
   * Returns whether this barrier is in a broken state.
   *
   * @return whether this barrier is in a broken state
   */
  boolean isBroken();

  /**
   * Resets the barrier to its initial state.
   */
  void reset();

  @Override
  AsyncDistributedCyclicBarrier async();
}

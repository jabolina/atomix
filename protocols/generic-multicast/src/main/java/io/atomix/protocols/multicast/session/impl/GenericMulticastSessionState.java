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
package io.atomix.protocols.multicast.session.impl;

import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Generic multicast session state
 */
public class GenericMulticastSessionState {
  private final String clientId;
  private final SessionId sessionId;
  private final long timeout;
  private final PartitionId partitionId;
  private final PrimitiveType primitiveType;
  private final PrimitiveDescriptor descriptor;
  private final Set<Consumer<PrimitiveState>> stateListeners = new CopyOnWriteArraySet<>();
  private volatile PrimitiveState state = PrimitiveState.CONNECTED;
  private volatile Long suspendedTime;
  private volatile long commandRequest;

  public GenericMulticastSessionState(
      String clientId,
      SessionId sessionId,
      long timeout,
      PartitionId partitionId,
      PrimitiveType primitiveType,
      PrimitiveDescriptor descriptor) {
    this.clientId = clientId;
    this.sessionId = sessionId;
    this.timeout = timeout;
    this.partitionId = partitionId;
    this.primitiveType = primitiveType;
    this.descriptor = descriptor;
  }

  /**
   * Returns the client identifier.
   *
   * @return The client identifier.
   */
  public String clientId() {
    return clientId;
  }

  /**
   * Returns the session ID
   *
   * @return The session ID
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * Returns the client session ID.
   *
   * @return The client session ID.
   */
  public PartitionId partitionId() {
    return partitionId;
  }

  /**
   * Returns the session type.
   *
   * @return The session type.
   */
  public PrimitiveType primitiveType() {
    return primitiveType;
  }

  /**
   * Returns the primitive descriptor
   * @return the primitive descriptor
   */
  public PrimitiveDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  public PrimitiveState state() {
    return state;
  }

  /**
   * Returns the next command request sequence number for the session.
   *
   * @return The next command request sequence number for the session.
   */
  public long nextCommandRequest() {
    return ++commandRequest;
  }

  /**
   * Updates the session state.
   *
   * @param state The updates session state.
   */
  public void setState(PrimitiveState state) {
    if (this.state != state) {
      if (this.state != PrimitiveState.EXPIRED && this.state != PrimitiveState.CLOSED) {
        this.state = state;
        if (state == PrimitiveState.SUSPENDED) {
          if (suspendedTime == null) {
            suspendedTime = System.currentTimeMillis();
          }
        } else {
          suspendedTime = null;
        }
        stateListeners.forEach(l -> l.accept(state));
      }
    } else if (this.state == PrimitiveState.SUSPENDED && System.currentTimeMillis() - suspendedTime > timeout) {
      setState(PrimitiveState.EXPIRED);
    }
  }

  /**
   * Registers a state change listener on the session manager.
   *
   * @param listener The state change listener callback.
   */
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    stateListeners.add(checkNotNull(listener));
  }

  /**
   * Removes a state change listener.
   *
   * @param listener the listener to remove
   */
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    stateListeners.remove(checkNotNull(listener));
  }
}

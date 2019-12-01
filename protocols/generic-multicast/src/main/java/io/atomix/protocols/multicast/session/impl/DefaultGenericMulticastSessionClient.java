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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;
import io.atomix.protocols.multicast.session.GenericMulticastSessionClient;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The client session is responsible for maintaining a client's connection to the cluster and coordinating
 * the submission of {@link PrimitiveOperation operations} to various nodes in the cluster. Client
 * sessions are single-use objects that represent the context within which a cluster can guarantee linearizable
 * semantics for state machine operations. When a session is opened, the session will register
 * itself with the cluster by attempting to contact each of the known servers.
 * <p>
 * Sessions are responsible for sequencing concurrent operations to ensure they're applied to the system state
 * in the order in which they were submitted by the client. To do so, the session coordinates with its server-side
 * counterpart using unique per-operation sequence numbers.
 * <p>
 * In the event that the client session expires, clients are responsible for opening a new session by creating and
 * opening a new session object.
 */
public class DefaultGenericMulticastSessionClient implements GenericMulticastSessionClient {
  private Logger log;
  private final GenericMulticastSessionState sessionState;
  private final GenericMulticastSessionConnection sessionConnection;
  private final GenericMulticastSessionInvoker sessionInvoker;
  private final ThreadContext threadContext;
  private final PrimitiveDescriptor descriptor;
  private final Map<EventType, Set<Consumer<PrimitiveEvent>>> eventListeners = Maps.newConcurrentMap();

  public DefaultGenericMulticastSessionClient(
      GenericMulticastSessionState sessionState,
      GenericMulticastSessionConnection sessionConnection,
      GenericMulticastSessionInvoker sessionInvoker,
      ThreadContextFactory threadContextFactory,
      PrimitiveDescriptor primitiveDescriptor) {
    this.threadContext = threadContextFactory.createContext();
    this.sessionState = sessionState;
    this.sessionConnection = sessionConnection;
    this.sessionInvoker = sessionInvoker;
    this.descriptor = primitiveDescriptor;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
        .addValue(descriptor.name())
        .add("type", descriptor.type())
        .build());
  }

  @Override
  public String name() {
    return sessionState.clientId();
  }

  @Override
  public PrimitiveType type() {
    return sessionState.primitiveType();
  }

  @Override
  public PrimitiveState getState() {
    return sessionState.state();
  }

  @Override
  public SessionId sessionId() {
    return sessionState.sessionId();
  }

  @Override
  public PartitionId partitionId() {
    return sessionState.partitionId();
  }

  @Override
  public ThreadContext context() {
    return threadContext;
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    eventListeners.computeIfAbsent(eventType.canonicalize(), t -> Sets.newLinkedHashSet()).add(listener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    eventListeners.computeIfAbsent(eventType.canonicalize(), t -> Sets.newLinkedHashSet()).remove(listener);
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    sessionState.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    sessionState.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<SessionClient> connect() {
    sessionConnection.protocol().register(sessionId(), this::handleEvent, threadContext);
    return Futures.completedFuture(this);
  }

  @Override
  public CompletableFuture<Void> close() {
    if (sessionState != null) {
      sessionState.setState(PrimitiveState.CLOSED);
    }

    sessionConnection.protocol().unregister(sessionId());
    return Futures.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return close();
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    GenericMulticastSessionInvoker invoker = this.sessionInvoker;
    if (invoker == null) {
      return Futures.exceptionalFuture(new IllegalStateException("Not ready!"));
    }

    return invoker.invoke(operation);
  }

  /**
   * Handles a primitive event.
   */
  private void handleEvent(PrimitiveEvent event) {
    log.trace("Received {}", event);
    Set<Consumer<PrimitiveEvent>> listeners = eventListeners.get(event.type());
    if (listeners != null) {
      listeners.forEach(l -> l.accept(event));
    }
  }
}

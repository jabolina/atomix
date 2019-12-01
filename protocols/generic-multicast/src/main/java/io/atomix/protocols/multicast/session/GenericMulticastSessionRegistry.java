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

import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.impl.GenericMulticastSession;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Generic multicast sessions registry
 */
public class GenericMulticastSessionRegistry {
  private final Map<Long, GenericMulticastSession> sessions = new ConcurrentHashMap<>();

  /**
   * Adds a session.
   *
   * @param session: session to add
   * @return session that was associated with the key
   */
  public GenericMulticastSession add(GenericMulticastSession session) {
    GenericMulticastSession old = sessions.putIfAbsent(session.sessionId().id(), session);
    return old != null ? old : session;
  }

  /**
   * Closes a session.
   *
   * @param sessionId: id of the session to be removed
   * @return the removed session
   */
  public GenericMulticastSession remove(SessionId sessionId) {
    return sessions.remove(sessionId.id());
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  public GenericMulticastSession get(SessionId sessionId) {
    return get(sessionId.id());
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  public GenericMulticastSession get(long sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Returns the collection of registered sessions.
   *
   * @return The collection of registered sessions.
   */
  public Collection<GenericMulticastSession> sessions() {
    return sessions.values();
  }

  /**
   * Returns a set of sessions associated with the given service.
   *
   * @param primitiveId the service identifier
   * @return a collection of sessions associated with the given service
   */
  public Collection<GenericMulticastSession> sessions(PrimitiveId primitiveId) {
    return sessions().stream()
        .filter(session -> session.service().serviceId().equals(primitiveId))
        .filter(session -> session.getState().active())
        .collect(Collectors.toList());
  }

  /**
   * Removes all sessions registered for the given service.
   *
   * @param primitiveId the service identifier
   */
  public void remove(PrimitiveId primitiveId) {
    sessions.entrySet().removeIf(e -> e.getValue().service().serviceId().equals(primitiveId));
  }
}

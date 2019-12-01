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

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.multicast.impl.GenericMulticastSession;
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;

import java.util.Collection;
import java.util.Map;

public class GenericMulticastSessionManager {
  private final Map<Long, GenericMulticastSession> sessions = Maps.newConcurrentMap();
  private final GenericMulticastServiceContext genericMulticastService;
  private final PrimitiveService primitiveService;

  public GenericMulticastSessionManager(GenericMulticastServiceContext genericMulticastService, PrimitiveService primitiveService) {
    this.genericMulticastService = genericMulticastService;
    this.primitiveService = primitiveService;
  }

  /**
   * If the session with the given does not exists a new one is created
   *
   * @param sessionId id of the session
   * @param memberId id of the member
   * @return a new or already created session
   */
  public GenericMulticastSession getOrCreate(long sessionId, MemberId memberId) {
    GenericMulticastSession session = get(sessionId);
    if (session == null) {
      session = create(sessionId, memberId);
    }

    return session;
  }

  /**
   * Get the session for the given id
   *
   * @param sessionId: session with the given id
   * @return the session or null if no exists
   */
  public GenericMulticastSession get(long sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Create a new session with the given id and member id
   *
   * @param sessionId id of the session
   * @param memberId id of the member
   * @return a new session
   */
  public GenericMulticastSession create(long sessionId, MemberId memberId) {
    GenericMulticastSession session = new GenericMulticastSession(
        SessionId.from(sessionId),
        memberId,
        primitiveService.serializer(),
        genericMulticastService);
    if (sessions.putIfAbsent(sessionId, session) == null) {
      primitiveService.register(session);
    }

    return session;
  }

  /**
   * If the sessions with given id exists, the sessions is expired
   *
   * @param sessionId id of the session to be expired
   */
  public void expire(long sessionId) {
    GenericMulticastSession session = sessions.get(sessionId);
    if (session != null) {
      session.expire();
      primitiveService.expire(session.sessionId());
    }
  }

  /**
   * If the session with given id exists, the session is closed
   *
   * @param sessionId id of the session to be closed
   */
  public void close(long sessionId) {
    GenericMulticastSession session = sessions.remove(sessionId);
    if (session != null) {
      session.close();
      primitiveService.close(session.sessionId());
    }
  }

  /**
   * Return all the sessions
   *
   * @return all sessions
   */
  public Collection<GenericMulticastSession> values() {
    return sessions.values();
  }
}

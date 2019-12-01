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
package io.atomix.protocols.multicast.impl;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.impl.AbstractSession;
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

/**
 * Basic generic multicast session
 */
public class GenericMulticastSession extends AbstractSession {
  private final Logger log;
  private final GenericMulticastServiceContext service;
  private State state = State.OPEN;

  public GenericMulticastSession(
      SessionId sessionId,
      MemberId memberId,
      Serializer serializer,
      GenericMulticastServiceContext service) {
    super(sessionId, service.serviceName(), service.serviceType(), memberId, serializer);
    this.service = service;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(getClass())
        .addValue(service.serviceName())
        .add("session", sessionId)
        .build());
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void publish(PrimitiveEvent event) {
    if (!State.OPEN.equals(state)) {
      log.trace("Ignoring event due to not open state");
      return;
    }

    service.threadContext().execute(() -> {
      log.trace("Sending {} to {}", event, memberId());
      service.protocol().event(memberId(), sessionId(), event);
    });
  }

  public GenericMulticastServiceContext service() {
    return service;
  }

  /**
   * expire session
   */
  public void expire() {
    state = State.EXPIRED;
  }

  /**
   * close session
   */
  public void close() {
    state = State.CLOSED;
  }
}

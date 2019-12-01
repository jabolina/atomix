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

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.BlockingAwareSessionClient;
import io.atomix.primitive.session.impl.RecoveringSessionClient;
import io.atomix.primitive.session.impl.RetryingSessionClient;
import io.atomix.protocols.multicast.GenericMulticastClient;
import io.atomix.protocols.multicast.protocol.GenericMulticastClientProtocol;
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;
import io.atomix.protocols.multicast.session.GenericMulticastSessionClient;
import io.atomix.protocols.multicast.session.impl.DefaultGenericMulticastSessionClient;
import io.atomix.protocols.multicast.session.impl.GenericMulticastSessionConnection;
import io.atomix.protocols.multicast.session.impl.GenericMulticastSessionInvoker;
import io.atomix.protocols.multicast.session.impl.GenericMulticastSessionState;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Default generic multicast client
 */
public class DefaultGenericMulticastClient implements GenericMulticastClient {
  private final String clientName;
  private final PartitionId partitionId;
  private final ClusterMembershipService membershipService;
  private final GenericMulticastClientProtocol protocol;
  private final SessionIdService sessionIdService;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;
  private final boolean closeOnStop;

  public DefaultGenericMulticastClient(
      String clientName,
      PartitionId partitionId,
      ClusterMembershipService membershipService,
      GenericMulticastClientProtocol protocol,
      SessionIdService sessionIdService,
      ThreadContextFactory threadContextFactory,
      boolean closeOnStop) {
    this.clientName = clientName;
    this.partitionId = partitionId;
    this.membershipService = membershipService;
    this.protocol = protocol;
    this.sessionIdService = sessionIdService;
    this.threadContextFactory = threadContextFactory;
    this.threadContext = threadContextFactory.createContext();
    this.closeOnStop = closeOnStop;
  }

  @Override
  public String clientName() {
    return clientName;
  }

  @Override
  public GenericMulticastSessionClient.Builder sessionBuilder(String pName, PrimitiveType pType, ServiceConfig conf) {
    return new GenericMulticastSessionClient.Builder() {
      @Override
      public SessionClient build() {
        Supplier<CompletableFuture<SessionClient>> factory = () -> sessionIdService.nextSessionId()
            .thenApply(sessionId -> {
              ThreadContext context = threadContextFactory.createContext();
              PrimitiveDescriptor descriptor = new PrimitiveDescriptor(
                  pName,
                  pType.name(),
                  Serializer.using(pType.namespace()).encode(conf),
                  readConsistency
              );
              LoggerContext loggerContext = LoggerContext.builder(SessionClient.class)
                  .addValue(descriptor.name())
                  .add("type", descriptor.type())
                  .build();
              GenericMulticastSessionState sessionState = new GenericMulticastSessionState(
                  clientName,
                  sessionId,
                  1000,
                  partitionId,
                  pType,
                  descriptor);
              GenericMulticastSessionConnection sessionConnection = new GenericMulticastSessionConnection(
                  protocol,
                  membershipService,
                  context,
                  loggerContext);
              GenericMulticastSessionInvoker sessionInvoker = new GenericMulticastSessionInvoker(
                  sessionState,
                  sessionConnection,
                  context,
                  loggerContext);
              return new DefaultGenericMulticastSessionClient(
                  sessionState,
                  sessionConnection,
                  sessionInvoker,
                  threadContextFactory,
                  descriptor);
            });

        SessionClient proxy;
        ThreadContext context = threadContextFactory.createContext();

        if (recovery == Recovery.RECOVER) {
          proxy = new RecoveringSessionClient(
              clientName,
              partitionId,
              pName,
              pType,
              factory,
              context);
        } else {
          proxy = Futures.get(factory.get());
        }

        if (maxRetries > 0) {
          proxy = new RetryingSessionClient(
              proxy,
              context,
              maxRetries,
              retryDelay);
        }

        return new BlockingAwareSessionClient(proxy, context);
      }
    };
  }

  @Override
  public CompletableFuture<Void> close() {
    threadContext.close();

    if (closeOnStop) {
      threadContextFactory.close();
    }

    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", clientName)
        .toString();
  }

  /**
   * Default generic multicast client builder
   */
  public static class Builder extends GenericMulticastClient.Builder {
    @Override
    public GenericMulticastClient build() {
      Logger logger = ContextualLoggerFactory.getLogger(GenericMulticastClient.class,
          LoggerContext.builder(GenericMulticastClient.class)
              .addValue(clientName)
              .build());

      boolean closeOnStop = false;
      ThreadContextFactory factory;

      if (threadContextFactory == null) {
        factory = threadModel.factory("gmcast-client-" + clientName + "%d", threadPoolSize, logger);
        closeOnStop = true;
      } else {
        factory = threadContextFactory;
      }

      return new DefaultGenericMulticastClient(
          clientName,
          partitionId,
          clusterService,
          protocol,
          sessionIdService,
          factory,
          closeOnStop);
    }
  }
}

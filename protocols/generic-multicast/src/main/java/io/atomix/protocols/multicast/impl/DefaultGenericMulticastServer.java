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

import io.atomix.primitive.impl.ClasspathScanningPrimitiveTypeRegistry;
import io.atomix.primitive.partition.impl.DefaultMemberGroupService;
import io.atomix.protocols.multicast.GenericMulticastServer;
import io.atomix.protocols.multicast.protocol.GenericMulticastServerContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultGenericMulticastServer implements GenericMulticastServer {
  private final GenericMulticastServerContext context;

  private DefaultGenericMulticastServer(GenericMulticastServerContext context) {
    this.context = checkNotNull(context, "Server context cannot be null!");
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public CompletableFuture<GenericMulticastServer> start() {
    return context.start().thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return context.isRunning();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return context.stop();
  }

  public static class Builder extends GenericMulticastServer.Builder {

    @Override
    public GenericMulticastServer build() {
      Logger log = ContextualLoggerFactory.getLogger(DefaultGenericMulticastServer.class, LoggerContext
          .builder(DefaultGenericMulticastServer.class)
          .addValue(serverName)
          .build());
      boolean closeOnStop = false;
      ThreadContextFactory factory = threadContextFactory;
      if (threadContextFactory == null) {
        factory = threadModel.factory("gmcast-server-" + serverName + "-%d", threadPoolSize, log);
        closeOnStop = true;
      }

      return new DefaultGenericMulticastServer(new GenericMulticastServerContext(
          serverName,
          membershipService,
          new DefaultMemberGroupService(membershipService, memberGroupProvider),
          protocol,
          factory,
          closeOnStop,
          primitiveTypes != null
              ? primitiveTypes
              : new ClasspathScanningPrimitiveTypeRegistry(Thread.currentThread().getContextClassLoader()),
          timestampProvider,
          primaryElection
      ));
    }
  }
}

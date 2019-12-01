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
package io.atomix.protocols.multicast.partition.impl;

import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.protocols.multicast.GenericMulticastServer;
import io.atomix.protocols.multicast.impl.DefaultGenericMulticastServer;
import io.atomix.protocols.multicast.partition.GenericMulticastNamespaces;
import io.atomix.protocols.multicast.partition.GenericMulticastPartition;
import io.atomix.protocols.multicast.protocol.GenericMulticastServerCommunicator;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class GenericMulticastPartitionServer implements Managed<GenericMulticastPartitionServer> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final GenericMulticastPartition partition;
  private final PartitionManagementService managementService;
  private final MemberGroupProvider memberGroupProvider;
  private final ThreadContextFactory threadContextFactory;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicCounter timestampProvider;
  private GenericMulticastServer server;

  public GenericMulticastPartitionServer(
      GenericMulticastPartition partition,
      PartitionManagementService managementService,
      MemberGroupProvider memberGroupProvider,
      ThreadContextFactory threadContextFactory,
      AtomicCounter timestampProvider) {
    this.partition = partition;
    this.managementService = managementService;
    this.memberGroupProvider = memberGroupProvider;
    this.threadContextFactory = threadContextFactory;
    this.timestampProvider = timestampProvider;
  }

  @Override
  public CompletableFuture<GenericMulticastPartitionServer> start() {
    synchronized (this) {
      server = buildServer();
    }

    return server.start().thenApply(s -> {
      log.debug("Started gmcast server for {}", partition.id());
      started.set(true);
      return this;
    });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (server != null) {
      log.debug("Stopping gmcast server in {}", partition.id());
      return server.stop().exceptionally(throwable -> {
        log.error("Failed stopping server for {}", partition.id(), throwable);
        return null;
      }).thenRun(() -> started.set(false));
    }

    started.set(false);
    return CompletableFuture.completedFuture(null);
  }

  private GenericMulticastServer buildServer() {
    return DefaultGenericMulticastServer.builder()
        .withServerName(partition.name())
        .withMembershipService(managementService.getMembershipService())
        .withMemberGroupProvider(memberGroupProvider)
        .withProtocol(new GenericMulticastServerCommunicator(
            partition.name(),
            Serializer.using(GenericMulticastNamespaces.GENERIC_MULTICAST_PROTOCOL),
            managementService.getMessagingService()))
        .withPrimaryElection(managementService.getElectionService().getElectionFor(partition.id()))
        .withPrimitiveTypes(managementService.getPrimitiveTypes())
        .withThreadContextFactory(threadContextFactory)
        .withTimestampProvider(timestampProvider)
        .build();
  }
}

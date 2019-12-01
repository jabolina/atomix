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

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.protocols.multicast.GenericMulticastClient;
import io.atomix.protocols.multicast.partition.GenericMulticastNamespaces;
import io.atomix.protocols.multicast.partition.GenericMulticastPartitionGroupConfig;
import io.atomix.protocols.multicast.protocol.GenericMulticastCommunicator;
import io.atomix.protocols.multicast.session.GenericMulticastSessionClient;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class GenericMulticastPartitionClient implements PartitionClient, Managed<GenericMulticastPartitionClient> {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final DefaultGenericMulticastPartition partition;
  private final PartitionManagementService managementService;
  private final ThreadContextFactory threadContextFactory;
  private final GenericMulticastPartitionGroupConfig config;
  private volatile GenericMulticastClient client;

  GenericMulticastPartitionClient(
      DefaultGenericMulticastPartition partition,
      PartitionManagementService managementService,
      ThreadContextFactory threadContextFactory,
      GenericMulticastPartitionGroupConfig config) {
    this.partition = partition;
    this.managementService = managementService;
    this.threadContextFactory = threadContextFactory;
    this.config = config;
  }

  @Override
  public GenericMulticastSessionClient.Builder sessionBuilder(
      String primitiveName,
      PrimitiveType type,
      ServiceConfig serviceConfig) {
    return client.sessionBuilder(primitiveName, type, serviceConfig);
  }

  @Override
  public CompletableFuture<GenericMulticastPartitionClient> start() {
    synchronized (GenericMulticastPartitionClient.this) {
      client = newClient();
      log.debug("Started client for {}", partition.id());
    }

    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return client != null;
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (!isRunning()) {
      return CompletableFuture.completedFuture(null);
    }

    return client.close();
  }

  private GenericMulticastClient newClient() {
    return GenericMulticastClient.builder()
        .withClientName(partition.name())
        .withPartitionId(partition.id())
        .withMembershipService(managementService.getMembershipService())
        .withProtocol(new GenericMulticastCommunicator(
            partition.name(),
            Serializer.using(GenericMulticastNamespaces.GENERIC_MULTICAST_PROTOCOL),
            managementService.getMessagingService()))
        .withSessionIdProvider(managementService.getSessionIdService())
        .withThreadContextFactory(threadContextFactory)
        .build();
  }
}

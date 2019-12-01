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

import io.atomix.cluster.MemberId;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.protocols.multicast.partition.GenericMulticastPartition;
import io.atomix.protocols.multicast.partition.GenericMulticastPartitionGroupConfig;
import io.atomix.protocols.multicast.service.GenericMulticastSingletonAtomixClient;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContextFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Generic multicast partition
 */
public class DefaultGenericMulticastPartition extends GenericMulticastPartition {
  private final PartitionId partitionId;
  private final GenericMulticastPartitionGroupConfig config;
  private final MemberGroupProvider memberGroupProvider;
  private PrimaryElection election;
  private GenericMulticastPartitionClient client;
  private GenericMulticastPartitionServer server;
  private AtomicCounter timestampProvider;

  public DefaultGenericMulticastPartition(
      PartitionId partitionId,
      GenericMulticastPartitionGroupConfig config) {
    this.partitionId = partitionId;
    this.config = config;
    this.memberGroupProvider = config.getMemberGroupProvider();
    this.timestampProvider = config.getTimestampProvider();
  }

  @Override
  public String name() {
    return String.format("%s-partition-%d", partitionId.group(), partitionId.id());
  }

  @Override
  public PartitionId id() {
    return partitionId;
  }

  @Override
  public long term() {
    return Futures.get(election.getTerm()).term();
  }

  @Override
  public Collection<MemberId> members() {
    return Futures.get(election.getTerm())
        .candidates()
        .stream()
        .map(GroupMember::memberId)
        .collect(Collectors.toList());
  }

  @Override
  public GenericMulticastPartitionClient getClient() {
    return client;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitionId", id())
        .toString();
  }

  public CompletableFuture<Partition> join(PartitionManagementService managementService, ThreadContextFactory factory) {
    election = managementService.getElectionService().getElectionFor(partitionId);
    client = new GenericMulticastPartitionClient(this, managementService, factory, config);
    server = new GenericMulticastPartitionServer(this, managementService, memberGroupProvider, factory, timestampProvider(new AtomicInteger(0)));
    return server.start()
        .thenCompose(v -> client.start())
        .thenApply(v -> this);
  }

  public CompletableFuture<Partition> connect(PartitionManagementService service, ThreadContextFactory factory) {
    election = service.getElectionService().getElectionFor(partitionId);
    client = new GenericMulticastPartitionClient(this, service, factory, config);
    return client.start().thenApply(v -> null);
  }

  public CompletableFuture<Void> close() {
    if (client == null) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    client.stop().whenComplete((cRes, cErr) -> {
      if (server != null) {
        server.stop().whenComplete((sRes, sErr) -> future.complete(null));
      } else {
        future.complete(null);
      }
    });

    return future;
  }

  public DefaultGenericMulticastPartition setTimestampProvider(AtomicCounter timestampProvider) {
    this.timestampProvider = timestampProvider;
    return this;
  }

  private AtomicCounter timestampProvider(AtomicInteger tries) {
    int basePort = 15678;
    if (tries.get() > 150) {
      throw new RuntimeException("Could not start Atomix");
    }

    if (timestampProvider == null) {
      try {
        int port = basePort + tries.get();
        String[] members = {"localhost:" + port};
        timestampProvider = GenericMulticastSingletonAtomixClient.instance(port, members)
            .atomix().getAtomicCounter(config.getName() + "-ts-counter");
      } catch (Exception e) {
        tries.incrementAndGet();
        return timestampProvider(tries);
      }
    }

    return timestampProvider;
  }
}

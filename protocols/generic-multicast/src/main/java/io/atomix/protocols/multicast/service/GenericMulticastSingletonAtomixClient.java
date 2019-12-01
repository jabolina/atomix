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
package io.atomix.protocols.multicast.service;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.net.Address;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Since the {@link io.atomix.protocols.multicast.GenericMulticastProtocol} needs a consensus as primitive
 * to be available to decide upon the messages timestamps, will be created an Atomix client for inner usage.
 * <p>
 * This instance will be a singleton and will use multicast to find nodes across multiple replicas.
 * </p>
 */
public final class GenericMulticastSingletonAtomixClient {
  private static volatile GenericMulticastSingletonAtomixClient instance;
  private final Atomix atomix;

  private GenericMulticastSingletonAtomixClient(String serverName, int port, String[] members) {
    LoggerFactory.getLogger(getClass()).info("Creating client {}", serverName);
    this.atomix = Atomix.builder()
        .withMemberId(serverName)
        .withAddress(Address.from("localhost", port))
        .withMembershipProvider(BootstrapDiscoveryProvider
            .builder()
            .withNodes(clusterMembers(members))
            .build())
        .withPartitionGroups(partitionGroup(serverName, members))
        .withManagementGroup(managementGroup(serverName, members))
        .build();
  }

  public static GenericMulticastSingletonAtomixClient instance(int port, String[] members) throws InterruptedException, ExecutionException, TimeoutException {
    if (instance == null) {
      synchronized (GenericMulticastSingletonAtomixClient.class) {
        if (instance == null) {
          String name = "localhost:" + port;
          GenericMulticastSingletonAtomixClient temp = new GenericMulticastSingletonAtomixClient(name, port, members);
          temp.atomix.start().get(30, TimeUnit.SECONDS);
          instance = temp;
        }
      }
    }

    return instance;
  }

  private List<Node> clusterMembers(String[] members) {
    return Arrays.stream(members)
        .map(addr -> Node.builder()
            .withId(addr)
            .withAddress(Address.from(addr))
            .build()
        ).collect(Collectors.toList());
  }

  private String diskPath(String name, String posfix) {
    return "./cluster/" + name + "/" + UUID.randomUUID().toString() + posfix;
  }

  private ManagedPartitionGroup partitionGroup(String name, String[] members) {
    return RaftPartitionGroup
        .builder("raft")
        .withStorageLevel(StorageLevel.DISK)
        .withDataDirectory(new File(diskPath(name, "/gmcast-sys-partition")))
        .withStorageLevel(StorageLevel.MAPPED)
        .withNumPartitions(1)
        .withMembers(members)
        .build();
  }

  private ManagedPartitionGroup managementGroup(String name, String[] members) {
    return RaftPartitionGroup
        .builder(name + "-management")
        .withDataDirectory(new File(diskPath(name, "/gmcast-sys-management")))
        .withStorageLevel(StorageLevel.MAPPED)
        .withNumPartitions(1)
        .withMembers(members)
        .build();
  }

  public Atomix atomix() {
    return atomix;
  }
}

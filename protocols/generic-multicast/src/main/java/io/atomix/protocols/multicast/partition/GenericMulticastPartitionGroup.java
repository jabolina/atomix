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
package io.atomix.protocols.multicast.partition;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.impl.AtomicCounterProxy;
import io.atomix.core.counter.impl.BlockingAtomicCounter;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.Murmur3Partitioner;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.proxy.impl.DefaultProxyClient;
import io.atomix.primitive.proxy.impl.DefaultProxySession;
import io.atomix.protocols.multicast.GenericMulticastClient;
import io.atomix.protocols.multicast.GenericMulticastProtocol;
import io.atomix.protocols.multicast.impl.DefaultGenericMulticastClient;
import io.atomix.protocols.multicast.partition.impl.DefaultGenericMulticastPartition;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Generic multicast partition group
 */
public class GenericMulticastPartitionGroup implements ManagedPartitionGroup {
  public static final Type TYPE = new Type();

  /**
   * Returns a new generic multicast partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(new GenericMulticastPartitionGroupConfig().setName(name));
  }

  public static class Type implements PartitionGroup.Type<GenericMulticastPartitionGroupConfig> {
    private static final String NAME = "gmcast";

    @Override
    public Namespace namespace() {
      return Namespace.builder()
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 400)
          .register(Namespaces.BASIC)
          .setRegistrationRequired(false)
          .setCompatible(false)
          .register(GenericMulticastPartitionGroupConfig.class)
          .register(MemberGroupStrategy.class)
          .register(BlockingAtomicCounter.class)
          .register(AtomicCounterProxy.class)
          .register(DefaultProxyClient.class)
          .register(CopyOnWriteArrayList.class)
          .register(PartitionId.class)
          .register(Murmur3Partitioner.class)
          .register(DefaultProxySession.class)
          .register(OrderedFuture.class)
          .register(ch.qos.logback.classic.Logger.class)
          .build();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(GenericMulticastPartitionGroupConfig config) {
      return new GenericMulticastPartitionGroup(config);
    }

    @Override
    public GenericMulticastPartitionGroupConfig newConfig() {
      return new GenericMulticastPartitionGroupConfig();
    }

    @Override
    public String name() {
      return NAME;
    }
  }

  private static Collection<DefaultGenericMulticastPartition> buildPartitions(
      GenericMulticastPartitionGroupConfig config) {
    List<DefaultGenericMulticastPartition> partitions = new ArrayList<>(config.getPartitions());
    for (int idx = 0; idx < config.getPartitions(); idx++) {
      partitions.add(new DefaultGenericMulticastPartition(PartitionId.from(config.getName(), idx + 1), config));
    }
    return partitions;
  }

  private final Logger logger;
  private final String name;
  private final GenericMulticastPartitionGroupConfig config;
  private final Map<PartitionId, DefaultGenericMulticastPartition> partitions = Maps.newConcurrentMap();
  private final List<DefaultGenericMulticastPartition> sortedPartitions = Lists.newCopyOnWriteArrayList();
  private final List<PartitionId> partitionIds = Lists.newCopyOnWriteArrayList();
  private ThreadContextFactory threadContextFactory;

  private GenericMulticastPartitionGroup(GenericMulticastPartitionGroupConfig config) {
    this.config = config;
    this.name = Preconditions.checkNotNull(config.getName());
    this.logger = ContextualLoggerFactory.getLogger(DefaultGenericMulticastClient.class,
        LoggerContext.builder(GenericMulticastClient.class)
        .addValue(config.getName())
        .build());

    buildPartitions(config).forEach(p -> {
      this.partitions.put(p.id(), p);
      this.sortedPartitions.add(p);
      this.partitionIds.add(p.id());
    });
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
    int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory(
        "gmcast-partition-group-" + name + "-%d", threadPoolSize, logger);
    return CompletableFuture.allOf(partitions.values().stream()
        .map(partition -> partition.join(managementService, threadContextFactory)).toArray(CompletableFuture[]::new))
        .thenApply(v -> {
          logger.debug("Partition group joined!");
          return this;
        });
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory(
        "gmcast-partition-group-" + name + "-%d", threadPoolSize, logger);
    return CompletableFuture.allOf(partitions.values().stream()
        .map(partition -> partition.connect(managementService, threadContextFactory)).toArray(CompletableFuture[]::new))
        .thenApply(v -> {
          logger.debug("Partition group started!");
          return this;
        });
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.allOf(partitions.values().stream()
        .map(DefaultGenericMulticastPartition::close).toArray(CompletableFuture[]::new))
        .whenCompleteAsync((res, err) -> {
          logger.debug("Partition group closed!");
          if (threadContextFactory != null) {
            threadContextFactory.close();
          }
        });
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type type() {
    return TYPE;
  }

  @Override
  public PrimitiveProtocol.Type protocol() {
    return GenericMulticastProtocol.TYPE;
  }

  @Override
  public ProxyProtocol newProtocol() {
    return GenericMulticastProtocol.builder(name)
        .withClients(7)
        .withRecovery(Recovery.RECOVER)
        .build();
  }

  @Override
  public Partition getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition> getPartitions() {
    return (Collection) sortedPartitions;
  }

  @Override
  public List<PartitionId> getPartitionIds() {
    return partitionIds;
  }

  @Override
  public PartitionGroupConfig config() {
    return config;
  }

  /**
   * Generic multicast group partition builder
   */
  public static class Builder extends PartitionGroup.Builder<GenericMulticastPartitionGroupConfig> {
    protected Builder(GenericMulticastPartitionGroupConfig config) {
      super(config);
    }

    public Builder withTimestampProvider(AtomicCounter timestampProvider) {
      config.setTimestampProvider(Objects.requireNonNull(timestampProvider, "Timestamp provider cannot be null!"));
      return this;
    }

    /**
     * Sets the number of partitions.
     *
     * @param numPartitions the number of partitions
     * @return the partition group builder
     * @throws IllegalArgumentException if the number of partitions is not positive
     */
    public Builder withNumPartitions(int numPartitions) {
      config.setPartitions(numPartitions);
      return this;
    }

    /**
     * Sets the member group strategy.
     *
     * @param memberGroupStrategy the member group strategy
     * @return the partition group builder
     */
    public Builder withMemberGroupStrategy(MemberGroupStrategy memberGroupStrategy) {
      config.setMemberGroupStrategy(memberGroupStrategy);
      return this;
    }

    @Override
    public GenericMulticastPartitionGroup build() {
      return new GenericMulticastPartitionGroup(config);
    }
  }
}

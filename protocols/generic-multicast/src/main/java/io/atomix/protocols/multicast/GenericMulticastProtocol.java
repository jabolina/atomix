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
package io.atomix.protocols.multicast;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.DefaultProxyClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.protocols.multicast.partition.impl.DefaultGenericMulticastPartition;
import io.atomix.utils.config.ConfigurationException;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Generic multicast protocol
 */
public class GenericMulticastProtocol implements ProxyProtocol {
  public static final Type TYPE = new Type();
  protected final GenericMulticastProtocolConfig config;

  GenericMulticastProtocol(GenericMulticastProtocolConfig config) {
    this.config = config;
  }

  /**
   * Returns a new generic multicast protocol builder.
   *
   * @return a new generic multicast protocol builder
   */
  public static GenericMulticastProtocolBuilder builder() {
    return new GenericMulticastProtocolBuilder(new GenericMulticastProtocolConfig());
  }

  /**
   * Returns a new generic multicast protocol builder for the given group.
   *
   * @param group the partition group
   * @return a new generic multicast protocol builder for the given group
   */
  public static GenericMulticastProtocolBuilder builder(String group) {
    return new GenericMulticastProtocolBuilder(new GenericMulticastProtocolConfig().setGroup(group));
  }

  @Override
  public String group() {
    return config.getGroup();
  }

  private Collection<SessionClient> clients(String primitiveName,
      PrimitiveType primitiveType,
      PartitionService partitionService,
      ServiceConfig serviceConfig) {
    PartitionGroup partitionGroup = partitionService.getPartitionGroup(this);
    if (partitionGroup == null) {
      throw new ConfigurationException("No partition group found configured for generic multicast algorithm");
    }

    return partitionGroup.getPartitions().stream()
        .map(partition -> ((DefaultGenericMulticastPartition) partition).getClient()
            .sessionBuilder(primitiveName, primitiveType, serviceConfig)
            .withMaxRetries(config.getMaxRetries())
            .withMaxTimeout(config.getMaxTimeout())
            .withMinTimeout(config.getMinTimeout())
            .withRecoveryStrategy(config.getRecovery())
            .withRetryDelay(config.getRetryDelay())
            .build())
        .collect(Collectors.toList());
  }

  @Override
  public <S> ProxyClient<S> newProxy(String primitiveName, PrimitiveType primitiveType,
                                     Class<S> serviceType, ServiceConfig serviceConfig,
                                     PartitionService partitionService) {
    Collection<SessionClient> partitions = clients(primitiveName, primitiveType, partitionService, serviceConfig);
    return new DefaultProxyClient<>(
        primitiveName,
        primitiveType,
        this,
        serviceType,
        partitions,
        config.getPartitioner()
    );
  }

  @Override
  public Type type() {
    return TYPE;
  }

  /**
   * Generic multicast protocol type
   */
  public static final class Type implements PrimitiveProtocol.Type<GenericMulticastProtocolConfig> {
    private static final String NAME = "generic-multicast";

    @Override
    public PrimitiveProtocol newProtocol(GenericMulticastProtocolConfig config) {
      return new GenericMulticastProtocol(config);
    }

    @Override
    public GenericMulticastProtocolConfig newConfig() {
      return new GenericMulticastProtocolConfig();
    }

    @Override
    public String name() {
      return NAME;
    }
  }
}

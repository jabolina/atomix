/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.server.management.impl;

import io.atomix.server.ServerConfig;
import io.atomix.server.management.Node;
import io.atomix.server.management.NodeService;
import io.atomix.server.management.PartitionService;
import io.atomix.server.management.PrimaryElectionService;
import io.atomix.server.management.ProtocolManagementService;
import io.atomix.server.management.ServiceProvider;
import io.atomix.server.management.ServiceRegistry;
import io.atomix.server.protocol.ProtocolTypeRegistry;
import io.atomix.service.ServiceTypeRegistry;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.concurrent.ThreadService;

/**
 * Protocol management service implementation.
 */
@Component(ServerConfig.class)
public class ProtocolManagementServiceImpl implements ProtocolManagementService {
  @Dependency
  private NodeService nodeService;
  @Dependency
  private ServiceRegistry serviceRegistry;
  @Dependency
  private ServiceProvider serviceProvider;
  @Dependency
  private ThreadService threadService;
  @Dependency
  private ProtocolTypeRegistry protocolTypeRegistry;
  @Dependency
  private ServiceTypeRegistry serviceTypeRegistry;
  @Dependency
  private PrimaryElectionService primaryElectionService;
  @Dependency
  private PartitionService partitionService;

  @Override
  public Node getNode() {
    return nodeService.getNode();
  }

  @Override
  public ServiceRegistry getServiceRegistry() {
    return serviceRegistry;
  }

  @Override
  public ServiceProvider getServiceProvider() {
    return serviceProvider;
  }

  @Override
  public ThreadService getThreadService() {
    return threadService;
  }

  @Override
  public ProtocolTypeRegistry getProtocolTypeRegistry() {
    return protocolTypeRegistry;
  }

  @Override
  public ServiceTypeRegistry getServiceTypeRegistry() {
    return serviceTypeRegistry;
  }

  @Override
  public PrimaryElectionService getPrimaryElectionService() {
    return primaryElectionService;
  }
}

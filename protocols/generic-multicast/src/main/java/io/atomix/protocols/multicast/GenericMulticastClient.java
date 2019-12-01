/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.protocols.multicast.impl.DefaultGenericMulticastClient;
import io.atomix.protocols.multicast.protocol.GenericMulticastClientProtocol;
import io.atomix.protocols.multicast.session.GenericMulticastSessionClient;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Generic multicast client interface to submit commands
 */
public interface GenericMulticastClient {

  /**
   * Returns a new generic multicast client builder.
   *
   * @return a new generic multicast client builder
   */
  static Builder builder() {
    return new DefaultGenericMulticastClient.Builder();
  }

  /**
   * Returns the client name
   * @return the client name
   */
  String clientName();

  /**
   * Creates a new generic multicast proxy session builder.
   *
   * @param primitiveName the primitive name
   * @param type the primitive type
   * @param config the service configuration
   * @return a new generic multicast proxy session builder
   */
  GenericMulticastSessionClient.Builder sessionBuilder(String primitiveName, PrimitiveType type, ServiceConfig config);

  /**
   * Closes the generic multicast client.
   *
   * @return future to be completed once the client is closed
   */
  CompletableFuture<Void> close();


  /**
   * Builds a new generic multicast client.
   * <p>
   * New client builders should be constructed using the static {@link #builder()} factory method.
   */
  abstract class Builder implements io.atomix.utils.Builder<GenericMulticastClient> {
    protected String clientName = "gmcast";
    protected PartitionId partitionId;
    protected ClusterMembershipService clusterService;
    protected GenericMulticastClientProtocol protocol;
    protected SessionIdService sessionIdService;
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    protected ThreadContextFactory threadContextFactory;

    /**
     * Sets the client name.
     *
     * @param clientName The client name.
     * @return The client builder.
     * @throws NullPointerException if {@code clientName} is null
     */
    public Builder withClientName(String clientName) {
      this.clientName = checkNotNull(clientName, "clientName cannot be null");
      return this;
    }

    /**
     * Sets the client partition ID.
     *
     * @param partitionId the client partition ID
     * @return the client builder
     */
    public Builder withPartitionId(PartitionId partitionId) {
      this.partitionId = checkNotNull(partitionId, "partitionId cannot be null");
      return this;
    }

    /**
     * Sets the cluster membership service.
     *
     * @param membershipService the cluster membership service
     * @return the client builder
     */
    public Builder withMembershipService(ClusterMembershipService membershipService) {
      this.clusterService = checkNotNull(membershipService, "membershipService cannot be null");
      return this;
    }

    /**
     * Sets the client protocol.
     *
     * @param protocol the client protocol
     * @return the client builder
     */
    public Builder withProtocol(GenericMulticastClientProtocol protocol) {
      this.protocol = checkNotNull(protocol, "protocol cannot be null");
      return this;
    }

    /**
     * Sets the session ID provider.
     *
     * @param sessionIdService the session ID provider
     * @return the client builder
     */
    public Builder withSessionIdProvider(SessionIdService sessionIdService) {
      this.sessionIdService = checkNotNull(sessionIdService, "sessionIdProvider cannot be null");
      return this;
    }

    /**
     * Sets the client thread model.
     *
     * @param threadModel the client thread model
     * @return the client builder
     * @throws NullPointerException if the thread model is null
     */
    public Builder withThreadModel(ThreadModel threadModel) {
      this.threadModel = checkNotNull(threadModel, "threadModel cannot be null");
      return this;
    }

    /**
     * Sets the client thread pool size.
     *
     * @param threadPoolSize The client thread pool size.
     * @return The client builder.
     * @throws IllegalArgumentException if the thread pool size is not positive
     */
    public Builder withThreadPoolSize(int threadPoolSize) {
      checkArgument(threadPoolSize > 0, "threadPoolSize must be positive");
      this.threadPoolSize = threadPoolSize;
      return this;
    }

    /**
     * Sets the client thread context factory.
     *
     * @param threadContextFactory the client thread context factory
     * @return the client builder
     * @throws NullPointerException if the factory is null
     */
    public Builder withThreadContextFactory(ThreadContextFactory threadContextFactory) {
      this.threadContextFactory = checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
      return this;
    }
  }
}

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
package io.atomix.protocols.multicast.protocol;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.ManagedMemberGroupService;
import io.atomix.primitive.partition.MemberGroup;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.protocol.message.request.Request;
import io.atomix.protocols.multicast.protocol.message.request.RestoreRequest;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.protocols.multicast.protocol.message.response.RestoreResponse;
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class GenericMulticastServerContext implements Managed<Void> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String serverName;
  private final ClusterMembershipService clusterMembershipService;
  private final ManagedMemberGroupService memberGroupService;
  private final GenericMulticastServerProtocol protocol;
  private final ThreadContextFactory threadContextFactory;
  private final boolean closeOnStop;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final Map<String, CompletableFuture<GenericMulticastServiceContext>> services = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicCounter timestampProvider;
  private final PrimaryElection primaryElection;
  private MemberGroup memberGroup;

  public GenericMulticastServerContext(
      String serverName,
      ClusterMembershipService clusterMembershipService,
      ManagedMemberGroupService memberGroupService,
      GenericMulticastServerProtocol protocol,
      ThreadContextFactory threadContextFactory,
      boolean closeOnStop,
      PrimitiveTypeRegistry primitiveTypes,
      AtomicCounter timestampProvider,
      PrimaryElection primaryElection) {
    this.serverName = serverName;
    this.clusterMembershipService = clusterMembershipService;
    this.memberGroupService = memberGroupService;
    this.protocol = protocol;
    this.threadContextFactory = threadContextFactory;
    this.closeOnStop = closeOnStop;
    this.primitiveTypes = primitiveTypes;
    this.timestampProvider = timestampProvider;
    this.primaryElection = checkNotNull(primaryElection, "Primary election cannot be null!");
  }

  @Override
  public CompletableFuture<Void> start() {
    registerHandlers();
    return memberGroupService.start().thenCompose(v -> {
      this.memberGroup = memberGroupService.getMemberGroup(clusterMembershipService.getLocalMember());
      if (memberGroup != null) {
        return primaryElection.enter(new GroupMember(clusterMembershipService.getLocalMember().id(), memberGroup.id()));
      }
      return CompletableFuture.completedFuture(null);
    }).thenApply(v -> {
      started.set(true);
      return null;
    });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    unregisterHandlers();
    started.set(false);
    List<CompletableFuture<Void>> futures = services.values().stream()
        .map(future -> future.thenCompose(GenericMulticastServiceContext::close))
        .collect(Collectors.toList());
    return Futures.allOf(futures).exceptionally(throwable -> {
      log.error("Failed closing services", throwable);
      return null;
    }).thenCompose(v -> memberGroupService.stop()).exceptionally(throwable -> {
      log.error("Failed stopping member group service", throwable);
      return null;
    }).thenRunAsync(() -> {
      if (closeOnStop) {
        threadContextFactory.close();
      }
    });
  }

  private CompletableFuture<GenericMulticastServiceContext> service(Request request) {
    return services.computeIfAbsent(request.descriptor().name(), n -> {
      PrimitiveType primitiveType = primitiveTypes.getPrimitiveType(request.descriptor().type());
      GenericMulticastServiceContext service = new GenericMulticastServiceContext(
          serverName,
          PrimitiveId.from(request.descriptor().name()),
          primitiveType,
          request.descriptor(),
          threadContextFactory.createContext(),
          clusterMembershipService,
          protocol,
          timestampProvider,
          primaryElection
      );

      OrderedFuture<GenericMulticastServiceContext> future = new OrderedFuture<>();
      service.open().whenComplete((v, e) -> {
        if (e != null) {
          future.completeExceptionally(e);
        } else {
          future.complete(service);
        }
      });

      return future;
    });
  }

  /**
   * Register all handlers
   */
  private void registerHandlers() {
    protocol.registerExecuteHandler(this::execute);
    protocol.registerComputeHandler(this::compute);
    protocol.registerGatherHandler(this::gather);
    protocol.registerCloseHandler(this::close);
    protocol.registerRestoreHandler(this::restore);
  }

  /**
   * Unregister all handlers
   */
  private void unregisterHandlers() {
    protocol.unregisterExecuteHandler();
    protocol.unregisterComputeHandler();
    protocol.unregisterGatherHandler();
    protocol.unregisterCloseHandler();
    protocol.unregisterRestoreHandler();
  }

  /**
   * Handler execute request
   * @param request: execute request
   * @return future with request response
   */
  private CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    return service(request).thenCompose(context -> context.execute(request));
  }

  /**
   * Handler compute request
   * @param request: compute request
   * @return future with request response
   */
  private CompletableFuture<ComputeResponse> compute(ComputeRequest request) {
    return service(request).thenCompose(context -> context.compute(request));
  }

  /**
   * Handler gather request
   * @param request: gather request
   * @return future with request response
   */
  private CompletableFuture<GatherResponse> gather(GatherRequest request) {
    return service(request).thenCompose(context -> context.gather(request));
  }

  /**
   * Handler close request
   * @param request: close request
   * @return future with request response
   */
  private CompletableFuture<CloseResponse> close(CloseRequest request) {
    return service(request).thenCompose(context -> context.close(request));
  }

  /**
   * Handler restore request
   *
   * @param request: restore request
   * @return future with restore response
   */
  private CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    return service(request).thenCompose(context -> context.restore(request));
  }
}

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
package io.atomix.protocols.multicast.role;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.multicast.exception.GenericMulticastError;
import io.atomix.protocols.multicast.impl.GenericMulticastSession;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.RestoreRequest;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GenericMulticastResponse;
import io.atomix.protocols.multicast.protocol.message.response.RestoreResponse;
import io.atomix.protocols.multicast.role.conflict.GenericMulticastConflictHandler;
import io.atomix.protocols.multicast.service.GenericMulticastServiceContext;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractGenericMulticastRole implements GenericMulticastRole {
  protected final Logger log;
  final GenericMulticastConflictHandler conflictHandler;
  final GenericMulticastServiceContext serviceContext;
  final Issuer issuer;
  private boolean running;

  AbstractGenericMulticastRole(GenericMulticastServiceContext serviceContext, GenericMulticastConflictHandler handler) {
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(getClass())
        .addValue(serviceContext.serviceName())
        .build());
    this.serviceContext = serviceContext;
    this.conflictHandler = handler;
    GenericMulticastDeliverSequence sequence = new GenericMulticastDeliverSequence(handler, log, serviceContext);
    this.issuer = new BatchedIssuer(serviceContext, sequence, log);
    this.running = false;
  }

  @Override
  public CompletableFuture<RestoreResponse> onRestore(RestoreRequest request) {
    if (request.term() != serviceContext.term()) {
      // term changed while restoring, not up-to-date anymore
      return Futures.completedFuture(RestoreResponse.builder()
          .withError(GenericMulticastError.Error.COMMAND_FAILURE)
          .withStatus(GenericMulticastResponse.Status.ERROR)
          .withTs(0)
          .withIdentifier(UUID.randomUUID())
          .build());
    }

    HeapBuffer buffer = HeapBuffer.allocate();
    try {
      Collection<GenericMulticastSession> sessions = serviceContext.sessionManager().values();
      buffer.writeInt(sessions.size());
      buffer.writeLong(serviceContext.currentIndex());
      for (Session session: sessions) {
        buffer.writeLong(session.sessionId().id());
        buffer.writeString(session.memberId().id());
      }

      serviceContext.primitiveService().backup(new DefaultBackupOutput(buffer, serviceContext.primitiveService().serializer()));
      buffer.flip();
      byte[] bytes = buffer.readBytes(buffer.remaining());
      return Futures.completedFuture(RestoreResponse.builder()
          .withIdentifier(UUID.randomUUID())
          .withTs(System.currentTimeMillis())
          .withStatus(GenericMulticastResponse.Status.OK)
          .withResult(bytes)
          .build());
    } finally {
      log.info("Restore finished answering");
      buffer.release();
    }
  }

  @Override
  public <T extends OperationResponse> CompletableFuture<T> commit(OperationRequest req, T res, OperationResponse.Builder<?, T> builder, long timestamp, long index) {
    CompletableFuture<T> future = new CompletableFuture<>();
    OperationResponse.Builder<?, T> build = preBuild(builder, res);
    Session session = serviceContext.sessionManager().getOrCreate(req.session(), req.memberId());
    serviceContext.threadContext().execute(() -> future.complete(apply(build, new DefaultCommit<>(
        serviceContext.setIndex(index),
        req.operation().id(),
        req.operation().value(),
        serviceContext.setSession(session),
        serviceContext.setTimestamp(timestamp)
    ))));
    return future;
  }

  ExecuteResponse queryAndCommit(ExecuteRequest request) {
    OperationResponse.Builder<?, ExecuteResponse> builder = ExecuteResponse.builder()
        .withTs(request.sequence())
        .withIdentifier(request.identifier())
        .withStatus(GenericMulticastResponse.Status.OK);
    Session session = serviceContext.sessionManager().getOrCreate(request.session(), request.memberId());
    return apply(builder, new DefaultCommit<>(
        serviceContext.getIndex(),
        request.operation().id(),
        request.operation().value(),
        serviceContext.setSession(session),
        serviceContext.currentTimestamp()));
  }

  private <T extends OperationResponse> T apply(OperationResponse.Builder<?, T> builder, DefaultCommit<byte[]> commit) {
    try {
      byte[] bytes = serviceContext.primitiveService().apply(commit);
      return builder.withResult(bytes)
          .build();
    } catch (Exception e) {
      return builder
          .withStatus(GenericMulticastResponse.Status.ERROR)
          .withError(GenericMulticastError.Error.PROTOCOL_ERROR)
          .build();
    }
  }

  private <T extends OperationResponse> OperationResponse.Builder<?, T> preBuild(OperationResponse.Builder<?, T> builder, OperationResponse res) {
    return builder
        .withTs(res.ts())
        .withResult(res.result())
        .withStatus(res.status())
        .withIdentifier(res.identifier())
        .withError(res.error());
  }

  private <T extends OperationRequest, U extends OperationRequest.Builder<U, T>> OperationRequest.Builder<U, T> preBuild(OperationRequest.Builder<U, T> builder, OperationRequest request, Session session) {
    return builder
        .withIdentifier(request.identifier())
        .withMemberId(request.memberId())
        .withOperation(request.operation())
        .withSequence(request.sequence())
        .withState(request.state())
        .withDescriptor(request.descriptor())
        .withSession(session.sessionId().id());
  }

  protected <T extends OperationRequest> T build(OperationRequest.Builder<?, T> builder, OperationRequest request, Session session) {
    return preBuild(builder, request, session).build();
  }

  protected <T extends OperationResponse> T build(OperationResponse.Builder<?, T> builder, OperationResponse res) {
    return preBuild(builder, res).build();
  }

  protected Session session(OperationRequest request) {
    Session session = serviceContext.sessionManager().get(request.session());
    if (session == null) {
      serviceContext.sessionManager().create(request.session(), request.memberId());
      for (MemberId participant: serviceContext.participants()) {
        serviceContext.sessionManager().getOrCreate(request.session(), participant);
      }
    }
    return session;
  }

  @Override
  public CompletableFuture<GenericMulticastRole> start() {
    running = true;
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public CompletableFuture<Void> stop() {
    running = false;
    issuer.close();
    return CompletableFuture.completedFuture(null);
  }
}

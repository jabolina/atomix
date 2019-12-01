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
package io.atomix.protocols.multicast.session.impl;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.protocols.multicast.exception.GenericMulticastError;
import io.atomix.protocols.multicast.exception.GenericMulticastException;
import io.atomix.protocols.multicast.protocol.GenericMulticastClientProtocol;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GenericMulticastResponse;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class GenericMulticastSessionInvoker {
  private static final int[] FIBONACCI = new int[]{1, 1, 2, 3, 5};
  private static final Predicate<Throwable> EXCEPTION_PREDICATE = e ->
      e instanceof ConnectException
          || e instanceof TimeoutException
          || e instanceof ClosedChannelException;
  private static final Predicate<Throwable> EXPIRED_PREDICATE = e ->
      e instanceof GenericMulticastException.UnknownClient
          || e instanceof GenericMulticastException.UnknownSession;
  private static final Predicate<Throwable> CLOSED_PREDICATE = e ->
      e instanceof GenericMulticastException.ClosedSession
          || e instanceof GenericMulticastException.UnknownService;
  private final GenericMulticastSessionState sessionState;
  private final GenericMulticastSessionConnection sessionConnection;
  private final ThreadContext context;
  private final Logger log;
  private final Map<UUID, OperationAttempt> attempts = new LinkedHashMap<>();

  public GenericMulticastSessionInvoker(
      GenericMulticastSessionState sessionState,
      GenericMulticastSessionConnection sessionConnection,
      ThreadContext context,
      LoggerContext loggerContext) {
    this.sessionState = sessionState;
    this.sessionConnection = sessionConnection;
    this.context = context;
    this.log = ContextualLoggerFactory.getLogger(getClass(), loggerContext);
  }


  /**
   * Returns the generic multicast client protocol
   *
   * @return The generic multicast client protocol
   */
  public GenericMulticastClientProtocol protocol() {
    return sessionConnection.protocol();
  }

  /**
   * Returns the thread context
   *
   * @return The thread context
   */
  public ThreadContext context() {
    return context;
  }

  /**
   * Submits a operation to the cluster.
   *
   * @param operation The operation to submit.
   * @return A completable future to be completed once the command has been submitted.
   */
  public CompletableFuture<byte[]> invoke(PrimitiveOperation operation) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    if (OperationType.COMMAND.equals(operation.id().type())
        || OperationType.QUERY.equals(operation.id().type())) {
      execute(operation, future);
    } else {
      future.completeExceptionally(new GenericMulticastException.ApplicationException("Operation not known"));
    }

    return future;
  }

  private void execute(PrimitiveOperation operation, CompletableFuture<byte[]> future) {
    execute(ExecuteRequest.builder()
        .withMemberId(sessionConnection.membershipService().getLocalMember().id())
        .withIdentifier(UUID.randomUUID())
        .withSession(sessionState.sessionId().id())
        .withSequence(sessionState.nextCommandRequest())
        .withDescriptor(sessionState.descriptor())
        .withOperation(operation)
        .withState(OperationRequest.State.S0)
        .build(),
        future);
  }

  private void execute(ExecuteRequest request, CompletableFuture<byte[]> future) {
    invoke(new ExecuteAttempt(request.identifier(), request, future));
  }

  /**
   * Submits an operation attempt.
   *
   * @param operation The attempt to submit.
   */
  private <T extends OperationRequest, U extends OperationResponse> void invoke(OperationAttempt<T, U> operation) {
    if (PrimitiveState.CLOSED.equals(sessionState.state())) {
      operation.fail(new GenericMulticastException.ClosedSession("Session already closed"));
    } else {
      attempts.put(operation.identifier, operation);
      operation.send();
      operation.future.whenComplete((res, err) -> attempts.remove(operation.identifier));
    }
  }

  private void resubmit(OperationAttempt<?, ?> attempt) {
    log.trace("Retrying operation {}", attempt);
    attempt.retry();
  }

  /**
   * Operation attempt.
   */
  private abstract class OperationAttempt<T extends OperationRequest, U extends OperationResponse> implements BiConsumer<U, Throwable> {
    protected final UUID identifier;
    protected final int attempt;
    protected final T request;
    protected final CompletableFuture<byte[]> future;

    OperationAttempt(UUID identifier, int attempt, T request, CompletableFuture<byte[]> future) {
      this.identifier = identifier;
      this.attempt = attempt;
      this.request = request;
      this.future = future;
    }

    /**
     * Sends the attempt.
     */
    protected abstract void send();

    /**
     * Returns the next instance of the attempt.
     *
     * @return The next instance of the attempt.
     */
    protected abstract OperationAttempt<T, U> next();

    /**
     * Returns a new instance of the default exception for the operation.
     *
     * @return A default exception for the operation.
     */
    protected abstract Throwable exception();

    /**
     * Completes the operation successfully.
     *
     * @param response The operation response.
     */
    protected abstract void complete(U response);

    /**
     * Completes the operation with an exception.
     *
     * @param error The completion exception.
     */
    protected void complete(Throwable error) {
      fail(error);
    }

    /**
     * Fails the attempt.
     */
    public void fail() {
      fail(exception());
    }

    /**
     * Fails the attempt with the given exception.
     *
     * @param t The exception with which to fail the attempt.
     */
    public void fail(Throwable t) {
      if (EXPIRED_PREDICATE.test(t)) {
        sessionState.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(t)) {
        sessionState.setState(PrimitiveState.CLOSED);
      }
    }

    /**
     * Immediately retries the attempt.
     */
    public void retry() {
      if (context.isCurrentContext()) {
        invoke(next());
      } else {
        context.execute(() -> invoke(next()));
      }
    }

    /**
     * Retries the attempt after the given duration.
     *
     * @param delay The duration after which to retry the attempt.
     */
    public void retry(Duration delay) {
      context.schedule(delay, () -> invoke(next()));
    }
  }

  /**
   * Command attempt
   */
  private final class ExecuteAttempt extends OperationAttempt<ExecuteRequest, ExecuteResponse> {
    ExecuteAttempt(UUID uuid, ExecuteRequest request, CompletableFuture<byte[]> future) {
      super(uuid, 1, request, future);
    }

    ExecuteAttempt(UUID uuid, int attempt, ExecuteRequest request, CompletableFuture<byte[]> future) {
      super(uuid, attempt, request, future);
    }

    @Override
    protected void send() {
      sessionConnection.execute(request).whenCompleteAsync(this, context);
    }

    @Override
    protected OperationAttempt<ExecuteRequest, ExecuteResponse> next() {
      return new ExecuteAttempt(identifier, this.attempt + 1, request, future);
    }

    @Override
    protected Throwable exception() {
      return new PrimitiveException.CommandFailure("Failed to complete command!");
    }

    @Override
    protected void complete(ExecuteResponse response) {
      log.trace("Final delivering [{}]", response);
      future.complete(response.result());
    }

    @Override
    public void accept(ExecuteResponse response, Throwable throwable) {
      if (throwable == null) {
        if (GenericMulticastResponse.Status.OK.equals(response.status())) {
          complete(response);
        }
        // Some problem occurred while issuing the command, can retry now
        else if (GenericMulticastError.Error.COMMAND_FAILURE.equals(response.error().error())) {
          resubmit(this);
        }
        // Client of session does not exists anymore to issue commands, so session is expired
        else if (GenericMulticastError.Error.UNKNOWN_CLIENT.equals(response.error().error())
            || GenericMulticastError.Error.UNKNOWN_SESSION.equals(response.error().error())) {
          complete(response.error().createException());
          sessionState.setState(PrimitiveState.EXPIRED);
        }
        // The service is unknown by the session or the session is already closed
        else if (GenericMulticastError.Error.UNKNOWN_SERVICE.equals(response.error().error())
            || GenericMulticastError.Error.CLOSED_SESSION.equals(response.error().error())) {
          complete(response.error().createException());
          sessionState.setState(PrimitiveState.CLOSED);
        } else {
          retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
        }
      } else if (EXCEPTION_PREDICATE.test(throwable) || (throwable instanceof CompletionException && EXCEPTION_PREDICATE.test(throwable.getCause()))) {
        sessionConnection.reset(sessionConnection.members());
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        fail(throwable);
      }
    }
  }
}

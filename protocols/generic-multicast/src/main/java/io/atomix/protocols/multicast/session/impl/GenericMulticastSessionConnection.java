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

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.protocols.multicast.exception.GenericMulticastError;
import io.atomix.protocols.multicast.member.GenericMulticastMemberSelector;
import io.atomix.protocols.multicast.member.MemberSelector;
import io.atomix.protocols.multicast.member.SessionConnectionSelector;
import io.atomix.protocols.multicast.protocol.GenericMulticastClientProtocol;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GenericMulticastRequest;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GenericMulticastResponse;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 */
public final class GenericMulticastSessionConnection {
  private static final Predicate<GenericMulticastResponse> COMPLETE_PREDICATE = res ->
      GenericMulticastResponse.Status.OK.equals(res.status())
          || GenericMulticastError.Error.COMMAND_FAILURE.equals(res.error().error())
          || GenericMulticastError.Error.APPLICATION_ERROR.equals(res.error().error())
          || GenericMulticastError.Error.UNKNOWN_CLIENT.equals(res.error().error())
          || GenericMulticastError.Error.UNKNOWN_SESSION.equals(res.error().error())
          || GenericMulticastError.Error.UNKNOWN_SERVICE.equals(res.error().error())
          || GenericMulticastError.Error.PROTOCOL_ERROR.equals(res.error().error());
  private final GenericMulticastClientProtocol protocol;
  private final MemberSelector selector;
  private final ThreadContext threadContext;
  private final Logger log;
  private MemberId currentNode;
  private int selectionId;
  private final ClusterMembershipService membershipService;

  public GenericMulticastSessionConnection(
      GenericMulticastClientProtocol protocol,
      ClusterMembershipService clusterMembershipService,
      ThreadContext context,
      LoggerContext loggerContext) {
    this.protocol = protocol;
    this.selector = new GenericMulticastMemberSelector(
        clusterMembershipService.getMembers().stream()
            .map(Member::id)
            .collect(Collectors.toList()),
        SessionConnectionSelector.ANY
    );
    this.threadContext = context;
    this.membershipService = clusterMembershipService;
    this.log = ContextualLoggerFactory.getLogger(getClass(), loggerContext);
  }

  public ClusterMembershipService membershipService() {
    return this.membershipService;
  }

  /**
   * Get the session client protocol
   *
   * @return the session client protocol
   */
  public GenericMulticastClientProtocol protocol() {
    return protocol;
  }

  /**
   * Resets the member selector.
   */
  public void reset() {
    selector.reset();
  }

  /**
   * Resets the member selector.
   *
   * @param servers the selector servers
   */
  public void reset(Collection<MemberId> servers) {
    selector.reset(servers);
  }

  /**
   * Returns the current selector leader.
   *
   * @return The current selector leader.
   */
  public MemberId node() {
    return selector.current();
  }

  /**
   * Returns the current set of members.
   *
   * @return The current set of members.
   */
  public Collection<MemberId> members() {
    return selector.members();
  }

  /**
   * Connects to the cluster.
   */
  private MemberId next() {
    // If a connection was already established then use that connection.
    if (currentNode != null) {
      return currentNode;
    }

    if (!selector.hasNext()) {
      log.debug("Failed to connect to the cluster");
      selector.reset();
      return null;
    }

    this.currentNode = selector.next();
    this.selectionId++;
    return currentNode;
  }

  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    CompletableFuture<ExecuteResponse> future = new CompletableFuture<>();
    if (threadContext.isCurrentContext()) {
      invoke(request, protocol::execute, future);
    } else {
      threadContext.execute(() -> invoke(request, protocol::execute, future));
    }

    return future;
  }

  /**
   * Sends the given request attempt to the cluster.
   */
  private <T extends GenericMulticastRequest, U extends GenericMulticastResponse> void invoke(T request, BiFunction<MemberId, T, CompletableFuture<U>> sender, CompletableFuture<U> future) {
    invoke(request, sender, future, 0);
  }

  /**
   * Sends the given request attempt to the cluster.
   */
  private <T extends GenericMulticastRequest, U extends GenericMulticastResponse> void invoke(T request, BiFunction<MemberId, T, CompletableFuture<U>> sender, CompletableFuture<U> future, int attempt) {
    MemberId node = next();
    if (node != null) {
      log.trace("Sending {} to {}", request, node);
      int selectedAt = selectionId;
      sender.apply(node, request).whenCompleteAsync((res, err) -> {
        if (err != null || res != null) {
          response(request, res, err, sender, future, attempt, selectedAt);
        } else {
          future.complete(null);
        }
      });
    } else {
      future.completeExceptionally(new ConnectException("Could not connect to cluster"));
    }
  }

  /**
   * Handles a response from the cluster.
   */
  private <T extends GenericMulticastRequest, U extends GenericMulticastResponse> void response(T request, U response, Throwable error, BiFunction<MemberId, T, CompletableFuture<U>> sender, CompletableFuture<U> future, int attempt, long selectedAt) {
    if (error == null) {
      log.trace("Treating response [{}] for [{}]", response, request);
      if (COMPLETE_PREDICATE.test(response)) {
        future.complete(response);
        selector.reset();
      } else {
        retry(request, response.error().createException(), sender, future, attempt + 1, selectedAt);
      }
    } else {
      if (error instanceof CompletionException) {
        error = error.getCause();
      }

      log.debug("{} failed. Reason:", request, error);

      if (error instanceof SocketException || error instanceof TimeoutException || error instanceof ClosedChannelException) {
        if (attempt < selector.members().size() + 1) {
          retry(request, error, sender, future, attempt + 1, selectedAt);
        } else {
          future.completeExceptionally(error);
        }
      } else {
        future.completeExceptionally(error);
      }
    }
  }

  /**
   * Retry a request due to a request failure, resetting the connection if necessary.
   */
  private <T extends GenericMulticastRequest, U extends GenericMulticastResponse> void retry(T request, Throwable error, BiFunction<MemberId, T, CompletableFuture<U>> sender, CompletableFuture<U> future, int attempt, long selectedAt) {
    if (this.selectionId == selectedAt) {
      log.trace("Resetting connections selector. Cause:", error);
      this.currentNode = null;
    }

    invoke(request, sender, future, attempt);
  }
}

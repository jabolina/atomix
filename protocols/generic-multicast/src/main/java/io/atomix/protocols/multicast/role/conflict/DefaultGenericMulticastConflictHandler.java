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
package io.atomix.protocols.multicast.role.conflict;

import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Default conflict handler to find relationship between messages.
 * <p>
 * All messages sent will go through here, to verify and build the message
 * conflict relationship between them. This will try to work something like the
 * Java {@link java.util.concurrent.ConcurrentHashMap} implementation. Will exists
 * {@link DefaultGenericMulticastConflictHandler#SEGMENTS} buckets, when a request arrive,
 * it will be hashed by {@link OperationRequest#hashCode()} and then will be found to which
 * segment this request belongs to.
 * <p>
 * After that, the list of previous requests will be traversed and will be found which
 * previous requests belongs to the same bucket, and then will be verified if the requests
 * in the same bucket conflicts.
 *
 */
public class DefaultGenericMulticastConflictHandler implements GenericMulticastConflictHandler {
  private static final int SEGMENTS = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 32), 4);
  private static final int SEGMENT_MASK;
  private static final int SEGMENT_SHIFT;

  static {
    int sshift = 0;
    int ssize = 1;
    while (ssize < SEGMENTS) {
      ++sshift;
      ssize <<= 1;
    }

    SEGMENT_MASK  = ssize - 1;
    SEGMENT_SHIFT = 32 - sshift;
  }

  @Override
  public boolean conflict(OperationRequest request, Collection<OperationRequest> previousSet) {
    if (request == null || previousSet.isEmpty()) {
      return false;
    }

    if (previousSet.size() == 1 && previousSet.iterator().next().identifier().equals(request.identifier())) {
      return false;
    }

    long h = hash(request);
    return build(previousSet).filter(entry -> entry.hash == h)
        .anyMatch(entry -> operationConflicts(entry.operation.id().type(), request.operation().id().type()));
  }

  /**
   * Create an entry for every item on the previous set,
   * this entry will contain the object hash and the type of operation
   *
   * @param requests: requests on previous set
   * @return stream of created entry objects
   */
  private Stream<Entry> build(Collection<OperationRequest> requests) {
    return requests.stream().map(r -> new Entry(hash(r), r.operation()));
  }

  /**
   * Create a hash code from the request. This hash function was
   * retrieved from the hash function available at the ConcurrentHashMap
   * {@see <a href="https://github.com/openjdk/jdk14/blob/master/src/java.base/share/classes/java/util/concurrent/ConcurrentHashMap.java">in GitHub</a>}
   *
   * @param request: request to be hashed
   * @return the hash code from the request applying
   */
  private long hash(OperationRequest request) {
    long h = request.hashCode();
    return h >>> SEGMENT_SHIFT & SEGMENT_MASK;
  }

  /**
   * Verify if the type of operations {@link OperationType} conflicts.
   * <p>
   * If both requests are for reading, there will be no conflict and there is no need
   * to order the requests. Any other thing different from this, must be ordered, since
   * concurrent reads and writes needs to be performed in the right order.
   *
   * @param operationA: operation type to be verified
   * @param operationB: operation type to be verified
   * @return true if operation conflicts, false otherwise
   */
  private boolean operationConflicts(OperationType operationA, OperationType operationB) {
    return !OperationType.QUERY.equals(operationA) || !OperationType.QUERY.equals(operationB);
  }

  /**
   * Entry class, to holds the information about the request hash
   * and the type of operation.
   */
  private static final class Entry {
    protected final long hash;
    protected final PrimitiveOperation operation;

    private Entry(long hash, PrimitiveOperation operation) {
      this.hash = hash;
      this.operation = operation;
    }
  }
}

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
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;

import java.util.Collection;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Conflict handler that uses a universal hash function to verify conflict relationship.
 * <p>
 * First will hash the {@link PrimitiveDescriptor#name()} name, since messages for different
 * primitives will not have any relationship. After hashing the primitive name, will verify the
 * relationship for the requests with the same primitive as destination.
 */
public class UniversalHashConflictHandler implements GenericMulticastConflictHandler {
  private static final Random GENERATOR = new Random(System.currentTimeMillis()) {
    @Override
    public int nextInt() {
      return nextInt((Integer.MAX_VALUE / 2) - 1);
    }

    @Override
    public int nextInt(int bound) {
      return Math.abs(super.nextInt(bound));
    }
  };
  private static final int SIZE = GENERATOR.nextInt();
  private static final int P;
  private static final int A;
  private static final int B;

  static {
    int possiblePrime = GENERATOR.nextInt(2 * SIZE);
    while (possiblePrime < SIZE || !isPrime(possiblePrime)) {
      possiblePrime = GENERATOR.nextInt(2 * SIZE);
    }

    P = possiblePrime;
    A = GENERATOR.nextInt(P);
    int possibleB = GENERATOR.nextInt(P);
    while (possibleB == A) {
      possibleB = GENERATOR.nextInt(P);
    }
    B = possibleB;
  }

  @Override
  public boolean conflict(OperationRequest request, Collection<OperationRequest> previousSet) {
    if (request == null || previousSet.isEmpty()) {
      return false;
    }

    int h = hash(request);
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

  private int hash(OperationRequest request) {
    int hashCode = request.descriptor().name().hashCode();
    return ((A * hashCode + B) % P) % SIZE;
  }

  /**
   * Verify if a given number is prime. Forcing that 2 and 3 are not evaluated as primes.
   *
   * @param prime: number to verified
   * @return true if is prime, false otherwise
   */
  private static boolean isPrime(int prime) {
    if (prime <= 3 || prime % 2 == 0) {
      // This is forcing to not use small prime numbers
      return false;
    }
    int divisor = 3;
    while ((divisor <= Math.sqrt(prime)) && (prime % divisor != 0)) {
      divisor += 2;
    }

    return prime % divisor != 0;
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
    protected final int hash;
    protected final PrimitiveOperation operation;

    private Entry(int hash, PrimitiveOperation operation) {
      this.hash = hash;
      this.operation = operation;
    }
  }
}

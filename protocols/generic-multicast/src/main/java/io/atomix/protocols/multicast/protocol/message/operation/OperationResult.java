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
package io.atomix.protocols.multicast.protocol.message.operation;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation result
 */
public final class OperationResult {
  private final long id;
  private final byte[] result;
  private final Throwable error;

  public OperationResult(long id, byte[] result, Throwable error) {
    this.id = id;
    this.result = result;
    this.error = error;
  }

  public static OperationResult success(long id, byte[] result) {
    return new OperationResult(id, result, null);
  }

  public static OperationResult failed(long id, Throwable error) {
    return new OperationResult(id, null, error);
  }

  /**
   * Returns the result id.
   *
   * @return The result id.
   */
  public long id() {
    return id;
  }

  /**
   * Returns the result value.
   *
   * @return The result value.
   */
  public byte[] result() {
    return result;
  }

  /**
   * Returns the operation error.
   *
   * @return the operation error
   */
  public Throwable error() {
    return error;
  }

  /**
   * Returns whether the operation succeeded.
   *
   * @return whether the operation succeeded
   */
  public boolean success() {
    return error == null;
  }

  /**
   * Returns whether the operation failed.
   *
   * @return whether the operation failed
   */
  public boolean failed() {
    return !success();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("error", error)
        .add("result", ArraySizeHashPrinter.of(result))
        .toString();
  }
}

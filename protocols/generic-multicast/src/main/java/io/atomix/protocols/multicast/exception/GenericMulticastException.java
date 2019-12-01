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
package io.atomix.protocols.multicast.exception;

import java.util.Objects;

/**
 * Base generic multicast protocol exception.
 * <p>
 * This is the base exception type for all generic multicast protocol exceptions. Protocol exceptions must be
 * associated with a {@link GenericMulticastError.Error} which is used for more efficient serialization.
 */
public class GenericMulticastException extends RuntimeException {
  private static final String ERROR_NPE_MESSAGE = "Error type cannot be null!";
  private final GenericMulticastError.Error error;

  protected GenericMulticastException(GenericMulticastError.Error error, String message, Object ... args) {
    super(message != null ? String.format(message, args) : null);
    this.error = Objects.requireNonNull(error, ERROR_NPE_MESSAGE);
  }

  protected GenericMulticastException(GenericMulticastError.Error error, Throwable cause, String message, Object ... args) {
    super(String.format(message, args), cause);
    this.error = Objects.requireNonNull(error, ERROR_NPE_MESSAGE);
  }

  protected GenericMulticastException(GenericMulticastError.Error error, Throwable cause) {
    super(cause);
    this.error = Objects.requireNonNull(error, ERROR_NPE_MESSAGE);
  }

  /**
   * Returns the exception type
   *
   * @return The exception type
   */
  public GenericMulticastError.Error error() {
    return error;
  }

  public abstract static class OperationFailure extends GenericMulticastException {
    public OperationFailure(GenericMulticastError.Error type, String message, Object... args) {
      super(type, message, args);
    }
  }

  public static class CommandFailure extends GenericMulticastException {
    public CommandFailure(String message, Object ... args) {
      super(GenericMulticastError.Error.COMMAND_FAILURE, message, args);
    }
  }

  public static class IllegalMemberState extends GenericMulticastException {
    public IllegalMemberState(String message, Object... args) {
      super(GenericMulticastError.Error.ILLEGAL_MEMBER_STATE, message, args);
    }
  }

  public static class ApplicationException extends GenericMulticastException {
    public ApplicationException(String message, Object... args) {
      super(GenericMulticastError.Error.APPLICATION_ERROR, message, args);
    }

    public ApplicationException(Throwable cause) {
      super(GenericMulticastError.Error.APPLICATION_ERROR, cause);
    }
  }

  public static class ProtocolError extends GenericMulticastException {
    public ProtocolError(String message, Object ... args) {
      super(GenericMulticastError.Error.PROTOCOL_ERROR, message, args);
    }
  }

  public static class UnknownClient extends GenericMulticastException {
    public UnknownClient(String message, Object... args) {
      super(GenericMulticastError.Error.UNKNOWN_CLIENT, message, args);
    }
  }

  public static class UnknownSession extends GenericMulticastException {
    public UnknownSession(String message, Object... args) {
      super(GenericMulticastError.Error.UNKNOWN_SESSION, message, args);
    }
  }

  public static class UnknownService extends GenericMulticastException {
    public UnknownService(String message, Object... args) {
      super(GenericMulticastError.Error.UNKNOWN_SERVICE, message, args);
    }
  }

  public static class ClosedSession extends GenericMulticastException {
    public ClosedSession(String message, Object... args) {
      super(GenericMulticastError.Error.CLOSED_SESSION, message, args);
    }
  }

  public static class Unavailable extends GenericMulticastException {
    public Unavailable(String message, Object ... args) {
      super(GenericMulticastError.Error.UNAVAILABLE, message, args);
    }
  }
}

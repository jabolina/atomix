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

import io.atomix.primitive.PrimitiveException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base type for generic multicast error
 */
public final class GenericMulticastError {
  private final Error error;
  private final String message;

  public GenericMulticastError(Error error, String message) {
    this.error = checkNotNull(error, "type cannot be null");
    this.message = message;
  }

  public Error error() {
    return error;
  }

  public String message() {
    return message;
  }

  public PrimitiveException createException() {
    return error.createException(message);
  }

  public enum Error {
    /**
     * Error when executing command
     */
    COMMAND_FAILURE {
      @Override
      PrimitiveException createException() {
        return createException("Failed to execute command");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.CommandFailure(message) : createException();
      }
    },

    /**
     * Internal protocol error
     */
    PROTOCOL_ERROR {
      @Override
      PrimitiveException createException() {
        return createException("Failed to execute protocol");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.ServiceException(message) : createException();
      }
    },

    /**
     * Service is unavailable error
     */
    UNAVAILABLE {
      @Override
      PrimitiveException createException() {
        return createException("Service is unavailable");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.Unavailable(message) : createException();
      }
    },

    /**
     * Unknown client error.
     */
    UNKNOWN_CLIENT {
      @Override
      PrimitiveException createException() {
        return createException("Unknown client");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.UnknownClient(message) : createException();
      }
    },

    /**
     * Unknown session error.
     */
    UNKNOWN_SESSION {
      @Override
      PrimitiveException createException() {
        return createException("Unknown member session");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.UnknownSession(message) : createException();
      }
    },

    /**
     * Unknown state machine error.
     */
    UNKNOWN_SERVICE {
      @Override
      PrimitiveException createException() {
        return createException("Unknown primitive service");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.UnknownService(message) : createException();
      }
    },

    /**
     * Closed session error.
     */
    CLOSED_SESSION {
      @Override
      PrimitiveException createException() {
        return createException("Closed session");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.ClosedSession(message) : createException();
      }
    },

    /**
     * User application error.
     */
    APPLICATION_ERROR {
      @Override
      PrimitiveException createException() {
        return createException("An application error occurred");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.ServiceException(message) : createException();
      }
    },

    /**
     * Illegal member state error.
     */
    ILLEGAL_MEMBER_STATE {
      @Override
      PrimitiveException createException() {
        return createException("Illegal member state");
      }

      @Override
      PrimitiveException createException(String message) {
        return message != null ? new PrimitiveException.Unavailable(message) : createException();
      }
    };

    /**
     * Creates an exception with a default message.
     *
     * @return the exception
     */
    abstract PrimitiveException createException();

    /**
     * Creates an exception with the given message.
     *
     * @param message the exception message
     * @return the exception
     */
    abstract PrimitiveException createException(String message);
  }
}

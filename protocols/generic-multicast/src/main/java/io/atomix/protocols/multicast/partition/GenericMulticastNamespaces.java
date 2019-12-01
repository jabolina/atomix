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
package io.atomix.protocols.multicast.partition;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.Replication;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.event.impl.DefaultEventType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionMetadata;
import io.atomix.protocols.multicast.ReadConsistency;
import io.atomix.protocols.multicast.exception.GenericMulticastError;
import io.atomix.protocols.multicast.protocol.PrimitiveDescriptor;
import io.atomix.protocols.multicast.protocol.message.operation.OperationRequest;
import io.atomix.protocols.multicast.protocol.message.operation.OperationResponse;
import io.atomix.protocols.multicast.protocol.message.request.CloseRequest;
import io.atomix.protocols.multicast.protocol.message.request.ComputeRequest;
import io.atomix.protocols.multicast.protocol.message.request.ExecuteRequest;
import io.atomix.protocols.multicast.protocol.message.request.GatherRequest;
import io.atomix.protocols.multicast.protocol.message.response.CloseResponse;
import io.atomix.protocols.multicast.protocol.message.response.ComputeResponse;
import io.atomix.protocols.multicast.protocol.message.response.ExecuteResponse;
import io.atomix.protocols.multicast.protocol.message.response.GatherResponse;
import io.atomix.protocols.multicast.protocol.message.response.GenericMulticastResponse;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

import java.util.Collections;
import java.util.UUID;

public final class GenericMulticastNamespaces {

  public static final Namespace GENERIC_MULTICAST_PROTOCOL = Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(Collections.emptyList().getClass())
      .register(MemberId.class)
      .register(PrimitiveOperation.class)
      .register(SessionId.class)
      .register(SessionMetadata.class)
      .register(CloseRequest.class)
      .register(CloseResponse.class)
      .register(ExecuteRequest.class)
      .register(ExecuteResponse.class)
      .register(GatherRequest.class)
      .register(GatherResponse.class)
      .register(ComputeRequest.class)
      .register(ComputeResponse.class)
      .register(GenericMulticastResponse.Status.class)
      .register(OperationRequest.class)
      .register(OperationResponse.class)
      .register(OperationRequest.State.class)
      .register(PrimitiveDescriptor.class)
      .register(UUID.class)
      .register(ReadConsistency.class)
      .register(OperationId.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(GenericMulticastError.class)
      .register(GenericMulticastError.Error.class)
      .register(PrimitiveEvent.class)
      .register(DefaultEventType.class)
      .register(Replication.class)
      .build("GenericMulticast");
}

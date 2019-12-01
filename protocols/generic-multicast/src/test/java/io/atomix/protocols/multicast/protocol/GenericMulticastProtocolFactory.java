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
import io.atomix.cluster.MemberId;

import java.util.Map;

/**
 * Factory for generic multicast server and client
 */
public final class GenericMulticastProtocolFactory {
  private final Map<MemberId, GenericMulticastServerProtocolTest> servers = Maps.newConcurrentMap();
  private final Map<MemberId, GenericMulticastClientProtocolTest> clients = Maps.newConcurrentMap();

  public GenericMulticastServerProtocolTest newServerProtocol(MemberId memberId) {
    return new GenericMulticastServerProtocolTest(memberId, servers, clients);
  }

  public GenericMulticastClientProtocolTest newClientProtocol(MemberId memberId) {
    return new GenericMulticastClientProtocolTest(memberId, servers, clients);
  }
}

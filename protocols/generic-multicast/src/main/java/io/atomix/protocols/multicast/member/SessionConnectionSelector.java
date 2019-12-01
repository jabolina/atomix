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
package io.atomix.protocols.multicast.member;

import io.atomix.cluster.MemberId;

import java.util.Collections;
import java.util.List;

/**
 * Strategy for selecting nodes to which to connect and submit operations.
 * <p>
 * Selection strategies prioritize communication with certain servers over others. When the client
 * loses its connection or cluster membership changes, the client will request a list of servers to
 * which the client can connect. The address list should be prioritized. At the moment exists only
 * the weightless priority.
 */
public enum SessionConnectionSelector {
  /**
   * Weightless priority, where any member can be selected at any order.
   * The {@code ANY} selection strategy allows the client to connect to any server in the cluster. Clients
   * will attempt to connect to a random server, and the client will persist its connection with the first server
   * through which it is able to communicate. If the client becomes disconnected from a server, it will attempt
   * to connect to the next random server again.
   */
  ANY {
    @Override
    public List<MemberId> order(List<MemberId> members) {
      Collections.shuffle(members);
      return members;
    }
  };

  public abstract List<MemberId> order(List<MemberId> members);
}

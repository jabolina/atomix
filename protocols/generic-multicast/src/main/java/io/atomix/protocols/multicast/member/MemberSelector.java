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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public interface MemberSelector extends Iterator<MemberId>, AutoCloseable {

  /**
   * Returns the address selector state.
   *
   * @return The address selector state.
   */
  ConnectionSelectionState state();

  /**
   * Returns the current address selection.
   *
   * @return The current address selection.
   */
  MemberId current();

  /**
   * Returns the current set of servers.
   *
   * @return The current set of servers.
   */
  Set<MemberId> members();

  /**
   * Resets the member iterator.
   *
   * @param members The collection of members.
   * @return The member selector.
   */
  MemberSelector reset(Collection<MemberId> members);

  /**
   * Resets the member iterator.
   *
   * @return The member selector.
   */
  MemberSelector reset();
}

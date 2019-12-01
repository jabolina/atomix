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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.atomix.cluster.MemberId;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public class GenericMulticastMemberSelector implements MemberSelector {
  private final SessionConnectionSelector selector;
  private volatile MemberId selected;
  private Set<MemberId> members;
  private Collection<MemberId> selecteds;
  private Iterator<MemberId> memberIdIterator;

  public GenericMulticastMemberSelector(Collection<MemberId> members, SessionConnectionSelector selector) {
    this.members = new LinkedHashSet<>(members);
    this.selecteds = selector.order(Lists.newLinkedList(members));
    this.selector = selector;
  }

  @Override
  public ConnectionSelectionState state() {
    if (memberIdIterator == null) {
      return ConnectionSelectionState.RESET;
    }

    if (hasNext()) {
      return ConnectionSelectionState.ITERATE;
    }

    return ConnectionSelectionState.COMPLETE;
  }

  @Override
  public MemberId current() {
    return selected;
  }

  @Override
  public Set<MemberId> members() {
    return members;
  }

  @Override
  public MemberSelector reset(Collection<MemberId> members) {
    if (!matches(this.members, members)) {
      this.selected = null;
      this.members = Sets.newLinkedHashSet(members);
      this.selecteds = selector.order(Lists.newLinkedList(members));
      this.memberIdIterator = null;
    }

    return this;
  }

  @Override
  public MemberSelector reset() {
    if (memberIdIterator != null) {
      this.selecteds = selector.order(Lists.newLinkedList(members));
      this.memberIdIterator = null;
    }

    return this;
  }

  @Override
  public boolean hasNext() {
    return memberIdIterator == null ? !selecteds.isEmpty() : memberIdIterator.hasNext();
  }

  @Override
  public MemberId next() {
    if (memberIdIterator == null) {
      memberIdIterator = selecteds.iterator();
    }
    this.selected = memberIdIterator.next();
    return this.selected;
  }

  @Override
  public void close() {
    selected = null;
  }

  /**
   * Returns a boolean value indicating whether the servers in the first list match the servers in the second list.
   */
  private boolean matches(Collection<MemberId> left, Collection<MemberId> right) {
    if (left.size() != right.size()) {
      return false;
    }

    for (MemberId address : left) {
      if (!right.contains(address)) {
        return false;
      }
    }
    return true;
  }
}

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
import io.atomix.primitive.partition.Partition;

import java.util.Collection;

public abstract class GenericMulticastPartition implements Partition {

  private volatile int idx = 0;

  private int next(Collection collection) {
    return (idx + 1) % collection.size();
  }

  /**
   * Returns the partition name
   *
   * @return the partition name
   */
  public abstract String name();

  @Override
  public Collection<MemberId> backups() {
    return members();
  }

  @Override
  public MemberId primary() {
    Collection<MemberId> members = members();
    idx = next(members);
    return members.toArray(new MemberId[0])[idx];
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.client.solrj.cloud;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
/**An implementation that fetches the state of each replica in a collection
 * and it also provides the leader of shards
 *
 */
public interface ShardStateProvider {

  Replica.State getState(Replica replica);

  Replica getLeader(Slice slice);

  /**Gete the leader of the slice. Wait for one if there is no leader
   */
  Replica getLeader(Slice slice, int timeout) throws InterruptedException;

  boolean isActive(Replica replica);

  boolean isActive(Slice slice);
}

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
/**An implementation that fetches the state of each replica/slice and leaders of shards in a collection.
 * The state is embedded with other topology information in state.json in the original SolrCLoud design.
 * But, it is possible to have another implementation where the state and leader information can be stored and read
 * from other places.
 *
 */
public interface ShardStateProvider {

  /**Get the state of  a given {@link Replica}
   */
  Replica.State getState(Replica replica);

  /**Get the leader {@link Replica} of the shard
   */
  Replica getLeader(Slice slice);

  /**Get the leader of the slice. Wait for one if there is no leader
   * Throws an {@link InterruptedException} if interrupted in between
   */
  Replica getLeader(Slice slice, int timeout) throws InterruptedException;


  Replica getLeader(String collection, String slice, int timeout) throws InterruptedException;


  /**CHeck if the replica is active
   *
   */
  boolean isActive(Replica replica);

  /**Check if the slice is active
   */
  boolean isActive(Slice slice);
}

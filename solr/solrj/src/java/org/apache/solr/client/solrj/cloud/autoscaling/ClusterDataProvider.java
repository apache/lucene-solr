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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ClusterDataProvider extends Closeable {
  /**Get the value of each tag for a given node
   *
   * @param node node name
   * @param tags tag names
   * @return a map of tag vs value
   */
  Map<String, Object> getNodeValues(String node, Collection<String> tags);

  /**
   * Get the details of each replica in a node. It attempts to fetch as much details about
   * the replica as mentioned in the keys list. It is not necessary to give al details
   * <p>
   * the format is {collection:shard :[{replicadetails}]}
   */
  Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys);

  Collection<String> getNodes();

  /**Get the collection-specific policy
   */
  String getPolicyNameByCollection(String coll);

  @Override
  default void close() throws IOException {
  }
}

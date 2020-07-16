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

package org.apache.solr.cloud.api.collections.assign;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Initial cluster state for assigner.
 */
public interface AssignerClusterState {
  Set<String> getNodes();
  Set<String> getLiveNodes();
  Collection<String> getCollections();

  default AssignerNodeState getNodeState(String nodeName) {
    return getNodeState(nodeName, Collections.emptySet(), Collections.emptySet());
  }

  AssignerNodeState getNodeState(String nodeName, Collection<String> nodeKeys, Collection<String> replicaKeys);

  AssignerCollectionState getCollectionState(String collectionName, Collection<String> replicaKeys);

  // other cluster properties
  Map<String, Object> getProperties();

}

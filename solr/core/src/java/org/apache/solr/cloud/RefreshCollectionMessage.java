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

package org.apache.solr.cloud;

import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.data.Stat;

/**
 * Refresh the Cluster State for a given collection
 *
 */
public class RefreshCollectionMessage implements Overseer.Message {
  public final String collection;

  public RefreshCollectionMessage(String collection) {
    this.collection = collection;
  }



  @Override
  public ClusterState run(ClusterState clusterState, Overseer overseer, ZkStateWriter zkStateWriter)
      throws Exception {
    Stat stat =
        overseer
            .getZkStateReader()
            .getZkClient()
            .exists(ZkStateReader.getCollectionPath(collection), null, true);
    if (stat == null) {
      // collection does not exist
      return clusterState.copyWith(collection, null);
    }
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll != null && !coll.isModified(stat.getVersion(), stat.getCversion())) {
      // our state is up to date
      return clusterState;
    } else {
      overseer.getZkStateReader().forceUpdateCollection(collection);
      coll = overseer.getZkStateReader().getCollection(collection);

      // During collection creation for a PRS collection, the cluster state (state.json) for the
      // collection is written to ZK directly by the node (that received the CREATE request).
      // Hence, we need the overseer's ZkStateWriter and the overseer's internal copy of the cluster
      // state
      // to be updated to contain that collection via this refresh.

      zkStateWriter.updateClusterState(
          it -> it.copyWith(collection, overseer.getZkStateReader().getCollection(collection)));
      return clusterState.copyWith(collection, coll);
    }
  }
}

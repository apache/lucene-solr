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


import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;

public class DeleteNodeCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public DeleteNodeCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    ocmh.checkRequired(message, "node");
    String node = message.getStr("node");
    if (!state.liveNodesContain(node)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Source Node: " + node + " is not live");
    }
    List<ZkNodeProps> sourceReplicas = ReplaceNodeCmd.getReplicasOfNode(node, state);
    cleanupReplicas(results, state, sourceReplicas, ocmh, node);
  }

  static void cleanupReplicas(NamedList results,
                              ClusterState clusterState,
                              List<ZkNodeProps> sourceReplicas,
                              OverseerCollectionMessageHandler ocmh, String node) throws InterruptedException {
    CountDownLatch cleanupLatch = new CountDownLatch(sourceReplicas.size());
    for (ZkNodeProps sourceReplica : sourceReplicas) {
      log.info("Deleting replica for collection={} shard={} on node={}", sourceReplica.getStr(COLLECTION_PROP), sourceReplica.getStr(SHARD_ID_PROP), node);
      NamedList deleteResult = new NamedList();
      try {
        ((DeleteReplicaCmd)ocmh.commandMap.get(DELETEREPLICA)).deleteReplica(clusterState, sourceReplica.plus("parallel", "true"), deleteResult, () -> {
          cleanupLatch.countDown();
          if (deleteResult.get("failure") != null) {
            synchronized (results) {
              results.add("failure", String.format(Locale.ROOT, "Failed to delete replica for collection=%s shard=%s" +
                  " on node=%s", sourceReplica.getStr(COLLECTION_PROP), sourceReplica.getStr(SHARD_ID_PROP), node));
            }
          }
        });
      } catch (KeeperException e) {
        log.warn("Error deleting ", e);
        cleanupLatch.countDown();
      } catch (Exception e) {
        log.warn("Error deleting ", e);
        cleanupLatch.countDown();
        throw e;
      }
    }
    log.debug("Waiting for delete node action to complete");
    cleanupLatch.await(5, TimeUnit.MINUTES);
  }


}

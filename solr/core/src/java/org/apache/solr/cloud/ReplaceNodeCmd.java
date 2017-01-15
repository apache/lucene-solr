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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class ReplaceNodeCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public ReplaceNodeCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    ocmh.checkRequired(message, "source", "target");
    String source = message.getStr("source");
    String target = message.getStr("target");
    String async = message.getStr("async");
    boolean parallel = message.getBool("parallel", false);
    ClusterState clusterState = zkStateReader.getClusterState();

    if (!clusterState.liveNodesContain(source)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Source Node: " + source + " is not live");
    }
    if (!clusterState.liveNodesContain(target)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Target Node: " + target + " is not live");
    }
    List<ZkNodeProps> sourceReplicas = getReplicasOfNode(source, clusterState);

    List<ZkNodeProps> createdReplicas = new ArrayList<>();

    AtomicBoolean anyOneFailed = new AtomicBoolean(false);
    CountDownLatch countDownLatch = new CountDownLatch(sourceReplicas.size());

    for (ZkNodeProps sourceReplica : sourceReplicas) {
      NamedList nl = new NamedList();
      log.info("Going to create replica for collection={} shard={} on node={}", sourceReplica.getStr(COLLECTION_PROP), sourceReplica.getStr(SHARD_ID_PROP), target);
      ZkNodeProps msg = sourceReplica.plus("parallel", String.valueOf(parallel)).plus(CoreAdminParams.NODE, target);
      if(async!=null) msg.getProperties().put(ASYNC, async);
      final ZkNodeProps addedReplica = ocmh.addReplica(clusterState,
          msg, nl, () -> {
            countDownLatch.countDown();
            if (nl.get("failure") != null) {
              String errorString = String.format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" +
                  " on node=%s", sourceReplica.getStr(COLLECTION_PROP), sourceReplica.getStr(SHARD_ID_PROP), target);
              log.warn(errorString);
              // one replica creation failed. Make the best attempt to
              // delete all the replicas created so far in the target
              // and exit
              synchronized (results) {
                results.add("failure", errorString);
                anyOneFailed.set(true);
              }
            } else {
              log.debug("Successfully created replica for collection={} shard={} on node={}",
                  sourceReplica.getStr(COLLECTION_PROP), sourceReplica.getStr(SHARD_ID_PROP), target);
            }
          });

      if (addedReplica != null) {
        createdReplicas.add(addedReplica);
      }
    }

    log.debug("Waiting for replace node action to complete");
    countDownLatch.await(5, TimeUnit.MINUTES);
    log.debug("Finished waiting for replace node action to complete");

    if (anyOneFailed.get()) {
      log.info("Failed to create some replicas. Cleaning up all replicas on target node");
      CountDownLatch cleanupLatch = new CountDownLatch(createdReplicas.size());
      for (ZkNodeProps createdReplica : createdReplicas) {
        NamedList deleteResult = new NamedList();
        try {
          ocmh.deleteReplica(zkStateReader.getClusterState(), createdReplica.plus("parallel", "true"), deleteResult, () -> {
            cleanupLatch.countDown();
            if (deleteResult.get("failure") != null) {
              synchronized (results) {
                results.add("failure", "Could not cleanup, because of : " + deleteResult.get("failure"));
              }
            }
          });
        } catch (KeeperException e) {
          cleanupLatch.countDown();
          log.warn("Error deleting replica ", e);
        } catch (Exception e) {
          log.warn("Error deleting replica ", e);
          cleanupLatch.countDown();
          throw e;
        }
      }
      cleanupLatch.await(5, TimeUnit.MINUTES);
    }


    // we have reached this far means all replicas could be recreated
    //now cleanup the replicas in the source node
    DeleteNodeCmd.cleanupReplicas(results, state, sourceReplicas, ocmh, source, async);
    results.add("success", "REPLACENODE action completed successfully from  : " + source + " to : " + target);
  }

  static List<ZkNodeProps> getReplicasOfNode(String source, ClusterState state) {
    List<ZkNodeProps> sourceReplicas = new ArrayList<>();
    for (Map.Entry<String, DocCollection> e : state.getCollectionsMap().entrySet()) {
      for (Slice slice : e.getValue().getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (source.equals(replica.getNodeName())) {
            ZkNodeProps props = new ZkNodeProps(
                COLLECTION_PROP, e.getKey(),
                SHARD_ID_PROP, slice.getName(),
                ZkStateReader.CORE_NAME_PROP, replica.getCoreName(),
                ZkStateReader.REPLICA_PROP, replica.getName(),
                CoreAdminParams.NODE, source);
            sourceReplicas.add(props
            );
          }
        }
      }
    }
    return sourceReplicas;
  }
}

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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.AfterClass;

/**
 * Base class for tests that require more than one SolrCloud
 *
 * Derived tests should call {@link #doSetupClusters(String[], Function, BiConsumer)} in a {@code BeforeClass}
 * static method.  This configures and starts the {@link MiniSolrCloudCluster} instances, available
 * via the {@code clusterId2cluster} variable.  The clusters' shutdown is handled automatically.
 */
public abstract class MultiSolrCloudTestCase extends SolrTestCaseJ4 {

  protected static Map<String,MiniSolrCloudCluster> clusterId2cluster = new HashMap<String,MiniSolrCloudCluster>();

  protected static abstract class DefaultClusterCreateFunction implements Function<String,MiniSolrCloudCluster> {

    public DefaultClusterCreateFunction() {
    }

    protected abstract int nodesPerCluster(String clusterId);

    @Override
    public MiniSolrCloudCluster apply(String clusterId) {
      try {
        final MiniSolrCloudCluster cluster = new SolrCloudTestCase
            .Builder(nodesPerCluster(clusterId), createTempDir())
            .addConfig("conf", configset("cloud-dynamic"))
            .build();
        return cluster;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  protected static abstract class DefaultClusterInitFunction implements BiConsumer<String,MiniSolrCloudCluster> {

    final private int numShards;
    final private int numReplicas;
    final private int maxShardsPerNode;

    public DefaultClusterInitFunction(int numShards, int numReplicas, int maxShardsPerNode) {
      this.numShards = numShards;
      this.numReplicas = numReplicas;
      this.maxShardsPerNode = maxShardsPerNode;
    }

    protected void doAccept(String collection, MiniSolrCloudCluster cluster) {
      try {
        CollectionAdminRequest
        .createCollection(collection, "conf", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .processAndWait(cluster.getSolrClient(), SolrCloudTestCase.DEFAULT_TIMEOUT);

        AbstractDistribZkTestBase.waitForRecoveriesToFinish(collection, cluster.getSolrClient().getZkStateReader(), false, true, SolrCloudTestCase.DEFAULT_TIMEOUT);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  protected static void doSetupClusters(final String[] clusterIds,
      final Function<String,MiniSolrCloudCluster> createFunc,
      final BiConsumer<String,MiniSolrCloudCluster> initFunc) throws Exception {

    for (final String clusterId : clusterIds) {
      assertFalse("duplicate clusterId "+clusterId, clusterId2cluster.containsKey(clusterId));
      MiniSolrCloudCluster cluster = createFunc.apply(clusterId);
      initFunc.accept(clusterId, cluster);
      clusterId2cluster.put(clusterId, cluster);
    }
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    for (MiniSolrCloudCluster cluster : clusterId2cluster.values()) {
      cluster.shutdown();
    }
    clusterId2cluster.clear();
  }

}

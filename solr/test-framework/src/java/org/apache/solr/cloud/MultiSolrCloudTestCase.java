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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Base class for tests that require more than one SolrCloud
 *
 * Derived tests should call {@link #doSetupClusters(String[], Function, BiConsumer)} in a {@code BeforeClass}
 * static method.  This configures and starts the {@link MiniSolrCloudCluster} instances, available
 * via the {@code clusterId2cluster} variable.  The clusters' shutdown is handled automatically.
 */
public abstract class MultiSolrCloudTestCase extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected volatile Map<String,MiniSolrCloudCluster> clusterId2cluster;

  protected static abstract class DefaultClusterCreateFunction implements Function<String,MiniSolrCloudCluster> {

    public DefaultClusterCreateFunction() {
    }

    protected abstract int nodesPerCluster(String clusterId);

    @Override
    public MiniSolrCloudCluster apply(String clusterId) {
      try {
        final MiniSolrCloudCluster cluster = new SolrCloudTestCase
            .Builder(nodesPerCluster(clusterId), SolrTestUtil.createTempDir())
            .addConfig("conf", SolrTestUtil.configset("cloud-dynamic"))
            .formatZk(true).build();
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
        .processAsync(cluster.getSolrClient());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  protected void doSetupClusters(final String[] clusterIds,
      final Function<String,MiniSolrCloudCluster> createFunc,
      final BiConsumer<String,MiniSolrCloudCluster> initFunc) throws Exception {

    for (final String clusterId : clusterIds) {
      assertFalse("duplicate clusterId "+clusterId, clusterId2cluster.containsKey(clusterId));
      MiniSolrCloudCluster cluster = createFunc.apply(clusterId);
      initFunc.accept(clusterId, cluster);
      MiniSolrCloudCluster old = clusterId2cluster.put(clusterId, cluster);
      if (old != null) {
        old.shutdown();
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    clusterId2cluster = new ConcurrentHashMap<>();
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    clusterId2cluster.forEach((s, miniSolrCloudCluster) -> {
      try {
        miniSolrCloudCluster.shutdown();
      } catch (Exception e) {
        log.error("", e);
      }
    });
    clusterId2cluster = null;
    super.tearDown();
  }

}

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
package org.apache.solr.handler.component;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.MultiSolrCloudTestCase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShardsWhitelistTest extends MultiSolrCloudTestCase {

  /**
   * The cluster with this key will include an explicit list of host whitelisted (all hosts in both the clusters)
   */
  private static final String EXPLICIT_CLUSTER_KEY = "explicitCluster";
  /**
   * The cluster with this key will not include an explicit list of host whitelisted, will rely on live_nodes
   */
  private static final String IMPLICIT_CLUSTER_KEY = "implicitCluster";
  private static final String EXPLICIT_WHITELIST_PROPERTY = "solr.tests.ShardsWhitelistTest.explicitWhitelist.";
  protected static final String COLLECTION_NAME = "ShardsWhitelistTestCollection";

  private static int numShards;
  private static int numReplicas;
  private static int maxShardsPerNode;
  private static int nodesPerCluster;

  private static void appendClusterNodes(final StringBuilder sb, final String delimiter,
      final MiniSolrCloudCluster cluster) {
    cluster.getJettySolrRunners().forEach((jetty) -> sb.append(jetty.getBaseUrl().toString() + delimiter));
  }

  @BeforeClass
  public static void setupClusters() throws Exception {

    final String[] clusterIds = new String[] {IMPLICIT_CLUSTER_KEY, EXPLICIT_CLUSTER_KEY};

    numShards = 2; // +random().nextInt(2);
    numReplicas = 1; // +random().nextInt(2);
    maxShardsPerNode = 1; // +random().nextInt(2);
    nodesPerCluster = (numShards * numReplicas + (maxShardsPerNode - 1)) / maxShardsPerNode;

    final StringBuilder sb = new StringBuilder();

    doSetupClusters(clusterIds,
        new DefaultClusterCreateFunction() {

          @Override
          public MiniSolrCloudCluster apply(String clusterId) {
            try {
              final MiniSolrCloudCluster cluster = new SolrCloudTestCase.Builder(nodesPerCluster(clusterId),
                  createTempDir())
                      .addConfig("conf", configset("cloud-dynamic"))
                      .withSolrXml(MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace(
                          MiniSolrCloudCluster.SOLR_TESTS_SHARDS_WHITELIST, EXPLICIT_WHITELIST_PROPERTY + clusterId))
                      .build();
              return cluster;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          protected int nodesPerCluster(String clusterId) {
            return nodesPerCluster;
          }
        },
        new DefaultClusterInitFunction(numShards, numReplicas, maxShardsPerNode) {
          @Override
          public void accept(String clusterId, MiniSolrCloudCluster cluster) {
            appendClusterNodes(sb, ",", cluster);
            if (clusterId.equals(EXPLICIT_CLUSTER_KEY)) {
              System.setProperty(EXPLICIT_WHITELIST_PROPERTY + clusterId, sb.toString());
              for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
                try {
                  runner.stop();
                  runner.start(true);
                } catch (Exception e) {
                  throw new RuntimeException("Unable to restart runner", e);
                }
              }
            }
            doAccept(COLLECTION_NAME, cluster);
          }
        });
  }

  @AfterClass
  public static void afterTests() {
    System.clearProperty(EXPLICIT_WHITELIST_PROPERTY + EXPLICIT_CLUSTER_KEY);
  }

  private HttpShardHandlerFactory getShardHandlerFactory(String clusterId) {
    return (HttpShardHandlerFactory) clusterId2cluster.get(clusterId).getJettySolrRunner(0).getCoreContainer()
        .getShardHandlerFactory();
  }

  @Test
  public void test() throws Exception {
    assertThat(getShardHandlerFactory(EXPLICIT_CLUSTER_KEY).getWhitelistHostChecker().getWhitelistHosts(), notNullValue());
    assertThat(getShardHandlerFactory(IMPLICIT_CLUSTER_KEY).getWhitelistHostChecker().getWhitelistHosts(), nullValue());

    assertThat(getShardHandlerFactory(EXPLICIT_CLUSTER_KEY).getWhitelistHostChecker().hasExplicitWhitelist(), is(true));
    assertThat(getShardHandlerFactory(IMPLICIT_CLUSTER_KEY).getWhitelistHostChecker().hasExplicitWhitelist(), is(false));
    for (MiniSolrCloudCluster cluster : clusterId2cluster.values()) {
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        URI uri = runner.getBaseUrl().toURI();
        assertThat(getShardHandlerFactory(EXPLICIT_CLUSTER_KEY).getWhitelistHostChecker().getWhitelistHosts(),
            hasItem(uri.getHost() + ":" + uri.getPort()));
      }
    }

    MiniSolrCloudCluster implicitCluster = clusterId2cluster.get(IMPLICIT_CLUSTER_KEY);
    MiniSolrCloudCluster explicitCluster = clusterId2cluster.get(EXPLICIT_CLUSTER_KEY);

    for (Map.Entry<String,MiniSolrCloudCluster> entry : clusterId2cluster.entrySet()) {
      List<SolrInputDocument> docs = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
        docs.add(new SolrInputDocument("id", entry.getKey() + i));
      }
      MiniSolrCloudCluster cluster = entry.getValue();
      cluster.getSolrClient().add(COLLECTION_NAME, docs);
      cluster.getSolrClient().commit(COLLECTION_NAME, true, true);

      // test using ClusterState elements
      assertThat("No shards specified, should work in both clusters",
          numDocs("*:*", null, cluster), is(10));
      assertThat("Both shards specified, should work in both clusters",
          numDocs("*:*", "shard1,shard2", cluster), is(10));
      assertThat("Both shards specified with collection name, should work in both clusters",
          numDocs("*:*", COLLECTION_NAME + "_shard1", cluster), is(numDocs("*:*", "shard1", cluster)));

      // test using explicit urls from within the cluster
      assertThat("Shards has the full URLs, should be allowed since they are internal. Cluster=" + entry.getKey(),
          numDocs("*:*", getShardUrl("shard1", cluster) + "," + getShardUrl("shard2", cluster), cluster), is(10));
      assertThat("Full URL without scheme",
          numDocs("*:*", getShardUrl("shard1", cluster).replaceAll("http://", "") + ","
              + getShardUrl("shard2", cluster).replaceAll("http://", ""), cluster),
          is(10));

      // Mix shards with URLs
      assertThat("Mix URL and cluster state object",
          numDocs("*:*", "shard1," + getShardUrl("shard2", cluster), cluster), is(10));
      assertThat("Mix URL and cluster state object",
          numDocs("*:*", getShardUrl("shard1", cluster) + ",shard2", cluster), is(10));
    }

    // explicit whitelist includes all the nodes in both clusters. Requests should be allowed to go through
    assertThat("A request to the explicit cluster with shards that point to the implicit one",
        numDocs(
            "id:implicitCluster*",
            getShardUrl("shard1", implicitCluster) + "," + getShardUrl("shard2", implicitCluster),
            explicitCluster),
        is(10));

    assertThat("A request to the explicit cluster with shards that point to the both clusters",
        numDocs(
            "*:*",
            getShardUrl("shard1", implicitCluster)
                + "," + getShardUrl("shard2", implicitCluster)
                + "," + getShardUrl("shard1", explicitCluster)
                + "," + getShardUrl("shard2", explicitCluster),
            explicitCluster),
        is(20));

    // Implicit shouldn't allow requests to the other cluster
    assertForbidden("id:explicitCluster*",
        getShardUrl("shard1", explicitCluster) + "," + getShardUrl("shard2", explicitCluster),
        implicitCluster);

    assertForbidden("id:explicitCluster*",
        "shard1," + getShardUrl("shard2", explicitCluster),
        implicitCluster);

    assertForbidden("id:explicitCluster*",
        getShardUrl("shard1", explicitCluster) + ",shard2",
        implicitCluster);

    assertForbidden("id:explicitCluster*",
        getShardUrl("shard1", explicitCluster),
        implicitCluster);

    assertThat("A typical internal request, should be handled locally",
        numDocs(
            "id:explicitCluster*",
            null,
            implicitCluster,
            "distrib", "false",
            "shard.url", getShardUrl("shard2", explicitCluster),
            "shards.purpose", "64",
            "isShard", "true"),
        is(0));
  }

  private void assertForbidden(String query, String shards, MiniSolrCloudCluster cluster) throws IOException {
    ignoreException("not on the shards whitelist");
    try {
      numDocs(
          query,
          shards,
          cluster);
      fail("Expecting failure for shards parameter: '" + shards + "'");
    } catch (SolrServerException e) {
      assertThat(e.getCause(), instanceOf(SolrException.class));
      assertThat(((SolrException) e.getCause()).code(), is(SolrException.ErrorCode.FORBIDDEN.code));
      assertThat(((SolrException) e.getCause()).getMessage(), containsString("not on the shards whitelist"));
    }
    unIgnoreException("not on the shards whitelist");
  }

  private String getShardUrl(String shardName, MiniSolrCloudCluster cluster) {
    return cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME)
        .getSlice(shardName).getReplicas().iterator().next().getCoreUrl();
  }

  private int numDocs(String queryString, String shardsParamValue, MiniSolrCloudCluster cluster, String... otherParams)
      throws SolrServerException, IOException {
    SolrQuery q = new SolrQuery(queryString);
    if (shardsParamValue != null) {
      q.set("shards", shardsParamValue);
    }
    if (otherParams != null) {
      assert otherParams.length % 2 == 0;
      for (int i = 0; i < otherParams.length; i += 2) {
        q.set(otherParams[i], otherParams[i + 1]);
      }
    }
    return (int) cluster.getSolrClient().query(COLLECTION_NAME, q).getResults().getNumFound();
  }

}

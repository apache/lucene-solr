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

import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.MultiSolrCloudTestCase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  protected final String COLLECTION_NAME = "ShardsWhitelistTestCollection";

  private volatile int numShards;
  private volatile int numReplicas;
  private volatile int maxShardsPerNode;
  private volatile int nodesPerCluster;

  private static void appendClusterNodes(final StringBuilder sb, final String delimiter,
      final MiniSolrCloudCluster cluster) {
    cluster.getJettySolrRunners().forEach((jetty) -> sb.append(jetty.getBaseUrl().toString() + delimiter));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    final String[] clusterIds = new String[] {IMPLICIT_CLUSTER_KEY, EXPLICIT_CLUSTER_KEY};

    numShards = 2; // +random().nextInt(2);
    numReplicas = 1; // +random().nextInt(2);
    maxShardsPerNode = 1; // +random().nextInt(2);
    nodesPerCluster = numShards * numReplicas;

    final StringBuilder sb = new StringBuilder();

    doSetupClusters(clusterIds,
        new DefaultClusterCreateFunction() {

          @Override
          public MiniSolrCloudCluster apply(String clusterId) {
            try {
              final MiniSolrCloudCluster cluster = new SolrCloudTestCase.Builder(nodesPerCluster(clusterId), SolrTestUtil.createTempDir())
                      .addConfig("conf", SolrTestUtil.configset("cloud-dynamic"))
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
                  runner.start(true, true);
                } catch (Exception e) {
                  throw new RuntimeException("Unable to restart runner", e);
                }
              }
            }
            doAccept(COLLECTION_NAME, cluster);
          }
        });
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
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
        assertThat(getShardHandlerFactory(EXPLICIT_CLUSTER_KEY).getWhitelistHostChecker().getWhitelistHosts(),
            hasItem(runner.getHost() + ":" + runner.getLocalPort()));
      }
    }

    MiniSolrCloudCluster implicitCluster = clusterId2cluster.get(IMPLICIT_CLUSTER_KEY);
    MiniSolrCloudCluster explicitCluster = clusterId2cluster.get(EXPLICIT_CLUSTER_KEY);


    clusterId2cluster.forEach((s, miniSolrCloudCluster) -> {

      miniSolrCloudCluster.waitForActiveCollection(COLLECTION_NAME, numShards, numShards);

      List<SolrInputDocument> docs = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
        docs.add(new SolrInputDocument("id", s + i));
      }
      MiniSolrCloudCluster cluster = miniSolrCloudCluster;
      try {
        cluster.getSolrClient().add(COLLECTION_NAME, docs);

      cluster.getSolrClient().commit(COLLECTION_NAME, true, true);

      // test using ClusterState elements
      assertThat("No shards specified, should work in both clusters",
          numDocs("*:*", null, cluster), is(10));
      assertThat("Both shards specified, should work in both clusters",
          numDocs("*:*", "s1,s2", cluster), is(10));

      // test using explicit urls from within the cluster
      assertThat("Shards has the full URLs, should be allowed since they are internal. Cluster=" + s,
          numDocs("*:*", getShardUrl("s1", cluster) + "," + getShardUrl("s2", cluster), cluster), is(10));
      assertThat("Full URL without scheme",
          numDocs("*:*", getShardUrl("s1", cluster).replaceAll("http://", "") + ","
              + getShardUrl("s2", cluster).replaceAll("http://", ""), cluster),
          is(10));

      // Mix shards with URLs
      assertThat("Mix URL and cluster state object",
          numDocs("*:*", "s1," + getShardUrl("s2", cluster), cluster), is(10));
      assertThat("Mix URL and cluster state object",
          numDocs("*:*", getShardUrl("s1", cluster) + ",s2", cluster), is(10));
      } catch (SolrServerException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    });

    // explicit whitelist includes all the nodes in both clusters. Requests should be allowed to go through
    assertThat("A request to the explicit cluster with shards that point to the implicit one",
        numDocs(
            "id:implicitCluster*",
            getShardUrl("s1", implicitCluster) + "," + getShardUrl("s2", implicitCluster),
            explicitCluster),
        is(10));

    assertThat("A request to the explicit cluster with shards that point to the both clusters",
        numDocs(
            "*:*",
            getShardUrl("s1", implicitCluster)
                + "," + getShardUrl("s2", implicitCluster)
                + "," + getShardUrl("s1", explicitCluster)
                + "," + getShardUrl("s2", explicitCluster),
            explicitCluster),
        is(20));

    // Implicit shouldn't allow requests to the other cluster
    assertForbidden("id:explicitCluster*",
        getShardUrl("s1", explicitCluster) + "," + getShardUrl("s2", explicitCluster),
        implicitCluster);

    assertForbidden("id:explicitCluster*",
        "s1," + getShardUrl("s2", explicitCluster),
        implicitCluster);

    assertForbidden("id:explicitCluster*",
        getShardUrl("s1", explicitCluster) + ",s2",
        implicitCluster);

    assertForbidden("id:explicitCluster*",
        getShardUrl("s1", explicitCluster),
        implicitCluster);

    assertThat("A typical internal request, should be handled locally",
        numDocs(
            "id:explicitCluster*",
            null,
            implicitCluster,
            "distrib", "false",
            "shard.url", getShardUrl("s2", explicitCluster),
            "shards.purpose", "64",
            "isShard", "true"),
        is(0));
  }

  private void assertForbidden(String query, String shards, MiniSolrCloudCluster cluster) throws Exception {
    ignoreException("not on the shards whitelist");
    try {
      numDocs(
          query,
          shards,
          cluster);
      fail("Expecting failure for shards parameter: '" + shards + "'");
    } catch (BaseHttpSolrClient.RemoteSolrException e) {
      assertThat(e.code(), is(SolrException.ErrorCode.FORBIDDEN.code));
      assertThat(e.getMessage(), containsString("not on the shards whitelist"));
    }
    unIgnoreException("not on the shards whitelist");
  }

  private String getShardUrl(String shardName, MiniSolrCloudCluster cluster) {
    return cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME)
        .getSlice(shardName).getReplicas().iterator().next().getCoreUrl();
  }

  private int numDocs(String queryString, String shardsParamValue, MiniSolrCloudCluster cluster, String... otherParams)
      throws SolrServerException, BaseHttpSolrClient.RemoteSolrException, IOException {
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

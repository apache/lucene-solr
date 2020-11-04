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


import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.UrlScheme;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.solr.common.cloud.UrlScheme.HTTP;
import static org.apache.solr.common.cloud.UrlScheme.HTTPS;
import static org.apache.solr.common.cloud.UrlScheme.USE_LIVENODES_URL_SCHEME;
import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * We want to make sure that when migrating between http and https modes the
 * replicas will not be rejoined as new nodes, but rather take off where it left
 * off in the cluster.
 */
@Slow
@SuppressSSL
public class SSLMigrationTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String clusterPropAction = CollectionAction.CLUSTERPROP.toString().toLowerCase(Locale.ROOT);

  @Test
  public void test() throws Exception {
    // Migrate from HTTP -> HTTPS with rolling restart, relying on UrlScheme.USE_LIVENODES_URL_SCHEME cluster prop
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, cloudClient.getZkStateReader(), false, true, 15, SECONDS);
    assertReplicaInformation(HTTP);
    doTestMigrateSSLRollingRestart();
  }

  private void doTestMigrateSSLRollingRestart() throws Exception {
    final ZkStateReader zkr = cloudClient.getZkStateReader();

    // only go from http -> https for this test ... going the other way is unsupported, 
    // users will just have to restart all nodes and hope for the best
    SSLTestConfig sslConfig = new SSLTestConfig(true, false);
    setUrlScheme(HTTPS);
    setUseLiveNodesUrlScheme(true);

    HttpClientUtil.setSocketFactoryRegistryProvider(sslConfig.buildClientSocketFactoryRegistryProvider());

    // tricky ~ the ZkController relies on this property during init to determine if the node has TLS enabled
    System.setProperty(UrlScheme.HTTPS_PORT_PROP, "SOME PORT");
    
    // the control jetty doesn't get restarted as part of this test, so ignore it
    final String controlJettyPort = ":"+controlJetty.getLocalPort()+"_";
    Set<String> expectedLiveNodesSet = zkr.getClusterState().getLiveNodes().stream()
        .filter(n -> !n.contains(controlJettyPort)).collect(Collectors.toSet());

    // do a rolling restart of each Jetty node and then verify each comes back with https:// enabled in a reasonable time
    for (int i = 0; i < this.jettys.size(); i++) {
      JettySolrRunner runner = jettys.get(i);
      runner.stop();

      try {
        Thread.sleep(1000L); // give time for leaders to start re-elect elsewhere
      } catch (InterruptedException ie) {
        Thread.interrupted();
      }

      JettyConfig config = JettyConfig.builder()
          .setContext(context)
          .setPort(runner.getLocalPort())
          .stopAtShutdown(false)
          .withServlets(getExtraServlets())
          .withFilters(getExtraRequestFilters())
          .withSSLConfig(sslConfig.buildServerSSLConfig())
          .build();

      Properties props = new Properties();
      if (getSolrConfigFile() != null)
        props.setProperty("solrconfig", getSolrConfigFile());
      if (getSchemaFile() != null)
        props.setProperty("schema", getSchemaFile());
      props.setProperty("solr.data.dir", getDataDir(testDir + "/shard" + i + "/data"));

      JettySolrRunner newRunner = new JettySolrRunner(runner.getSolrHome(), props, config);
      newRunner.start();
      jettys.set(i, newRunner);
    }

    // poll to see all restarted nodes come back with https
    TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      int count = 0;
      for (String node : expectedLiveNodesSet) {
        if (node.contains(controlJettyPort)) {
          continue; // ignore the control Jetty as we didn't restart it ^
        }
        if (zkr.getClusterState().liveNodesContain(node)) {
          try {
            if (HTTPS.equals(getSchemeForLiveNode(zkr.getZkClient(), node))) {
              ++count;
            }
          } catch (Exception ignore) {
            log.warn("Failed to get urlScheme from live node {}", node, ignore);
          }
        }
      }

      if (count == expectedLiveNodesSet.size()) {
        break;
      }

      try {
        Thread.sleep(500L);
      } catch (InterruptedException ie) {
        Thread.interrupted();
      }
    }

    Set<String> actualLiveNodes = zkr.getClusterState().getLiveNodes().stream()
        .filter(n -> !n.contains(controlJettyPort)).collect(Collectors.toSet());
    assertEquals(expectedLiveNodesSet, actualLiveNodes);

    // fail the test if any restarted nodes aren't using https
    for (String liveNode : actualLiveNodes) {
      assertEquals("Node "+liveNode+" still on http!","https",
          getSchemeForLiveNode(zkr.getZkClient(), liveNode));
    }

    // the collection should be recovered
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkr, false, true, 15, SECONDS);
    assertReplicaInformation(HTTPS);
    setUseLiveNodesUrlScheme(false);
  }

  private void assertReplicaInformation(String urlScheme) {
    List<Replica> replicas = getReplicas();
    assertEquals("Wrong number of replicas found", 4, replicas.size());
    for (Replica replica : replicas) {
      assertTrue("Replica didn't have the proper urlScheme (expected '" + urlScheme + "') in the ClusterState",
          StringUtils.startsWith(replica.getBaseUrl(), urlScheme));
    }
  }

  private List<Replica> getReplicas() {
    List<Replica> replicas = new ArrayList<Replica>();

    DocCollection collection = this.cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION);
    for (Slice slice : collection.getSlices()) {
      replicas.addAll(slice.getReplicas());
    }
    return replicas;
  }

  private void setUrlScheme(String value) throws IOException, SolrServerException {
    setClusterProp(makeMap("action", clusterPropAction, "name", URL_SCHEME, "val", value));
  }

  private void setUseLiveNodesUrlScheme(boolean b) throws IOException, SolrServerException {
    setClusterProp(makeMap("action", clusterPropAction, "name", USE_LIVENODES_URL_SCHEME, "val", String.valueOf(b)));
  }

  private void setClusterProp(@SuppressWarnings("rawtypes") Map m) throws IOException, SolrServerException {
    @SuppressWarnings("unchecked")
    SolrParams params = new MapSolrParams(m);
    @SuppressWarnings({"rawtypes"})
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    Set<String> urls = new HashSet<>();
    for (Replica replica : getReplicas()) {
      urls.add(replica.getStr(ZkStateReader.BASE_URL_PROP));
    }

    //Create new SolrServer to configure new HttpClient w/ SSL config
    try (SolrClient client = getLBHttpSolrClient(urls.toArray(new String[]{}))) {
      client.request(request);
    }
  }

  private String getSchemeForLiveNode(SolrZkClient zkClient, String liveNode) throws Exception {
    String scheme = "http";
    final String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + liveNode;
    byte[] data = zkClient.getData(nodePath, null, null, true);
    if (data != null) {
      scheme = new String(data, StandardCharsets.UTF_8);
    }
    return scheme;
  }
}

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

package org.apache.solr.common.cloud;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.common.cloud.UrlScheme.HTTP;
import static org.apache.solr.common.cloud.UrlScheme.HTTPS;
import static org.apache.solr.common.cloud.UrlScheme.USE_LIVENODES_URL_SCHEME;
import static org.apache.solr.common.cloud.ZkStateReader.LIVE_NODES_ZKNODE;
import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UrlSchemeTest extends SolrTestCase {

  @Test
  public void testApplyUrlScheme() throws Exception {
    assumeWorkingMockito();

    final UrlScheme t = UrlScheme.INSTANCE;

    //  mock a SolrZkClient with some live nodes and cluster props set.
    String liveNode1 = "192.168.1.1:8983_solr";
    String liveNode2 = "127.0.0.1:8983_solr";
    String liveNode3 = "127.0.0.1_";
    String liveNode4 = "127.0.0.1:61631_l_%2Fig";
    Map<String,Object> clusterProps = Collections.singletonMap(URL_SCHEME, HTTPS);

    SolrZkClient zkClient = mock(SolrZkClient.class);
    when(zkClient.getData(LIVE_NODES_ZKNODE + "/" + liveNode1, null, null, true)).thenReturn("https".getBytes(UTF_8));
    when(zkClient.getData(LIVE_NODES_ZKNODE + "/" + liveNode2, null, null, true)).thenReturn("http".getBytes(UTF_8));
    when(zkClient.getData(LIVE_NODES_ZKNODE + "/" + liveNode3, null, null, true)).thenReturn("https".getBytes(UTF_8));
    when(zkClient.getData(LIVE_NODES_ZKNODE + "/" + liveNode4, null, null, true)).thenReturn("https".getBytes(UTF_8));
    when(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true)).thenReturn(Utils.toJSON(clusterProps));

    // in a live cluster, this gets called during ZkController initialization and then it is expected that the urlScheme does not change
    t.initFromClusterProps(zkClient);
    assertTrue(t.isOnServer());

    try {
      t.setUrlScheme(HTTP);
      fail("Shouldn't be able to change the urlScheme once set!");
    } catch (IllegalStateException expected) {
      // expected
    }

    assertEquals("https://192.168.1.1:8983/solr", t.getBaseUrlForNodeName(liveNode1));
    assertEquals("https://127.0.0.1", t.getBaseUrlForNodeName(liveNode3));

    SortedSet<String> liveNodes = new TreeSet<>();
    liveNodes.add("192.168.1.1:8983_solr"); // uses https, see mock ^
    t.onChange(null, liveNodes);

    // global https applies, no match in live nodes
    assertEquals("https://127.0.0.1:8983/solr", t.applyUrlScheme("127.0.0.1:8983/solr"));
    assertEquals("https://127.0.0.1:8983/solr", t.applyUrlScheme("http://127.0.0.1:8983/solr"));

    // global http applies, no match in live nodes
    when(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true))
        .thenReturn(Utils.toJSON(Collections.singletonMap(URL_SCHEME, HTTP)));
    t.initFromClusterProps(zkClient);

    assertEquals("http://127.0.0.1:8983/solr", t.applyUrlScheme("127.0.0.1:8983/solr"));
    assertEquals("http://127.0.0.1:8983/solr", t.applyUrlScheme("https://127.0.0.1:8983/solr"));

    clusterProps = new HashMap<>();
    clusterProps.put(URL_SCHEME, HTTPS);
    clusterProps.put(USE_LIVENODES_URL_SCHEME, "true");
    when(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true))
        .thenReturn(Utils.toJSON(clusterProps));
    t.initFromClusterProps(zkClient);

    liveNodes = new TreeSet<>();
    liveNodes.add("127.0.0.1:8983_solr");
    t.onChange(null, liveNodes);
    // live node's http scheme takes precedent over global https
    assertEquals("http://127.0.0.1:8983/solr", t.applyUrlScheme("https://127.0.0.1:8983/solr"));
    assertEquals("http://127.0.0.1:8983/solr", t.applyUrlScheme("127.0.0.1:8983/solr"));

    liveNodes = new TreeSet<>();
    t.onChange(null, liveNodes);
    // no match in live nodes, so global https applies
    assertEquals("https://127.0.0.1:8983/", t.applyUrlScheme("127.0.0.1:8983/"));

    // back to http
    clusterProps = new HashMap<>();
    clusterProps.put(URL_SCHEME, "http");
    clusterProps.put(USE_LIVENODES_URL_SCHEME, "true");
    when(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true))
        .thenReturn(Utils.toJSON(clusterProps));
    t.initFromClusterProps(zkClient);

    liveNodes = new TreeSet<>();
    t.onChange(null, liveNodes); // test node not in live nodes, which is ok

    assertEquals("http://127.0.0.1:8983", t.applyUrlScheme("http://127.0.0.1:8983"));
    assertEquals("http://127.0.0.1:8983", t.applyUrlScheme("https://127.0.0.1:8983"));
    assertEquals("http://127.0.0.1:8983", t.applyUrlScheme("127.0.0.1:8983"));

    // live node has https and global urlScheme is http, so expect https
    liveNodes = new TreeSet<>();
    liveNodes.add("127.0.0.1_");
    t.onChange(null, liveNodes);
    assertEquals("https://127.0.0.1", t.applyUrlScheme("127.0.0.1"));

    liveNodes = new TreeSet<>();
    liveNodes.add("127.0.0.1:61631_l_%2Fig");
    t.onChange(null, liveNodes);
    assertEquals("https://127.0.0.1:61631/l_/ig", t.applyUrlScheme("127.0.0.1:61631/l_/ig"));
    assertEquals("http://127.0.0.1:61632/l_/ig", t.applyUrlScheme("127.0.0.1:61632/l_/ig"));
  }
}

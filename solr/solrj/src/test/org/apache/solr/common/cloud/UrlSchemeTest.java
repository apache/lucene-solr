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
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.common.cloud.UrlScheme.USE_LIVENODES_URL_SCHEME;
import static org.apache.solr.common.cloud.ZkStateReader.LIVE_NODES_ZKNODE;
import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UrlSchemeTest extends SolrTestCase {

  @Test
  public void testApplyUrlScheme() throws Exception {
    String liveNode1 = "192.168.1.1:8983_solr";
    String liveNode2 = "127.0.0.1:8983_solr";
    String liveNode3 = "127.0.0.1_";

    SolrZkClient zkClient = mock(SolrZkClient.class);
    when(zkClient.getData(LIVE_NODES_ZKNODE + "/" + liveNode1, null, null, true)).thenReturn("https".getBytes(UTF_8));
    when(zkClient.getData(LIVE_NODES_ZKNODE + "/" + liveNode2, null, null, true)).thenReturn("http".getBytes(UTF_8));
    when(zkClient.getData(LIVE_NODES_ZKNODE + "/" + liveNode3, null, null, true)).thenReturn("https".getBytes(UTF_8));
    UrlScheme.INSTANCE.setZkClient(zkClient);

    UrlScheme.INSTANCE.setUrlScheme(UrlScheme.HTTPS);
    SortedSet<String> liveNodes = new TreeSet<>();
    liveNodes.add("192.168.1.1:8983_solr");
    UrlScheme.INSTANCE.onChange(null, liveNodes);

    // global https applies, no match in live nodes
    Optional<String> opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("https://127.0.0.1:8983/solr", opt.get());

    // global http applies, no match in live nodes
    UrlScheme.INSTANCE.setUrlScheme(UrlScheme.HTTP);
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983/solr", opt.get());

    // live node's http scheme takes precedent over global https
    UrlScheme.INSTANCE.setUrlScheme(UrlScheme.HTTPS);
    UrlScheme.INSTANCE.setUseLiveNodesUrlScheme(true);
    liveNodes = new TreeSet<>();
    liveNodes.add("127.0.0.1:8983_solr");
    UrlScheme.INSTANCE.onChange(null, liveNodes);
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983/solr", opt.get());

    // no scheme in the stored_url
    opt = UrlScheme.INSTANCE.applyUrlScheme("127.0.0.1:8983/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983/solr", opt.get());

    UrlScheme.INSTANCE.onChange(Collections.singletonMap(URL_SCHEME, "https"));
    UrlScheme.INSTANCE.setUseLiveNodesUrlScheme(true);
    liveNodes = new TreeSet<>();
    liveNodes.add("127.0.0.1:8983_solr");
    UrlScheme.INSTANCE.onChange(null, liveNodes);
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    // http because the live node entry doesn't have https
    assertEquals("http://127.0.0.1:8983/solr", opt.get());

    UrlScheme.INSTANCE.onChange(Collections.singletonMap(URL_SCHEME, "http"));
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1/solr", opt.get());

    // Change to using https
    UrlScheme.INSTANCE.onChange(Collections.singletonMap(URL_SCHEME, "https"));
    UrlScheme.INSTANCE.setUseLiveNodesUrlScheme(true);
    liveNodes = new TreeSet<>();
    UrlScheme.INSTANCE.onChange(null, liveNodes);
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("https://127.0.0.1:8983/", opt.get());

    // back to http
    Map<String,Object> clusterProps = new HashMap<>();
    clusterProps.put(URL_SCHEME, "http");
    clusterProps.put(USE_LIVENODES_URL_SCHEME, true);
    UrlScheme.INSTANCE.onChange(clusterProps);
    liveNodes = new TreeSet<>();
    UrlScheme.INSTANCE.onChange(null, liveNodes);

    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983", opt.get());

    opt = UrlScheme.INSTANCE.applyUrlScheme("127.0.0.1:8983");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983", opt.get());

    // live node has https and global urlScheme is http, so expect https
    liveNodes = new TreeSet<>();
    liveNodes.add("127.0.0.1_");
    UrlScheme.INSTANCE.onChange(null, liveNodes);
    opt = UrlScheme.INSTANCE.applyUrlScheme("127.0.0.1");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("https://127.0.0.1", opt.get());
  }
}

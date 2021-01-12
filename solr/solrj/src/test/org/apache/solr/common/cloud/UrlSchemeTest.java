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

import org.apache.solr.SolrTestCase;
import org.junit.Test;

import static org.apache.solr.common.cloud.UrlScheme.HTTP;
import static org.apache.solr.common.cloud.UrlScheme.HTTPS;

public class UrlSchemeTest extends SolrTestCase {

  @Test
  public void testApplyUrlScheme() {

    final UrlScheme t = UrlScheme.INSTANCE;
    t.setUrlScheme(HTTPS);

    //  mock a SolrZkClient with some live nodes and cluster props set.
    String liveNode1 = "192.168.1.1:8983_solr";
    String liveNode2 = "127.0.0.1:8983_solr";
    String liveNode3 = "127.0.0.1:80_";
    String liveNode4 = "127.0.0.1:61631_l_%2Fig";
    String liveNode5 = "some_weird_hostname-here:8983_solr%2Fx";

    assertEquals("https://192.168.1.1:8983/solr", t.getBaseUrlForNodeName(liveNode1));
    assertEquals("https://127.0.0.1:8983/solr", t.getBaseUrlForNodeName(liveNode2));
    assertEquals("https://127.0.0.1:80", t.getBaseUrlForNodeName(liveNode3));
    assertEquals("https://127.0.0.1:61631/l_/ig", t.getBaseUrlForNodeName(liveNode4));
    // heal wrong scheme too
    assertEquals("https://127.0.0.1:8983/solr", t.applyUrlScheme("127.0.0.1:8983/solr"));
    assertEquals("https://127.0.0.1:8983/solr", t.applyUrlScheme("http://127.0.0.1:8983/solr"));

    t.setUrlScheme(HTTP);
    assertEquals("http://192.168.1.1:8983/solr", t.getBaseUrlForNodeName(liveNode1));
    assertEquals("http://127.0.0.1:8983/solr", t.getBaseUrlForNodeName(liveNode2));
    assertEquals("http://127.0.0.1:80", t.getBaseUrlForNodeName(liveNode3));
    assertEquals("http://127.0.0.1:61631/l_/ig", t.getBaseUrlForNodeName(liveNode4));
    assertEquals("http://some_weird_hostname-here:8983/solr/x", t.getBaseUrlForNodeName(liveNode5));

    // heal wrong scheme too
    assertEquals("http://127.0.0.1:8983/solr", t.applyUrlScheme("https://127.0.0.1:8983/solr"));
  }
}

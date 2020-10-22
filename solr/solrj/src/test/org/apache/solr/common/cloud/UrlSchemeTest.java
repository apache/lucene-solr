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
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

import static org.apache.solr.common.cloud.UrlScheme.NODE_NAME_SCHEME_DELIM;
import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;

public class UrlSchemeTest extends SolrTestCase {

  @Test
  public void testApplyUrlScheme() {
    UrlScheme.INSTANCE.setUrlScheme(UrlScheme.HTTPS);
    SortedSet<String> liveNodes = new TreeSet<>();
    liveNodes.add("https"+NODE_NAME_SCHEME_DELIM+"192.168.1.1:8983_solr");
    UrlScheme.INSTANCE.onChange(null, liveNodes);

    // global https applies
    Optional<String> opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("https://127.0.0.1:8983/solr", opt.get());

    UrlScheme.INSTANCE.setUrlScheme(UrlScheme.HTTP);
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983/solr", opt.get());

    // live node's scheme takes precedent over global
    UrlScheme.INSTANCE.setUseLiveNodesUrlScheme(true);
    liveNodes = new TreeSet<>();
    liveNodes.add("http"+NODE_NAME_SCHEME_DELIM+"127.0.0.1:8983_solr");
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
    assertEquals("https://127.0.0.1:8983/solr", opt.get());

    UrlScheme.INSTANCE.onChange(Collections.singletonMap(URL_SCHEME, "http"));
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1/solr");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1/solr", opt.get());

    UrlScheme.INSTANCE.onChange(Collections.singletonMap(URL_SCHEME, "https"));
    UrlScheme.INSTANCE.setUseLiveNodesUrlScheme(true);
    liveNodes = new TreeSet<>();
    liveNodes.add("http"+NODE_NAME_SCHEME_DELIM+"127.0.0.1:8983_");
    UrlScheme.INSTANCE.onChange(null, liveNodes);
    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983/");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983/", opt.get());

    opt = UrlScheme.INSTANCE.applyUrlScheme("${scheme}://127.0.0.1:8983");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983", opt.get());

    opt = UrlScheme.INSTANCE.applyUrlScheme("127.0.0.1:8983");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("http://127.0.0.1:8983", opt.get());

    liveNodes = new TreeSet<>();
    liveNodes.add("https"+NODE_NAME_SCHEME_DELIM+"127.0.0.1_");
    UrlScheme.INSTANCE.onChange(null, liveNodes);
    opt = UrlScheme.INSTANCE.applyUrlScheme("127.0.0.1");
    assertNotNull(opt);
    assertTrue(opt.isPresent());
    assertEquals("https://127.0.0.1", opt.get());
  }
}

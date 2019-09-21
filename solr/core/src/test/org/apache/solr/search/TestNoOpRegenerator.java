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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/** Tests that NoOpRegenerator does what it should */
public class TestNoOpRegenerator extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-noopregen.xml", "schema-minimal.xml");
  }
  
  @SuppressWarnings("unchecked")
  public void testRegeneration() throws Exception {
    assertU(adoc("id", "1"));
    assertU(adoc("id", "2"));
    assertU(commit());
    
    // add some items
    h.getCore().withSearcher(searcher -> {
      assertEquals(2, searcher.maxDoc());
      SolrCache<Object,Object> cache = searcher.getCache("myPerSegmentCache");
      assertEquals(0, cache.size());
      cache.put("key1", "value1");
      cache.put("key2", "value2");
      assertEquals(2, cache.size());
      return null;
    });
    
    // add a doc and commit: we should see our cached items still there
    assertU(adoc("id", "3"));
    assertU(commit());

    h.getCore().withSearcher(searcher -> {
      assertEquals(3, searcher.maxDoc());
      SolrCache<Object,Object> cache = searcher.getCache("myPerSegmentCache");
      assertEquals(2, cache.size());
      assertEquals("value1", cache.get("key1"));
      assertEquals("value2", cache.get("key2"));
      return null;
    });
  }
}

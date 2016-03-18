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
import org.apache.solr.common.util.NamedList;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class TestSolrFieldCacheMBean extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-minimal.xml");
  }

  @Test
  public void testEntryList() throws Exception {
    // ensure entries to FieldCache
    assertU(adoc("id", "id0"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "id asc"), "//*[@numFound='1']");

    SolrFieldCacheMBean mbean = new SolrFieldCacheMBean();
    NamedList stats = mbean.getStatistics();
    assert(new Integer(stats.get("entries_count").toString()) > 0);
    assertNotNull(stats.get("entry#0"));

    // Test again with entry list disabled
    System.setProperty("disableSolrFieldCacheMBeanEntryList", "true");
    try {
      mbean = new SolrFieldCacheMBean();
      stats = mbean.getStatistics();
      assert(new Integer(stats.get("entries_count").toString()) > 0);
      assertNull(stats.get("entry#0"));
    } finally {
      System.clearProperty("disableSolrFieldCacheMBeanEntryList");
    }
  }
}

/**
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

import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.request.SolrQueryRequest;

public class TestIndexSearcher extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema11.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  public String getCoreName() { return "basic"; }


  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
  }
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
  }


  public void testReopen() {
    assertU(adoc("id","1", "v_t","Hello Dude"));
    assertU(adoc("id","2", "v_t","Hello Yonik"));
    assertU(commit());

    SolrQueryRequest sr1 = req("q","foo");
    SolrIndexReader r1 = sr1.getSearcher().getReader();

    assertU(adoc("id","3", "v_s","{!literal}"));
    assertU(adoc("id","4", "v_s","other stuff"));
    assertU(commit());

    SolrQueryRequest sr2 = req("q","foo");
    SolrIndexReader r2 = sr2.getSearcher().getReader();

    // make sure the readers share the first segment
    // Didn't work w/ older versions of lucene2.9 going from segment -> multi
    assertEquals(r1.getLeafReaders()[0], r2.getLeafReaders()[0]);

    assertU(adoc("id","5", "v_f","3.14159"));
    assertU(adoc("id","6", "v_f","8983"));
    assertU(commit());

    SolrQueryRequest sr3 = req("q","foo");
    SolrIndexReader r3 = sr3.getSearcher().getReader();
    // make sure the readers share segments
    // assertEquals(r1.getLeafReaders()[0], r3.getLeafReaders()[0]);
    assertEquals(r2.getLeafReaders()[0], r3.getLeafReaders()[0]);
    assertEquals(r2.getLeafReaders()[1], r3.getLeafReaders()[1]);

    sr1.close();
    sr2.close();            

    // should currently be 1, but this could change depending on future index management
    int baseRefCount = r3.getRefCount();
    assertEquals(1, baseRefCount);

    assertU(commit());
    SolrQueryRequest sr4 = req("q","foo");
    SolrIndexReader r4 = sr4.getSearcher().getReader();

    // force an index change so the registered searcher won't be the one we are testing (and
    // then we should be able to test the refCount going all the way to 0
    assertU(adoc("id","7", "v_f","7574"));
    assertU(commit()); 

    // test that reader didn't change (according to equals at least... which uses the wrapped reader)
    assertEquals(r3,r4);
    assertEquals(baseRefCount+1, r4.getRefCount());
    sr3.close();
    assertEquals(baseRefCount, r4.getRefCount());
    sr4.close();
    assertEquals(baseRefCount-1, r4.getRefCount());
  }
}
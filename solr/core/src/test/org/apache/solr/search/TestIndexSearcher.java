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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.io.IOException;

public class TestIndexSearcher extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {

    // we need a consistent segmentation because reopen test validation
    // dependso n merges not happening when it doesn't expect
    System.setProperty("solr.tests.mergePolicy", LogDocMergePolicy.class.getName());

    initCore("solrconfig.xml","schema.xml");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    optimize();
    assertU((commit()));
  }

  private String getStringVal(SolrQueryRequest sqr, String field, int doc) throws IOException {
    SchemaField sf = sqr.getSchema().getField(field);
    ValueSource vs = sf.getType().getValueSource(sf, null);
    Map context = ValueSource.newContext(sqr.getSearcher());
    vs.createWeight(context, sqr.getSearcher());
    IndexReaderContext topReaderContext = sqr.getSearcher().getTopReaderContext();
    List<LeafReaderContext> leaves = topReaderContext.leaves();
    int idx = ReaderUtil.subIndex(doc, leaves);
    LeafReaderContext leaf = leaves.get(idx);
    FunctionValues vals = vs.getValues(context, leaf);
    return vals.strVal(doc-leaf.docBase);
  }

  public void testReopen() throws Exception {

    assertU(adoc("id","1", "v_t","Hello Dude", "v_s1","string1"));
    assertU(adoc("id","2", "v_t","Hello Yonik", "v_s1","string2"));
    assertU(commit());

    SolrQueryRequest sr1 = req("q","foo");
    IndexReader r1 = sr1.getSearcher().getRawReader();

    String sval1 = getStringVal(sr1, "v_s1",0);
    assertEquals("string1", sval1);

    assertU(adoc("id","3", "v_s1","{!literal}"));
    assertU(adoc("id","4", "v_s1","other stuff"));
    assertU(commit());

    SolrQueryRequest sr2 = req("q","foo");
    IndexReader r2 = sr2.getSearcher().getRawReader();

    // make sure the readers share the first segment
    // Didn't work w/ older versions of lucene2.9 going from segment -> multi
    assertEquals(r1.leaves().get(0).reader(), r2.leaves().get(0).reader());

    assertU(adoc("id","5", "v_f","3.14159"));
    assertU(adoc("id","6", "v_f","8983", "v_s1","string6"));
    assertU(commit());

    SolrQueryRequest sr3 = req("q","foo");
    IndexReader r3 = sr3.getSearcher().getRawReader();
    // make sure the readers share segments
    // assertEquals(r1.getLeafReaders()[0], r3.getLeafReaders()[0]);
    assertEquals(r2.leaves().get(0).reader(), r3.leaves().get(0).reader());
    assertEquals(r2.leaves().get(1).reader(), r3.leaves().get(1).reader());

    sr1.close();
    sr2.close();            

    // should currently be 1, but this could change depending on future index management
    int baseRefCount = r3.getRefCount();
    assertEquals(1, baseRefCount);

    Object sr3SearcherRegAt = sr3.getSearcher().getStatistics().get("registeredAt");
    assertU(commit()); // nothing has changed
    SolrQueryRequest sr4 = req("q","foo");
    assertSame("nothing changed, searcher should be the same",
               sr3.getSearcher(), sr4.getSearcher());
    assertEquals("nothing changed, searcher should not have been re-registered",
                 sr3SearcherRegAt, sr4.getSearcher().getStatistics().get("registeredAt"));
    IndexReader r4 = sr4.getSearcher().getRawReader();

    // force an index change so the registered searcher won't be the one we are testing (and
    // then we should be able to test the refCount going all the way to 0
    assertU(adoc("id","7", "v_f","7574"));
    assertU(commit()); 

    // test that reader didn't change
    assertSame(r3, r4);
    assertEquals(baseRefCount, r4.getRefCount());
    sr3.close();
    assertEquals(baseRefCount, r4.getRefCount());
    sr4.close();
    assertEquals(baseRefCount-1, r4.getRefCount());


    SolrQueryRequest sr5 = req("q","foo");
    IndexReaderContext rCtx5 = sr5.getSearcher().getTopReaderContext();

    assertU(delI("1"));
    assertU(commit());
    SolrQueryRequest sr6 = req("q","foo");
    IndexReaderContext rCtx6 = sr6.getSearcher().getTopReaderContext();
    assertEquals(1, rCtx6.leaves().get(0).reader().numDocs()); // only a single doc left in the first segment
    assertTrue( !rCtx5.leaves().get(0).reader().equals(rCtx6.leaves().get(0).reader()) );  // readers now different

    sr5.close();
    sr6.close();
  }


  // make sure we don't leak searchers (SOLR-3391)
  public void testCloses() {
    assertU(adoc("id","1"));
    assertU(commit("openSearcher","false"));  // this was enough to trigger SOLR-3391

    int maxDoc = random().nextInt(20) + 1;

    // test different combinations of commits
    for (int i=0; i<100; i++) {

      if (random().nextInt(100) < 50) {
        String id = Integer.toString(random().nextInt(maxDoc));
        assertU(adoc("id",id));
      } else {
        boolean soft = random().nextBoolean();
        boolean optimize = random().nextBoolean();
        boolean openSearcher = random().nextBoolean();

        if (optimize) {
          assertU(optimize("openSearcher",""+openSearcher, "softCommit",""+soft));
        } else {
          assertU(commit("openSearcher",""+openSearcher, "softCommit",""+soft));
        }
      }
    }

  }
}

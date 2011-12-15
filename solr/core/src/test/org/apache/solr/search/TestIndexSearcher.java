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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.ReaderUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

import java.util.Map;
import java.io.IOException;

public class TestIndexSearcher extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
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
    ReaderContext topReaderContext = sqr.getSearcher().getTopReaderContext();
    AtomicReaderContext[] leaves = ReaderUtil.leaves(topReaderContext);
    int idx = ReaderUtil.subIndex(doc, leaves);
    AtomicReaderContext leaf = leaves[idx];
    FunctionValues vals = vs.getValues(context, leaf);
    return vals.strVal(doc-leaf.docBase);
  }

  public void testReopen() throws Exception {

    assertU(adoc("id","1", "v_t","Hello Dude", "v_s1","string1"));
    assertU(adoc("id","2", "v_t","Hello Yonik", "v_s1","string2"));
    assertU(commit());

    SolrQueryRequest sr1 = req("q","foo");
    ReaderContext rCtx1 = sr1.getSearcher().getTopReaderContext();

    String sval1 = getStringVal(sr1, "v_s1",0);
    assertEquals("string1", sval1);

    assertU(adoc("id","3", "v_s1","{!literal}"));
    assertU(adoc("id","4", "v_s1","other stuff"));
    assertU(commit());

    SolrQueryRequest sr2 = req("q","foo");
    ReaderContext rCtx2 = sr2.getSearcher().getTopReaderContext();

    // make sure the readers share the first segment
    // Didn't work w/ older versions of lucene2.9 going from segment -> multi
    assertEquals(ReaderUtil.leaves(rCtx1)[0].reader, ReaderUtil.leaves(rCtx2)[0].reader);

    assertU(adoc("id","5", "v_f","3.14159"));
    assertU(adoc("id","6", "v_f","8983", "v_s1","string6"));
    assertU(commit());

    SolrQueryRequest sr3 = req("q","foo");
    ReaderContext rCtx3 = sr3.getSearcher().getTopReaderContext();
    // make sure the readers share segments
    // assertEquals(r1.getLeafReaders()[0], r3.getLeafReaders()[0]);
    assertEquals(ReaderUtil.leaves(rCtx2)[0].reader, ReaderUtil.leaves(rCtx3)[0].reader);
    assertEquals(ReaderUtil.leaves(rCtx2)[1].reader, ReaderUtil.leaves(rCtx3)[1].reader);

    sr1.close();
    sr2.close();            

    // should currently be 1, but this could change depending on future index management
    int baseRefCount = rCtx3.reader.getRefCount();
    assertEquals(1, baseRefCount);

    assertU(commit());
    SolrQueryRequest sr4 = req("q","foo");
    ReaderContext rCtx4 = sr4.getSearcher().getTopReaderContext();

    // force an index change so the registered searcher won't be the one we are testing (and
    // then we should be able to test the refCount going all the way to 0
    assertU(adoc("id","7", "v_f","7574"));
    assertU(commit()); 

    // test that reader didn't change (according to equals at least... which uses the wrapped reader)
    assertEquals(rCtx3.reader, rCtx4.reader);
    assertEquals(baseRefCount+1, rCtx4.reader.getRefCount());
    sr3.close();
    assertEquals(baseRefCount, rCtx4.reader.getRefCount());
    sr4.close();
    assertEquals(baseRefCount-1, rCtx4.reader.getRefCount());


    SolrQueryRequest sr5 = req("q","foo");
    ReaderContext rCtx5 = sr5.getSearcher().getTopReaderContext();

    assertU(delI("1"));
    assertU(commit());
    SolrQueryRequest sr6 = req("q","foo");
    ReaderContext rCtx6 = sr6.getSearcher().getTopReaderContext();
    assertEquals(1, ReaderUtil.leaves(rCtx6)[0].reader.numDocs()); // only a single doc left in the first segment
    assertTrue( !ReaderUtil.leaves(rCtx5)[0].reader.equals(ReaderUtil.leaves(rCtx6)[0].reader) );  // readers now different

    sr5.close();
    sr6.close();
  }
}

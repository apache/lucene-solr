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
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.DocValues;
import org.apache.lucene.search.FieldCache;

import java.util.Map;
import java.io.IOException;

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

  private String getStringVal(SolrQueryRequest sqr, String field, int doc) throws IOException {
    SchemaField sf = sqr.getSchema().getField(field);
    ValueSource vs = sf.getType().getValueSource(sf, null);
    Map context = ValueSource.newContext();
    vs.createWeight(context, sqr.getSearcher());
    SolrIndexReader sr = sqr.getSearcher().getReader();
    int idx = SolrIndexReader.readerIndex(doc, sr.getLeafOffsets());
    int base = sr.getLeafOffsets()[idx];
    SolrIndexReader sub = sr.getLeafReaders()[idx];
    DocValues vals = vs.getValues(context, sub);
    return vals.strVal(doc-base);
  }

  public void testReopen() throws Exception {

    assertU(adoc("id","1", "v_t","Hello Dude", "v_s","string1"));
    assertU(adoc("id","2", "v_t","Hello Yonik", "v_s","string2"));
    assertU(commit());

    SolrQueryRequest sr1 = req("q","foo");
    SolrIndexReader r1 = sr1.getSearcher().getReader();

    String sval1 = getStringVal(sr1, "v_s",0);
    assertEquals("string1", sval1);

    assertU(adoc("id","3", "v_s","{!literal}"));
    assertU(adoc("id","4", "v_s","other stuff"));
    assertU(commit());

    SolrQueryRequest sr2 = req("q","foo");
    SolrIndexReader r2 = sr2.getSearcher().getReader();

    // make sure the readers share the first segment
    // Didn't work w/ older versions of lucene2.9 going from segment -> multi
    assertEquals(r1.getLeafReaders()[0], r2.getLeafReaders()[0]);

    // make sure the String returned is the exact same instance (i.e. same FieldCache instance)
    assertTrue(sval1 == getStringVal(sr2,"v_s",0));

    assertU(adoc("id","5", "v_f","3.14159"));
    assertU(adoc("id","6", "v_f","8983", "v_s","string6"));
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


    SolrQueryRequest sr5 = req("q","foo");
    SolrIndexReader r5 = sr5.getSearcher().getReader();
    String beforeDelete = getStringVal(sr5, "v_s",1);

    assertU(delI("1"));
    assertU(commit());
    SolrQueryRequest sr6 = req("q","foo");
    SolrIndexReader r6 = sr4.getSearcher().getReader();
    assertEquals(1, r6.getLeafReaders()[0].numDocs()); // only a single doc left in the first segment
    assertTrue( !r5.getLeafReaders()[0].equals(r6.getLeafReaders()[0]) );  // readers now different
    String afterDelete = getStringVal(sr6, "v_s",1);
    assertTrue( beforeDelete == afterDelete );  // same field cache is used even though deletions are different

    sr5.close();
    sr6.close();
  }
}
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
package org.apache.solr.uninverting;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.solr.legacy.LegacyFieldType;
import org.apache.solr.legacy.LegacyIntField;
import org.apache.solr.legacy.LegacyNumericRangeQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNumericTerms32 extends SolrTestCase {
  // distance of entries
  private static int distance;
  // shift the starting of the values to the left, to also have negative values:
  private static final int startOffset = - 1 << 15;
  // number of docs to generate for testing
  private static int noDocs;
  
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    noDocs = atLeast(4096);
    distance = (1 << 30) / noDocs;
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));
    
    final LegacyFieldType storedInt = new LegacyFieldType(LegacyIntField.TYPE_NOT_STORED);
    storedInt.setStored(true);
    storedInt.freeze();

    final LegacyFieldType storedInt8 = new LegacyFieldType(storedInt);
    storedInt8.setNumericPrecisionStep(8);

    final LegacyFieldType storedInt4 = new LegacyFieldType(storedInt);
    storedInt4.setNumericPrecisionStep(4);

    final LegacyFieldType storedInt2 = new LegacyFieldType(storedInt);
    storedInt2.setNumericPrecisionStep(2);

    LegacyIntField
      field8 = new LegacyIntField("field8", 0, storedInt8),
      field4 = new LegacyIntField("field4", 0, storedInt4),
      field2 = new LegacyIntField("field2", 0, storedInt2);
    
    Document doc = new Document();
    // add fields, that have a distance to test general functionality
    doc.add(field8); doc.add(field4); doc.add(field2);
    
    // Add a series of noDocs docs with increasing int values
    for (int l=0; l<noDocs; l++) {
      int val=distance*l+startOffset;
      field8.setIntValue(val);
      field4.setIntValue(val);
      field2.setIntValue(val);

      val=l-(noDocs/2);
      writer.addDocument(doc);
    }
  
    Map<String,Type> map = new HashMap<>();
    map.put("field2", Type.LEGACY_INTEGER);
    map.put("field4", Type.LEGACY_INTEGER);
    map.put("field8", Type.LEGACY_INTEGER);
    reader = UninvertingReader.wrap(writer.getReader(), map);
    searcher=newSearcher(reader);
    writer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    if (null != reader) {
      TestUtil.checkReader(reader);
      reader.close();
      reader = null;
    }
    if (null != directory) {
      directory.close();
      directory = null;
    }
  }
  
  private void testSorting(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    // 10 random tests, the index order is ascending,
    // so using a reverse sort field should retun descending documents
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      int lower=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      int upper=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        int a=lower; lower=upper; upper=a;
      }
      Query tq= LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, true);
      TopDocs topDocs = searcher.search(tq, noDocs, new Sort(new SortField(field, SortField.Type.INT, true)));
      if (topDocs.totalHits.value==0) continue;
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      int last = searcher.doc(sd[0].doc).getField(field).numericValue().intValue();
      for (int j=1; j<sd.length; j++) {
        int act = searcher.doc(sd[j].doc).getField(field).numericValue().intValue();
        assertTrue("Docs should be sorted backwards", last>act );
        last=act;
      }
    }
  }

  @Test
  public void testSorting_8bit() throws Exception {
    testSorting(8);
  }
  
  @Test
  public void testSorting_4bit() throws Exception {
    testSorting(4);
  }
  
  @Test
  public void testSorting_2bit() throws Exception {
    testSorting(2);
  }  
}

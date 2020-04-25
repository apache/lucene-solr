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
import org.apache.solr.legacy.LegacyLongField;
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

public class TestNumericTerms64 extends SolrTestCase {
  // distance of entries
  private static long distance;
  // shift the starting of the values to the left, to also have negative values:
  private static final long startOffset = - 1L << 31;
  // number of docs to generate for testing
  private static int noDocs;
  
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    noDocs = atLeast(4096);
    distance = (1L << 60) / noDocs;
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));

    final LegacyFieldType storedLong = new LegacyFieldType(LegacyLongField.TYPE_NOT_STORED);
    storedLong.setStored(true);
    storedLong.freeze();

    final LegacyFieldType storedLong8 = new LegacyFieldType(storedLong);
    storedLong8.setNumericPrecisionStep(8);

    final LegacyFieldType storedLong4 = new LegacyFieldType(storedLong);
    storedLong4.setNumericPrecisionStep(4);

    final LegacyFieldType storedLong6 = new LegacyFieldType(storedLong);
    storedLong6.setNumericPrecisionStep(6);

    final LegacyFieldType storedLong2 = new LegacyFieldType(storedLong);
    storedLong2.setNumericPrecisionStep(2);

    LegacyLongField
      field8 = new LegacyLongField("field8", 0L, storedLong8),
      field6 = new LegacyLongField("field6", 0L, storedLong6),
      field4 = new LegacyLongField("field4", 0L, storedLong4),
      field2 = new LegacyLongField("field2", 0L, storedLong2);

    Document doc = new Document();
    // add fields, that have a distance to test general functionality
    doc.add(field8); doc.add(field6); doc.add(field4); doc.add(field2);
    
    // Add a series of noDocs docs with increasing long values, by updating the fields
    for (int l=0; l<noDocs; l++) {
      long val=distance*l+startOffset;
      field8.setLongValue(val);
      field6.setLongValue(val);
      field4.setLongValue(val);
      field2.setLongValue(val);

      val=l-(noDocs/2);
      writer.addDocument(doc);
    }
    Map<String,Type> map = new HashMap<>();
    map.put("field2", Type.LEGACY_LONG);
    map.put("field4", Type.LEGACY_LONG);
    map.put("field6", Type.LEGACY_LONG);
    map.put("field8", Type.LEGACY_LONG);
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
      long lower=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      long upper=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      Query tq= LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, true);
      TopDocs topDocs = searcher.search(tq, noDocs, new Sort(new SortField(field, SortField.Type.LONG, true)));
      if (topDocs.totalHits.value==0) continue;
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      long last=searcher.doc(sd[0].doc).getField(field).numericValue().longValue();
      for (int j=1; j<sd.length; j++) {
        long act=searcher.doc(sd[j].doc).getField(field).numericValue().longValue();
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
  public void testSorting_6bit() throws Exception {
    testSorting(6);
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

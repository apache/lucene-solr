package org.apache.lucene.uninverting;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestNumericTerms64 extends LuceneTestCase {
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
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.disableSorting("field");

    // Add a series of noDocs docs with increasing long values, by updating the fields
    for (int l=0; l<noDocs; l++) {
      Document doc = writer.newDocument();
      long val=distance*l+startOffset;
      doc.addLong("field", val);
      writer.addDocument(doc);
    }
    Map<String,Type> map = new HashMap<>();
    map.put("field", Type.LONG);
    reader = UninvertingReader.wrap(writer.getReader(), map);
    searcher=newSearcher(reader);
    writer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    TestUtil.checkReader(reader);
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }
  
  public void testSorting() throws Exception {
    FieldTypes fieldTypes = reader.getFieldTypes();
    String field="field";
    // 10 random tests, the index order is ascending,
    // so using a reverse sort field should retun descending documents
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      long lower=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      long upper=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      Query tq = new ConstantScoreQuery(fieldTypes.newLongRangeFilter(field, lower, true, upper, true));
      TopDocs topDocs = searcher.search(tq, null, noDocs, new Sort(new SortField(field, SortField.Type.LONG, true)));
      if (topDocs.totalHits==0) continue;
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
}

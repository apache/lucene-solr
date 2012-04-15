package org.apache.lucene.search;

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

import java.util.Locale;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestMultiValuedNumericRangeQuery extends LuceneTestCase {

  /** Tests NumericRangeQuery on a multi-valued field (multiple numeric values per document).
   * This test ensures, that a classical TermRangeQuery returns exactly the same document numbers as
   * NumericRangeQuery (see SOLR-1322 for discussion) and the multiple precision terms per numeric value
   * do not interfere with multiple numeric values.
   */
  public void testMultiValuedNRQ() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(_TestUtil.nextInt(random(), 50, 1000)));
    
    DecimalFormat format = new DecimalFormat("00000000000", new DecimalFormatSymbols(Locale.US));
    
    int num = atLeast(500);
    for (int l = 0; l < num; l++) {
      Document doc = new Document();
      for (int m=0, c=random().nextInt(10); m<=c; m++) {
        int value = random().nextInt(Integer.MAX_VALUE);
        doc.add(newField("asc", format.format(value), StringField.TYPE_UNSTORED));
        doc.add(new IntField("trie", value));
      }
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher searcher=newSearcher(reader);
    num = atLeast(50);
    for (int i = 0; i < num; i++) {
      int lower=random().nextInt(Integer.MAX_VALUE);
      int upper=random().nextInt(Integer.MAX_VALUE);
      if (lower>upper) {
        int a=lower; lower=upper; upper=a;
      }
      TermRangeQuery cq=TermRangeQuery.newStringRange("asc", format.format(lower), format.format(upper), true, true);
      NumericRangeQuery<Integer> tq=NumericRangeQuery.newIntRange("trie", lower, upper, true, true);
      TopDocs trTopDocs = searcher.search(cq, 1);
      TopDocs nrTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", trTopDocs.totalHits, nrTopDocs.totalHits );
    }
    reader.close();
    directory.close();
  }
  
}

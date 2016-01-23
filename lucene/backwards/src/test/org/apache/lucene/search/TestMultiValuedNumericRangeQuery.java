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

import java.util.Random;
import java.util.Locale;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestMultiValuedNumericRangeQuery extends LuceneTestCase {

  /** Tests NumericRangeQuery on a multi-valued field (multiple numeric values per document).
   * This test ensures, that a classical TermRangeQuery returns exactly the same document numbers as
   * NumericRangeQuery (see SOLR-1322 for discussion) and the multiple precision terms per numeric value
   * do not interfere with multiple numeric values.
   */

  public void testMultiValuedNRQ() throws Exception {
    final Random rnd = newRandom();

    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, MaxFieldLength.UNLIMITED);
    
    DecimalFormat format = new DecimalFormat("00000000000", new DecimalFormatSymbols(Locale.US));
    
    for (int l=0; l<5000; l++) {
      Document doc = new Document();
      for (int m=0, c=rnd.nextInt(10); m<=c; m++) {
        int value = rnd.nextInt(Integer.MAX_VALUE);
        doc.add(new Field("asc", format.format(value), Field.Store.NO, Field.Index.NOT_ANALYZED));
        doc.add(new NumericField("trie", Field.Store.NO, true).setIntValue(value));
      }
      writer.addDocument(doc);
    }  
    writer.close();
    
    Searcher searcher=new IndexSearcher(directory, true);
    for (int i=0; i<50; i++) {
      int lower=rnd.nextInt(Integer.MAX_VALUE);
      int upper=rnd.nextInt(Integer.MAX_VALUE);
      if (lower>upper) {
        int a=lower; lower=upper; upper=a;
      }
      TermRangeQuery cq=new TermRangeQuery("asc", format.format(lower), format.format(upper), true, true);
      NumericRangeQuery<Integer> tq=NumericRangeQuery.newIntRange("trie", lower, upper, true, true);
      TopDocs trTopDocs = searcher.search(cq, 1);
      TopDocs nrTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", trTopDocs.totalHits, nrTopDocs.totalHits );
    }
    searcher.close();

    directory.close();
  }
  
}

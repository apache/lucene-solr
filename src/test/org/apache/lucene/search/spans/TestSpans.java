package org.apache.lucene.search.spans;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import junit.framework.TestCase;

import java.io.IOException;

public class TestSpans extends TestCase {
  private IndexSearcher searcher;

  public static final String field = "field";

  public void setUp() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer= new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(Field.Text(field, docFields[i]));
      writer.addDocument(doc);
    }
    writer.close();
    searcher = new IndexSearcher(directory);
  }

  private String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3",
    ""
  };

  public SpanTermQuery makeSpanTermQuery(String text) {
    return new SpanTermQuery(new Term(field, text));
  }
  
  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(query, field, searcher, results, this);
  }
  
  public void orderedSlopTest3(int slop, int[] expectedDocs) throws IOException {
    SpanTermQuery w1 = makeSpanTermQuery("w1");
    SpanTermQuery w2 = makeSpanTermQuery("w2");
    SpanTermQuery w3 = makeSpanTermQuery("w3");
    boolean ordered = true;
    SpanNearQuery snq = new SpanNearQuery( new SpanQuery[]{w1,w2,w3}, slop, ordered);
    checkHits(snq, expectedDocs);
  }
  
  public void testSpanNearOrdered01() throws Exception {
    orderedSlopTest3(0, new int[] {0});
  }

  public void testSpanNearOrdered02() throws Exception {
    orderedSlopTest3(1, new int[] {0,1});
  }

  public void testSpanNearOrdered03() throws Exception {
    orderedSlopTest3(2, new int[] {0,1,2});
  }

  public void testSpanNearOrdered04() throws Exception {
    orderedSlopTest3(3, new int[] {0,1,2,3});
  }

  public void testSpanNearOrdered05() throws Exception {
    orderedSlopTest3(4, new int[] {0,1,2,3});
  }
}

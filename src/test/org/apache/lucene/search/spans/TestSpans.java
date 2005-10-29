package org.apache.lucene.search.spans;

/**
 * Copyright 2005 The Apache Software Foundation
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
      doc.add(new Field(field, docFields[i], Field.Store.YES, Field.Index.TOKENIZED));
      writer.addDocument(doc);
    }
    writer.close();
    searcher = new IndexSearcher(directory);
//System.out.println("set up " + getName());
  }

  private String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3",
    "u2 u2 u1",
    "u2 xx u2 u1",
    "u2 u2 xx u1",
    "u2 xx u2 yy u1",
    "u2 xx u1 u2",
    "u2 u1 xx u2",
    "u1 u2 xx u2",
    "t1 t2 t1 t3 t2 t3"
  };

  public SpanTermQuery makeSpanTermQuery(String text) {
    return new SpanTermQuery(new Term(field, text));
  }
  
  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(query, field, searcher, results);
  }
  
  private void orderedSlopTest3SQ(
        SpanQuery q1,
        SpanQuery q2,
        SpanQuery q3,
        int slop,
        int[] expectedDocs) throws IOException {
    boolean ordered = true;
    SpanNearQuery snq = new SpanNearQuery( new SpanQuery[]{q1,q2,q3}, slop, ordered);
    checkHits(snq, expectedDocs);
  }
  
  public void orderedSlopTest3(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       makeSpanTermQuery("w1"),
       makeSpanTermQuery("w2"),
       makeSpanTermQuery("w3"),
       slop,
       expectedDocs);
  }
  
  public void orderedSlopTest3Equal(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       makeSpanTermQuery("w1"),
       makeSpanTermQuery("w3"),
       makeSpanTermQuery("w3"),
       slop,
       expectedDocs);
  }
  
  public void orderedSlopTest1Equal(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       makeSpanTermQuery("u2"),
       makeSpanTermQuery("u2"),
       makeSpanTermQuery("u1"),
       slop,
       expectedDocs);
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
  
  public void testSpanNearOrderedEqual01() throws Exception {
    orderedSlopTest3Equal(0, new int[] {});
  }

  public void testSpanNearOrderedEqual02() throws Exception {
    orderedSlopTest3Equal(1, new int[] {1});
  }

  public void testSpanNearOrderedEqual03() throws Exception {
    orderedSlopTest3Equal(2, new int[] {1});
  }

  public void testSpanNearOrderedEqual04() throws Exception {
    orderedSlopTest3Equal(3, new int[] {1,3});
  }
  
  public void testSpanNearOrderedEqual11() throws Exception {
    orderedSlopTest1Equal(0, new int[] {4});
  }
  
  public void testSpanNearOrderedEqual12() throws Exception {
    orderedSlopTest1Equal(0, new int[] {4});
  }
  
  public void testSpanNearOrderedEqual13() throws Exception {
    orderedSlopTest1Equal(1, new int[] {4,5,6});
  }
  
  public void testSpanNearOrderedEqual14() throws Exception {
    orderedSlopTest1Equal(2, new int[] {4,5,6,7});
  }

  public void testSpanNearOrderedEqual15() throws Exception {
    orderedSlopTest1Equal(3, new int[] {4,5,6,7});
  }

  public void testSpanNearOrderedOverlap() throws Exception {
    boolean ordered = true;
    int slop = 1;
    SpanNearQuery snq = new SpanNearQuery(
                              new SpanQuery[] {
                                makeSpanTermQuery("t1"),
                                makeSpanTermQuery("t2"),
                                makeSpanTermQuery("t3") },
                              slop,
                              ordered);
    Spans spans = snq.getSpans(searcher.getIndexReader());

    assertTrue("first range", spans.next());
    assertEquals("first doc", 11, spans.doc());
    assertEquals("first start", 0, spans.start());
    assertEquals("first end", 4, spans.end());

    assertTrue("second range", spans.next());
    assertEquals("second doc", 11, spans.doc());
    assertEquals("second start", 2, spans.start());
    assertEquals("second end", 6, spans.end());

    assertFalse("third range", spans.next());
  }

}

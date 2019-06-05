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
package org.apache.lucene.search.spans;


import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.lucene.search.spans.SpanTestUtil.assertFinished;
import static org.apache.lucene.search.spans.SpanTestUtil.assertNext;

public class TestFieldMaskingSpanQuery extends LuceneTestCase {

  protected static Document doc(Field[] fields) {
    Document doc = new Document();
    for (int i = 0; i < fields.length; i++) {
      doc.add(fields[i]);
    }
    return doc;
  }
  
  protected static Field field(String name, String value) {
    return newTextField(name, value, Field.Store.NO);
  }

  protected static IndexSearcher searcher;
  protected static Directory directory;
  protected static IndexReader reader;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    
    writer.addDocument(doc(new Field[] { field("id", "0")
                                         ,
                                         field("gender", "male"),
                                         field("first",  "james"),
                                         field("last",   "jones")     }));
                                               
    writer.addDocument(doc(new Field[] { field("id", "1")
                                         ,
                                         field("gender", "male"),
                                         field("first",  "james"),
                                         field("last",   "smith")
                                         ,
                                         field("gender", "female"),
                                         field("first",  "sally"),
                                         field("last",   "jones")     }));
    
    writer.addDocument(doc(new Field[] { field("id", "2")
                                         ,
                                         field("gender", "female"),
                                         field("first",  "greta"),
                                         field("last",   "jones")
                                         ,
                                         field("gender", "female"),
                                         field("first",  "sally"),
                                         field("last",   "smith")
                                         ,
                                         field("gender", "male"),
                                         field("first",  "james"),
                                         field("last",   "jones")     }));
     
    writer.addDocument(doc(new Field[] { field("id", "3")
                                         ,
                                         field("gender", "female"),
                                         field("first",  "lisa"),
                                         field("last",   "jones")
                                         ,
                                         field("gender", "male"),
                                         field("first",  "bob"),
                                         field("last",   "costas")     }));
    
    writer.addDocument(doc(new Field[] { field("id", "4")
                                         ,
                                         field("gender", "female"),
                                         field("first",  "sally"),
                                         field("last",   "smith")
                                         ,
                                         field("gender", "female"),
                                         field("first",  "linda"),
                                         field("last",   "dixit")
                                         ,
                                         field("gender", "male"),
                                         field("first",  "bubba"),
                                         field("last",   "jones")     }));
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = new IndexSearcher(getOnlyLeafReader(reader));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }

  protected void check(SpanQuery q, int[] docs) throws Exception {
    CheckHits.checkHitCollector(random(), q, null, searcher, docs);
  }

  public void testRewrite0() throws Exception {
    SpanQuery q = new FieldMaskingSpanQuery
      (new SpanTermQuery(new Term("last", "sally")) , "first");
    SpanQuery qr = (SpanQuery) searcher.rewrite(q);

    QueryUtils.checkEqual(q, qr);

    Set<Term> terms = new HashSet<>();
    qr.visit(QueryVisitor.termCollector(terms));
    assertEquals(1, terms.size());
  }
  
  public void testRewrite1() throws Exception {
    // mask an anon SpanQuery class that rewrites to something else.
    SpanQuery q = new FieldMaskingSpanQuery
      (new SpanTermQuery(new Term("last", "sally")) {
          @Override
          public Query rewrite(IndexReader reader) {
            return new SpanOrQuery(new SpanTermQuery(new Term("first", "sally")),
                new SpanTermQuery(new Term("first", "james")));
          }
        }, "first");

    SpanQuery qr = (SpanQuery) searcher.rewrite(q);

    QueryUtils.checkUnequal(q, qr);

    Set<Term> terms = new HashSet<>();
    qr.visit(QueryVisitor.termCollector(terms));
    assertEquals(2, terms.size());
  }
  
  public void testRewrite2() throws Exception {
    SpanQuery q1 = new SpanTermQuery(new Term("last", "smith"));
    SpanQuery q2 = new SpanTermQuery(new Term("last", "jones"));
    SpanQuery q = new SpanNearQuery(new SpanQuery[]
      { q1, new FieldMaskingSpanQuery(q2, "last")}, 1, true );
    Query qr = searcher.rewrite(q);

    QueryUtils.checkEqual(q, qr);

    HashSet<Term> set = new HashSet<>();
    qr.visit(QueryVisitor.termCollector(set));
    assertEquals(2, set.size());
  }
  
  public void testEquality1() {
    SpanQuery q1 = new FieldMaskingSpanQuery
      (new SpanTermQuery(new Term("last", "sally")) , "first");
    SpanQuery q2 = new FieldMaskingSpanQuery
      (new SpanTermQuery(new Term("last", "sally")) , "first");
    SpanQuery q3 = new FieldMaskingSpanQuery
      (new SpanTermQuery(new Term("last", "sally")) , "XXXXX");
    SpanQuery q4 = new FieldMaskingSpanQuery
      (new SpanTermQuery(new Term("last", "XXXXX")) , "first");
    SpanQuery q5 = new FieldMaskingSpanQuery
      (new SpanTermQuery(new Term("xXXX", "sally")) , "first");
    QueryUtils.checkEqual(q1, q2);
    QueryUtils.checkUnequal(q1, q3);
    QueryUtils.checkUnequal(q1, q4);
    QueryUtils.checkUnequal(q1, q5);
  }
  
  public void testNoop0() throws Exception {
    SpanQuery q1 = new SpanTermQuery(new Term("last", "sally"));
    SpanQuery q = new FieldMaskingSpanQuery(q1, "first");
    check(q, new int[] { /* :EMPTY: */ });
  }
  public void testNoop1() throws Exception {
    SpanQuery q1 = new SpanTermQuery(new Term("last", "smith"));
    SpanQuery q2 = new SpanTermQuery(new Term("last", "jones"));
    SpanQuery q = new SpanNearQuery(new SpanQuery[]
      { q1, new FieldMaskingSpanQuery(q2, "last")}, 0, true );
    check(q, new int[] { 1, 2 });
    q = new SpanNearQuery(new SpanQuery[]
      { new FieldMaskingSpanQuery(q1, "last"),
        new FieldMaskingSpanQuery(q2, "last")}, 0, true );
    check(q, new int[] { 1, 2 });
  }
  
  public void testSimple1() throws Exception {
    SpanQuery q1 = new SpanTermQuery(new Term("first", "james"));
    SpanQuery q2 = new SpanTermQuery(new Term("last", "jones"));
    SpanQuery q = new SpanNearQuery(new SpanQuery[]
      { q1, new FieldMaskingSpanQuery(q2, "first")}, -1, false );
    check(q, new int[] { 0, 2 });
    q = new SpanNearQuery(new SpanQuery[]
      { new FieldMaskingSpanQuery(q2, "first"), q1}, -1, false );
    check(q, new int[] { 0, 2 });
    q = new SpanNearQuery(new SpanQuery[]
      { q2, new FieldMaskingSpanQuery(q1, "last")}, -1, false );
    check(q, new int[] { 0, 2 });
    q = new SpanNearQuery(new SpanQuery[]
      { new FieldMaskingSpanQuery(q1, "last"), q2}, -1, false );
    check(q, new int[] { 0, 2 });

  }
  
  public void testSimple2() throws Exception {
    assumeTrue("Broken scoring: LUCENE-3723", 
        searcher.getSimilarity() instanceof TFIDFSimilarity);
    SpanQuery q1 = new SpanTermQuery(new Term("gender", "female"));
    SpanQuery q2 = new SpanTermQuery(new Term("last", "smith"));
    SpanQuery q = new SpanNearQuery(new SpanQuery[]
      { q1, new FieldMaskingSpanQuery(q2, "gender")}, -1, false );
    check(q, new int[] { 2, 4 });
    q = new SpanNearQuery(new SpanQuery[]
      { new FieldMaskingSpanQuery(q1, "id"),
        new FieldMaskingSpanQuery(q2, "id") }, -1, false );
    check(q, new int[] { 2, 4 });
  }

  public void testSpans0() throws Exception {
    SpanQuery q1 = new SpanTermQuery(new Term("gender", "female"));
    SpanQuery q2 = new SpanTermQuery(new Term("first",  "james"));
    SpanQuery q  = new SpanOrQuery(q1, new FieldMaskingSpanQuery(q2, "gender"));
    check(q, new int[] { 0, 1, 2, 3, 4 });

    Spans span = q.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertNext(span, 0,0,1);
    assertNext(span, 1,0,1);
    assertNext(span, 1,1,2);
    assertNext(span, 2,0,1);
    assertNext(span, 2,1,2);
    assertNext(span, 2,2,3);
    assertNext(span, 3,0,1);
    assertNext(span, 4,0,1);
    assertNext(span, 4,1,2);
    assertFinished(span);
  }
  
  public void testSpans1() throws Exception {
    SpanQuery q1 = new SpanTermQuery(new Term("first", "sally"));
    SpanQuery q2 = new SpanTermQuery(new Term("first", "james"));
    SpanQuery qA = new SpanOrQuery(q1, q2);
    SpanQuery qB = new FieldMaskingSpanQuery(qA, "id");
                                            
    check(qA, new int[] { 0, 1, 2, 4 });
    check(qB, new int[] { 0, 1, 2, 4 });
  
    Spans spanA = qA.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    Spans spanB = qB.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    
    while (spanA.nextDoc() != Spans.NO_MORE_DOCS) {
      assertNotSame("spanB not still going", Spans.NO_MORE_DOCS, spanB.nextDoc());
      while (spanA.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
        assertEquals("spanB start position", spanA.startPosition(), spanB.nextStartPosition());
        assertEquals("spanB end position", spanA.endPosition(), spanB.endPosition());
      }
      assertEquals("spanB start position", Spans.NO_MORE_POSITIONS, spanB.nextStartPosition());
    }
    assertEquals("spanB end doc", Spans.NO_MORE_DOCS, spanB.nextDoc());
  }
  
  public void testSpans2() throws Exception {
    assumeTrue("Broken scoring: LUCENE-3723",
        searcher.getSimilarity() instanceof TFIDFSimilarity);
    SpanQuery qA1 = new SpanTermQuery(new Term("gender", "female"));
    SpanQuery qA2 = new SpanTermQuery(new Term("first",  "james"));
    SpanQuery qA  = new SpanOrQuery(qA1, new FieldMaskingSpanQuery(qA2, "gender"));
    SpanQuery qB  = new SpanTermQuery(new Term("last",   "jones"));
    SpanQuery q   = new SpanNearQuery(new SpanQuery[]
      { new FieldMaskingSpanQuery(qA, "id"),
        new FieldMaskingSpanQuery(qB, "id") }, -1, false );
    check(q, new int[] { 0, 1, 2, 3 });

    Spans span = q.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertNext(span, 0,0,1);
    assertNext(span, 1,1,2);
    assertNext(span, 2,0,1);
    assertNext(span, 2,2,3);
    assertNext(span, 3,0,1);
    assertFinished(span);
  }
  
  public String s(int doc, int start, int end) {
    return "s(" + doc + "," + start + "," + end +")";
  }
  
}

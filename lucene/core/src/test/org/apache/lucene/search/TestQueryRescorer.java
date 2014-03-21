package org.apache.lucene.search;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryRescorer extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard oz the the the the the the", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "wizard")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "oz")), Occur.SHOULD);
    IndexSearcher searcher = newSearcher(r);

    // We rely on more tokens = lower score:
    searcher.setSimilarity(new DefaultSimilarity());

    TopDocs hits = searcher.search(bq, 10);
    assertEquals(2, hits.totalHits);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    // Now, resort using PhraseQuery:
    PhraseQuery pq = new PhraseQuery();
    pq.setSlop(5);
    pq.add(new Term("field", "wizard"));
    pq.add(new Term("field", "oz"));

    TopDocs hits2 = QueryRescorer.rescore(searcher, hits, pq, 2.0, 10);

    // Resorting changed the order:
    assertEquals(2, hits2.totalHits);
    assertEquals("1", searcher.doc(hits2.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits2.scoreDocs[1].doc).get("id"));

    // Resort using SpanNearQuery:
    SpanTermQuery t1 = new SpanTermQuery(new Term("field", "wizard"));
    SpanTermQuery t2 = new SpanTermQuery(new Term("field", "oz"));
    SpanNearQuery snq = new SpanNearQuery(new SpanQuery[] {t1, t2}, 0, true);

    TopDocs hits3 = QueryRescorer.rescore(searcher, hits, snq, 2.0, 10);

    // Resorting changed the order:
    assertEquals(2, hits3.totalHits);
    assertEquals("1", searcher.doc(hits3.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits3.scoreDocs[1].doc).get("id"));

    r.close();
    dir.close();
  }

  public void testCustomCombine() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard oz the the the the the the", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "wizard")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "oz")), Occur.SHOULD);
    IndexSearcher searcher = newSearcher(r);

    TopDocs hits = searcher.search(bq, 10);
    assertEquals(2, hits.totalHits);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    // Now, resort using PhraseQuery, but with an
    // opposite-world combine:
    PhraseQuery pq = new PhraseQuery();
    pq.setSlop(5);
    pq.add(new Term("field", "wizard"));
    pq.add(new Term("field", "oz"));
    
    TopDocs hits2 = new QueryRescorer(pq) {
        @Override
        protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
          float score = firstPassScore;
          if (secondPassMatches) {
            score -= 2.0 * secondPassScore;
          }
          return score;
        }
      }.rescore(searcher, hits, 10);

    // Resorting didn't change the order:
    assertEquals(2, hits2.totalHits);
    assertEquals("0", searcher.doc(hits2.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits2.scoreDocs[1].doc).get("id"));

    r.close();
    dir.close();
  }

  public void testExplain() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard oz the the the the the the", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "wizard")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "oz")), Occur.SHOULD);
    IndexSearcher searcher = newSearcher(r);

    TopDocs hits = searcher.search(bq, 10);
    assertEquals(2, hits.totalHits);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    // Now, resort using PhraseQuery:
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("field", "wizard"));
    pq.add(new Term("field", "oz"));

    Rescorer rescorer = new QueryRescorer(pq) {
        @Override
        protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
          float score = firstPassScore;
          if (secondPassMatches) {
            score += 2.0 * secondPassScore;
          }
          return score;
        }
      };

    TopDocs hits2 = rescorer.rescore(searcher, hits, 10);

    // Resorting changed the order:
    assertEquals(2, hits2.totalHits);
    assertEquals("1", searcher.doc(hits2.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits2.scoreDocs[1].doc).get("id"));

    int docID = hits2.scoreDocs[0].doc;
    Explanation explain = rescorer.explain(searcher,
                                           searcher.explain(bq, docID),
                                           docID);
    String s = explain.toString();
    assertTrue(s.contains("TestQueryRescorer$"));
    assertTrue(s.contains("combined first and second pass score"));
    assertTrue(s.contains("first pass score"));
    assertTrue(s.contains("= second pass score"));
    assertEquals(hits2.scoreDocs[0].score, explain.getValue(), 0.0f);

    docID = hits2.scoreDocs[1].doc;
    explain = rescorer.explain(searcher,
                               searcher.explain(bq, docID),
                               docID);
    s = explain.toString();
    assertTrue(s.contains("TestQueryRescorer$"));
    assertTrue(s.contains("combined first and second pass score"));
    assertTrue(s.contains("first pass score"));
    assertTrue(s.contains("no second pass score"));
    assertFalse(s.contains("= second pass score"));
    assertTrue(s.contains("NON-MATCH"));
    assertEquals(hits2.scoreDocs[1].score, explain.getValue(), 0.0f);

    r.close();
    dir.close();
  }

  public void testMissingSecondPassScore() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    doc.add(newTextField("field", "wizard the the the the the oz", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    // 1 extra token, but wizard and oz are close;
    doc.add(newTextField("field", "wizard oz the the the the the the", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    // Do ordinary BooleanQuery:
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "wizard")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "oz")), Occur.SHOULD);
    IndexSearcher searcher = newSearcher(r);

    TopDocs hits = searcher.search(bq, 10);
    assertEquals(2, hits.totalHits);
    assertEquals("0", searcher.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(hits.scoreDocs[1].doc).get("id"));

    // Now, resort using PhraseQuery, no slop:
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("field", "wizard"));
    pq.add(new Term("field", "oz"));

    TopDocs hits2 = QueryRescorer.rescore(searcher, hits, pq, 2.0, 10);

    // Resorting changed the order:
    assertEquals(2, hits2.totalHits);
    assertEquals("1", searcher.doc(hits2.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits2.scoreDocs[1].doc).get("id"));

    // Resort using SpanNearQuery:
    SpanTermQuery t1 = new SpanTermQuery(new Term("field", "wizard"));
    SpanTermQuery t2 = new SpanTermQuery(new Term("field", "oz"));
    SpanNearQuery snq = new SpanNearQuery(new SpanQuery[] {t1, t2}, 0, true);

    TopDocs hits3 = QueryRescorer.rescore(searcher, hits, snq, 2.0, 10);

    // Resorting changed the order:
    assertEquals(2, hits3.totalHits);
    assertEquals("1", searcher.doc(hits3.scoreDocs[0].doc).get("id"));
    assertEquals("0", searcher.doc(hits3.scoreDocs[1].doc).get("id"));

    r.close();
    dir.close();
  }
}

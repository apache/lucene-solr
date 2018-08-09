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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestReqOptSumScorer extends LuceneTestCase {

  public void testBasics() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setMergePolicy(
        // retain doc id order
        newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    doc.add(new StringField("f", "bar", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "bar", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    doc.add(new StringField("f", "bar", Store.NO));
    w.addDocument(doc);
    w.forceMerge(1);

    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query query = new BooleanQuery.Builder()
        .add(new ConstantScoreQuery(new TermQuery(new Term("f", "foo"))), Occur.MUST)
        .add(new ConstantScoreQuery(new TermQuery(new Term("f", "bar"))), Occur.SHOULD)
        .build();
    Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    Scorer scorer = weight.scorer(context);
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    scorer.setMinCompetitiveScore(Math.nextDown(1f));
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    scorer.setMinCompetitiveScore(Math.nextUp(1f));
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    assertEquals(0, scorer.iterator().nextDoc());
    scorer.setMinCompetitiveScore(Math.nextUp(1f));
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    reader.close();
    dir.close();
  }

  public void testRandomFrequentOpt() throws IOException {
    doTestRandom(0.5);
  }

  public void testRandomRareOpt() throws IOException {
    doTestRandom(0.05);
  }

  private void doTestRandom(double optFreq) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      int numAs = random().nextBoolean() ? 0 : 1 + random().nextInt(5);
      int numBs = random().nextDouble() < optFreq ? 0 : 1 + random().nextInt(5);
      Document doc = new Document();
      for (int j = 0; j < numAs; ++j) {
        doc.add(new StringField("f", "A", Store.NO));
      }
      for (int j = 0; j < numBs; ++j) {
        doc.add(new StringField("f", "B", Store.NO));
      }
      if (random().nextBoolean()) {
        doc.add(new StringField("f", "C", Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(r);

    Query mustTerm = new TermQuery(new Term("f", "A"));
    Query shouldTerm = new TermQuery(new Term("f", "B"));
    Query query = new BooleanQuery.Builder()
        .add(mustTerm, Occur.MUST)
        .add(shouldTerm, Occur.SHOULD)
        .build();

    TopScoreDocCollector coll = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE);
    searcher.search(query, coll);
    ScoreDoc[] expected = coll.topDocs().scoreDocs;

    // Also test a filtered query, since it does not compute the score on all
    // matches.
    query = new BooleanQuery.Builder()
        .add(query, Occur.MUST)
        .add(new TermQuery(new Term("f", "C")), Occur.FILTER)
        .build();

    coll = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE);
    searcher.search(query, coll);
    ScoreDoc[] expectedFiltered = coll.topDocs().scoreDocs;

    for (int i = 0; i < 4; ++i) {
      Query must = mustTerm;
      if (i % 2 == 1) {
        must = new RandomApproximationQuery(must, random());
      }
      Query should = shouldTerm;
      if (i >= 2) {
        should = new RandomApproximationQuery(should, random());
      }
    
      query = new BooleanQuery.Builder()
          .add(must, Occur.MUST)
          .add(should, Occur.SHOULD)
          .build();

      coll = TopScoreDocCollector.create(10, null, 1);
      searcher.search(query, coll);
      ScoreDoc[] actual = coll.topDocs().scoreDocs;

      CheckHits.checkEqual(query, expected, actual);

      query = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new RandomApproximationQuery(new TermQuery(new Term("f", "C")), random()), Occur.FILTER)
          .build();

      coll = TopScoreDocCollector.create(10, null, 1);
      searcher.search(query, coll);
      ScoreDoc[] actualFiltered = coll.topDocs().scoreDocs;

      CheckHits.checkEqual(query, expectedFiltered, actualFiltered);
    }

    r.close();
    dir.close();
  }

}

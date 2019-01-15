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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

public class TestBooleanOr extends LuceneTestCase {

  private static String FIELD_T = "T";
  private static String FIELD_C = "C";

  private TermQuery t1 = new TermQuery(new Term(FIELD_T, "files"));
  private TermQuery t2 = new TermQuery(new Term(FIELD_T, "deleting"));
  private TermQuery c1 = new TermQuery(new Term(FIELD_C, "production"));
  private TermQuery c2 = new TermQuery(new Term(FIELD_C, "optimize"));

  private IndexSearcher searcher = null;
  private Directory dir;
  private IndexReader reader;
  

  private long search(Query q) throws IOException {
    QueryUtils.check(random(), q,searcher);
    return searcher.search(q, 1000).totalHits.value;
  }

  public void testElements() throws IOException {
    assertEquals(1, search(t1));
    assertEquals(1, search(t2));
    assertEquals(1, search(c1));
    assertEquals(1, search(c2));
  }

  /**
   * <code>T:files T:deleting C:production C:optimize </code>
   * it works.
   */
  public void testFlat() throws IOException {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    q.add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
    q.add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
    assertEquals(1, search(q.build()));
  }

  /**
   * <code>(T:files T:deleting) (+C:production +C:optimize)</code>
   * it works.
   */
  public void testParenthesisMust() throws IOException {
    BooleanQuery.Builder q3 = new BooleanQuery.Builder();
    q3.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q3.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    BooleanQuery.Builder q4 = new BooleanQuery.Builder();
    q4.add(new BooleanClause(c1, BooleanClause.Occur.MUST));
    q4.add(new BooleanClause(c2, BooleanClause.Occur.MUST));
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(q3.build(), BooleanClause.Occur.SHOULD);
    q2.add(q4.build(), BooleanClause.Occur.SHOULD);
    assertEquals(1, search(q2.build()));
  }

  /**
   * <code>(T:files T:deleting) +(C:production C:optimize)</code>
   * not working. results NO HIT.
   */
  public void testParenthesisMust2() throws IOException {
    BooleanQuery.Builder q3 = new BooleanQuery.Builder();
    q3.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q3.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    BooleanQuery.Builder q4 = new BooleanQuery.Builder();
    q4.add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
    q4.add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(q3.build(), BooleanClause.Occur.SHOULD);
    q2.add(q4.build(), BooleanClause.Occur.MUST);
    assertEquals(1, search(q2.build()));
  }

  /**
   * <code>(T:files T:deleting) (C:production C:optimize)</code>
   * not working. results NO HIT.
   */
  public void testParenthesisShould() throws IOException {
    BooleanQuery.Builder q3 = new BooleanQuery.Builder();
    q3.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q3.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    BooleanQuery.Builder q4 = new BooleanQuery.Builder();
    q4.add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
    q4.add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(q3.build(), BooleanClause.Occur.SHOULD);
    q2.add(q4.build(), BooleanClause.Occur.SHOULD);
    assertEquals(1, search(q2.build()));
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    //
    dir = newDirectory();


    //
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    //
    Document d = new Document();
    d.add(newField(
        FIELD_T,
        "Optimize not deleting all files",
        TextField.TYPE_STORED));
    d.add(newField(
        FIELD_C,
        "Deleted When I run an optimize in our production environment.",
        TextField.TYPE_STORED));

    //
    writer.addDocument(d);

    reader = writer.getReader();
    //
    searcher = newSearcher(reader);
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void testBooleanScorerMax() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())));

    int docCount = atLeast(10000);

    for(int i=0;i<docCount;i++) {
      Document doc = new Document();
      doc.add(newField("field", "a", TextField.TYPE_NOT_STORED));
      riw.addDocument(doc);
    }

    riw.forceMerge(1);
    IndexReader r = riw.getReader();
    riw.close();

    IndexSearcher s = newSearcher(r);
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);

    Weight w = s.createWeight(s.rewrite(bq.build()), ScoreMode.COMPLETE, 1);

    assertEquals(1, s.getIndexReader().leaves().size());
    BulkScorer scorer = w.bulkScorer(s.getIndexReader().leaves().get(0));

    final FixedBitSet hits = new FixedBitSet(docCount);
    final AtomicInteger end = new AtomicInteger();
    LeafCollector c = new SimpleCollector() {

        @Override
        public void collect(int doc) {
          assertTrue("collected doc=" + doc + " beyond max=" + end, doc < end.intValue());
          hits.set(doc);
        }
        
        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE_NO_SCORES;
        }
      };

    while (end.intValue() < docCount) {
      final int min = end.intValue();
      final int inc = TestUtil.nextInt(random(), 1, 1000);
      final int max = end.addAndGet(inc);
      scorer.score(c, null, min, max);
    }

    assertEquals(docCount, hits.cardinality());
    r.close();
    dir.close();
  }

  private static BulkScorer scorer(int... matches) {
    return new BulkScorer() {
      final ScoreAndDoc scorer = new ScoreAndDoc();
      int i = 0;
      @Override
      public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        collector.setScorer(scorer);
        while (i < matches.length && matches[i] < min) {
          i += 1;
        }
        while (i < matches.length && matches[i] < max) {
          scorer.doc = matches[i];
          if (acceptDocs == null || acceptDocs.get(scorer.doc)) {
            collector.collect(scorer.doc);
          }
          i += 1;
        }
        if (i == matches.length) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        return RandomNumbers.randomIntBetween(random(), max, matches[i]);
      }
      @Override
      public long cost() {
        return matches.length;
      }
    };
  }

  // Make sure that BooleanScorer keeps working even if the sub clauses return
  // next matching docs which are less than the actual next match
  public void testSubScorerNextIsNotMatch() throws IOException {
    final List<BulkScorer> optionalScorers = Arrays.asList(
        scorer(100000, 1000001, 9999999),
        scorer(4000, 1000051),
        scorer(5000, 100000, 9999998, 9999999)
    );
    Collections.shuffle(optionalScorers, random());
    BooleanScorer scorer = new BooleanScorer(null, optionalScorers, 1, random().nextBoolean());
    final List<Integer> matches = new ArrayList<>();
    scorer.score(new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {
        matches.add(doc);
      }
      
    }, null);
    assertEquals(Arrays.asList(4000, 5000, 100000, 1000001, 1000051, 9999998, 9999999), matches);
  }
}

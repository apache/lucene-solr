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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

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
  

  private int search(Query q) throws IOException {
    QueryUtils.check(random(), q,searcher);
    return searcher.search(q, null, 1000).totalHits;
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
    BooleanQuery q = new BooleanQuery();
    q.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    q.add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
    q.add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
    assertEquals(1, search(q));
  }

  /**
   * <code>(T:files T:deleting) (+C:production +C:optimize)</code>
   * it works.
   */
  public void testParenthesisMust() throws IOException {
    BooleanQuery q3 = new BooleanQuery();
    q3.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q3.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    BooleanQuery q4 = new BooleanQuery();
    q4.add(new BooleanClause(c1, BooleanClause.Occur.MUST));
    q4.add(new BooleanClause(c2, BooleanClause.Occur.MUST));
    BooleanQuery q2 = new BooleanQuery();
    q2.add(q3, BooleanClause.Occur.SHOULD);
    q2.add(q4, BooleanClause.Occur.SHOULD);
    assertEquals(1, search(q2));
  }

  /**
   * <code>(T:files T:deleting) +(C:production C:optimize)</code>
   * not working. results NO HIT.
   */
  public void testParenthesisMust2() throws IOException {
    BooleanQuery q3 = new BooleanQuery();
    q3.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q3.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    BooleanQuery q4 = new BooleanQuery();
    q4.add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
    q4.add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
    BooleanQuery q2 = new BooleanQuery();
    q2.add(q3, BooleanClause.Occur.SHOULD);
    q2.add(q4, BooleanClause.Occur.MUST);
    assertEquals(1, search(q2));
  }

  /**
   * <code>(T:files T:deleting) (C:production C:optimize)</code>
   * not working. results NO HIT.
   */
  public void testParenthesisShould() throws IOException {
    BooleanQuery q3 = new BooleanQuery();
    q3.add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
    q3.add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
    BooleanQuery q4 = new BooleanQuery();
    q4.add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
    q4.add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
    BooleanQuery q2 = new BooleanQuery();
    q2.add(q3, BooleanClause.Occur.SHOULD);
    q2.add(q4, BooleanClause.Occur.SHOULD);
    assertEquals(1, search(q2));
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
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

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
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);

    Weight w = s.createNormalizedWeight(bq);

    assertEquals(1, s.getIndexReader().leaves().size());
    Scorer scorer = w.scorer(s.getIndexReader().leaves().get(0), false, true, null);

    final FixedBitSet hits = new FixedBitSet(docCount);
    final AtomicInteger end = new AtomicInteger();
    Collector c = new Collector() {
        @Override
        public void setNextReader(AtomicReaderContext sub) {
        }

        @Override
        public void collect(int doc) {
          assertTrue("collected doc=" + doc + " beyond max=" + end, doc < end.intValue());
          hits.set(doc);
        }

        @Override
        public void setScorer(Scorer scorer) {
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }
      };

    while (end.intValue() < docCount) {
      final int inc = _TestUtil.nextInt(random(), 1, 1000);
      end.getAndAdd(inc);
      scorer.score(c, end.intValue(), -1);
    }

    assertEquals(docCount, hits.cardinality());
    r.close();
    dir.close();
  }
}

package org.apache.lucene.search.posfilter;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public abstract class IntervalTestBase extends LuceneTestCase {

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;

  enum AssertionType { POSITIONS, OFFSETS }

  protected static void checkMatches(Scorer scorer, int[][] expectedResults, AssertionType type) throws IOException {

    int doc;
    int docUpto = -1;

    while ((doc = scorer.nextDoc()) != DocsEnum.NO_MORE_DOCS) {

      docUpto++;
      if (doc != expectedResults[docUpto][0])
        fail("Expected next hit in document " + expectedResults[docUpto][0] + " but was in " + doc);

      int posUpto = 0;
      while (scorer.nextPosition() != DocsEnum.NO_MORE_POSITIONS) {

        if (posUpto > ((expectedResults[docUpto].length - 1) / 2) - 1)
          fail("Unexpected hit in document " + doc + ": " + scorer.toString());

        if (type == AssertionType.POSITIONS) {
          if (expectedResults[docUpto][posUpto * 2 + 1] != scorer.startPosition() ||
              expectedResults[docUpto][posUpto * 2 + 2] != scorer.endPosition())
            fail("Expected next position in document " + doc + " to be [" + expectedResults[docUpto][posUpto * 2 + 1] + ", " +
                expectedResults[docUpto][posUpto * 2 + 2] + "] but was [" + scorer.startPosition() + ", " + scorer.endPosition() + "]");
        } else {
          // check offsets
          if (expectedResults[docUpto][posUpto * 2 + 1] != scorer.startOffset() ||
              expectedResults[docUpto][posUpto * 2 + 2] != scorer.endOffset())
            fail("Expected next offset in document to be [" + expectedResults[docUpto][posUpto * 2 + 1] + ", " +
                expectedResults[docUpto][posUpto * 2 + 2] + "] but was [" + scorer.startOffset() + ", " + scorer.endOffset() + "]");
        }

        posUpto++;
      }

      if (posUpto < (expectedResults[docUpto].length - 1) / 2)
        fail("Missing expected hit in document " + expectedResults[docUpto][0] + ": [" +
            expectedResults[docUpto][posUpto] + ", " + expectedResults[docUpto][posUpto + 1] + "]");

    }

    if (docUpto < expectedResults.length - 1)
      fail("Missing expected match to document " + expectedResults[docUpto + 1][0]);

  }

  /**
   * Run a query against a searcher, and check that the collected intervals from the query match
   * the expected results.
   * @param q the query
   * @param searcher the searcher
   * @param expectedResults an int[][] detailing the expected results, in the format
   *                        { { docid1, startoffset1, endoffset1, startoffset2, endoffset2, ... },
   *                          { docid2, startoffset1, endoffset1, startoffset2, endoffset2, ...}, ... }
   * @throws IOException
   */
  public static void checkIntervalOffsets(Query q, IndexSearcher searcher, int[][] expectedResults) throws IOException {

    Weight weight = searcher.createNormalizedWeight(q);
    LeafReaderContext ctx = (LeafReaderContext) searcher.getTopReaderContext();
    Scorer scorer = weight.scorer(ctx, DocsEnum.FLAG_OFFSETS, ctx.reader().getLiveDocs());
    checkMatches(scorer, expectedResults, AssertionType.OFFSETS);

  }

  /**
   * Run a query against a searcher, and check that the collected intervals from the query match
   * the expected results.
   * @param q the query
   * @param searcher the searcher
   * @param expectedResults an int[][] detailing the expected results, in the format
   *                        { { docid1, startpos1, endpos1, startpos2, endpos2, ... },
   *                          { docid2, startpos1, endpos1, startpos2, endpos2, ...}, ... }
   * @throws IOException
   */
  public static void checkIntervals(Query q, IndexSearcher searcher, int[][] expectedResults) throws IOException {

    Weight weight = searcher.createNormalizedWeight(q);
    LeafReaderContext ctx = (LeafReaderContext) searcher.getTopReaderContext();
    Scorer scorer = weight.scorer(ctx, DocsEnum.FLAG_POSITIONS, ctx.reader().getLiveDocs());
    checkMatches(scorer, expectedResults, AssertionType.POSITIONS);

  }

  public static void checkScores(Query q, IndexSearcher searcher, int... expectedDocs) throws IOException {
    TopDocs hits = searcher.search(q, 1000);
    Assert.assertEquals("Wrong number of hits", expectedDocs.length, hits.totalHits);
    for (int i = 0; i < expectedDocs.length; i++) {
      Assert.assertEquals("Docs not scored in order", expectedDocs[i], hits.scoreDocs[i].doc);
    }
    CheckHits.checkExplanations(q, "field", searcher);
  }

  protected abstract void addDocs(RandomIndexWriter writer) throws IOException;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    //config.setCodec(Codec.forName("SimpleText"));
    //config.setCodec(Codec.forName("Asserting"));
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config);
    addDocs(writer);
    reader = SlowCompositeReaderWrapper.wrap(writer.getReader());
    writer.close();
    searcher = new IndexSearcher(reader);
  }

  @After
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  public TermQuery makeTermQuery(String text) {
    return new TermQuery(new Term(TestBasicIntervals.field, text));
  }

  protected Query makeOrQuery(Query... queries) {
    BooleanQuery q = new BooleanQuery();
    for (Query subquery : queries) {
      q.add(subquery, BooleanClause.Occur.SHOULD);
    }
    return q;
  }

  protected Query makeAndQuery(Query... queries) {
    BooleanQuery q = new BooleanQuery();
    for (Query subquery : queries) {
      q.add(subquery, BooleanClause.Occur.MUST);
    }
    return q;
  }

  protected Query makeBooleanQuery(BooleanClause... clauses) {
    BooleanQuery q = new BooleanQuery();
    for (BooleanClause clause : clauses) {
      q.add(clause);
    }
    return q;
  }

  protected BooleanClause makeBooleanClause(String text, BooleanClause.Occur occur) {
    return new BooleanClause(makeTermQuery(text), occur);
  }

  public static class Match implements Comparable<Match> {

    public final int docid;
    public final int start;
    public final int end;
    public final int startOffset;
    public final int endOffset;
    public final boolean composite;

    public Match(int docid, Interval interval, boolean composite) {
      this.docid = docid;
      this.start = interval.begin;
      this.end = interval.end;
      this.startOffset = interval.offsetBegin;
      this.endOffset = interval.offsetEnd;
      this.composite = composite;
    }

    @Override
    public int compareTo(Match o) {
      if (this.docid != o.docid)
        return this.docid - o.docid;
      if (this.start != o.start)
        return this.start - o.start;
      return o.end - this.end;
    }

    @Override
    public String toString() {
      return String.format("%d:%d[%d]->%d[%d]%s",
                            docid, start, startOffset, end, endOffset, composite ? "C" : "");
    }
  }


}

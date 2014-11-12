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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PositionsCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;

public abstract class IntervalTestBase extends LuceneTestCase {

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;

  public static class AssertingPositionsCollector extends PositionsCollector {

    enum AssertionType { POSITIONS, OFFSETS }

    private final int[][] expectedResults;
    private final AssertionType type;

    private int docUpto = -1;
    private int posUpto = -1;

    private int currentDoc = -1;
    private int posRemaining = 0;

    public AssertingPositionsCollector(int[][] expectedResults, AssertionType type) {
      this.expectedResults = expectedResults;
      this.type = type;
    }

    @Override
    public int postingFeatures() {
      if (type == AssertionType.POSITIONS)
        return DocsEnum.FLAG_POSITIONS;
      else
        return DocsEnum.FLAG_OFFSETS;
    }

    @Override
    protected void collectPosition(int doc, Interval interval) {

      if (doc != currentDoc) {
        if (posRemaining > 0) {
          int missingPos = expectedResults[docUpto].length - (posRemaining * 2);
          fail("Missing expected hit in document " + expectedResults[docUpto][0] + ": [" +
              expectedResults[docUpto][missingPos] + ", " + expectedResults[docUpto][missingPos + 1] + "]");
        }
        docUpto++;
        if (docUpto > expectedResults.length - 1)
          fail("Unexpected hit in document " + doc + ": " + interval.toString());

        currentDoc = expectedResults[docUpto][0];
        posUpto = -1;
        posRemaining = (expectedResults[docUpto].length - 1) / 2;
      }

      if (doc != currentDoc)
        fail("Expected next hit in document " + currentDoc + " but was in " + doc + ": " + interval.toString());

      posUpto++;
      posRemaining--;

      if (posUpto > ((expectedResults[docUpto].length - 1) / 2) - 1)
        fail("Unexpected hit in document " + doc + ": " + interval.toString());

      if (type == AssertionType.POSITIONS) {
        if (expectedResults[docUpto][posUpto * 2 + 1] != interval.begin ||
            expectedResults[docUpto][posUpto * 2 + 2] != interval.end)
          fail("Expected next position in document to be [" + expectedResults[docUpto][posUpto * 2 + 1] + ", " +
              expectedResults[docUpto][posUpto * 2 + 2] + "] but was [" + interval.begin + ", " + interval.end + "]");
      }
      else {
        // check offsets
        if (expectedResults[docUpto][posUpto * 2 + 1] != interval.offsetBegin ||
            expectedResults[docUpto][posUpto * 2 + 2] != interval.offsetEnd)
          fail("Expected next offset in document to be [" + expectedResults[docUpto][posUpto * 2 + 1] + ", " +
              expectedResults[docUpto][posUpto * 2 + 2] + "] but was [" + interval.offsetBegin + ", " + interval.offsetEnd + "]");
      }
    }

    public void assertAllMatched() {
      if (docUpto < expectedResults.length - 1) {
        fail("Expected a hit in document " + expectedResults[docUpto + 1][0]);
      }
    }
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

    //MatchCollector m = new MatchCollector();
    AssertingPositionsCollector c = new AssertingPositionsCollector(expectedResults, AssertingPositionsCollector.AssertionType.OFFSETS);
    searcher.search(q, c);
    c.assertAllMatched();

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

    AssertingPositionsCollector c = new AssertingPositionsCollector(expectedResults, AssertingPositionsCollector.AssertionType.POSITIONS);
    searcher.search(q, c);
    c.assertAllMatched();

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
    reader = writer.getReader();
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

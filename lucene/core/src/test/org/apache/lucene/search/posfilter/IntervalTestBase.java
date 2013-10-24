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
import java.util.Iterator;

public abstract class IntervalTestBase extends LuceneTestCase {

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;

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
    PositionsCollector c = new PositionsCollector(expectedResults.length + 1);
    searcher.search(q, c);

    PositionsCollector.DocPositions[] matches = c.getPositions();
    Assert.assertEquals("Incorrect number of hits", expectedResults.length, c.getNumDocs());
    for (int i = 0; i < expectedResults.length; i++) {
      int expectedDocMatches[] = expectedResults[i];
      int docid = expectedDocMatches[0];
      Iterator<Interval> matchIt = matches[i].positions.iterator();
      for (int j = 1; j < expectedDocMatches.length; j += 2) {
        String expectation = "Expected match at docid " + docid + ", offset " + expectedDocMatches[j];
        Assert.assertTrue(expectation, matchIt.hasNext());
        Interval match = matchIt.next();
        System.err.println(match);
        Assert.assertEquals("Incorrect docid", matches[i].doc, docid);
        Assert.assertEquals("Incorrect match offset", expectedDocMatches[j], match.offsetBegin);
        Assert.assertEquals("Incorrect match end offset", expectedDocMatches[j + 1], match.offsetEnd);
      }
      Assert.assertFalse("Unexpected matches!", matchIt.hasNext());
    }

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

    PositionsCollector c = new PositionsCollector(expectedResults.length + 1);
    searcher.search(q, c);

    PositionsCollector.DocPositions[] matches = c.getPositions();
    Assert.assertEquals("Incorrect number of hits", expectedResults.length, c.getNumDocs());
    for (int i = 0; i < expectedResults.length; i++) {
      int expectedDocMatches[] = expectedResults[i];
      int docid = expectedDocMatches[0];
      Iterator<Interval> matchIt = matches[i].positions.iterator();
      for (int j = 1; j < expectedDocMatches.length; j += 2) {
        String expectation = "Expected match at docid " + docid + ", position " + expectedDocMatches[j];
        Assert.assertTrue(expectation, matchIt.hasNext());
        Interval match = matchIt.next();
        System.err.println(docid + ":" + match);
        Assert.assertEquals("Incorrect docid", matches[i].doc, docid);
        Assert.assertEquals("Incorrect match start position", expectedDocMatches[j], match.begin);
        Assert.assertEquals("Incorrect match end position", expectedDocMatches[j + 1], match.end);
      }
      Assert.assertFalse("Unexpected matches!", matchIt.hasNext());
    }

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
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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

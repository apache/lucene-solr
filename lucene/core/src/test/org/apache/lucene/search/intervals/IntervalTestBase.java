package org.apache.lucene.search.intervals;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Collector;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Copyright (c) 2012 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public abstract class IntervalTestBase extends LuceneTestCase {

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;

  /**
   * Run a query against a searcher, and check that the collected intervals from the query match
   * the expected results.
   * @param q the query
   * @param searcher the searcher
   * @param expectedResults and int[][] detailing the expected results, in the format
   *                        { { docid1, startoffset1, endoffset1, startoffset2, endoffset2, ... },
   *                          { docid2, startoffset1, endoffset1, startoffset2, endoffset2, ...}, ... }
   * @throws IOException
   */
  public static void checkIntervalOffsets(Query q, IndexSearcher searcher, int[][] expectedResults) throws IOException {

    MatchCollector m = new MatchCollector();
    searcher.search(q, m);

    Assert.assertEquals("Incorrect number of hits", expectedResults.length, m.getHitCount());
    Iterator<Match> matchIt = m.getMatches().iterator();
    for (int i = 0; i < expectedResults.length; i++) {
      int docMatches[] = expectedResults[i];
      int docid = docMatches[0];
      for (int j = 1; j < docMatches.length; j += 2) {
        String expectation = "Expected match at docid " + docid + ", position " + docMatches[j];
        Assert.assertTrue(expectation, matchIt.hasNext());
        Match match = matchIt.next();
        System.err.println(match);
        Assert.assertEquals("Incorrect docid", match.docid, docid);
        Assert.assertEquals("Incorrect match offset", docMatches[j], match.startOffset);
        Assert.assertEquals("Incorrect match end offset", docMatches[j + 1], match.endOffset);
      }
    }
    Assert.assertFalse("Unexpected matches!", matchIt.hasNext());

  }

  /**
   * Run a query against a searcher, and check that the collected intervals from the query match
   * the expected results.
   * @param q the query
   * @param searcher the searcher
   * @param expectedResults and int[][] detailing the expected results, in the format
   *                        { { docid1, startpos1, endpos1, startpos2, endpos2, ... },
   *                          { docid2, startpos1, endpos1, startpos2, endpos2, ...}, ... }
   * @throws IOException
   */
  public static void checkIntervals(Query q, IndexSearcher searcher, int[][] expectedResults) throws IOException {

    MatchCollector m = new MatchCollector();
    searcher.search(q, m);

    Assert.assertEquals("Incorrect number of hits", expectedResults.length, m.getHitCount());
    Iterator<Match> matchIt = m.getMatches().iterator();
    for (int i = 0; i < expectedResults.length; i++) {
      int docMatches[] = expectedResults[i];
      int docid = docMatches[0];
      for (int j = 1; j < docMatches.length; j += 2) {
        String expectation = "Expected match at docid " + docid + ", position " + docMatches[j];
        Assert.assertTrue(expectation, matchIt.hasNext());
        Match match = matchIt.next();
        System.out.println(match);
        Assert.assertEquals("Incorrect docid", docid, match.docid);
        Assert.assertEquals("Incorrect match start position", docMatches[j], match.start);
        Assert.assertEquals("Incorrect match end position", docMatches[j + 1], match.end);
      }
    }
    Assert.assertFalse("Unexpected matches!", matchIt.hasNext());

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
    config.setCodec(Codec.forName("Lucene41"));
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

  public static class Match implements Comparable<TestDisjunctionIntervalIterator.Match> {

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

  public static class MatchCollector extends Collector implements IntervalCollector {

    private IntervalIterator intervals;
    private Interval current;
    private Set<Match> matches = new TreeSet<Match>();
    private int hitCount;

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.intervals = scorer.intervals(true);
    }

    @Override
    public void collect(int doc) throws IOException {
      hitCount++;
      intervals.scorerAdvanced(doc);
      while ((current = intervals.next()) != null) {
        //System.out.println(doc + ":" + current);
        intervals.collect(this);
      }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    @Override
    public void collectLeafPosition(Scorer scorer, Interval interval, int docID) {
      matches.add(new Match(docID, interval, false));
    }

    @Override
    public void collectComposite(Scorer scorer, Interval interval, int docID) {
      matches.add(new Match(docID, interval, true));
    }

    @Override
    public Weight.PostingFeatures postingFeatures() {
      return Weight.PostingFeatures.OFFSETS;
    }

    public Set<Match> getMatches() {
      return matches;
    }

    public int getHitCount() {
      return hitCount;
    }
  }
}

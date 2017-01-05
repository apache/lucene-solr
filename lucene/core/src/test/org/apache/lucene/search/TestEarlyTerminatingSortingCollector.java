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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MockRandomMergePolicy;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestEarlyTerminatingSortingCollector extends LuceneTestCase {

  private int numDocs;
  private List<String> terms;
  private Directory dir;
  private final Sort sort = new Sort(new SortField("ndv1", SortField.Type.LONG));
  private RandomIndexWriter iw;
  private IndexReader reader;
  private final int forceMergeMaxSegmentCount = 5;

  private Document randomDocument() {
    final Document doc = new Document();
    doc.add(new NumericDocValuesField("ndv1", random().nextInt(10)));
    doc.add(new NumericDocValuesField("ndv2", random().nextInt(10)));
    doc.add(new StringField("s", RandomPicks.randomFrom(random(), terms), Store.YES));
    return doc;
  }

  private void createRandomIndex(boolean singleSortedSegment) throws IOException {
    dir = newDirectory();
    numDocs = atLeast(150);
    final int numTerms = TestUtil.nextInt(random(), 1, numDocs / 5);
    Set<String> randomTerms = new HashSet<>();
    while (randomTerms.size() < numTerms) {
      randomTerms.add(TestUtil.randomSimpleString(random()));
    }
    terms = new ArrayList<>(randomTerms);
    final long seed = random().nextLong();
    final IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    if (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
      // MockRandomMP randomly wraps the leaf readers which makes merging angry
      iwc.setMergePolicy(newTieredMergePolicy());
    }
    iwc.setMergeScheduler(new SerialMergeScheduler()); // for reproducible tests
    iwc.setIndexSort(sort);
    iw = new RandomIndexWriter(new Random(seed), dir, iwc);
    iw.setDoRandomForceMerge(false); // don't do this, it may happen anyway with MockRandomMP
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = randomDocument();
      iw.addDocument(doc);
      if (i == numDocs / 2 || (i != numDocs - 1 && random().nextInt(8) == 0)) {
        iw.commit();
      }
      if (random().nextInt(15) == 0) {
        final String term = RandomPicks.randomFrom(random(), terms);
        iw.deleteDocuments(new Term("s", term));
      }
    }
    if (singleSortedSegment) {
      // because of deletions, there might still be a single flush segment in
      // the index, although want want a sorted segment so it needs to be merged
      iw.getReader().close(); // refresh
      iw.addDocument(new Document());
      iw.commit();
      iw.addDocument(new Document());
      iw.forceMerge(1);
    }
    else if (random().nextBoolean()) {
      iw.forceMerge(forceMergeMaxSegmentCount);
    }
    reader = iw.getReader();
  }
  
  private void closeIndex() throws IOException {
    reader.close();
    iw.close();
    dir.close();
  }

  public void testEarlyTermination() throws IOException {
    final int iters = atLeast(8);
    for (int i = 0; i < iters; ++i) {
      createRandomIndex(false);
      for (int j = 0; j < iters; ++j) {
        final IndexSearcher searcher = newSearcher(reader);
        final int numHits = TestUtil.nextInt(random(), 1, numDocs);
        final Sort sort = new Sort(new SortField("ndv1", SortField.Type.LONG, false));
        final boolean fillFields = random().nextBoolean();
        final boolean trackDocScores = random().nextBoolean();
        final boolean trackMaxScore = random().nextBoolean();
        final TopFieldCollector collector1 = TopFieldCollector.create(sort, numHits, fillFields, trackDocScores, trackMaxScore);
        final TopFieldCollector collector2 = TopFieldCollector.create(sort, numHits, fillFields, trackDocScores, trackMaxScore);

        final Query query;
        if (random().nextBoolean()) {
          query = new TermQuery(new Term("s", RandomPicks.randomFrom(random(), terms)));
        } else {
          query = new MatchAllDocsQuery();
        }
        searcher.search(query, collector1);
        searcher.search(query, new EarlyTerminatingSortingCollector(collector2, sort, numHits));
        assertTrue(collector1.getTotalHits() >= collector2.getTotalHits());
        assertTopDocsEquals(collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);
      }
      closeIndex();
    }
  }
  
  public void testCanEarlyTerminate() {
    assertTrue(EarlyTerminatingSortingCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("a", SortField.Type.LONG))));

    assertTrue(EarlyTerminatingSortingCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));

    assertTrue(EarlyTerminatingSortingCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));

    assertFalse(EarlyTerminatingSortingCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG, true)),
        new Sort(new SortField("a", SortField.Type.LONG, false))));

    assertFalse(EarlyTerminatingSortingCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG))));

    assertFalse(EarlyTerminatingSortingCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("c", SortField.Type.STRING))));

    assertFalse(EarlyTerminatingSortingCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("c", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));
  }

  public void testEarlyTerminationDifferentSorter() throws IOException {
    createRandomIndex(true);

    Sort sort = new Sort(new SortField("ndv2", SortField.Type.LONG, false));
    Collector c = new EarlyTerminatingSortingCollector(TopFieldCollector.create(sort, 10, true, true, true), sort, 10);
    IndexSearcher searcher = newSearcher(reader);
    Exception e = expectThrows(IllegalStateException.class,
                               () -> {
                                 searcher.search(new MatchAllDocsQuery(), c);
                               });
    assertEquals("Cannot early terminate with sort order <long: \"ndv2\"> if segments are sorted with <long: \"ndv1\">", e.getMessage());
    closeIndex();
  }

  private static void assertTopDocsEquals(ScoreDoc[] scoreDocs1, ScoreDoc[] scoreDocs2) {
    assertEquals(scoreDocs1.length, scoreDocs2.length);
    for (int i = 0; i < scoreDocs1.length; ++i) {
      final ScoreDoc scoreDoc1 = scoreDocs1[i];
      final ScoreDoc scoreDoc2 = scoreDocs2[i];
      assertEquals(scoreDoc1.doc, scoreDoc2.doc);
      assertEquals(scoreDoc1.score, scoreDoc2.score, 0.001f);
    }
  }

  private class TestTerminatedEarlySimpleCollector extends SimpleCollector {
    private boolean collectedSomething;
    public boolean collectedSomething() {
      return collectedSomething;
    }
    @Override
    public void collect(int doc) throws IOException {
      collectedSomething = true;
    }
    @Override
    public boolean needsScores() {
      return false;
    }
  }

  private class TestEarlyTerminatingSortingcollectorQueryTimeout implements QueryTimeout {
    final private boolean shouldExit;
    public TestEarlyTerminatingSortingcollectorQueryTimeout(boolean shouldExit) {
      this.shouldExit = shouldExit;
    }
    public boolean shouldExit() {
      return shouldExit;
    }
  }

  public void testTerminatedEarly() throws IOException {
    final int iters = atLeast(8);
    for (int i = 0; i < iters; ++i) {
      createRandomIndex(true);

      final IndexSearcher searcher = new IndexSearcher(reader); // future TODO: use newSearcher(reader);
      final Query query = new MatchAllDocsQuery(); // search for everything/anything

      final TestTerminatedEarlySimpleCollector collector1 = new TestTerminatedEarlySimpleCollector();
      searcher.search(query, collector1);

      final TestTerminatedEarlySimpleCollector collector2 = new TestTerminatedEarlySimpleCollector();
      final EarlyTerminatingSortingCollector etsCollector = new EarlyTerminatingSortingCollector(collector2, sort, 1);
      searcher.search(query, etsCollector);

      assertTrue("collector1="+collector1.collectedSomething()+" vs. collector2="+collector2.collectedSomething(), collector1.collectedSomething() == collector2.collectedSomething());

      if (collector1.collectedSomething()) {
        // we collected something and since we modestly asked for just one document we should have terminated early
        assertTrue("should have terminated early (searcher.reader="+searcher.reader+")", etsCollector.terminatedEarly());
      }
      closeIndex();
    }
  }

}

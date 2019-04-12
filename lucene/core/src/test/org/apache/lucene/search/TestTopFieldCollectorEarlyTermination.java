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
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MockRandomMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher.TerminationStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;

public class TestTopFieldCollectorEarlyTermination extends LuceneTestCase {

  private int numDocs;
  private List<String> terms;
  private Directory dir;
  private final Sort sort = new Sort(new SortField("ndv1", SortField.Type.LONG));
  private IndexWriter writer;
  private RandomIndexWriter iw;
  private IndexReader reader;
  private static final int FORCE_MERGE_MAX_SEGMENT_COUNT = 5;

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
    createRandomTerms();
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
      iw.forceMerge(1);
    }
    else if (random().nextBoolean()) {
      iw.forceMerge(FORCE_MERGE_MAX_SEGMENT_COUNT);
    }
    reader = iw.getReader();
    if (reader.numDocs() == 0) {
      iw.addDocument(new Document());
      reader.close();
      reader = iw.getReader();
    }
  }
  
  private void closeIndex() throws IOException {
    reader.close();
    if (iw != null) {
      iw.close();
    }
    if (writer != null) {
      writer.close();
    }
    dir.close();
  }

  public void testEarlyTermination() throws IOException {
    doTestEarlyTermination(false);
  }

  public void testEarlyTerminationWhenPaging() throws IOException {
    doTestEarlyTermination(true);
  }


  private void doTestEarlyTermination(boolean paging) throws IOException {
    final int iters = atLeast(8);
    for (int i = 0; i < iters; ++i) {
      createRandomIndex(false);
      int maxSegmentSize = 0;
      for (LeafReaderContext ctx : reader.leaves()) {
        maxSegmentSize = Math.max(ctx.reader().numDocs(), maxSegmentSize);
      }
      for (int j = 0; j < iters; ++j) {
        final IndexSearcher searcher = newSearcher(reader);
        final int numHits = TestUtil.nextInt(random(), 1, numDocs);
        FieldDoc after;
        if (paging) {
          assert searcher.getIndexReader().numDocs() > 0;
          TopFieldDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
          after = (FieldDoc) td.scoreDocs[td.scoreDocs.length - 1];
        } else {
          after = null;
        }
        final TopFieldCollector collector1 = TopFieldCollector.create(sort, numHits, after, TerminationStrategy.NONE);
        final TopFieldCollector collector2 = TopFieldCollector.create(sort, numHits, after, 1, Integer.MAX_VALUE);

        final Query query;
        if (random().nextBoolean()) {
          query = new TermQuery(new Term("s", RandomPicks.randomFrom(random(), terms)));
        } else {
          query = new MatchAllDocsQuery();
        }
        searcher.search(query, collector1);
        searcher.search(query, collector2);
        TopDocs td1 = collector1.topDocs();
        TopDocs td2 = collector2.topDocs();

        assertFalse(collector1.isEarlyTerminated());
        if (paging == false && maxSegmentSize > numHits && query instanceof MatchAllDocsQuery) {
          // Make sure that we sometimes early terminate
          assertTrue(collector2.isEarlyTerminated());
        }
        if (collector2.isEarlyTerminated()) {
          assertTrue(td2.totalHits.value >= td1.scoreDocs.length);
          assertTrue(td2.totalHits.value <= reader.maxDoc());
        } else {
          assertEquals(td2.totalHits.value, td1.totalHits.value);
        }
        assertTopDocsEquals(td1.scoreDocs, td2.scoreDocs);
      }
      closeIndex();
    }
  }
  
  public void testCanEarlyTerminateOnDocId() {
    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        new Sort(SortField.FIELD_DOC)));
    
    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        null));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        null));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("b", SortField.Type.LONG))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        new Sort(new SortField("b", SortField.Type.LONG))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        new Sort(new SortField("b", SortField.Type.LONG), SortField.FIELD_DOC)));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(SortField.FIELD_DOC)));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), SortField.FIELD_DOC),
        new Sort(SortField.FIELD_DOC)));
  }

  public void testCanEarlyTerminateOnPrefix() {
    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("a", SortField.Type.LONG))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG, true)),
        null));
    
    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG, true)),
        new Sort(new SortField("a", SortField.Type.LONG, false))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("c", SortField.Type.STRING))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("c", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));
  }

  private Document numberedDocument(int num, int iterm) {
    final Document doc = new Document();
    doc.add(new NumericDocValuesField("ndv1", num));
    doc.add(new StringField("s", terms.get(iterm % terms.size()), Store.YES));
    return doc;
  }

  private void createRandomTerms() {
    final int numTerms = TestUtil.nextInt(random(), 1, numDocs / 5);
    Set<String> randomTerms = new HashSet<>();
    while (randomTerms.size() < numTerms) {
      randomTerms.add(TestUtil.randomSimpleString(random()));
    }
    terms = new ArrayList<>(randomTerms);
  }

  private void createUniformIndex() throws IOException {
    dir = newDirectory();
    numDocs = atLeast(150);
    int numSegs = atLeast(5);
    // Create segments of random pre-determined sizes so we can distribute the documents uniformly
    // among them
    int docsRemaining = numDocs;
    List<Integer> segmentSizes = new ArrayList<>();
    for (int i = 0; i < numSegs - 1; i++) {
      int size = random().nextInt(docsRemaining - numSegs + i);
      segmentSizes.add(size);
      docsRemaining -= size;
    }
    segmentSizes.add(docsRemaining);
    createRandomTerms();
    final long seed = random().nextLong();
    final IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    // one segment per commit so we can control the segment sizes
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setMaxBufferedDocs(numDocs);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setIndexSort(sort);
    writer = new IndexWriter(dir, iwc);
    for (int seg = 0; seg < numSegs; seg++) {
      int size = segmentSizes.get(seg);
      double step = numDocs / (double) size;
      for (int i = 0; i < size; i++) {
        int num = (int) Math.round(i * step);
        Document doc = numberedDocument(num, num);
        writer.addDocument(doc);
      }
      writer.commit();
    }
    reader = DirectoryReader.open(writer);
  }  

  private void createSkewedIndex() throws IOException {
    dir = newDirectory();
    numDocs = atLeast(150);
    createRandomTerms();
    final long seed = random().nextLong();
    final IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    // one segment per commit so we can control the segment sizes
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setIndexSort(sort);
    writer = new IndexWriter(dir, iwc);
    for (int i = 0; i < numDocs; ++i) {
      // insert the documents in order, so successive segments have increasingly larger documents
      writer.addDocument(numberedDocument(i, i));
      if (random().nextInt(numDocs / 10) == 0) {
        // Make about 10 random-sized segments
        writer.commit();
      }
    }
    reader = DirectoryReader.open(writer);
  }

  public void testProratedEarlyTermination() throws IOException {
    final int iters = atLeast(8);
    for (int i = 0; i < iters; ++i) {
      createUniformIndex();
      ExecutorService exec = Executors.newFixedThreadPool(8, new NamedThreadFactory("TestTopFieldCollectorEarlyTermination"));
      try {
        final IndexSearcher searcher = new IndexSearcher(reader, exec);
        for (int j = 0; j < 10 * iters; ++j) {
          final int numHits = TestUtil.nextInt(random(), 1, 50);
          final CollectorManager<TopFieldCollector, TopFieldDocs> collectorManager =
              TopFieldCollector.createManager(sort, numHits, null, TerminationStrategy.RESULT_COUNT_PRORATED);

          final Query query = new MatchAllDocsQuery();
          TopDocs expected = searcher.search(query, numHits, sort);
          TopDocs td = searcher.search(query, collectorManager);
          assertTopDocsEquals(expected.scoreDocs, td.scoreDocs);
        }
      } finally {
        exec.shutdown();
        closeIndex();
      }
    }
  }

  public void testNoProrating() throws IOException {
    final int iters = atLeast(8);
    for (int i = 0; i < iters; ++i) {
      createSkewedIndex();
      ExecutorService exec = Executors.newFixedThreadPool(8, new NamedThreadFactory("TestTopFieldCollectorEarlyTermination"));
      try {
        final IndexSearcher searcher = new IndexSearcher(reader, exec);
        for (int j = 0; j < 10 * iters; ++j) {
          final int numHits = TestUtil.nextInt(random(), 5, 50);
          // We do not see any ranking errors when prorating is disabled even when the index is skewed
          final Query query = new MatchAllDocsQuery();
          TopDocs expected = searcher.search(query, numHits, sort);
          final CollectorManager<TopFieldCollector, TopFieldDocs> noProratingManager =
              TopFieldCollector.createManager(sort, numHits, null, TerminationStrategy.HIT_COUNT);
          TopDocs td2 = searcher.search(query, noProratingManager);
          assertTopDocsEquals(expected.scoreDocs, td2.scoreDocs);
        }
      } finally {
        exec.shutdown();
        closeIndex();
      }
    }
  }

  private static void assertTopDocsEquals(ScoreDoc[] scoreDocs1, ScoreDoc[] scoreDocs2) {
    assertEquals("result set sizes differ", scoreDocs1.length, scoreDocs2.length);
    for (int i = 0; i < scoreDocs1.length; ++i) {
      final ScoreDoc scoreDoc1 = scoreDocs1[i];
      final ScoreDoc scoreDoc2 = scoreDocs2[i];
      assertEquals("incorrect result #" + i, scoreDoc1.doc, scoreDoc2.doc);
      assertEquals("incorrect score for result #" + i, scoreDoc1.score, scoreDoc2.score, 0f);
      //System.out.println(scoreDoc1.doc);
    }
  }

}

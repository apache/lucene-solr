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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MockRandomMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;

public class TestGlobalStateCMEarlyTermination extends LuceneTestCase {
  private int numDocs;
  private List<String> terms;
  private Directory dir;
  private final Sort sort = new Sort(new SortField("ndv1", SortField.Type.LONG));
  private RandomIndexWriter iw;
  private IndexReader reader;
  private static final int FORCE_MERGE_MAX_SEGMENT_COUNT = 5;

  private Document randomDocument() {
    final Document doc = new Document();
    doc.add(new NumericDocValuesField("ndv1", random().nextInt(10)));
    doc.add(new NumericDocValuesField("ndv2", random().nextInt(10)));
    doc.add(new StringField("s", RandomPicks.randomFrom(random(), terms), Field.Store.YES));
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
    iw.close();
    dir.close();
  }

  public void testGlobalStateEarlyTermination() throws IOException {
    createRandomIndex(false);
    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("TestGlobalStateCMEarlyTermination"));
    final IndexSearcher searcher = new IndexSearcher(reader, service);
    final int numHits = TestUtil.nextInt(random(), 1, numDocs);
    final int totalHitsThreshold = TestUtil.nextInt(random(), 1, (numHits - 1));

    final GlobalStateCollectorManager collectorManager = new GlobalStateCollectorManager(sort, numHits, totalHitsThreshold);
    final TopFieldCollector collector1 = TopFieldCollector.create(sort, numHits, null, Integer.MAX_VALUE);

    final Query query;
    if (random().nextBoolean()) {
      query = new TermQuery(new Term("s", RandomPicks.randomFrom(random(), terms)));
    } else {
      query = new MatchAllDocsQuery();
    }
    TopDocs td2 = searcher.search(query, collectorManager);
    searcher.search(query, collector1);
    TopDocs td1 = collector1.topDocs();

    assertFalse(collector1.isEarlyTerminated());
    assertTrue(td2.totalHits.value >= td1.scoreDocs.length);
    assertTrue(td2.totalHits.value <= reader.maxDoc());
    CheckHits.checkEqual(query, td1.scoreDocs, td2.scoreDocs);
    closeIndex();
    service.shutdown();
  }

  public void testGlobalStateSingleSegmentEarlyTermination() throws IOException {
    createRandomIndex(true);
    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("TestGlobalStateCMEarlyTermination"));
    final IndexSearcher searcher = new IndexSearcher(reader, service);
    final int numHits = TestUtil.nextInt(random(), 1, numDocs);
    final int totalHitsThreshold = TestUtil.nextInt(random(), 1, (numHits - 1));

    final GlobalStateCollectorManager collectorManager = new GlobalStateCollectorManager(sort, numHits, totalHitsThreshold);
    final TopFieldCollector collector1 = TopFieldCollector.create(sort, numHits, null, Integer.MAX_VALUE);

    final Query query;
    if (random().nextBoolean()) {
      query = new TermQuery(new Term("s", RandomPicks.randomFrom(random(), terms)));
    } else {
      query = new MatchAllDocsQuery();
    }
    TopDocs td2 = searcher.search(query, collectorManager);
    searcher.search(query, collector1);
    TopDocs td1 = collector1.topDocs();

    assertFalse(collector1.isEarlyTerminated());
    assertTrue(td2.totalHits.value >= td1.scoreDocs.length);
    assertTrue(td2.totalHits.value <= reader.maxDoc());
    CheckHits.checkEqual(query, td1.scoreDocs, td2.scoreDocs);
    closeIndex();
    service.shutdown();
  }
}

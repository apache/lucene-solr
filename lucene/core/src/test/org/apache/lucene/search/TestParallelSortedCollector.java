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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

public class TestParallelSortedCollector extends LuceneTestCase  {

  public void testIsApplicable() throws IOException {
    Sort indexSort = new Sort(new SortField("N", SortField.Type.INT, random().nextBoolean()),
        new SortField("NN", SortField.Type.INT, random().nextBoolean()));
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexSort(indexSort);
    try (Directory dir = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc)) {
      w.addDocument(new Document());
      w.commit();
      w.addDocument(new Document());
      try (IndexReader reader = w.getReader()) {
        // exactly the index sort, with lots of hits
        assertTrue(ParallelSortedCollector.isApplicable(indexSort, 1000, reader.leaves()));

        // too few hits to apply
        assertFalse(ParallelSortedCollector.isApplicable(indexSort, 10, reader.leaves()));

        // a prefix of the index sort
        assertTrue(ParallelSortedCollector.isApplicable(new Sort(indexSort.getSort()[0]), 1000, reader.leaves()));

        // the index sort prefix, but in reverse
        SortField reversedN = new SortField("N", SortField.Type.INT, !indexSort.getSort()[0].reverse);
        assertFalse(ParallelSortedCollector.isApplicable(new Sort(reversedN), 1000, reader.leaves()));

        // The same field, but with Long precision. Probably we could support this combination though:
        SortField longN = new SortField("N", SortField.Type.LONG, !indexSort.getSort()[0].reverse);
        assertFalse(ParallelSortedCollector.isApplicable(new Sort(longN), 1000, reader.leaves()));

        // docid sort
        assertTrue(ParallelSortedCollector.isApplicable(new Sort(SortField.FIELD_DOC), 1000, reader.leaves()));

        // score (relevance) sort
        assertFalse(ParallelSortedCollector.isApplicable(new Sort(SortField.FIELD_SCORE), 1000, reader.leaves()));
      }
    }
    Sort nonNumericSort = new Sort(new SortField("S", SortField.Type.STRING));
    iwc = newIndexWriterConfig();
    iwc.setIndexSort(nonNumericSort);
    try (Directory dir = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc)) {
      w.addDocument(new Document());
      w.commit();
      w.addDocument(new Document());
      try (IndexReader reader = w.getReader()) {
        // exactly the index sort, with lots of hits, but non-numeric
        assertFalse(ParallelSortedCollector.isApplicable(nonNumericSort, 1000, reader.leaves()));

        // docid sort
        assertTrue(ParallelSortedCollector.isApplicable(new Sort(SortField.FIELD_DOC), 1000, reader.leaves()));

        // score (relevance) sort
        assertFalse(ParallelSortedCollector.isApplicable(new Sort(SortField.FIELD_SCORE), 1000, reader.leaves()));
      }
    }
  }

  public void testSortNoResults() throws Exception {
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      TopDocsCollector<Entry> tdc = ParallelSortedCollector.create(sort[i], 10, null, HitsThresholdChecker.createShared(Integer.MAX_VALUE),
                                                                   new ParallelSortedCollector.MaxScoreTerminator(10, 0, null));
      TopDocs td = tdc.topDocs();
      assertEquals(0, td.totalHits.value);
    }
  }

  public void testTotalHits() throws Exception {
    Directory dir = newDirectory();
    Sort sort = new Sort(new SortField("foo", SortField.Type.LONG));
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig()
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setIndexSort(sort));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 3));
    w.addDocuments(Arrays.asList(doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(2, reader.leaves().size());
    w.close();

    for (int totalHitsThreshold = 0; totalHitsThreshold < 20; ++ totalHitsThreshold) {
      for (FieldDoc after : new FieldDoc[] { null, new FieldDoc(4, Float.NaN, new Object[] { 2L })}) {
        ParallelSortedCollector collector = ParallelSortedCollector.create(sort, 2, after,
            HitsThresholdChecker.create(totalHitsThreshold),
            new ParallelSortedCollector.MaxScoreTerminator(2, totalHitsThreshold, 0));
        assertEquals(1, collector.maxScoreTerminator.interval); // check on every hit to ensure maximal early termination
        ScoreAndDoc scorer = new ScoreAndDoc();

        LeafCollector leafCollector1 = collector.getLeafCollector(reader.leaves().get(0));
        leafCollector1.setScorer(scorer);

        scorer.doc = 0;
        scorer.score = 3;
        leafCollector1.collect(0);

        scorer.doc = 1;
        scorer.score = 3;
        if (totalHitsThreshold <= 2) {
          expectThrows(CollectionTerminatedException.class, () -> leafCollector1.collect(1));
          assertEquals(new TotalHits(2, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), collector.topDocs().totalHits);
          continue;
        } else {
          leafCollector1.collect(1);
        }

        LeafCollector leafCollector2 = collector.getLeafCollector(reader.leaves().get(1));
        leafCollector2.setScorer(scorer);

        scorer.doc = 1;
        scorer.score = 3;
        if (totalHitsThreshold == 3) {
          expectThrows(CollectionTerminatedException.class, () -> leafCollector2.collect(1));
          assertEquals(new TotalHits(3, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), collector.topDocs().totalHits);
          continue;
        } else {
          leafCollector2.collect(1);
        }

        scorer.doc = 5;
        scorer.score = 4;
        if (totalHitsThreshold == 4) {
          expectThrows(CollectionTerminatedException.class, () -> leafCollector2.collect(1));
          TopDocs topDocs = collector.topDocs();
          assertEquals(new TotalHits(4, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), topDocs.totalHits);
          continue;
        } else {
          leafCollector2.collect(1);
        }

        TopDocs topDocs = collector.topDocs();
        assertEquals(new TotalHits(4, TotalHits.Relation.EQUAL_TO), topDocs.totalHits);
      }
    }

    reader.close();
    dir.close();
  }

  public void testRandomMaxScoreTermination() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    boolean ascending = random().nextBoolean();
    Sort indexSort = new Sort(new SortField("N", SortField.Type.INT, ascending),
                              new SortField("NN", SortField.Type.INT, ascending));
    iwc.setIndexSort(indexSort);
    // Sometimes index the sort key in order since this is an adversarial case for strategies that rely on
    // fair distribution in the index
    boolean randomizeSortKey = random().nextBoolean();
    try (Directory dir = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc)) {
      int numDocs = atLeast(1000);
      int sortKeyRange = random().nextInt(numDocs); // have some duplicate sort key values, to test docid tiebreaking?
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        if (usually()) {
          doc.add(new StringField("f", "A", Field.Store.NO));
        }
        if (rarely()) {
          doc.add(new StringField("f", "B", Field.Store.NO));
        }
        if (usually()) {
          if (randomizeSortKey) {
            doc.add(new NumericDocValuesField("N", random().nextInt(sortKeyRange)));
          } else {
            doc.add(new NumericDocValuesField("N", i));
          }
        }
        doc.add(new NumericDocValuesField("NN", random().nextInt()));
        if (random().nextFloat() < 3f / numDocs) {
          w.commit();
        }
        w.addDocument(doc);
      }
      try (IndexReader indexReader = w.getReader()) {
        Query[] queries = new Query[]{
            new TermQuery(new Term("f", "A")),
            new TermQuery(new Term("f", "B")),
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("f", "A")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("f", "B")), BooleanClause.Occur.MUST)
                .build()
        };
        int numHits = atLeast(100);
        Sort[] sorts = new Sort[]{new Sort(new SortField[]{SortField.FIELD_DOC}), indexSort,
                                  new Sort(new SortField[]{new SortField("N", SortField.Type.INT, ascending)})};
        for (Query query : queries) {
          Sort sort = sorts[random().nextInt(sorts.length)];
          FieldDoc after;
          if (random().nextBoolean() && sort.getSort().length == 1) {
            after = new FieldDoc(random().nextInt(numDocs), 0f, new Object[]{random().nextInt(sortKeyRange)});
          } else {
            after = null;
          }
          System.out.println("query=" + query + " sort=" + sort + " after=" + after + " indexSort=" + indexSort);
          TopDocs tdc = doConcurrentSearchWithThreshold(numHits, after, 0, query, sort, indexReader);
          TopDocs tdc2 = doSearchWithThreshold(numHits, after, 0, query, sort, indexReader);

          assertTrue(tdc.totalHits.value > 0);
          assertTrue(tdc2.totalHits.value > 0);
          //System.out.println("sort=" + sort + " randomizeSortKey=" + randomizeSortKey);
          CheckHits.checkEqual(query, tdc.scoreDocs, tdc2.scoreDocs);
        }
      }
    }
  }

  private TopDocs doSearchWithThreshold(int numResults, FieldDoc after, int threshold, Query q, Sort sort, IndexReader indexReader) throws IOException {
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TopFieldCollector tdc = TopFieldCollector.create(sort, numResults, after, threshold);
    searcher.search(q, tdc);
    return tdc.topDocs();
  }

  private TopDocs doConcurrentSearchWithThreshold(int numResults, FieldDoc after, int threshold, Query q, Sort sort, IndexReader indexReader) throws IOException {
    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("TestTopDocsCollector"));
    try {
      IndexSearcher searcher = new IndexSearcher(indexReader, service);

      CollectorManager<? extends Collector, TopFieldDocs> collectorManager = ParallelSortedCollector.createManager(sort, numResults,
          after, threshold, 4);

      TopDocs tdc = searcher.search(q, collectorManager);

      return tdc;
    } finally {
      service.shutdown();
    }
  }

}

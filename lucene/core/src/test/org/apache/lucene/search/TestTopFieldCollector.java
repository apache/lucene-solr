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
import java.util.Comparator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.SortField.FIELD_SCORE;

public class TestTopFieldCollector extends LuceneTestCase {
  private IndexSearcher is;
  private IndexReader ir;
  private Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      iw.addDocument(doc);
    }
    ir = iw.getReader();
    iw.close();
    is = newSearcher(ir);
  }
  
  @Override
  public void tearDown() throws Exception {
    ir.close();
    dir.close();
    super.tearDown();
  }

  private TopFieldCollector doSearchWithThreshold(int numResults, int thresHold, Query q, Sort sort, IndexReader indexReader) throws IOException {
    IndexSearcher searcher = newSearcher(indexReader, true, true, false);
    TopFieldCollector tdc = TopFieldCollector.create(sort, numResults, thresHold);
    searcher.search(q, tdc);
    return tdc;
  }

  private TopDocs doConcurrentSearchWithThreshold(int numResults, int threshold, Query q, Sort sort, IndexReader indexReader) throws IOException {
    IndexSearcher searcher = newSearcher(indexReader, true, true, true);
    CollectorManager<TopFieldCollector,TopFieldDocs> collectorManager = TopFieldCollector.createSharedManager(sort, numResults,
            null, threshold);
    TopDocs tdc = searcher.search(q, collectorManager);
    return tdc;
  }
  
  public void testSortWithoutFillFields() throws Exception {
    
    // There was previously a bug in TopFieldCollector when fillFields was set
    // to false - the same doc and score was set in ScoreDoc[] array. This test
    // asserts that if fillFields is false, the documents are set properly. It
    // does not use Searcher's default search methods (with Sort) since all set
    // fillFields to true.
    Sort[] sort = new Sort[] { new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, Integer.MAX_VALUE);
      
      is.search(q, tdc);
      
      ScoreDoc[] sd = tdc.topDocs().scoreDocs;
      for(int j = 1; j < sd.length; j++) {
        assertTrue(sd[j].doc != sd[j - 1].doc);
      }
      
    }
  }

  public void testSort() throws Exception {

    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, Integer.MAX_VALUE);
      
      is.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(Float.isNaN(sd[j].score));
      }
    }
  }

  public void testSharedHitcountCollector() throws Exception {
    IndexSearcher concurrentSearcher = newSearcher(ir, true, true, true);

    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, Integer.MAX_VALUE);

      is.search(q, tdc);

      CollectorManager<TopFieldCollector,TopFieldDocs> tsdc = TopFieldCollector.createSharedManager(sort[i], 10, null, Integer.MAX_VALUE);

      TopDocs td = tdc.topDocs();
      TopDocs td2 = concurrentSearcher.search(q, tsdc);
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(Float.isNaN(sd[j].score));
      }

      CheckHits.checkEqual(q, td.scoreDocs, td2.scoreDocs);
    }
  }

  public void testSortWithoutTotalHitTracking() throws Exception {
    Sort sort = new Sort(SortField.FIELD_DOC);
    for(int i = 0; i < 2; i++) {
      Query q = new MatchAllDocsQuery();
      // check that setting trackTotalHits to false does not throw an NPE because
      // the index is not sorted
      TopDocsCollector<Entry> tdc;
      if (i % 2 == 0) {
        tdc =  TopFieldCollector.create(sort, 10, 1);
      } else {
        FieldDoc fieldDoc = new FieldDoc(1, Float.NaN, new Object[] { 1 });
        tdc = TopFieldCollector.create(sort, 10, fieldDoc, 1);
      }

      is.search(q, tdc);

      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(Float.isNaN(sd[j].score));
      }
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
        TopFieldCollector collector = TopFieldCollector.create(sort, 2, after, totalHitsThreshold);
        ScoreAndDoc scorer = new ScoreAndDoc();

        LeafCollector leafCollector1 = collector.getLeafCollector(reader.leaves().get(0));
        leafCollector1.setScorer(scorer);

        scorer.doc = 0;
        scorer.score = 3;
        leafCollector1.collect(0);

        scorer.doc = 1;
        scorer.score = 3;
        leafCollector1.collect(1);

        LeafCollector leafCollector2 = collector.getLeafCollector(reader.leaves().get(1));
        leafCollector2.setScorer(scorer);

        scorer.doc = 1;
        scorer.score = 3;
        if (totalHitsThreshold < 3) {
          expectThrows(CollectionTerminatedException.class, () -> leafCollector2.collect(1));
          TopDocs topDocs = collector.topDocs();
          assertEquals(new TotalHits(3, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), topDocs.totalHits);
          continue;
        } else {
          leafCollector2.collect(1);
        }

        scorer.doc = 5;
        scorer.score = 4;
        if (totalHitsThreshold == 3) {
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

  private static class ScoreAndDoc extends Scorable {
    int doc = -1;
    float score;
    Float minCompetitiveScore = null;

    @Override
    public void setMinCompetitiveScore(float minCompetitiveScore) {
      this.minCompetitiveScore = minCompetitiveScore;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public float score() throws IOException {
      return score;
    }
  }

  public void testSetMinCompetitiveScore() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(2, reader.leaves().size());
    w.close();

    Sort sort = new Sort(FIELD_SCORE, new SortField("foo", SortField.Type.LONG));
    TopFieldCollector collector = TopFieldCollector.create(sort, 2, null, 2);
    ScoreAndDoc scorer = new ScoreAndDoc();

    LeafCollector leafCollector = collector.getLeafCollector(reader.leaves().get(0));
    leafCollector.setScorer(scorer);
    assertNull(scorer.minCompetitiveScore);

    scorer.doc = 0;
    scorer.score = 1;
    leafCollector.collect(0);
    assertNull(scorer.minCompetitiveScore);

    scorer.doc = 1;
    scorer.score = 2;
    leafCollector.collect(1);
    assertNull(scorer.minCompetitiveScore);
    
    scorer.doc = 2;
    scorer.score = 3;
    leafCollector.collect(2);
    assertEquals(2f, scorer.minCompetitiveScore, 0f);

    scorer.doc = 3;
    scorer.score = 0.5f;
    // Make sure we do not call setMinCompetitiveScore for non-competitive hits
    scorer.minCompetitiveScore = Float.NaN;
    leafCollector.collect(3);
    assertTrue(Float.isNaN(scorer.minCompetitiveScore));

    scorer.doc = 4;
    scorer.score = 4;
    leafCollector.collect(4);
    assertEquals(3f, scorer.minCompetitiveScore, 0f);

    // Make sure the min score is set on scorers on new segments
    scorer = new ScoreAndDoc();
    leafCollector = collector.getLeafCollector(reader.leaves().get(1));
    leafCollector.setScorer(scorer);
    assertEquals(3f, scorer.minCompetitiveScore, 0f);

    scorer.doc = 0;
    scorer.score = 1;
    leafCollector.collect(0);
    assertEquals(3f, scorer.minCompetitiveScore, 0f);

    scorer.doc = 1;
    scorer.score = 4;
    leafCollector.collect(1);
    assertEquals(4f, scorer.minCompetitiveScore, 0f);

    reader.close();
    dir.close();
  }

  public void testTotalHitsWithScore() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(2, reader.leaves().size());
    w.close();

    for (int totalHitsThreshold = 0; totalHitsThreshold < 20; ++ totalHitsThreshold) {
      Sort sort = new Sort(FIELD_SCORE, new SortField("foo", SortField.Type.LONG));
      TopFieldCollector collector = TopFieldCollector.create(sort, 2, null, totalHitsThreshold);
      ScoreAndDoc scorer = new ScoreAndDoc();

      LeafCollector leafCollector = collector.getLeafCollector(reader.leaves().get(0));
      leafCollector.setScorer(scorer);

      scorer.doc = 0;
      scorer.score = 3;
      leafCollector.collect(0);

      scorer.doc = 1;
      scorer.score = 3;
      leafCollector.collect(1);

      leafCollector = collector.getLeafCollector(reader.leaves().get(1));
      leafCollector.setScorer(scorer);

      scorer.doc = 1;
      scorer.score = 3;
      leafCollector.collect(1);

      scorer.doc = 5;
      scorer.score = 4;
      leafCollector.collect(1);

      TopDocs topDocs = collector.topDocs();
      assertEquals(4, topDocs.totalHits.value);
      assertEquals(totalHitsThreshold < 4, scorer.minCompetitiveScore != null);
      assertEquals(totalHitsThreshold < 4 ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation);
    }

    reader.close();
    dir.close();
  }

  public void testSortNoResults() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, Integer.MAX_VALUE);
      TopDocs td = tdc.topDocs();
      assertEquals(0, td.totalHits.value);
    }
  }

  public void testComputeScoresOnlyOnce() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    StringField text = new StringField("text", "foo", Store.NO);
    doc.add(text);
    NumericDocValuesField relevance = new NumericDocValuesField("relevance", 1);
    doc.add(relevance);
    w.addDocument(doc);
    text.setStringValue("bar");
    w.addDocument(doc);
    text.setStringValue("baz");
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    Query foo = new TermQuery(new Term("text", "foo"));
    Query bar = new TermQuery(new Term("text", "bar"));
    foo = new BoostQuery(foo, 2);
    Query baz = new TermQuery(new Term("text", "baz"));
    baz = new BoostQuery(baz, 3);
    Query query = new BooleanQuery.Builder()
        .add(foo, Occur.SHOULD)
        .add(bar, Occur.SHOULD)
        .add(baz, Occur.SHOULD)
        .build();
    final IndexSearcher searcher = new IndexSearcher(reader);
    for (Sort sort : new Sort[] {new Sort(FIELD_SCORE), new Sort(new SortField("f", SortField.Type.SCORE))}) {
      final TopFieldCollector topCollector = TopFieldCollector.create(sort, TestUtil.nextInt(random(), 1, 2), Integer.MAX_VALUE);
      final Collector assertingCollector = new Collector() {
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
          final LeafCollector in = topCollector.getLeafCollector(context);
          return new FilterLeafCollector(in) {
            @Override
            public void setScorer(final Scorable scorer) throws IOException {
              Scorable s = new FilterScorable(scorer) {

                int lastComputedDoc = -1;

                @Override
                public float score() throws IOException {
                  if (lastComputedDoc == docID()) {
                    throw new AssertionError("Score computed twice on " + docID());
                  }
                  lastComputedDoc = docID();
                  return scorer.score();
                }

              };
              super.setScorer(s);
            }
          };
        }
        @Override
        public ScoreMode scoreMode() {
          return topCollector.scoreMode();
        }
      };
      searcher.search(query, assertingCollector);
    }
    reader.close();
    w.close();
    dir.close();
  }

  public void testPopulateScores() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    TextField field = new TextField("f", "foo bar", Store.NO);
    doc.add(field);
    NumericDocValuesField sortField = new NumericDocValuesField("sort", 0);
    doc.add(sortField);
    w.addDocument(doc);

    field.setStringValue("");
    sortField.setLongValue(3);
    w.addDocument(doc);

    field.setStringValue("foo foo bar");
    sortField.setLongValue(2);
    w.addDocument(doc);

    w.flush();

    field.setStringValue("foo");
    sortField.setLongValue(2);
    w.addDocument(doc);

    field.setStringValue("bar bar bar");
    sortField.setLongValue(0);
    w.addDocument(doc);

    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    for (String queryText : new String[] { "foo", "bar" }) {
      Query query = new TermQuery(new Term("f", queryText));
      for (boolean reverse : new boolean[] {false, true}) {
        ScoreDoc[] sortedByDoc = searcher.search(query, 10).scoreDocs;
        Arrays.sort(sortedByDoc, Comparator.comparingInt(sd -> sd.doc));

        Sort sort = new Sort(new SortField("sort", SortField.Type.LONG, reverse));
        ScoreDoc[] sortedByField = searcher.search(query, 10, sort).scoreDocs;
        ScoreDoc[] sortedByFieldClone = sortedByField.clone();
        TopFieldCollector.populateScores(sortedByFieldClone, searcher, query);
        for (int i = 0; i < sortedByFieldClone.length; ++i) {
          assertEquals(sortedByFieldClone[i].doc, sortedByField[i].doc);
          assertSame(((FieldDoc) sortedByFieldClone[i]).fields, ((FieldDoc) sortedByField[i]).fields);
          assertEquals(sortedByFieldClone[i].score,
              sortedByDoc[Arrays.binarySearch(sortedByDoc, sortedByFieldClone[i], Comparator.comparingInt(sd -> sd.doc))].score, 0f);
        }
      }
    }

    reader.close();
    dir.close();
  }

  public void testConcurrentMinScore() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(3, reader.leaves().size());
    w.close();

    Sort sort = new Sort(SortField.FIELD_SCORE, SortField.FIELD_DOC);
    CollectorManager<TopFieldCollector, TopFieldDocs> manager =
        TopFieldCollector.createSharedManager(sort, 2, null, 0);
    TopFieldCollector collector = manager.newCollector();
    TopFieldCollector collector2 = manager.newCollector();
    assertTrue(collector.minScoreAcc == collector2.minScoreAcc);
    MaxScoreAccumulator minValueChecker = collector.minScoreAcc;
    // force the check of the global minimum score on every round
    minValueChecker.modInterval = 0;

    ScoreAndDoc scorer = new ScoreAndDoc();
    ScoreAndDoc scorer2 = new ScoreAndDoc();

    LeafCollector leafCollector = collector.getLeafCollector(reader.leaves().get(0));
    leafCollector.setScorer(scorer);
    LeafCollector leafCollector2 = collector2.getLeafCollector(reader.leaves().get(1));
    leafCollector2.setScorer(scorer2);

    scorer.doc = 0;
    scorer.score = 3;
    leafCollector.collect(0);
    assertNull(minValueChecker.get());
    assertNull(scorer.minCompetitiveScore);

    scorer2.doc = 0;
    scorer2.score = 6;
    leafCollector2.collect(0);
    assertNull(minValueChecker.get());
    assertNull(scorer2.minCompetitiveScore);

    scorer.doc = 1;
    scorer.score = 2;
    leafCollector.collect(1);
    assertEquals(2f, minValueChecker.get().score, 0f);
    assertEquals(2f, scorer.minCompetitiveScore, 0f);
    assertNull(scorer2.minCompetitiveScore);

    scorer2.doc = 1;
    scorer2.score = 9;
    leafCollector2.collect(1);
    assertEquals(6f, minValueChecker.get().score, 0f);
    assertEquals(2f, scorer.minCompetitiveScore, 0f);
    assertEquals(6f, scorer2.minCompetitiveScore, 0f);

    scorer2.doc = 2;
    scorer2.score = 7;
    leafCollector2.collect(2);
    assertEquals(7f, minValueChecker.get().score, 0f);
    assertEquals(2f, scorer.minCompetitiveScore, 0f);
    assertEquals(7f, scorer2.minCompetitiveScore, 0f);

    scorer2.doc = 3;
    scorer2.score = 1;
    leafCollector2.collect(3);
    assertEquals(7f, minValueChecker.get().score, 0f);
    assertEquals(2f, scorer.minCompetitiveScore, 0f);
    assertEquals(7f, scorer2.minCompetitiveScore, 0f);

    scorer.doc = 2;
    scorer.score = 10;
    leafCollector.collect(2);
    assertEquals(7f, minValueChecker.get().score, 0f);
    assertEquals(7f, scorer.minCompetitiveScore, 0f);
    assertEquals(7f, scorer2.minCompetitiveScore, 0f);

    scorer.doc = 3;
    scorer.score = 11;
    leafCollector.collect(3);
    assertEquals(10f, minValueChecker.get().score, 0f);
    assertEquals(10f, scorer.minCompetitiveScore, 0f);
    assertEquals(7f, scorer2.minCompetitiveScore, 0f);

    TopFieldCollector collector3 = manager.newCollector();
    LeafCollector leafCollector3 = collector3.getLeafCollector(reader.leaves().get(2));
    ScoreAndDoc scorer3 = new ScoreAndDoc();
    leafCollector3.setScorer(scorer3);
    assertEquals(10f, scorer3.minCompetitiveScore, 0f);

    scorer3.doc = 0;
    scorer3.score = 1f;
    leafCollector3.collect(0);
    assertEquals(10f, minValueChecker.get().score, 0f);
    assertEquals(10f, scorer3.minCompetitiveScore, 0f);

    scorer.doc = 4;
    scorer.score = 11;
    leafCollector.collect(4);
    assertEquals(11f, minValueChecker.get().score, 0f);
    assertEquals(11f, scorer.minCompetitiveScore, 0f);
    assertEquals(7f, scorer2.minCompetitiveScore, 0f);
    assertEquals(10f, scorer3.minCompetitiveScore, 0f);

    scorer3.doc = 1;
    scorer3.score = 2f;
    leafCollector3.collect(1);
    assertEquals(11f, minValueChecker.get().score, 0f);
    assertEquals(11f, scorer.minCompetitiveScore, 0f);
    assertEquals(7f, scorer2.minCompetitiveScore, 0f);
    assertEquals(11f, scorer3.minCompetitiveScore, 0f);


    TopFieldDocs topDocs = manager.reduce(Arrays.asList(collector, collector2, collector3));
    assertEquals(11, topDocs.totalHits.value);
    assertEquals(new TotalHits(11, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), topDocs.totalHits);

    leafCollector.setScorer(scorer);
    leafCollector2.setScorer(scorer2);
    leafCollector3.setScorer(scorer3);

    reader.close();
    dir.close();
  }

  public void testRandomMinCompetitiveScore() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      int numAs = 1 + random().nextInt(5);
      int numBs = random().nextFloat() < 0.5f ?  0 : 1 + random().nextInt(5);
      int numCs = random().nextFloat() < 0.1f ?  0 : 1 + random().nextInt(5);
      Document doc = new Document();
      for (int j = 0; j < numAs; ++j) {
        doc.add(new StringField("f", "A", Field.Store.NO));
      }
      for (int j = 0; j < numBs; ++j) {
        doc.add(new StringField("f", "B", Field.Store.NO));
      }
      for (int j = 0; j < numCs; ++j) {
        doc.add(new StringField("f", "C", Field.Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader indexReader = w.getReader();
    w.close();
    Query[] queries = new Query[]{
        new TermQuery(new Term("f", "A")),
        new TermQuery(new Term("f", "B")),
        new TermQuery(new Term("f", "C")),
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f", "A")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("f", "B")), BooleanClause.Occur.SHOULD)
            .build()
    };
    for (Query query : queries) {
      Sort sort = new Sort(new SortField[]{SortField.FIELD_SCORE, SortField.FIELD_DOC});
      TopFieldCollector fieldCollector = doSearchWithThreshold(5, 0, query, sort, indexReader);
      TopDocs tdc = doConcurrentSearchWithThreshold(5, 0, query, sort, indexReader);
      TopDocs tdc2 = fieldCollector.topDocs();

      assertTrue(tdc.totalHits.value > 0);
      assertTrue(tdc2.totalHits.value > 0);
      CheckHits.checkEqual(query, tdc.scoreDocs, tdc2.scoreDocs);
    }

    indexReader.close();
    dir.close();
  }
  
  public void testRelationVsTopDocsCount() throws Exception {
    Sort sort = new Sort(SortField.FIELD_SCORE, SortField.FIELD_DOC);
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      Document doc = new Document();
      doc.add(new TextField("f", "foo bar", Store.NO));
      w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc));
      w.flush();
      w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc));
      w.flush();
      
      try (IndexReader reader = DirectoryReader.open(w)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopFieldCollector collector = TopFieldCollector.create(sort, 2, 10);
        searcher.search(new TermQuery(new Term("f", "foo")), collector);
        assertEquals(10, collector.totalHits);
        assertEquals(TotalHits.Relation.EQUAL_TO, collector.totalHitsRelation);
        
        collector = TopFieldCollector.create(sort, 2, 2);
        searcher.search(new TermQuery(new Term("f", "foo")), collector);
        assertTrue(10 >= collector.totalHits);
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, collector.totalHitsRelation);
        
        collector = TopFieldCollector.create(sort, 10, 2);
        searcher.search(new TermQuery(new Term("f", "foo")), collector);
        assertEquals(10, collector.totalHits);
        assertEquals(TotalHits.Relation.EQUAL_TO, collector.totalHitsRelation);
      }
    }
  }

}

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
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

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
  
  public void testSortWithoutFillFields() throws Exception {
    
    // There was previously a bug in TopFieldCollector when fillFields was set
    // to false - the same doc and score was set in ScoreDoc[] array. This test
    // asserts that if fillFields is false, the documents are set properly. It
    // does not use Searcher's default search methods (with Sort) since all set
    // fillFields to true.
    Sort[] sort = new Sort[] { new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true);
      
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
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true);
      
      is.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(Float.isNaN(sd[j].score));
      }
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
        tdc =  TopFieldCollector.create(sort, 10, false);
      } else {
        FieldDoc fieldDoc = new FieldDoc(1, Float.NaN, new Object[] { 1 });
        tdc = TopFieldCollector.create(sort, 10, fieldDoc, false);
      }

      is.search(q, tdc);

      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(Float.isNaN(sd[j].score));
      }
    }
  }

  public void testSortNoResults() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true);
      TopDocs td = tdc.topDocs();
      assertEquals(0, td.totalHits);
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
    for (Sort sort : new Sort[] {new Sort(SortField.FIELD_SCORE), new Sort(new SortField("f", SortField.Type.SCORE))}) {
      final TopFieldCollector topCollector = TopFieldCollector.create(sort, TestUtil.nextInt(random(), 1, 2), true);
      final Collector assertingCollector = new Collector() {
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
          final LeafCollector in = topCollector.getLeafCollector(context);
          return new FilterLeafCollector(in) {
            @Override
            public void setScorer(final Scorer scorer) throws IOException {
              Scorer s = new Scorer(null) {

                int lastComputedDoc = -1;

                @Override
                public float score() throws IOException {
                  if (lastComputedDoc == docID()) {
                    throw new AssertionError("Score computed twice on " + docID());
                  }
                  lastComputedDoc = docID();
                  return scorer.score();
                }

                @Override
                public float getMaxScore(int upTo) throws IOException {
                  return scorer.getMaxScore(upTo);
                }

                @Override
                public int docID() {
                  return scorer.docID();
                }

                @Override
                public DocIdSetIterator iterator() {
                  return scorer.iterator();
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

}

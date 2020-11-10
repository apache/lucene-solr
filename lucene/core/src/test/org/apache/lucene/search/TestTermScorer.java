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
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestTermScorer extends LuceneTestCase {
  protected Directory directory;
  private static final String FIELD = "field";
  
  protected String[] values = new String[] {"all", "dogs dogs", "like",
      "playing", "fetch", "all"};
  protected IndexSearcher indexSearcher;
  protected LeafReader indexReader;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(newLogMergePolicy())
        .setSimilarity(new ClassicSimilarity()));
    for (int i = 0; i < values.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(FIELD, values[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    indexReader = getOnlyLeafReader(writer.getReader());
    writer.close();
    indexSearcher = newSearcher(indexReader, false);
    indexSearcher.setSimilarity(new ClassicSimilarity());
  }
  
  @Override
  public void tearDown() throws Exception {
    indexReader.close();
    directory.close();
    super.tearDown();
  }

  public void test() throws IOException {
    
    Term allTerm = new Term(FIELD, "all");
    TermQuery termQuery = new TermQuery(allTerm);
    
    Weight weight = indexSearcher.createWeight(termQuery, ScoreMode.COMPLETE, 1);
    assertTrue(indexSearcher.getTopReaderContext() instanceof LeafReaderContext);
    LeafReaderContext context = (LeafReaderContext)indexSearcher.getTopReaderContext();
    BulkScorer ts = weight.bulkScorer(context);
    // we have 2 documents with the term all in them, one document for all the
    // other values
    final List<TestHit> docs = new ArrayList<>();
    // must call next first
    
    ts.score(new SimpleCollector() {
      private int base = 0;
      private Scorable scorer;
      
      @Override
      public void setScorer(Scorable scorer) {
        this.scorer = scorer;
      }
      
      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();
        doc = doc + base;
        docs.add(new TestHit(doc, score));
        assertTrue("score " + score + " is not greater than 0", score > 0);
        assertTrue("Doc: " + doc + " does not equal 0 or doc does not equal 5",
            doc == 0 || doc == 5);
      }
      
      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        base = context.docBase;
      }
      
      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
      }
    }, null);
    assertTrue("docs Size: " + docs.size() + " is not: " + 2, docs.size() == 2);
    TestHit doc0 = docs.get(0);
    TestHit doc5 = docs.get(1);
    // The scores should be the same
    assertTrue(doc0.score + " does not equal: " + doc5.score,
        doc0.score == doc5.score);
  }
  
  public void testNext() throws Exception {
    
    Term allTerm = new Term(FIELD, "all");
    TermQuery termQuery = new TermQuery(allTerm);
    
    Weight weight = indexSearcher.createWeight(termQuery, ScoreMode.COMPLETE, 1);
    assertTrue(indexSearcher.getTopReaderContext() instanceof LeafReaderContext);
    LeafReaderContext context = (LeafReaderContext) indexSearcher.getTopReaderContext();
    Scorer ts = weight.scorer(context);
    assertTrue("next did not return a doc",
        ts.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue("next did not return a doc",
        ts.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue("next returned a doc and it should not have",
        ts.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
  }
  
  public void testAdvance() throws Exception {
    
    Term allTerm = new Term(FIELD, "all");
    TermQuery termQuery = new TermQuery(allTerm);
    
    Weight weight = indexSearcher.createWeight(termQuery, ScoreMode.COMPLETE, 1);
    assertTrue(indexSearcher.getTopReaderContext() instanceof LeafReaderContext);
    LeafReaderContext context = (LeafReaderContext) indexSearcher.getTopReaderContext();
    Scorer ts = weight.scorer(context);
    assertTrue("Didn't skip", ts.iterator().advance(3) != DocIdSetIterator.NO_MORE_DOCS);
    // The next doc should be doc 5
    assertTrue("doc should be number 5", ts.docID() == 5);
  }
  
  private static class TestHit {
    public int doc;
    public float score;
    
    public TestHit(int doc, float score) {
      this.doc = doc;
      this.score = score;
    }
    
    @Override
    public String toString() {
      return "TestHit{" + "doc=" + doc + ", score=" + score + "}";
    }
  }

  public void testDoesNotLoadNorms() throws IOException {
    Term allTerm = new Term(FIELD, "all");
    TermQuery termQuery = new TermQuery(allTerm);
    
    LeafReader forbiddenNorms = new FilterLeafReader(indexReader) {
      @Override
      public NumericDocValues getNormValues(String field) throws IOException {
        fail("Norms should not be loaded");
        // unreachable
        return null;
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
      }
    };
    // We don't use newSearcher because it sometimes runs checkIndex which loads norms
    IndexSearcher indexSearcher = new IndexSearcher(forbiddenNorms);
    
    Weight weight = indexSearcher.createWeight(termQuery, ScoreMode.COMPLETE, 1);
    expectThrows(AssertionError.class, () -> {
      weight.scorer(forbiddenNorms.getContext()).iterator().nextDoc();
    });
    
    Weight weight2 = indexSearcher.createWeight(termQuery, ScoreMode.COMPLETE_NO_SCORES, 1);
    // should not fail this time since norms are not necessary
    weight2.scorer(forbiddenNorms.getContext()).iterator().nextDoc();
  }

  public void testRandomTopDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = TEST_NIGHTLY ? atLeast(128 * 8 * 8 * 3) : atLeast(500); // at night, make sure some terms have skip data
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        int freq = TestUtil.nextInt(random(), 1, 1 << random().nextInt(3));
        for (int k = 0; k < freq; ++k) {
          doc.add(new TextField("foo", Integer.toString(start + j), Store.NO));
        }
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    for (int iter = 0; iter < 15; ++iter) {
      Query query = new TermQuery(new Term("foo", Integer.toString(iter)));

      TopScoreDocCollector collector1 = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE); // COMPLETE
      TopScoreDocCollector collector2 = TopScoreDocCollector.create(10, null, 1); // TOP_SCORES
      
      searcher.search(query, collector1);
      searcher.search(query, collector2);
      CheckHits.checkEqual(query, collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);

      int filterTerm = random().nextInt(15);
      Query filteredQuery = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
          .build();

      collector1 = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE); // COMPLETE
      collector2 = TopScoreDocCollector.create(10, null, 1); // TOP_SCORES
      searcher.search(filteredQuery, collector1);
      searcher.search(filteredQuery, collector2);
      CheckHits.checkEqual(query, collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);
    }
    reader.close();
    dir.close();
  }

}

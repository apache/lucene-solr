package org.apache.lucene.search;

/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestTermScorer extends LuceneTestCase {
  protected Directory directory;
  private static final String FIELD = "field";
  
  protected String[] values = new String[] {"all", "dogs dogs", "like",
      "playing", "fetch", "all"};
  protected IndexSearcher indexSearcher;
  protected IndexReader indexReader;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMergePolicy(newLogMergePolicy())
        .setSimilarity(new DefaultSimilarity()));
    for (int i = 0; i < values.length; i++) {
      Document doc = new Document();
      doc
          .add(newField(FIELD, values[i], TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    indexReader = SlowCompositeReaderWrapper.wrap(writer.getReader());
    writer.close();
    indexSearcher = newSearcher(indexReader);
    indexSearcher.setSimilarity(new DefaultSimilarity());
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
    
    Weight weight = indexSearcher.createNormalizedWeight(termQuery);
    assertTrue(indexSearcher.getTopReaderContext() instanceof AtomicReaderContext);
    AtomicReaderContext context = (AtomicReaderContext)indexSearcher.getTopReaderContext();
    Scorer ts = weight.scorer(context, true, true, context.reader().getLiveDocs());
    // we have 2 documents with the term all in them, one document for all the
    // other values
    final List<TestHit> docs = new ArrayList<TestHit>();
    // must call next first
    
    ts.score(new Collector() {
      private int base = 0;
      private Scorer scorer;
      
      @Override
      public void setScorer(Scorer scorer) throws IOException {
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
      public void setNextReader(AtomicReaderContext context) {
        base = context.docBase;
      }
      
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }
    });
    assertTrue("docs Size: " + docs.size() + " is not: " + 2, docs.size() == 2);
    TestHit doc0 = docs.get(0);
    TestHit doc5 = docs.get(1);
    // The scores should be the same
    assertTrue(doc0.score + " does not equal: " + doc5.score,
        doc0.score == doc5.score);
    /*
     * Score should be (based on Default Sim.: All floats are approximate tf = 1
     * numDocs = 6 docFreq(all) = 2 idf = ln(6/3) + 1 = 1.693147 idf ^ 2 =
     * 2.8667 boost = 1 lengthNorm = 1 //there is 1 term in every document coord
     * = 1 sumOfSquaredWeights = (idf * boost) ^ 2 = 1.693147 ^ 2 = 2.8667
     * queryNorm = 1 / (sumOfSquaredWeights)^0.5 = 1 /(1.693147) = 0.590
     * 
     * score = 1 * 2.8667 * 1 * 1 * 0.590 = 1.69
     */
    assertTrue(doc0.score + " does not equal: " + 1.6931472f,
        doc0.score == 1.6931472f);
  }
  
  public void testNext() throws Exception {
    
    Term allTerm = new Term(FIELD, "all");
    TermQuery termQuery = new TermQuery(allTerm);
    
    Weight weight = indexSearcher.createNormalizedWeight(termQuery);
    assertTrue(indexSearcher.getTopReaderContext() instanceof AtomicReaderContext);
    AtomicReaderContext context = (AtomicReaderContext) indexSearcher.getTopReaderContext();
    Scorer ts = weight.scorer(context, true, true, context.reader().getLiveDocs());
    assertTrue("next did not return a doc",
        ts.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue("score is not correct", ts.score() == 1.6931472f);
    assertTrue("next did not return a doc",
        ts.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue("score is not correct", ts.score() == 1.6931472f);
    assertTrue("next returned a doc and it should not have",
        ts.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
  }
  
  public void testAdvance() throws Exception {
    
    Term allTerm = new Term(FIELD, "all");
    TermQuery termQuery = new TermQuery(allTerm);
    
    Weight weight = indexSearcher.createNormalizedWeight(termQuery);
    assertTrue(indexSearcher.getTopReaderContext() instanceof AtomicReaderContext);
    AtomicReaderContext context = (AtomicReaderContext) indexSearcher.getTopReaderContext();
    Scorer ts = weight.scorer(context, true, true, context.reader().getLiveDocs());
    assertTrue("Didn't skip", ts.advance(3) != DocIdSetIterator.NO_MORE_DOCS);
    // The next doc should be doc 5
    assertTrue("doc should be number 5", ts.docID() == 5);
  }
  
  private class TestHit {
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
  
}

package org.apache.lucene.misc;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.FieldNormModifier;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiNorms;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.DefaultSimilarityProvider;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests changing the norms after changing the simularity
 */
public class TestLengthNormModifier extends LuceneTestCase {
    public static int NUM_DOCS = 5;

    public Directory store;

    /** inverts the normal notion of lengthNorm */
    public static SimilarityProvider s = new DefaultSimilarityProvider() {
      @Override
      public Similarity get(String field) {
        return new DefaultSimilarity() {
          @Override
          public float computeNorm(FieldInvertState state) {
            return state.getBoost() * (discountOverlaps ? state.getLength() - state.getNumOverlap() : state.getLength());
          }
        };
      }
    };
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      store = newDirectory();
	IndexWriter writer = new IndexWriter(store, newIndexWriterConfig(
                                                                         TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
	
	for (int i = 0; i < NUM_DOCS; i++) {
	    Document d = new Document();
	    d.add(newField("field", "word",
			    Field.Store.YES, Field.Index.ANALYZED));
	    d.add(newField("nonorm", "word",
			    Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
		
	    for (int j = 1; j <= i; j++) {
		d.add(newField("field", "crap",
				Field.Store.YES, Field.Index.ANALYZED));
		d.add(newField("nonorm", "more words",
				Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
	    }
	    writer.addDocument(d);
	}
	writer.close();
    }
    
    @Override
    public void tearDown() throws Exception {
      store.close();
      super.tearDown();
    }
    
    public void testMissingField() throws Exception {
	FieldNormModifier fnm = new FieldNormModifier(store, s);
	try {
	    fnm.reSetNorms("nobodyherebutuschickens");
	} catch (IllegalStateException e) {
	    // expected
	}
    }
	
    public void testFieldWithNoNorm() throws Exception {

	IndexReader r = IndexReader.open(store, false);
	byte[] norms = MultiNorms.norms(r, "nonorm");

	// sanity check, norms should all be 1
	assertTrue("Whoops we have norms?", !r.hasNorms("nonorm"));
	assertNull(norms);

	r.close();
	
	FieldNormModifier fnm = new FieldNormModifier(store, s);
	try {
	    fnm.reSetNorms("nonorm");
	} catch (IllegalStateException e) {
	  // expected
	}

	// nothing should have changed
	r = IndexReader.open(store, false);
	
	norms = MultiNorms.norms(r, "nonorm");
	assertTrue("Whoops we have norms?", !r.hasNorms("nonorm"));
  assertNull(norms);

	r.close();
	
    }
	
    
    public void testGoodCases() throws Exception {
	
	IndexSearcher searcher;
	final float[] scores = new float[NUM_DOCS];
	float lastScore = 0.0f;
	
	// default similarity should put docs with shorter length first
  searcher = new IndexSearcher(store, false);
  searcher.search(new TermQuery(new Term("field", "word")), new Collector() {
    private int docBase = 0;
    private Scorer scorer;
    @Override
    public final void collect(int doc) throws IOException {
      scores[doc + docBase] = scorer.score();
    }
    @Override
    public void setNextReader(AtomicReaderContext context) {
      docBase = context.docBase;
    }
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  });
  searcher.close();
	
	lastScore = Float.MAX_VALUE;
	for (int i = 0; i < NUM_DOCS; i++) {
	    String msg = "i=" + i + ", "+scores[i]+" <= "+lastScore;
	    assertTrue(msg, scores[i] <= lastScore);
	    //System.out.println(msg);
	    lastScore = scores[i];
	}

	// override the norms to be inverted
  SimilarityProvider s = new DefaultSimilarityProvider() {
    @Override
    public Similarity get(String field) {
      return new DefaultSimilarity() {
        @Override
        public float computeNorm(FieldInvertState state) {
          return state.getBoost() * (discountOverlaps ? state.getLength() - state.getNumOverlap() : state.getLength());
        }
      };
    }
  };

	FieldNormModifier fnm = new FieldNormModifier(store, s);
	fnm.reSetNorms("field");

	// new norm (with default similarity) should put longer docs first
	searcher = new IndexSearcher(store, false);
	searcher.search(new TermQuery(new Term("field", "word")), new Collector() {
      private int docBase = 0;
      private Scorer scorer;
      @Override
      public final void collect(int doc) throws IOException {
        scores[doc + docBase] = scorer.score();
      }
      @Override
      public void setNextReader(AtomicReaderContext context) {
        docBase = context.docBase;
      }
      @Override
      public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
      }
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }
    });
    searcher.close();
	
	lastScore = 0.0f;
	for (int i = 0; i < NUM_DOCS; i++) {
	    String msg = "i=" + i + ", "+scores[i]+" >= "+lastScore;
	    assertTrue(msg, scores[i] >= lastScore);
	    //System.out.println(msg);
	    lastScore = scores[i];
	}
	
    }
}

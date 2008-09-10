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

import junit.framework.TestCase;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * Tests changing the norms after changing the simularity
 *
 * @version $Id:$
 */
public class TestLengthNormModifier extends TestCase {
    public TestLengthNormModifier(String name) {
	super(name);
    }

    public static byte DEFAULT_NORM = Similarity.encodeNorm(1.0f);
    
    public static int NUM_DOCS = 5;

    public Directory store = new RAMDirectory();

    /** inverts the normal notion of lengthNorm */
    public static Similarity s = new DefaultSimilarity() {
	    public float lengthNorm(String fieldName, int numTokens) {
		return (float)numTokens;
	    }
	};
    
    public void setUp() throws Exception {
	IndexWriter writer = new
	    IndexWriter(store, new SimpleAnalyzer(), true);
	
	IndexSearcher searcher;
	
	for (int i = 0; i < NUM_DOCS; i++) {
	    Document d = new Document();
	    d.add(new Field("field", "word",
			    Field.Store.YES, Field.Index.ANALYZED));
	    d.add(new Field("nonorm", "word",
			    Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
		
	    for (int j = 1; j <= i; j++) {
		d.add(new Field("field", "crap",
				Field.Store.YES, Field.Index.ANALYZED));
		d.add(new Field("nonorm", "more words",
				Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
	    }
	    writer.addDocument(d);
	}
	writer.close();
    }
    
    public void testMissingField() {
	LengthNormModifier lnm = new LengthNormModifier(store, s);
	try {
	    lnm.reSetNorms("nobodyherebutuschickens");
	} catch (Exception e) {
	    assertNull("caught something", e);
	}
    }
	
    public void testFieldWithNoNorm() throws Exception {

	IndexReader r = IndexReader.open(store);
	byte[] norms = r.norms("nonorm");

	// sanity check, norms should all be 1
	assertTrue("Whoops we have norms?", !r.hasNorms("nonorm"));
	for (int i = 0; i< norms.length; i++) {
	    assertEquals(""+i, DEFAULT_NORM, norms[i]);
	}

	r.close();
	
	LengthNormModifier lnm = new LengthNormModifier(store, s);
	try {
	    lnm.reSetNorms("nonorm");
	} catch (Exception e) {
	    assertNull("caught something", e);
	}

	// nothing should have changed
	r = IndexReader.open(store);
	
	norms = r.norms("nonorm");
	assertTrue("Whoops we have norms?", !r.hasNorms("nonorm"));
	for (int i = 0; i< norms.length; i++) {
	    assertEquals(""+i, DEFAULT_NORM, norms[i]);
	}

	r.close();
	
    }
	
    
    public void testGoodCases() throws Exception {
	
	IndexSearcher searcher;
	final float[] scores = new float[NUM_DOCS];
	float lastScore = 0.0f;
	
	// default similarity should put docs with shorter length first
	searcher = new IndexSearcher(store);
	searcher.search
	    (new TermQuery(new Term("field", "word")),
	     new HitCollector() {
		 public final void collect(int doc, float score) {
		     scores[doc] = score;
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
	Similarity s = new DefaultSimilarity() {
		public float lengthNorm(String fieldName, int numTokens) {
		    return (float)numTokens;
		}
	    };
	LengthNormModifier lnm = new LengthNormModifier(store, s);
	lnm.reSetNorms("field");

	// new norm (with default similarity) should put longer docs first
	searcher = new IndexSearcher(store);
	searcher.search
	    (new TermQuery(new Term("field", "word")),
	     new HitCollector() {
		 public final void collect(int doc, float score) {
		     scores[doc] = score;
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

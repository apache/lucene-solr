package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class TestConstantScoreRangeQuery extends BaseTestRangeFilter {
    
    /** threshold for comparing floats */
    public static final float SCORE_COMP_THRESH = 1e-6f;

    public TestConstantScoreRangeQuery(String name) {
	super(name);
    }
    public TestConstantScoreRangeQuery() {
        super();
    }

    Directory small;

    void assertEquals(String m, float e, float a) {
        assertEquals(m, e, a, SCORE_COMP_THRESH);
    }
    
    public void setUp() throws Exception {
        super.setUp();
        
        String[] data = new String [] {
            "A 1 2 3 4 5 6",
            "Z       4 5 6",
            null,
            "B   2   4 5 6",
            "Y     3   5 6",
            null,
            "C     3     6",
            "X       4 5 6"
        };
        
        small = new RAMDirectory();
        IndexWriter writer = new IndexWriter(small,
                                             new WhitespaceAnalyzer(),
                                             true);
        
        for (int i = 0; i < data.length; i++) {
            Document doc = new Document();
            doc.add(new Field("id", String.valueOf(i), Field.Store.YES, Field.Index.UN_TOKENIZED));//Field.Keyword("id",String.valueOf(i)));
            doc.add(new Field("all", "all", Field.Store.YES, Field.Index.UN_TOKENIZED));//Field.Keyword("all","all"));
            if (null != data[i]) {
                doc.add(new Field("data", data[i], Field.Store.YES, Field.Index.TOKENIZED));//Field.Text("data",data[i]));
            }
            writer.addDocument(doc);
        }
        
        writer.optimize();
        writer.close();
    }


    
    /** macro for readability */
    public static Query csrq(String f, String l, String h,
                             boolean il, boolean ih) {
        return new ConstantScoreRangeQuery(f,l,h,il,ih);
    }

    public void testBasics() throws IOException {
      QueryUtils.check(csrq("data","1","6",T,T));
      QueryUtils.check(csrq("data","A","Z",T,T));
      QueryUtils.checkUnequal(csrq("data","1","6",T,T), csrq("data","A","Z",T,T));
    }

    public void testEqualScores() throws IOException {
        // NOTE: uses index build in *this* setUp
        
        IndexReader reader = IndexReader.open(small);
	IndexSearcher search = new IndexSearcher(reader);

	Hits result;

        // some hits match more terms then others, score should be the same
        
        result = search.search(csrq("data","1","6",T,T));
        int numHits = result.length();
        assertEquals("wrong number of results", 6, numHits);
        float score = result.score(0);
        for (int i = 1; i < numHits; i++) {
            assertEquals("score for " + i +" was not the same",
                         score, result.score(i));
        }

    }

    public void testBoost() throws IOException {
        // NOTE: uses index build in *this* setUp

        IndexReader reader = IndexReader.open(small);
	IndexSearcher search = new IndexSearcher(reader);

      // test for correct application of query normalization
      // must use a non score normalizing method for this.
      Query q = csrq("data","1","6",T,T);
      q.setBoost(100);
      search.search(q,null, new HitCollector() {
                public void collect(int doc, float score) {
                    assertEquals("score for doc " + doc +" was not correct",
                                 1.0f, score);
                }
            });


      //
      // Ensure that boosting works to score one clause of a query higher
      // than another.
      //
      Query q1 = csrq("data","A","A",T,T);  // matches document #0
      q1.setBoost(.1f);
      Query q2 = csrq("data","Z","Z",T,T);  // matches document #1
      BooleanQuery bq = new BooleanQuery(true);
      bq.add(q1, BooleanClause.Occur.SHOULD);
      bq.add(q2, BooleanClause.Occur.SHOULD);

      Hits hits = search.search(bq);
      assertEquals(1, hits.id(0));
      assertEquals(0, hits.id(1));
      assertTrue(hits.score(0) > hits.score(1));

      q1 = csrq("data","A","A",T,T);  // matches document #0
      q1.setBoost(10f);
      q2 = csrq("data","Z","Z",T,T);  // matches document #1
      bq = new BooleanQuery(true);
      bq.add(q1, BooleanClause.Occur.SHOULD);
      bq.add(q2, BooleanClause.Occur.SHOULD);

      hits = search.search(bq);
      assertEquals(0, hits.id(0));
      assertEquals(1, hits.id(1));
      assertTrue(hits.score(0) > hits.score(1));
    }

    
    public void testBooleanOrderUnAffected() throws IOException {
        // NOTE: uses index build in *this* setUp
        
        IndexReader reader = IndexReader.open(small);
	IndexSearcher search = new IndexSearcher(reader);

        // first do a regular RangeQuery which uses term expansion so
        // docs with more terms in range get higher scores
        
        Query rq = new RangeQuery(new Term("data","1"),new Term("data","4"),T);

        Hits expected = search.search(rq);
        int numHits = expected.length();

        // now do a boolean where which also contains a
        // ConstantScoreRangeQuery and make sure hte order is the same
        
        BooleanQuery q = new BooleanQuery();
        q.add(rq, BooleanClause.Occur.MUST);//T, F);
        q.add(csrq("data","1","6", T, T), BooleanClause.Occur.MUST);//T, F);

        Hits actual = search.search(q);

        assertEquals("wrong numebr of hits", numHits, actual.length());
        for (int i = 0; i < numHits; i++) {
            assertEquals("mismatch in docid for hit#"+i,
                         expected.id(i), actual.id(i));
        }

    }


    

    
    public void testRangeQueryId() throws IOException {
        // NOTE: uses index build in *super* setUp

        IndexReader reader = IndexReader.open(index);
	IndexSearcher search = new IndexSearcher(reader);

        int medId = ((maxId - minId) / 2);
        
        String minIP = pad(minId);
        String maxIP = pad(maxId);
        String medIP = pad(medId);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
	Hits result;

        // test id, bounded on both ends
        
	result = search.search(csrq("id",minIP,maxIP,T,T));
	assertEquals("find all", numDocs, result.length());

	result = search.search(csrq("id",minIP,maxIP,T,F));
	assertEquals("all but last", numDocs-1, result.length());

	result = search.search(csrq("id",minIP,maxIP,F,T));
	assertEquals("all but first", numDocs-1, result.length());
        
	result = search.search(csrq("id",minIP,maxIP,F,F));
        assertEquals("all but ends", numDocs-2, result.length());
    
        result = search.search(csrq("id",medIP,maxIP,T,T));
        assertEquals("med and up", 1+ maxId-medId, result.length());
        
        result = search.search(csrq("id",minIP,medIP,T,T));
        assertEquals("up to med", 1+ medId-minId, result.length());

        // unbounded id

	result = search.search(csrq("id",minIP,null,T,F));
	assertEquals("min and up", numDocs, result.length());

	result = search.search(csrq("id",null,maxIP,F,T));
	assertEquals("max and down", numDocs, result.length());

	result = search.search(csrq("id",minIP,null,F,F));
	assertEquals("not min, but up", numDocs-1, result.length());
        
	result = search.search(csrq("id",null,maxIP,F,F));
	assertEquals("not max, but down", numDocs-1, result.length());
        
        result = search.search(csrq("id",medIP,maxIP,T,F));
        assertEquals("med and up, not max", maxId-medId, result.length());
        
        result = search.search(csrq("id",minIP,medIP,F,T));
        assertEquals("not min, up to med", medId-minId, result.length());

        // very small sets

	result = search.search(csrq("id",minIP,minIP,F,F));
	assertEquals("min,min,F,F", 0, result.length());
	result = search.search(csrq("id",medIP,medIP,F,F));
	assertEquals("med,med,F,F", 0, result.length());
	result = search.search(csrq("id",maxIP,maxIP,F,F));
	assertEquals("max,max,F,F", 0, result.length());
                     
	result = search.search(csrq("id",minIP,minIP,T,T));
	assertEquals("min,min,T,T", 1, result.length());
	result = search.search(csrq("id",null,minIP,F,T));
	assertEquals("nul,min,F,T", 1, result.length());

	result = search.search(csrq("id",maxIP,maxIP,T,T));
	assertEquals("max,max,T,T", 1, result.length());
	result = search.search(csrq("id",maxIP,null,T,F));
	assertEquals("max,nul,T,T", 1, result.length());

	result = search.search(csrq("id",medIP,medIP,T,T));
	assertEquals("med,med,T,T", 1, result.length());
        
    }

    public void testRangeQueryRand() throws IOException {
        // NOTE: uses index build in *super* setUp

        IndexReader reader = IndexReader.open(index);
	IndexSearcher search = new IndexSearcher(reader);

        String minRP = pad(minR);
        String maxRP = pad(maxR);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
	Hits result;
        Query q = new TermQuery(new Term("body","body"));

        // test extremes, bounded on both ends
        
	result = search.search(csrq("rand",minRP,maxRP,T,T));
	assertEquals("find all", numDocs, result.length());

	result = search.search(csrq("rand",minRP,maxRP,T,F));
	assertEquals("all but biggest", numDocs-1, result.length());

	result = search.search(csrq("rand",minRP,maxRP,F,T));
	assertEquals("all but smallest", numDocs-1, result.length());
        
	result = search.search(csrq("rand",minRP,maxRP,F,F));
        assertEquals("all but extremes", numDocs-2, result.length());
    
        // unbounded

	result = search.search(csrq("rand",minRP,null,T,F));
	assertEquals("smallest and up", numDocs, result.length());

	result = search.search(csrq("rand",null,maxRP,F,T));
	assertEquals("biggest and down", numDocs, result.length());

	result = search.search(csrq("rand",minRP,null,F,F));
	assertEquals("not smallest, but up", numDocs-1, result.length());
        
	result = search.search(csrq("rand",null,maxRP,F,F));
	assertEquals("not biggest, but down", numDocs-1, result.length());
        
        // very small sets

	result = search.search(csrq("rand",minRP,minRP,F,F));
	assertEquals("min,min,F,F", 0, result.length());
	result = search.search(csrq("rand",maxRP,maxRP,F,F));
	assertEquals("max,max,F,F", 0, result.length());
                     
	result = search.search(csrq("rand",minRP,minRP,T,T));
	assertEquals("min,min,T,T", 1, result.length());
	result = search.search(csrq("rand",null,minRP,F,T));
	assertEquals("nul,min,F,T", 1, result.length());

	result = search.search(csrq("rand",maxRP,maxRP,T,T));
	assertEquals("max,max,T,T", 1, result.length());
	result = search.search(csrq("rand",maxRP,null,T,F));
	assertEquals("max,nul,T,T", 1, result.length());
        
    }

}

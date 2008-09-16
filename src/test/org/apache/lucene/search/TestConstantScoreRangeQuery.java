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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.text.Collator;
import java.util.Locale;

import junit.framework.Assert;

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

    static public void assertEquals(String m, int e, int a) {
        Assert.assertEquals(m, e, a);
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
        IndexWriter writer = new IndexWriter(small, new WhitespaceAnalyzer(), true, 
                                             IndexWriter.MaxFieldLength.LIMITED);

      for (int i = 0; i < data.length; i++) {
            Document doc = new Document();
            doc.add(new Field("id", String.valueOf(i), Field.Store.YES, Field.Index.NOT_ANALYZED));//Field.Keyword("id",String.valueOf(i)));
            doc.add(new Field("all", "all", Field.Store.YES, Field.Index.NOT_ANALYZED));//Field.Keyword("all","all"));
            if (null != data[i]) {
                doc.add(new Field("data", data[i], Field.Store.YES, Field.Index.ANALYZED));//Field.Text("data",data[i]));
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

    /** macro for readability */
    public static Query csrq(String f, String l, String h,
                             boolean il, boolean ih, Collator c) {
        return new ConstantScoreRangeQuery(f,l,h,il,ih,c);
    }

    public void testBasics() throws IOException {
      QueryUtils.check(csrq("data","1","6",T,T));
      QueryUtils.check(csrq("data","A","Z",T,T));
      QueryUtils.checkUnequal(csrq("data","1","6",T,T), csrq("data","A","Z",T,T));
    }

    public void testBasicsCollating() throws IOException {
      Collator c = Collator.getInstance(Locale.ENGLISH);
      QueryUtils.check(csrq("data","1","6",T,T,c));
      QueryUtils.check(csrq("data","A","Z",T,T,c));
      QueryUtils.checkUnequal(csrq("data","1","6",T,T,c), csrq("data","A","Z",T,T,c));
    }

    public void testEqualScores() throws IOException {
        // NOTE: uses index build in *this* setUp
        
        IndexReader reader = IndexReader.open(small);
	IndexSearcher search = new IndexSearcher(reader);

  ScoreDoc[] result;

        // some hits match more terms then others, score should be the same
        
        result = search.search(csrq("data","1","6",T,T), null, 1000).scoreDocs;
        int numHits = result.length;
        assertEquals("wrong number of results", 6, numHits);
        float score = result[0].score;
        for (int i = 1; i < numHits; i++) {
            assertEquals("score for " + i +" was not the same",
                         score, result[i].score);
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

      ScoreDoc[] hits = search.search(bq, null, 1000).scoreDocs;
      assertEquals(1, hits[0].doc);
      assertEquals(0, hits[1].doc);
      assertTrue(hits[0].score > hits[1].score);

      q1 = csrq("data","A","A",T,T);  // matches document #0
      q1.setBoost(10f);
      q2 = csrq("data","Z","Z",T,T);  // matches document #1
      bq = new BooleanQuery(true);
      bq.add(q1, BooleanClause.Occur.SHOULD);
      bq.add(q2, BooleanClause.Occur.SHOULD);

      hits = search.search(bq, null, 1000).scoreDocs;
      assertEquals(0, hits[0].doc);
      assertEquals(1, hits[1].doc);
      assertTrue(hits[0].score > hits[1].score);
    }

    
    public void testBooleanOrderUnAffected() throws IOException {
        // NOTE: uses index build in *this* setUp
        
        IndexReader reader = IndexReader.open(small);
	IndexSearcher search = new IndexSearcher(reader);

        // first do a regular RangeQuery which uses term expansion so
        // docs with more terms in range get higher scores
        
        Query rq = new RangeQuery(new Term("data","1"),new Term("data","4"),T);

        ScoreDoc[] expected = search.search(rq, null, 1000).scoreDocs;
        int numHits = expected.length;

        // now do a boolean where which also contains a
        // ConstantScoreRangeQuery and make sure hte order is the same
        
        BooleanQuery q = new BooleanQuery();
        q.add(rq, BooleanClause.Occur.MUST);//T, F);
        q.add(csrq("data","1","6", T, T), BooleanClause.Occur.MUST);//T, F);

        ScoreDoc[] actual = search.search(q, null, 1000).scoreDocs;

        assertEquals("wrong numebr of hits", numHits, actual.length);
        for (int i = 0; i < numHits; i++) {
            assertEquals("mismatch in docid for hit#"+i,
                         expected[i].doc, actual[i].doc);
        }

    }


    

    
    public void testRangeQueryId() throws IOException {
        // NOTE: uses index build in *super* setUp

        IndexReader reader = IndexReader.open(signedIndex.index);
	IndexSearcher search = new IndexSearcher(reader);

        int medId = ((maxId - minId) / 2);
        
        String minIP = pad(minId);
        String maxIP = pad(maxId);
        String medIP = pad(medId);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
  ScoreDoc[] result;

        // test id, bounded on both ends
        
  result = search.search(csrq("id",minIP,maxIP,T,T), null, numDocs).scoreDocs;
  assertEquals("find all", numDocs, result.length);

  result = search.search(csrq("id",minIP,maxIP,T,F), null, numDocs).scoreDocs;
  assertEquals("all but last", numDocs-1, result.length);

  result = search.search(csrq("id",minIP,maxIP,F,T), null, numDocs).scoreDocs;
  assertEquals("all but first", numDocs-1, result.length);
        
  result = search.search(csrq("id",minIP,maxIP,F,F), null, numDocs).scoreDocs;
        assertEquals("all but ends", numDocs-2, result.length);
    
        result = search.search(csrq("id",medIP,maxIP,T,T), null, numDocs).scoreDocs;
        assertEquals("med and up", 1+ maxId-medId, result.length);
        
        result = search.search(csrq("id",minIP,medIP,T,T), null, numDocs).scoreDocs;
        assertEquals("up to med", 1+ medId-minId, result.length);

        // unbounded id

  result = search.search(csrq("id",minIP,null,T,F), null, numDocs).scoreDocs;
  assertEquals("min and up", numDocs, result.length);

  result = search.search(csrq("id",null,maxIP,F,T), null, numDocs).scoreDocs;
  assertEquals("max and down", numDocs, result.length);

  result = search.search(csrq("id",minIP,null,F,F), null, numDocs).scoreDocs;
  assertEquals("not min, but up", numDocs-1, result.length);
        
  result = search.search(csrq("id",null,maxIP,F,F), null, numDocs).scoreDocs;
  assertEquals("not max, but down", numDocs-1, result.length);
        
        result = search.search(csrq("id",medIP,maxIP,T,F), null, numDocs).scoreDocs;
        assertEquals("med and up, not max", maxId-medId, result.length);
        
        result = search.search(csrq("id",minIP,medIP,F,T), null, numDocs).scoreDocs;
        assertEquals("not min, up to med", medId-minId, result.length);

        // very small sets

  result = search.search(csrq("id",minIP,minIP,F,F), null, numDocs).scoreDocs;
  assertEquals("min,min,F,F", 0, result.length);
  result = search.search(csrq("id",medIP,medIP,F,F), null, numDocs).scoreDocs;
  assertEquals("med,med,F,F", 0, result.length);
  result = search.search(csrq("id",maxIP,maxIP,F,F), null, numDocs).scoreDocs;
  assertEquals("max,max,F,F", 0, result.length);
                     
  result = search.search(csrq("id",minIP,minIP,T,T), null, numDocs).scoreDocs;
  assertEquals("min,min,T,T", 1, result.length);
  result = search.search(csrq("id",null,minIP,F,T), null, numDocs).scoreDocs;
  assertEquals("nul,min,F,T", 1, result.length);

  result = search.search(csrq("id",maxIP,maxIP,T,T), null, numDocs).scoreDocs;
  assertEquals("max,max,T,T", 1, result.length);
  result = search.search(csrq("id",maxIP,null,T,F), null, numDocs).scoreDocs;
  assertEquals("max,nul,T,T", 1, result.length);

  result = search.search(csrq("id",medIP,medIP,T,T), null, numDocs).scoreDocs;
  assertEquals("med,med,T,T", 1, result.length);
        
    }

  
  public void testRangeQueryIdCollating() throws IOException {
    // NOTE: uses index build in *super* setUp

    IndexReader reader = IndexReader.open(signedIndex.index);
    IndexSearcher search = new IndexSearcher(reader);

    int medId = ((maxId - minId) / 2);
        
    String minIP = pad(minId);
    String maxIP = pad(maxId);
    String medIP = pad(medId);
    
    int numDocs = reader.numDocs();
        
    assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
    ScoreDoc[] result;
        
    Collator c = Collator.getInstance(Locale.ENGLISH);

    // test id, bounded on both ends
        
    result = search.search(csrq("id",minIP,maxIP,T,T,c), null, numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(csrq("id",minIP,maxIP,T,F,c), null, numDocs).scoreDocs;
    assertEquals("all but last", numDocs-1, result.length);

    result = search.search(csrq("id",minIP,maxIP,F,T,c), null, numDocs).scoreDocs;
    assertEquals("all but first", numDocs-1, result.length);
        
    result = search.search(csrq("id",minIP,maxIP,F,F,c), null, numDocs).scoreDocs;
    assertEquals("all but ends", numDocs-2, result.length);
    
    result = search.search(csrq("id",medIP,maxIP,T,T,c), null, numDocs).scoreDocs;
    assertEquals("med and up", 1+ maxId-medId, result.length);
        
    result = search.search(csrq("id",minIP,medIP,T,T,c), null, numDocs).scoreDocs;
    assertEquals("up to med", 1+ medId-minId, result.length);

    // unbounded id

    result = search.search(csrq("id",minIP,null,T,F,c), null, numDocs).scoreDocs;
    assertEquals("min and up", numDocs, result.length);

    result = search.search(csrq("id",null,maxIP,F,T,c), null, numDocs).scoreDocs;
    assertEquals("max and down", numDocs, result.length);

    result = search.search(csrq("id",minIP,null,F,F,c), null, numDocs).scoreDocs;
    assertEquals("not min, but up", numDocs-1, result.length);
        
    result = search.search(csrq("id",null,maxIP,F,F,c), null, numDocs).scoreDocs;
    assertEquals("not max, but down", numDocs-1, result.length);
        
    result = search.search(csrq("id",medIP,maxIP,T,F,c), null, numDocs).scoreDocs;
    assertEquals("med and up, not max", maxId-medId, result.length);
        
    result = search.search(csrq("id",minIP,medIP,F,T,c), null, numDocs).scoreDocs;
    assertEquals("not min, up to med", medId-minId, result.length);

    // very small sets

    result = search.search(csrq("id",minIP,minIP,F,F,c), null, numDocs).scoreDocs;
    assertEquals("min,min,F,F,c", 0, result.length);
    result = search.search(csrq("id",medIP,medIP,F,F,c), null, numDocs).scoreDocs;
    assertEquals("med,med,F,F,c", 0, result.length);
    result = search.search(csrq("id",maxIP,maxIP,F,F,c), null, numDocs).scoreDocs;
    assertEquals("max,max,F,F,c", 0, result.length);
                     
    result = search.search(csrq("id",minIP,minIP,T,T,c), null, numDocs).scoreDocs;
    assertEquals("min,min,T,T,c", 1, result.length);
    result = search.search(csrq("id",null,minIP,F,T,c), null, numDocs).scoreDocs;
    assertEquals("nul,min,F,T,c", 1, result.length);

    result = search.search(csrq("id",maxIP,maxIP,T,T,c), null, numDocs).scoreDocs;
    assertEquals("max,max,T,T,c", 1, result.length);
    result = search.search(csrq("id",maxIP,null,T,F,c), null, numDocs).scoreDocs;
    assertEquals("max,nul,T,T,c", 1, result.length);

    result = search.search(csrq("id",medIP,medIP,T,T,c), null, numDocs).scoreDocs;
    assertEquals("med,med,T,T,c", 1, result.length);
  }
    
  
    public void testRangeQueryRand() throws IOException {
        // NOTE: uses index build in *super* setUp

        IndexReader reader = IndexReader.open(signedIndex.index);
	IndexSearcher search = new IndexSearcher(reader);

        String minRP = pad(signedIndex.minR);
        String maxRP = pad(signedIndex.maxR);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
  ScoreDoc[] result;

        // test extremes, bounded on both ends
        
  result = search.search(csrq("rand",minRP,maxRP,T,T), null, numDocs).scoreDocs;
  assertEquals("find all", numDocs, result.length);

  result = search.search(csrq("rand",minRP,maxRP,T,F), null, numDocs).scoreDocs;
  assertEquals("all but biggest", numDocs-1, result.length);

  result = search.search(csrq("rand",minRP,maxRP,F,T), null, numDocs).scoreDocs;
  assertEquals("all but smallest", numDocs-1, result.length);
        
  result = search.search(csrq("rand",minRP,maxRP,F,F), null, numDocs).scoreDocs;
        assertEquals("all but extremes", numDocs-2, result.length);
    
        // unbounded

  result = search.search(csrq("rand",minRP,null,T,F), null, numDocs).scoreDocs;
  assertEquals("smallest and up", numDocs, result.length);

  result = search.search(csrq("rand",null,maxRP,F,T), null, numDocs).scoreDocs;
  assertEquals("biggest and down", numDocs, result.length);

  result = search.search(csrq("rand",minRP,null,F,F), null, numDocs).scoreDocs;
  assertEquals("not smallest, but up", numDocs-1, result.length);
        
  result = search.search(csrq("rand",null,maxRP,F,F), null, numDocs).scoreDocs;
  assertEquals("not biggest, but down", numDocs-1, result.length);
        
        // very small sets

  result = search.search(csrq("rand",minRP,minRP,F,F), null, numDocs).scoreDocs;
  assertEquals("min,min,F,F", 0, result.length);
  result = search.search(csrq("rand",maxRP,maxRP,F,F), null, numDocs).scoreDocs;
  assertEquals("max,max,F,F", 0, result.length);
                     
  result = search.search(csrq("rand",minRP,minRP,T,T), null, numDocs).scoreDocs;
  assertEquals("min,min,T,T", 1, result.length);
  result = search.search(csrq("rand",null,minRP,F,T), null, numDocs).scoreDocs;
  assertEquals("nul,min,F,T", 1, result.length);

  result = search.search(csrq("rand",maxRP,maxRP,T,T), null, numDocs).scoreDocs;
  assertEquals("max,max,T,T", 1, result.length);
  result = search.search(csrq("rand",maxRP,null,T,F), null, numDocs).scoreDocs;
  assertEquals("max,nul,T,T", 1, result.length);
        
    }

    public void testRangeQueryRandCollating() throws IOException {
        // NOTE: uses index build in *super* setUp

        // using the unsigned index because collation seems to ignore hyphens
        IndexReader reader = IndexReader.open(unsignedIndex.index);
        IndexSearcher search = new IndexSearcher(reader);

        String minRP = pad(unsignedIndex.minR);
        String maxRP = pad(unsignedIndex.maxR);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
        ScoreDoc[] result;
        
        Collator c = Collator.getInstance(Locale.ENGLISH);

        // test extremes, bounded on both ends
        
        result = search.search(csrq("rand",minRP,maxRP,T,T,c), null, numDocs).scoreDocs;
        assertEquals("find all", numDocs, result.length);

        result = search.search(csrq("rand",minRP,maxRP,T,F,c), null, numDocs).scoreDocs;
        assertEquals("all but biggest", numDocs-1, result.length);

        result = search.search(csrq("rand",minRP,maxRP,F,T,c), null, numDocs).scoreDocs;
        assertEquals("all but smallest", numDocs-1, result.length);
        
        result = search.search(csrq("rand",minRP,maxRP,F,F,c), null, numDocs).scoreDocs;
        assertEquals("all but extremes", numDocs-2, result.length);
    
        // unbounded

        result = search.search(csrq("rand",minRP,null,T,F,c), null, numDocs).scoreDocs;
        assertEquals("smallest and up", numDocs, result.length);

        result = search.search(csrq("rand",null,maxRP,F,T,c), null, numDocs).scoreDocs;
        assertEquals("biggest and down", numDocs, result.length);

        result = search.search(csrq("rand",minRP,null,F,F,c), null, numDocs).scoreDocs;
        assertEquals("not smallest, but up", numDocs-1, result.length);
        
        result = search.search(csrq("rand",null,maxRP,F,F,c), null, numDocs).scoreDocs;
        assertEquals("not biggest, but down", numDocs-1, result.length);
        
        // very small sets

        result = search.search(csrq("rand",minRP,minRP,F,F,c), null, numDocs).scoreDocs;
        assertEquals("min,min,F,F,c", 0, result.length);
        result = search.search(csrq("rand",maxRP,maxRP,F,F,c), null, numDocs).scoreDocs;
        assertEquals("max,max,F,F,c", 0, result.length);
                     
        result = search.search(csrq("rand",minRP,minRP,T,T,c), null, numDocs).scoreDocs;
        assertEquals("min,min,T,T,c", 1, result.length);
        result = search.search(csrq("rand",null,minRP,F,T,c), null, numDocs).scoreDocs;
        assertEquals("nul,min,F,T,c", 1, result.length);

        result = search.search(csrq("rand",maxRP,maxRP,T,T,c), null, numDocs).scoreDocs;
        assertEquals("max,max,T,T,c", 1, result.length);
        result = search.search(csrq("rand",maxRP,null,T,F,c), null, numDocs).scoreDocs;
        assertEquals("max,nul,T,T,c", 1, result.length);
    }
    
    public void testFarsi() throws Exception {
            
        /* build an index */
        RAMDirectory farsiIndex = new RAMDirectory();
        IndexWriter writer = new IndexWriter(farsiIndex, new SimpleAnalyzer(), T, 
                                             IndexWriter.MaxFieldLength.LIMITED);
        Document doc = new Document();
        doc.add(new Field("content","\u0633\u0627\u0628", 
                          Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("body", "body",
                          Field.Store.YES, Field.Index.NOT_ANALYZED));
        writer.addDocument(doc);
            
        writer.optimize();
        writer.close();

        IndexReader reader = IndexReader.open(farsiIndex);
        IndexSearcher search = new IndexSearcher(reader);

        // Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
        // RuleBasedCollator.  However, the Arabic Locale seems to order the Farsi
        // characters properly.
        Collator c = Collator.getInstance(new Locale("ar"));
        
        // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
        // orders the U+0698 character before the U+0633 character, so the single
        // index Term below should NOT be returned by a ConstantScoreRangeQuery
        // with a Farsi Collator (or an Arabic one for the case when Farsi is 
        // not supported).
        ScoreDoc[] result = search.search(csrq("content","\u062F", "\u0698", T, T, c), null, 1000).scoreDocs;
        assertEquals("The index Term should not be included.", 0, result.length);

        result = search.search(csrq("content", "\u0633", "\u0638", T, T, c), null, 1000).scoreDocs;
        assertEquals("The index Term should be included.", 1, result.length);
        search.close();
    }
}

package org.apache.lucene.search;

/**
 * Copyright 2005 Apache Software Foundation
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


import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

import org.apache.lucene.analysis.WhitespaceAnalyzer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;

import junit.framework.TestCase;

import java.util.Random;

/** Test BooleanQuery2 against BooleanQuery by overriding the standard query parser.
 * This also tests the scoring order of BooleanQuery.
 */
public class TestBoolean2 extends TestCase {
  private IndexSearcher searcher;

  public static final String field = "field";

  public void setUp() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer= new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(new Field(field, docFields[i], Field.Store.NO, Field.Index.TOKENIZED));
      writer.addDocument(doc);
    }
    writer.close();
    searcher = new IndexSearcher(directory);
  }

  private String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3"
  };

  public Query makeQuery(String queryText) throws ParseException {
    return (new QueryParser(field, new WhitespaceAnalyzer())).parse(queryText);
  }

  public void queriesTest(String queryText, int[] expDocNrs) throws Exception {
//System.out.println();
//System.out.println("Query: " + queryText);
    try {
      Query query1 = makeQuery(queryText);
      BooleanQuery.setUseScorer14(true);
      Hits hits1 = searcher.search(query1);

      Query query2 = makeQuery(queryText); // there should be no need to parse again...
      BooleanQuery.setUseScorer14(false);
      Hits hits2 = searcher.search(query2);

      CheckHits.checkHitsQuery(query2, hits1, hits2, expDocNrs);
    } finally { // even when a test fails.
      BooleanQuery.setUseScorer14(false);
    }
  }

  public void testQueries01() throws Exception {
    String queryText = "+w3 +xx";
    int[] expDocNrs = {2,3};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries02() throws Exception {
    String queryText = "+w3 xx";
    int[] expDocNrs = {2,3,1,0};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries03() throws Exception {
    String queryText = "w3 xx";
    int[] expDocNrs = {2,3,1,0};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries04() throws Exception {
    String queryText = "w3 -xx";
    int[] expDocNrs = {1,0};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries05() throws Exception {
    String queryText = "+w3 -xx";
    int[] expDocNrs = {1,0};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries06() throws Exception {
    String queryText = "+w3 -xx -w5";
    int[] expDocNrs = {1};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries07() throws Exception {
    String queryText = "-w3 -xx -w5";
    int[] expDocNrs = {};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries08() throws Exception {
    String queryText = "+w3 xx -w5";
    int[] expDocNrs = {2,3,1};
    queriesTest(queryText, expDocNrs);
  }

  public void testQueries09() throws Exception {
    String queryText = "+w3 +xx +w2 zz";
    int[] expDocNrs = {2, 3};
    queriesTest(queryText, expDocNrs);
  }

    public void testQueries10() throws Exception {
    String queryText = "+w3 +xx +w2 zz";
    int[] expDocNrs = {2, 3};
    searcher.setSimilarity(new DefaultSimilarity(){
      public float coord(int overlap, int maxOverlap) {
        return overlap / ((float)maxOverlap - 1);
      }
    });
    queriesTest(queryText, expDocNrs);
  }

  public void testRandomQueries() throws Exception {
    Random rnd = new Random(0);

    String[] vals = {"w1","w2","w3","w4","w5","xx","yy","zzz"};

    int tot=0;

    try {

      // increase number of iterations for more complete testing
      for (int i=0; i<1000; i++) {
        int level = rnd.nextInt(3);
        BooleanQuery q1 = randBoolQuery(new Random(i), level, field, vals, null);

        // Can't sort by relevance since floating point numbers may not quite
        // match up.
        Sort sort = Sort.INDEXORDER;

        BooleanQuery.setUseScorer14(false);
        Hits hits1 = searcher.search(q1,sort);
        if (hits1.length()>0) hits1.id(hits1.length()-1);

        BooleanQuery.setUseScorer14(true);
        Hits hits2 = searcher.search(q1,sort);
        if (hits2.length()>0) hits2.id(hits1.length()-1);
        tot+=hits2.length();
        CheckHits.checkEqual(q1, hits1, hits2);
      }

    } finally { // even when a test fails.
      BooleanQuery.setUseScorer14(false);
    }

    // System.out.println("Total hits:"+tot);
  }


  // used to set properties or change every BooleanQuery
  // generated from randBoolQuery.
  public static interface Callback {
    public void postCreate(BooleanQuery q);
  }

  // Random rnd is passed in so that the exact same random query may be created
  // more than once.
  public static BooleanQuery randBoolQuery(Random rnd, int level, String field, String[] vals, Callback cb) {
    BooleanQuery current = new BooleanQuery(rnd.nextInt()<0);
    for (int i=0; i<rnd.nextInt(vals.length)+1; i++) {
      int qType=0; // term query
      if (level>0) {
        qType = rnd.nextInt(10);
      }
      Query q;
      if (qType < 7) q = new TermQuery(new Term(field, vals[rnd.nextInt(vals.length)]));
      else q = randBoolQuery(rnd, level-1, field, vals, cb);

      int r = rnd.nextInt(10);
      BooleanClause.Occur occur;
      if (r<2) occur=BooleanClause.Occur.MUST_NOT;
      else if (r<5) occur=BooleanClause.Occur.MUST;
      else occur=BooleanClause.Occur.SHOULD;

      current.add(q, occur);
    }
    if (cb!=null) cb.postCreate(current);
    return current;
  }


}

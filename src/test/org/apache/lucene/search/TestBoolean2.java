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

import org.apache.lucene.analysis.WhitespaceAnalyzer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;

import junit.framework.TestCase;

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
    Query query1 = makeQuery(queryText);
    BooleanQuery.setUseScorer14(true);
    Hits hits1 = searcher.search(query1);

    Query query2 = makeQuery(queryText); // there should be no need to parse again...
    BooleanQuery.setUseScorer14(false);
    Hits hits2 = searcher.search(query2);

    CheckHits.checkHitsQuery(query2, hits1, hits2, expDocNrs, this);
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
}

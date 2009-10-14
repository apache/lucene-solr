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


import java.util.Random;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Test BooleanQuery2 against BooleanQuery by overriding the standard query parser.
 * This also tests the scoring order of BooleanQuery.
 */
public class TestBoolean2 extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexSearcher bigSearcher;
  private IndexReader reader;
  private static int NUM_EXTRA_DOCS = 6000;

  public static final String field = "field";
  private Directory dir2;
  private int mulFactor;

  public void setUp() throws Exception {
    super.setUp();
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer= new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(new Field(field, docFields[i], Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.close();
    searcher = new IndexSearcher(directory, true);

    // Make big index
    dir2 = new MockRAMDirectory(directory);

    // First multiply small test index:
    mulFactor = 1;
    int docCount = 0;
    do {
      final Directory copy = new RAMDirectory(dir2);
      IndexWriter w = new IndexWriter(dir2, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
      w.addIndexesNoOptimize(new Directory[] {copy});
      docCount = w.maxDoc();
      w.close();
      mulFactor *= 2;
    } while(docCount < 3000);

    IndexWriter w = new IndexWriter(dir2, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
    Document doc = new Document();
    doc.add(new Field("field2", "xxx", Field.Store.NO, Field.Index.ANALYZED));
    for(int i=0;i<NUM_EXTRA_DOCS/2;i++) {
      w.addDocument(doc);
    }
    doc = new Document();
    doc.add(new Field("field2", "big bad bug", Field.Store.NO, Field.Index.ANALYZED));
    for(int i=0;i<NUM_EXTRA_DOCS/2;i++) {
      w.addDocument(doc);
    }
    // optimize to 1 segment
    w.optimize();
    reader = w.getReader();
    w.close();
    bigSearcher = new IndexSearcher(reader);
  }

  public void tearDown() throws Exception {
    reader.close();
    dir2.close();
  }

  private String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3"
  };

  public Query makeQuery(String queryText) throws ParseException {
    Query q = (new QueryParser(field, new WhitespaceAnalyzer())).parse(queryText);
    return q;
  }

  public void queriesTest(String queryText, int[] expDocNrs) throws Exception {
//System.out.println();
//System.out.println("Query: " + queryText);

    Query query1 = makeQuery(queryText);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1000, false);
    searcher.search(query1, null, collector);
    ScoreDoc[] hits1 = collector.topDocs().scoreDocs;
    
    Query query2 = makeQuery(queryText); // there should be no need to parse again...
    collector = TopScoreDocCollector.create(1000, true);
    searcher.search(query2, null, collector);
    ScoreDoc[] hits2 = collector.topDocs().scoreDocs; 

    assertEquals(mulFactor * collector.totalHits,
                 bigSearcher.search(query1, 1).totalHits);
      
    CheckHits.checkHitsQuery(query2, hits1, hits2, expDocNrs);
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
    Random rnd = newRandom();

    String[] vals = {"w1","w2","w3","w4","w5","xx","yy","zzz"};

    int tot=0;

    BooleanQuery q1 = null;
    try {

      // increase number of iterations for more complete testing
      for (int i=0; i<1000; i++) {
        int level = rnd.nextInt(3);
        q1 = randBoolQuery(new Random(rnd.nextLong()), rnd.nextBoolean(), level, field, vals, null);
        
        // Can't sort by relevance since floating point numbers may not quite
        // match up.
        Sort sort = Sort.INDEXORDER;

        QueryUtils.check(q1,searcher);

        TopFieldCollector collector = TopFieldCollector.create(sort, 1000,
            false, true, true, true);

        searcher.search(q1, null, collector);
        ScoreDoc[] hits1 = collector.topDocs().scoreDocs;

        collector = TopFieldCollector.create(sort, 1000,
            false, true, true, false);
        
        searcher.search(q1, null, collector);
        ScoreDoc[] hits2 = collector.topDocs().scoreDocs;
        tot+=hits2.length;
        CheckHits.checkEqual(q1, hits1, hits2);

        BooleanQuery q3 = new BooleanQuery();
        q3.add(q1, BooleanClause.Occur.SHOULD);
        q3.add(new PrefixQuery(new Term("field2", "b")), BooleanClause.Occur.SHOULD);
        TopDocs hits4 = bigSearcher.search(q3, 1);
        assertEquals(mulFactor*collector.totalHits + NUM_EXTRA_DOCS/2, hits4.totalHits);
      }

    } catch (Exception e) {
      // For easier debugging
      System.out.println("failed query: " + q1);
      throw e;
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
  public static BooleanQuery randBoolQuery(Random rnd, boolean allowMust, int level, String field, String[] vals, Callback cb) {
    BooleanQuery current = new BooleanQuery(rnd.nextInt()<0);
    for (int i=0; i<rnd.nextInt(vals.length)+1; i++) {
      int qType=0; // term query
      if (level>0) {
        qType = rnd.nextInt(10);
      }
      Query q;
      if (qType < 3) {
        q = new TermQuery(new Term(field, vals[rnd.nextInt(vals.length)]));
      } else if (qType < 7) {
        q = new WildcardQuery(new Term(field, "w*"));
      } else {
        q = randBoolQuery(rnd, allowMust, level-1, field, vals, cb);
      }

      int r = rnd.nextInt(10);
      BooleanClause.Occur occur;
      if (r<2) {
        occur=BooleanClause.Occur.MUST_NOT;
      }
      else if (r<5) {
        if (allowMust) {
          occur=BooleanClause.Occur.MUST;
        } else {
          occur=BooleanClause.Occur.SHOULD;
        }
      } else {
        occur=BooleanClause.Occur.SHOULD;
      }

      current.add(q, occur);
    }
    if (cb!=null) cb.postCreate(current);
    return current;
  }


}

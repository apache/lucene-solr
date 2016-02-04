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



import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test BooleanQuery2 against BooleanQuery by overriding the standard query parser.
 * This also tests the scoring order of BooleanQuery.
 */
public class TestBoolean2 extends LuceneTestCase {
  private static IndexSearcher searcher;
  private static IndexSearcher bigSearcher;
  private static IndexReader reader;
  private static IndexReader littleReader;
  private static int NUM_EXTRA_DOCS = 6000;

  public static final String field = "field";
  private static Directory directory;
  private static Directory dir2;
  private static int mulFactor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(field, docFields[i], Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.close();
    littleReader = DirectoryReader.open(directory);
    searcher = newSearcher(littleReader);
    // this is intentionally using the baseline sim, because it compares against bigSearcher (which uses a random one)
    searcher.setSimilarity(new DefaultSimilarity());

    // Make big index
    dir2 = new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(directory));

    // First multiply small test index:
    mulFactor = 1;
    int docCount = 0;
    if (VERBOSE) {
      System.out.println("\nTEST: now copy index...");
    }
    do {
      if (VERBOSE) {
        System.out.println("\nTEST: cycle...");
      }
      final Directory copy = new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(dir2));
      RandomIndexWriter w = new RandomIndexWriter(random(), dir2);
      w.addIndexes(copy);
      docCount = w.maxDoc();
      w.close();
      mulFactor *= 2;
    } while(docCount < 3000);

    RandomIndexWriter w = new RandomIndexWriter(random(), dir2, 
        newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 50, 1000)));
    Document doc = new Document();
    doc.add(newTextField("field2", "xxx", Field.Store.NO));
    for(int i=0;i<NUM_EXTRA_DOCS/2;i++) {
      w.addDocument(doc);
    }
    doc = new Document();
    doc.add(newTextField("field2", "big bad bug", Field.Store.NO));
    for(int i=0;i<NUM_EXTRA_DOCS/2;i++) {
      w.addDocument(doc);
    }
    reader = w.getReader();
    bigSearcher = newSearcher(reader);
    w.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    littleReader.close();
    dir2.close();
    directory.close();
    searcher = null;
    reader = null;
    littleReader = null;
    dir2 = null;
    directory = null;
    bigSearcher = null;
  }

  private static String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3"
  };

  public void queriesTest(Query query, int[] expDocNrs) throws Exception {
    // The asserting searcher will sometimes return the bulk scorer and
    // sometimes return a default impl around the scorer so that we can
    // compare BS1 and BS2
    TopScoreDocCollector collector = TopScoreDocCollector.create(1000);
    searcher.search(query, collector);
    ScoreDoc[] hits1 = collector.topDocs().scoreDocs;

    collector = TopScoreDocCollector.create(1000);
    searcher.search(query, collector);
    ScoreDoc[] hits2 = collector.topDocs().scoreDocs; 

    assertEquals(mulFactor * collector.totalHits,
                 bigSearcher.search(query, 1).totalHits);
      
    CheckHits.checkHitsQuery(query, hits1, hits2, expDocNrs);
  }

  @Test
  public void testQueries01() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.MUST);
    int[] expDocNrs = {2,3};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries02() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.SHOULD);
    int[] expDocNrs = {2,3,1,0};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries03() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.SHOULD);
    int[] expDocNrs = {2,3,1,0};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries04() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.MUST_NOT);
    int[] expDocNrs = {1,0};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries05() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.MUST_NOT);
    int[] expDocNrs = {1,0};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries06() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.MUST_NOT);
    query.add(new TermQuery(new Term(field, "w5")), BooleanClause.Occur.MUST_NOT);
    int[] expDocNrs = {1};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries07() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST_NOT);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.MUST_NOT);
    query.add(new TermQuery(new Term(field, "w5")), BooleanClause.Occur.MUST_NOT);
    int[] expDocNrs = {};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries08() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(field, "w5")), BooleanClause.Occur.MUST_NOT);
    int[] expDocNrs = {2,3,1};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries09() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "w2")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "zz")), BooleanClause.Occur.SHOULD);
    int[] expDocNrs = {2, 3};
    queriesTest(query.build(), expDocNrs);
  }

  @Test
  public void testQueries10() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "w3")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "xx")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "w2")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "zz")), BooleanClause.Occur.SHOULD);

    int[] expDocNrs = {2, 3};
    Similarity oldSimilarity = searcher.getSimilarity(true);
    try {
      searcher.setSimilarity(new DefaultSimilarity(){
        @Override
        public float coord(int overlap, int maxOverlap) {
          return overlap / ((float)maxOverlap - 1);
        }
      });
      queriesTest(query.build(), expDocNrs);
    } finally {
      searcher.setSimilarity(oldSimilarity);
    }
  }

  @Test
  public void testRandomQueries() throws Exception {
    String[] vals = {"w1","w2","w3","w4","w5","xx","yy","zzz"};

    int tot=0;

    BooleanQuery q1 = null;
    try {

      // increase number of iterations for more complete testing
      int num = atLeast(20);
      for (int i=0; i<num; i++) {
        int level = random().nextInt(3);
        q1 = randBoolQuery(new Random(random().nextLong()), random().nextBoolean(), level, field, vals, null).build();
        
        // Can't sort by relevance since floating point numbers may not quite
        // match up.
        Sort sort = Sort.INDEXORDER;

        QueryUtils.check(random(), q1,searcher); // baseline sim
        try {
          // a little hackish, QueryUtils.check is too costly to do on bigSearcher in this loop.
          searcher.setSimilarity(bigSearcher.getSimilarity(true)); // random sim
          QueryUtils.check(random(), q1, searcher);
        } finally {
          searcher.setSimilarity(new DefaultSimilarity()); // restore
        }

        TopFieldCollector collector = TopFieldCollector.create(sort, 1000,
            false, true, true);

        searcher.search(q1, collector);
        ScoreDoc[] hits1 = collector.topDocs().scoreDocs;

        collector = TopFieldCollector.create(sort, 1000,
            false, true, true);
        
        searcher.search(q1, collector);
        ScoreDoc[] hits2 = collector.topDocs().scoreDocs;
        tot+=hits2.length;
        CheckHits.checkEqual(q1, hits1, hits2);

        BooleanQuery.Builder q3 = new BooleanQuery.Builder();
        q3.add(q1, BooleanClause.Occur.SHOULD);
        q3.add(new PrefixQuery(new Term("field2", "b")), BooleanClause.Occur.SHOULD);
        TopDocs hits4 = bigSearcher.search(q3.build(), 1);
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
    public void postCreate(BooleanQuery.Builder q);
  }

  // Random rnd is passed in so that the exact same random query may be created
  // more than once.
  public static BooleanQuery.Builder randBoolQuery(Random rnd, boolean allowMust, int level, String field, String[] vals, Callback cb) {
    BooleanQuery.Builder current = new BooleanQuery.Builder();
    current.setDisableCoord(rnd.nextBoolean());
    for (int i=0; i<rnd.nextInt(vals.length)+1; i++) {
      int qType=0; // term query
      if (level>0) {
        qType = rnd.nextInt(10);
      }
      Query q;
      if (qType < 3) {
        q = new TermQuery(new Term(field, vals[rnd.nextInt(vals.length)]));
      } else if (qType < 4) {
        String t1 = vals[rnd.nextInt(vals.length)];
        String t2 = vals[rnd.nextInt(vals.length)];
        q = new PhraseQuery(10, field, t1, t2); // slop increases possibility of matching
      } else if (qType < 7) {
        q = new WildcardQuery(new Term(field, "w*"));
      } else {
        q = randBoolQuery(rnd, allowMust, level-1, field, vals, cb).build();
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

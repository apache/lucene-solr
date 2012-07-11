package org.apache.lucene.search;

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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.spans.*;

/**
 * TestExplanations subclass that builds up super crazy complex queries
 * on the assumption that if the explanations work out right for them,
 * they should work for anything.
 */
public class TestComplexExplanations extends TestExplanations {

  /**
   * Override the Similarity used in our searcher with one that plays
   * nice with boosts of 0.0
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    searcher.setSimilarity(createQnorm1Similarity());
  }
  
  @Override
  public void tearDown() throws Exception {
    searcher.setSimilarity(IndexSearcher.getDefaultSimilarity());
    super.tearDown();
  }

  // must be static for weight serialization tests 
  private static DefaultSimilarity createQnorm1Similarity() {
    return new DefaultSimilarity() {
        @Override
        public float queryNorm(float sumOfSquaredWeights) {
          return 1.0f; // / (float) Math.sqrt(1.0f + sumOfSquaredWeights);
        }
      };
  }

  
  public void test1() throws Exception {
    
    BooleanQuery q = new BooleanQuery();

    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(1);
    phraseQuery.add(new Term(FIELD, "w1"));
    phraseQuery.add(new Term(FIELD, "w2"));
    q.add(phraseQuery, Occur.MUST);
    q.add(snear(st("w2"),
                sor("w5","zz"),
                4, true),
          Occur.SHOULD);
    q.add(snear(sf("w3",2), st("w2"), st("w3"), 5, true),
          Occur.SHOULD);
    
    Query t = new FilteredQuery(new TermQuery(new Term(FIELD, "xx")),
                                new ItemizedFilter(new int[] {1,3}));
    t.setBoost(1000);
    q.add(t, Occur.SHOULD);
    
    t = new ConstantScoreQuery(new ItemizedFilter(new int[] {0,2}));
    t.setBoost(30);
    q.add(t, Occur.SHOULD);
    
    DisjunctionMaxQuery dm = new DisjunctionMaxQuery(0.2f);
    dm.add(snear(st("w2"),
                 sor("w5","zz"),
                 4, true));
    dm.add(new TermQuery(new Term(FIELD, "QQ")));

    BooleanQuery xxYYZZ = new BooleanQuery();
    xxYYZZ.add(new TermQuery(new Term(FIELD, "xx")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "yy")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "zz")), Occur.MUST_NOT);

    dm.add(xxYYZZ);

    BooleanQuery xxW1 = new BooleanQuery();
    xxW1.add(new TermQuery(new Term(FIELD, "xx")), Occur.MUST_NOT);
    xxW1.add(new TermQuery(new Term(FIELD, "w1")), Occur.MUST_NOT);

    dm.add(xxW1);

    DisjunctionMaxQuery dm2 = new DisjunctionMaxQuery(0.5f);
    dm2.add(new TermQuery(new Term(FIELD, "w1")));
    dm2.add(new TermQuery(new Term(FIELD, "w2")));
    dm2.add(new TermQuery(new Term(FIELD, "w3")));
    dm.add(dm2);

    q.add(dm, Occur.SHOULD);

    BooleanQuery b = new BooleanQuery();
    b.setMinimumNumberShouldMatch(2);
    b.add(snear("w1","w2",1,true), Occur.SHOULD);
    b.add(snear("w2","w3",1,true), Occur.SHOULD);
    b.add(snear("w1","w3",3,true), Occur.SHOULD);

    q.add(b, Occur.SHOULD);
    
    qtest(q, new int[] { 0,1,2 });
  }

  public void test2() throws Exception {
    
    BooleanQuery q = new BooleanQuery();

    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(1);
    phraseQuery.add(new Term(FIELD, "w1"));
    phraseQuery.add(new Term(FIELD, "w2"));
    q.add(phraseQuery, Occur.MUST);
    q.add(snear(st("w2"),
                sor("w5","zz"),
                4, true),
          Occur.SHOULD);
    q.add(snear(sf("w3",2), st("w2"), st("w3"), 5, true),
          Occur.SHOULD);
    
    Query t = new FilteredQuery(new TermQuery(new Term(FIELD, "xx")),
                                new ItemizedFilter(new int[] {1,3}));
    t.setBoost(1000);
    q.add(t, Occur.SHOULD);
    
    t = new ConstantScoreQuery(new ItemizedFilter(new int[] {0,2}));
    t.setBoost(-20.0f);
    q.add(t, Occur.SHOULD);
    
    DisjunctionMaxQuery dm = new DisjunctionMaxQuery(0.2f);
    dm.add(snear(st("w2"),
                 sor("w5","zz"),
                 4, true));
    dm.add(new TermQuery(new Term(FIELD, "QQ")));

    BooleanQuery xxYYZZ = new BooleanQuery();
    xxYYZZ.add(new TermQuery(new Term(FIELD, "xx")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "yy")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "zz")), Occur.MUST_NOT);

    dm.add(xxYYZZ);

    BooleanQuery xxW1 = new BooleanQuery();
    xxW1.add(new TermQuery(new Term(FIELD, "xx")), Occur.MUST_NOT);
    xxW1.add(new TermQuery(new Term(FIELD, "w1")), Occur.MUST_NOT);

    dm.add(xxW1);

    DisjunctionMaxQuery dm2 = new DisjunctionMaxQuery(0.5f);
    dm2.add(new TermQuery(new Term(FIELD, "w1")));
    dm2.add(new TermQuery(new Term(FIELD, "w2")));
    dm2.add(new TermQuery(new Term(FIELD, "w3")));
    dm.add(dm2);

    q.add(dm, Occur.SHOULD);

    BooleanQuery b = new BooleanQuery();
    b.setMinimumNumberShouldMatch(2);
    b.add(snear("w1","w2",1,true), Occur.SHOULD);
    b.add(snear("w2","w3",1,true), Occur.SHOULD);
    b.add(snear("w1","w3",3,true), Occur.SHOULD);
    b.setBoost(0.0f);
    
    q.add(b, Occur.SHOULD);
    
    qtest(q, new int[] { 0,1,2 });
  }
  
  // :TODO: we really need more crazy complex cases.


  // //////////////////////////////////////////////////////////////////

  // The rest of these aren't that complex, but they are <i>somewhat</i>
  // complex, and they expose weakness in dealing with queries that match
  // with scores of 0 wrapped in other queries

  public void testT3() throws Exception {
    TermQuery query = new TermQuery(new Term(FIELD, "w1"));
    query.setBoost(0);
    bqtest(query, new int[] { 0,1,2,3 });
  }

  public void testMA3() throws Exception {
    Query q=new MatchAllDocsQuery();
    q.setBoost(0);
    bqtest(q, new int[] { 0,1,2,3 });
  }
  
  public void testFQ5() throws Exception {
    TermQuery query = new TermQuery(new Term(FIELD, "xx"));
    query.setBoost(0);
    bqtest(new FilteredQuery(query,
                             new ItemizedFilter(new int[] {1,3})),
           new int[] {3});
  }
  
  public void testCSQ4() throws Exception {
    Query q = new ConstantScoreQuery(new ItemizedFilter(new int[] {3}));
    q.setBoost(0);
    bqtest(q, new int[] {3});
  }
  
  public void testDMQ10() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);

    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "yy")), Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    boostedQuery.setBoost(100);
    query.add(boostedQuery, Occur.SHOULD);

    q.add(query);

    TermQuery xxBoostedQuery = new TermQuery(new Term(FIELD, "xx"));
    xxBoostedQuery.setBoost(0);

    q.add(xxBoostedQuery);
    q.setBoost(0.0f);
    bqtest(q, new int[] { 0,2,3 });
  }
  
  public void testMPQ7() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2"}));
    q.setSlop(1);
    q.setBoost(0.0f);
    bqtest(q, new int[] { 0,1,2 });
  }
  
  public void testBQ12() throws Exception {
    // NOTE: using qtest not bqtest
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "w1")), Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w2"));
    boostedQuery.setBoost(0);
    query.add(boostedQuery, Occur.SHOULD);
    
    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testBQ13() throws Exception {
    // NOTE: using qtest not bqtest
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "w1")), Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    boostedQuery.setBoost(0);
    query.add(boostedQuery, Occur.MUST_NOT);

    qtest(query, new int[] { 1,2,3 });
  }
  public void testBQ18() throws Exception {
    // NOTE: using qtest not bqtest
    BooleanQuery query = new BooleanQuery();
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w1"));
    boostedQuery.setBoost(0);
    query.add(boostedQuery, Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w2")), Occur.SHOULD);
    
    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testBQ21() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "w1")), Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w2")), Occur.SHOULD);
    query.setBoost(0);

    bqtest(query, new int[] { 0,1,2,3 });
  }
  public void testBQ22() throws Exception {
    BooleanQuery query = new BooleanQuery();
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w1"));
    boostedQuery.setBoost(0);
    query.add(boostedQuery, Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w2")), Occur.SHOULD);
    query.setBoost(0);

    bqtest(query, new int[] { 0,1,2,3 });
  }

  public void testST3() throws Exception {
    SpanQuery q = st("w1");
    q.setBoost(0);
    bqtest(q, new int[] {0,1,2,3});
  }
  public void testST6() throws Exception {
    SpanQuery q = st("xx");
    q.setBoost(0);
    qtest(q, new int[] {2,3});
  }

  public void testSF3() throws Exception {
    SpanQuery q = sf(("w1"),1);
    q.setBoost(0);
    bqtest(q, new int[] {0,1,2,3});
  }
  public void testSF7() throws Exception {
    SpanQuery q = sf(("xx"),3);
    q.setBoost(0);
    bqtest(q, new int[] {2,3});
  }
  
  public void testSNot3() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("QQ"));
    q.setBoost(0);
    bqtest(q, new int[] {0,1,2,3});
  }
  public void testSNot6() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("xx"));
    q.setBoost(0);
    bqtest(q, new int[] {0,1,2,3});
  }

  public void testSNot8() throws Exception {
    // NOTE: using qtest not bqtest
    SpanQuery f = snear("w1","w3",10,true);
    f.setBoost(0);
    SpanQuery q = snot(f, st("xx"));
    qtest(q, new int[] {0,1,3});
  }
  public void testSNot9() throws Exception {
    // NOTE: using qtest not bqtest
    SpanQuery t = st("xx");
    t.setBoost(0);
    SpanQuery q = snot(snear("w1","w3",10,true), t);
    qtest(q, new int[] {0,1,3});
  }


  

  
}

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

/**
 * TestExplanations subclass focusing on basic query types
 */
public class TestSimpleExplanations extends TestExplanations {

  // we focus on queries that don't rewrite to other queries.
  // if we get those covered well, then the ones that rewrite should
  // also be covered.
  

  /* simple term tests */
  
  public void testT1() throws Exception {
    qtest(new TermQuery(new Term(FIELD, "w1")), new int[] { 0,1,2,3 });
  }
  public void testT2() throws Exception {
    TermQuery termQuery = new TermQuery(new Term(FIELD, "w1"));
    termQuery.setBoost(100);
    qtest(termQuery, new int[] { 0,1,2,3 });
  }
  
  /* MatchAllDocs */
  
  public void testMA1() throws Exception {
    qtest(new MatchAllDocsQuery(), new int[] { 0,1,2,3 });
  }
  public void testMA2() throws Exception {
    Query q=new MatchAllDocsQuery();
    q.setBoost(1000);
    qtest(q, new int[] { 0,1,2,3 });
  }

  /* some simple phrase tests */
  
  public void testP1() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD, "w1"));
    phraseQuery.add(new Term(FIELD, "w2"));
    qtest(phraseQuery, new int[] { 0 });
  }
  public void testP2() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD, "w1"));
    phraseQuery.add(new Term(FIELD, "w3"));
    qtest(phraseQuery, new int[] { 1,3 });
  }
  public void testP3() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(1);
    phraseQuery.add(new Term(FIELD, "w1"));
    phraseQuery.add(new Term(FIELD, "w2"));
    qtest(phraseQuery, new int[] { 0,1,2 });
  }
  public void testP4() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(1);
    phraseQuery.add(new Term(FIELD, "w2"));
    phraseQuery.add(new Term(FIELD, "w3"));
    qtest(phraseQuery, new int[] { 0,1,2,3 });
  }
  public void testP5() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(1);
    phraseQuery.add(new Term(FIELD, "w3"));
    phraseQuery.add(new Term(FIELD, "w2"));
    qtest(phraseQuery, new int[] { 1,3 });
  }
  public void testP6() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(2);
    phraseQuery.add(new Term(FIELD, "w3"));
    phraseQuery.add(new Term(FIELD, "w2"));
    qtest(phraseQuery, new int[] { 0,1,3 });
  }
  public void testP7() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(3);
    phraseQuery.add(new Term(FIELD, "w3"));
    phraseQuery.add(new Term(FIELD, "w2"));
    qtest(phraseQuery, new int[] { 0,1,2,3 });
  }

  /* some simple filtered query tests */
  
  public void testFQ1() throws Exception {
    qtest(new FilteredQuery(new TermQuery(new Term(FIELD, "w1")),
                            new ItemizedFilter(new int[] {0,1,2,3})),
          new int[] {0,1,2,3});
  }
  public void testFQ2() throws Exception {
    qtest(new FilteredQuery(new TermQuery(new Term(FIELD, "w1")),
                            new ItemizedFilter(new int[] {0,2,3})),
          new int[] {0,2,3});
  }
  public void testFQ3() throws Exception {
    qtest(new FilteredQuery(new TermQuery(new Term(FIELD, "xx")),
                            new ItemizedFilter(new int[] {1,3})),
          new int[] {3});
  }
  public void testFQ4() throws Exception {
    TermQuery termQuery = new TermQuery(new Term(FIELD, "xx"));
    termQuery.setBoost(1000);
    qtest(new FilteredQuery(termQuery, new ItemizedFilter(new int[] {1,3})),
          new int[] {3});
  }
  public void testFQ6() throws Exception {
    Query q = new FilteredQuery(new TermQuery(new Term(FIELD, "xx")),
                                new ItemizedFilter(new int[] {1,3}));
    q.setBoost(1000);
    qtest(q, new int[] {3});
  }

  /* ConstantScoreQueries */
  
  public void testCSQ1() throws Exception {
    Query q = new ConstantScoreQuery(new ItemizedFilter(new int[] {0,1,2,3}));
    qtest(q, new int[] {0,1,2,3});
  }
  public void testCSQ2() throws Exception {
    Query q = new ConstantScoreQuery(new ItemizedFilter(new int[] {1,3}));
    qtest(q, new int[] {1,3});
  }
  public void testCSQ3() throws Exception {
    Query q = new ConstantScoreQuery(new ItemizedFilter(new int[] {0,2}));
    q.setBoost(1000);
    qtest(q, new int[] {0,2});
  }
  
  /* DisjunctionMaxQuery */
  
  public void testDMQ1() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.0f);
    q.add(new TermQuery(new Term(FIELD, "w1")));
    q.add(new TermQuery(new Term(FIELD, "w5")));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ2() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(new TermQuery(new Term(FIELD, "w1")));
    q.add(new TermQuery(new Term(FIELD, "w5")));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ3() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(new TermQuery(new Term(FIELD, "QQ")));
    q.add(new TermQuery(new Term(FIELD, "w5")));
    qtest(q, new int[] { 0 });
  }
  public void testDMQ4() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(new TermQuery(new Term(FIELD, "QQ")));
    q.add(new TermQuery(new Term(FIELD, "xx")));
    qtest(q, new int[] { 2,3 });
  }
  public void testDMQ5() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(FIELD, "QQ")), BooleanClause.Occur.MUST_NOT);

    q.add(booleanQuery);
    q.add(new TermQuery(new Term(FIELD, "xx")));
    qtest(q, new int[] { 2,3 });
  }
  public void testDMQ6() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST_NOT);
    booleanQuery.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.SHOULD);

    q.add(booleanQuery);
    q.add(new TermQuery(new Term(FIELD, "xx")));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ7() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST_NOT);
    booleanQuery.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.SHOULD);

    q.add(booleanQuery);
    q.add(new TermQuery(new Term(FIELD, "w2")));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ8() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);

    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    boostedQuery.setBoost(100);
    booleanQuery.add(boostedQuery, BooleanClause.Occur.SHOULD);
    q.add(booleanQuery);

    TermQuery xxBoostedQuery = new TermQuery(new Term(FIELD, "xx"));
    xxBoostedQuery.setBoost(100000);
    q.add(xxBoostedQuery);
    
    qtest(q, new int[] { 0,2,3 });
  }
  public void testDMQ9() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);

    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    boostedQuery.setBoost(100);
    booleanQuery.add(boostedQuery, BooleanClause.Occur.SHOULD);
    q.add(booleanQuery);

    TermQuery xxBoostedQuery = new TermQuery(new Term(FIELD, "xx"));
    xxBoostedQuery.setBoost(0);
    q.add(xxBoostedQuery);

    qtest(q, new int[] { 0,2,3 });
  }
  
  /* MultiPhraseQuery */
  
  public void testMPQ1() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2","w3", "xx"}));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testMPQ2() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2","w3"}));
    qtest(q, new int[] { 0,1,3 });
  }
  public void testMPQ3() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1","xx"}));
    q.add(ta(new String[] {"w2","w3"}));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testMPQ4() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2"}));
    qtest(q, new int[] { 0 });
  }
  public void testMPQ5() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2"}));
    q.setSlop(1);
    qtest(q, new int[] { 0,1,2 });
  }
  public void testMPQ6() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1","w3"}));
    q.add(ta(new String[] {"w2"}));
    q.setSlop(1);
    qtest(q, new int[] { 0,1,2,3 });
  }

  /* some simple tests of boolean queries containing term queries */
  
  public void testBQ1() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST);
    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testBQ2() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    qtest(query, new int[] { 2,3 });
  }
  public void testBQ3() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testBQ4() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testBQ5() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    
    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.MUST);
    innerQuery.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testBQ6() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(FIELD, "w5")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery, BooleanClause.Occur.MUST_NOT);

    qtest(outerQuery, new int[] { 1,2,3 });
  }
  public void testBQ7() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery, BooleanClause.Occur.MUST);

    qtest(outerQuery, new int[] { 0 });
  }
  public void testBQ8() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testBQ9() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testBQ10() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery, BooleanClause.Occur.MUST);

    qtest(outerQuery, new int[] { 1 });
  }
  public void testBQ11() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w1"));
    boostedQuery.setBoost(1000);
    query.add(boostedQuery, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testBQ14() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testBQ15() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.MUST_NOT);
    q.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testBQ16() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.SHOULD);

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);

    q.add(booleanQuery, BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1 });
  }
  public void testBQ17() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);

    q.add(booleanQuery, BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testBQ19() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST_NOT);
    query.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 0,1 });
  }
  
  public void testBQ20() throws Exception {
    BooleanQuery q = new BooleanQuery();
    q.setMinimumNumberShouldMatch(2);
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "zz")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "w5")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.SHOULD);
    
    qtest(q, new int[] { 0,3 });
    
  }

  /* BQ of TQ: using alt so some fields have zero boost and some don't */
  
  public void testMultiFieldBQ1() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.MUST);

    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ2() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);

    qtest(query, new int[] { 2,3 });
  }
  public void testMultiFieldBQ3() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);

    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ4() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ5() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.MUST);
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ6() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "w5")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery, BooleanClause.Occur.MUST_NOT);

    qtest(outerQuery, new int[] { 1,2,3 });
  }
  public void testMultiFieldBQ7() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(ALTFIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(ALTFIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery, BooleanClause.Occur.MUST);

    qtest(outerQuery, new int[] { 0 });
  }
  public void testMultiFieldBQ8() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(ALTFIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(ALTFIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ9() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery, BooleanClause.Occur.SHOULD);

    qtest(outerQuery, new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ10() throws Exception {
    BooleanQuery outerQuery = new BooleanQuery();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery innerQuery = new BooleanQuery();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery childLeft = new BooleanQuery();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft, BooleanClause.Occur.SHOULD);

    BooleanQuery childRight = new BooleanQuery();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight, BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery, BooleanClause.Occur.MUST);

    qtest(outerQuery, new int[] { 1 });
  }

  /* BQ of PQ: using alt so some fields have zero boost and some don't */
  
  public void testMultiFieldBQofPQ1() throws Exception {
    BooleanQuery query = new BooleanQuery();

    PhraseQuery leftChild = new PhraseQuery();
    leftChild.add(new Term(FIELD, "w1"));
    leftChild.add(new Term(FIELD, "w2"));
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery();
    rightChild.add(new Term(ALTFIELD, "w1"));
    rightChild.add(new Term(ALTFIELD, "w2"));
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 0 });
  }
  public void testMultiFieldBQofPQ2() throws Exception {
    BooleanQuery query = new BooleanQuery();

    PhraseQuery leftChild = new PhraseQuery();
    leftChild.add(new Term(FIELD, "w1"));
    leftChild.add(new Term(FIELD, "w3"));
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery();
    rightChild.add(new Term(ALTFIELD, "w1"));
    rightChild.add(new Term(ALTFIELD, "w3"));
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 1,3 });
  }
  public void testMultiFieldBQofPQ3() throws Exception {
    BooleanQuery query = new BooleanQuery();

    PhraseQuery leftChild = new PhraseQuery();
    leftChild.setSlop(1);
    leftChild.add(new Term(FIELD, "w1"));
    leftChild.add(new Term(FIELD, "w2"));
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery();
    rightChild.setSlop(1);
    rightChild.add(new Term(ALTFIELD, "w1"));
    rightChild.add(new Term(ALTFIELD, "w2"));
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 0,1,2 });
  }
  public void testMultiFieldBQofPQ4() throws Exception {
    BooleanQuery query = new BooleanQuery();

    PhraseQuery leftChild = new PhraseQuery();
    leftChild.setSlop(1);
    leftChild.add(new Term(FIELD, "w2"));
    leftChild.add(new Term(FIELD, "w3"));
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery();
    rightChild.setSlop(1);
    rightChild.add(new Term(ALTFIELD, "w2"));
    rightChild.add(new Term(ALTFIELD, "w3"));
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQofPQ5() throws Exception {
    BooleanQuery query = new BooleanQuery();

    PhraseQuery leftChild = new PhraseQuery();
    leftChild.setSlop(1);
    leftChild.add(new Term(FIELD, "w3"));
    leftChild.add(new Term(FIELD, "w2"));
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery();
    rightChild.setSlop(1);
    rightChild.add(new Term(ALTFIELD, "w3"));
    rightChild.add(new Term(ALTFIELD, "w2"));
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 1,3 });
  }
  public void testMultiFieldBQofPQ6() throws Exception {
    BooleanQuery query = new BooleanQuery();

    PhraseQuery leftChild = new PhraseQuery();
    leftChild.setSlop(2);
    leftChild.add(new Term(FIELD, "w3"));
    leftChild.add(new Term(FIELD, "w2"));
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery();
    rightChild.setSlop(2);
    rightChild.add(new Term(ALTFIELD, "w3"));
    rightChild.add(new Term(ALTFIELD, "w2"));
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 0,1,3 });
  }
  public void testMultiFieldBQofPQ7() throws Exception {
    BooleanQuery query = new BooleanQuery();

    PhraseQuery leftChild = new PhraseQuery();
    leftChild.setSlop(3);
    leftChild.add(new Term(FIELD, "w3"));
    leftChild.add(new Term(FIELD, "w2"));
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery();
    rightChild.setSlop(1);
    rightChild.add(new Term(ALTFIELD, "w3"));
    rightChild.add(new Term(ALTFIELD, "w2"));
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query, new int[] { 0,1,2,3 });
  }

}

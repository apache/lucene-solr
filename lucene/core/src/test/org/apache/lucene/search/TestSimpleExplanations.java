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


import java.util.Arrays;

import org.apache.lucene.index.Term;
import org.junit.Test;

/**
 * TestExplanations subclass focusing on basic query types
 */
public class TestSimpleExplanations extends BaseExplanationTestCase {

  // we focus on queries that don't rewrite to other queries.
  // if we get those covered well, then the ones that rewrite should
  // also be covered.
  

  /* simple term tests */
  
  public void testT1() throws Exception {
    qtest(new TermQuery(new Term(FIELD, "w1")), new int[] { 0,1,2,3 });
  }
  public void testT2() throws Exception {
    TermQuery termQuery = new TermQuery(new Term(FIELD, "w1"));
    qtest(new BoostQuery(termQuery, 100), new int[] { 0,1,2,3 });
  }
  
  /* MatchAllDocs */
  
  public void testMA1() throws Exception {
    qtest(new MatchAllDocsQuery(), new int[] { 0,1,2,3 });
  }
  public void testMA2() throws Exception {
    Query q=new MatchAllDocsQuery();
    qtest(new BoostQuery(q, 1000), new int[] { 0,1,2,3 });
  }

  /* some simple phrase tests */
  
  public void testP1() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(FIELD, "w1", "w2");
    qtest(phraseQuery, new int[] { 0 });
  }
  public void testP2() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(FIELD, "w1", "w3");
    qtest(phraseQuery, new int[] { 1,3 });
  }
  public void testP3() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(1, FIELD, "w1", "w2");
    qtest(phraseQuery, new int[] { 0,1,2 });
  }
  public void testP4() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(1, FIELD, "w2", "w3");
    qtest(phraseQuery, new int[] { 0,1,2,3 });
  }
  public void testP5() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(1, FIELD, "w3", "w2");
    qtest(phraseQuery, new int[] { 1,3 });
  }
  public void testP6() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(2, FIELD, "w3", "w2");
    qtest(phraseQuery, new int[] { 0,1,3 });
  }
  public void testP7() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(3, FIELD, "w3", "w2");
    qtest(phraseQuery, new int[] { 0,1,2,3 });
  }

  /* ConstantScoreQueries */
  
  public void testCSQ1() throws Exception {
    Query q = new ConstantScoreQuery(matchTheseItems(new int[] {0,1,2,3}));
    qtest(q, new int[] {0,1,2,3});
  }
  public void testCSQ2() throws Exception {
    Query q = new ConstantScoreQuery(matchTheseItems(new int[] {1,3}));
    qtest(q, new int[] {1,3});
  }
  public void testCSQ3() throws Exception {
    Query q = new ConstantScoreQuery(matchTheseItems(new int[] {0,2}));
    qtest(new BoostQuery(q, 1000), new int[] {0,2});
  }
  
  /* DisjunctionMaxQuery */
  
  public void testDMQ1() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            new TermQuery(new Term(FIELD, "w1")),
            new TermQuery(new Term(FIELD, "w5"))),
        0.0f);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ2() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            new TermQuery(new Term(FIELD, "w1")),
            new TermQuery(new Term(FIELD, "w5"))),
        0.5f);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ3() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            new TermQuery(new Term(FIELD, "QQ")),
            new TermQuery(new Term(FIELD, "w5"))),
        0.5f);
    qtest(q, new int[] { 0 });
  }
  public void testDMQ4() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            new TermQuery(new Term(FIELD, "QQ")),
            new TermQuery(new Term(FIELD, "xx"))),
        0.5f);
    qtest(q, new int[] { 2,3 });
  }
  public void testDMQ5() throws Exception {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(FIELD, "QQ")), BooleanClause.Occur.MUST_NOT);

    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            booleanQuery.build(),
            new TermQuery(new Term(FIELD, "xx"))),
        0.5f);
    qtest(q, new int[] { 2,3 });
  }
  public void testDMQ6() throws Exception {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST_NOT);
    booleanQuery.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.SHOULD);

    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            booleanQuery.build(),
            new TermQuery(new Term(FIELD, "xx"))),
        0.5f);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ7() throws Exception {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST_NOT);
    booleanQuery.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.SHOULD);

    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            booleanQuery.build(),
            new TermQuery(new Term(FIELD, "w2"))),
        0.5f);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ8() throws Exception {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);

    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    booleanQuery.add(new BoostQuery(boostedQuery, 100), BooleanClause.Occur.SHOULD);

    TermQuery xxBoostedQuery = new TermQuery(new Term(FIELD, "xx"));

    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(booleanQuery.build(), new BoostQuery(xxBoostedQuery, 100000)),
        0.5f);
    qtest(q, new int[] { 0,2,3 });
  }
  public void testDMQ9() throws Exception {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);

    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w5"));
    booleanQuery.add(new BoostQuery(boostedQuery, 100), BooleanClause.Occur.SHOULD);

    TermQuery xxBoostedQuery = new TermQuery(new Term(FIELD, "xx"));

    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(booleanQuery.build(), new BoostQuery(xxBoostedQuery, 0)),
        0.5f);

    qtest(q, new int[] { 0,2,3 });
  }
  
  /* MultiPhraseQuery */
  
  public void testMPQ1() throws Exception {
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(ta(new String[] {"w1"}));
    qb.add(ta(new String[] {"w2","w3", "xx"}));
    qtest(qb.build(), new int[] { 0,1,2,3 });
  }
  public void testMPQ2() throws Exception {
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(ta(new String[] {"w1"}));
    qb.add(ta(new String[] {"w2","w3"}));
    qtest(qb.build(), new int[] { 0,1,3 });
  }
  public void testMPQ3() throws Exception {
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(ta(new String[] {"w1","xx"}));
    qb.add(ta(new String[] {"w2","w3"}));
    qtest(qb.build(), new int[] { 0,1,2,3 });
  }
  public void testMPQ4() throws Exception {
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(ta(new String[] {"w1"}));
    qb.add(ta(new String[] {"w2"}));
    qtest(qb.build(), new int[] { 0 });
  }
  public void testMPQ5() throws Exception {
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(ta(new String[] {"w1"}));
    qb.add(ta(new String[] {"w2"}));
    qb.setSlop(1);
    qtest(qb.build(), new int[] { 0,1,2 });
  }
  public void testMPQ6() throws Exception {
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(ta(new String[] {"w1","w3"}));
    qb.add(ta(new String[] {"w2"}));
    qb.setSlop(1);
    qtest(qb.build(), new int[] { 0,1,2,3 });
  }

  /* some simple tests of boolean queries containing term queries */
  
  public void testBQ1() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST);
    qtest(query.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ2() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    qtest(query.build(), new int[] { 2,3 });
  }
  public void testBQ3() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    qtest(query.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ4() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ5() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    
    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.MUST);
    innerQuery.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ6() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(FIELD, "w5")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery.build(), BooleanClause.Occur.MUST_NOT);

    qtest(outerQuery.build(), new int[] { 1,2,3 });
  }
  public void testBQ7() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.setDisableCoord(random().nextBoolean());
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.MUST);

    qtest(outerQuery.build(), new int[] { 0 });
  }
  public void testBQ8() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.setDisableCoord(random().nextBoolean());
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ9() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.setDisableCoord(random().nextBoolean());
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ10() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.setDisableCoord(random().nextBoolean());
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.MUST);

    qtest(outerQuery.build(), new int[] { 1 });
  }
  public void testBQ11() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.setDisableCoord(random().nextBoolean());
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    TermQuery boostedQuery = new TermQuery(new Term(FIELD, "w1"));
    query.add(new BoostQuery(boostedQuery, 1000), BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ14() throws Exception {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.setDisableCoord(random().nextBoolean());
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    qtest(q.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ15() throws Exception {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.setDisableCoord(random().nextBoolean());
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.MUST_NOT);
    q.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    qtest(q.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ16() throws Exception {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.setDisableCoord(random().nextBoolean());
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);

    q.add(booleanQuery.build(), BooleanClause.Occur.SHOULD);
    qtest(q.build(), new int[] { 0,1 });
  }
  public void testBQ17() throws Exception {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.setDisableCoord(random().nextBoolean());
    q.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);

    q.add(booleanQuery.build(), BooleanClause.Occur.SHOULD);
    qtest(q.build(), new int[] { 0,1,2,3 });
  }
  public void testBQ19() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST_NOT);
    query.add(new TermQuery(new Term(FIELD, "w3")), BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 0,1 });
  }
  
  public void testBQ20() throws Exception {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.setDisableCoord(random().nextBoolean());
    q.setMinimumNumberShouldMatch(2);
    q.add(new TermQuery(new Term(FIELD, "QQQQQ")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "zz")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "w5")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.SHOULD);
    
    qtest(q.build(), new int[] { 0,3 });
    
  }
  
  public void testBQ21() throws Exception {
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.setDisableCoord(random().nextBoolean());
    q.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    q.add(new TermQuery(new Term(FIELD, "zz")), BooleanClause.Occur.SHOULD);
    
    qtest(q.build(), new int[] { 1,2,3 });
    
  }

  public void testBQ23() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.FILTER);
    query.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.FILTER);
    qtest(query.build(), new int[] { 0,1,2,3 });
  }

  public void testBQ24() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.FILTER);
    query.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    qtest(query.build(), new int[] { 0,1,2,3 });
  }

  public void testBQ25() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.FILTER);
    query.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST);
    qtest(query.build(), new int[] { 0,1,2,3 });
  }

  public void testBQ26() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.FILTER);
    query.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    qtest(query.build(), new int[] { 0,1 });
  }

  /* BQ of TQ: using alt so some fields have zero boost and some don't */
  
  public void testMultiFieldBQ1() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.MUST);

    qtest(query.build(), new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ2() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);

    qtest(query.build(), new int[] { 2,3 });
  }
  public void testMultiFieldBQ3() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.setDisableCoord(random().nextBoolean());
    query.add(new TermQuery(new Term(FIELD, "yy")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);

    qtest(query.build(), new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ4() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.setDisableCoord(random().nextBoolean());
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ5() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.setDisableCoord(random().nextBoolean());
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.MUST);
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ6() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "w5")), BooleanClause.Occur.SHOULD);
    outerQuery.add(innerQuery.build(), BooleanClause.Occur.MUST_NOT);

    qtest(outerQuery.build(), new int[] { 1,2,3 });
  }
  public void testMultiFieldBQ7() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(ALTFIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(ALTFIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.MUST);

    qtest(outerQuery.build(), new int[] { 0 });
  }
  public void testMultiFieldBQ8() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(ALTFIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(FIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(ALTFIELD, "xx")), BooleanClause.Occur.SHOULD);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.MUST_NOT);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.SHOULD);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ9() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(FIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.SHOULD);

    qtest(outerQuery.build(), new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ10() throws Exception {
    BooleanQuery.Builder outerQuery = new BooleanQuery.Builder();
    outerQuery.add(new TermQuery(new Term(FIELD, "w1")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder innerQuery = new BooleanQuery.Builder();
    innerQuery.add(new TermQuery(new Term(ALTFIELD, "qq")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childLeft = new BooleanQuery.Builder();
    childLeft.add(new TermQuery(new Term(FIELD, "xx")), BooleanClause.Occur.MUST_NOT);
    childLeft.add(new TermQuery(new Term(ALTFIELD, "w2")), BooleanClause.Occur.SHOULD);
    innerQuery.add(childLeft.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder childRight = new BooleanQuery.Builder();
    childRight.add(new TermQuery(new Term(ALTFIELD, "w3")), BooleanClause.Occur.MUST);
    childRight.add(new TermQuery(new Term(FIELD, "w4")), BooleanClause.Occur.MUST);
    innerQuery.add(childRight.build(), BooleanClause.Occur.MUST_NOT);

    outerQuery.add(innerQuery.build(), BooleanClause.Occur.MUST);

    qtest(outerQuery.build(), new int[] { 1 });
  }

  /* BQ of PQ: using alt so some fields have zero boost and some don't */
  
  public void testMultiFieldBQofPQ1() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    PhraseQuery leftChild = new PhraseQuery(FIELD, "w1", "w2");
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery(ALTFIELD, "w1", "w2");
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 0 });
  }
  public void testMultiFieldBQofPQ2() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    PhraseQuery leftChild = new PhraseQuery(FIELD, "w1", "w3");
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery(ALTFIELD, "w1", "w3");
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 1,3 });
  }
  public void testMultiFieldBQofPQ3() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    PhraseQuery leftChild = new PhraseQuery(1, FIELD, "w1", "w2");
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery(1, ALTFIELD, "w1", "w2");
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 0,1,2 });
  }
  public void testMultiFieldBQofPQ4() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    PhraseQuery leftChild = new PhraseQuery(1, FIELD, "w2", "w3");
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery(1, ALTFIELD, "w2", "w3");
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQofPQ5() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    PhraseQuery leftChild = new PhraseQuery(1, FIELD, "w3", "w2");
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery(1, ALTFIELD, "w3", "w2");
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 1,3 });
  }
  public void testMultiFieldBQofPQ6() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    PhraseQuery leftChild = new PhraseQuery(2, FIELD, "w3", "w2");
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery(2, ALTFIELD, "w3", "w2");
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 0,1,3 });
  }
  public void testMultiFieldBQofPQ7() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();

    PhraseQuery leftChild = new PhraseQuery(3, FIELD, "w3", "w2");
    query.add(leftChild, BooleanClause.Occur.SHOULD);

    PhraseQuery rightChild = new PhraseQuery(1, ALTFIELD, "w3", "w2");
    query.add(rightChild, BooleanClause.Occur.SHOULD);

    qtest(query.build(), new int[] { 0,1,2,3 });
  }
  
  public void testSynonymQuery() throws Exception {
    SynonymQuery query = new SynonymQuery(new Term(FIELD, "w1"), new Term(FIELD, "w2"));
    qtest(query, new int[] { 0,1,2,3 });
  }

  @Test
  public void testEquality() {

    Explanation e1 = Explanation.match(1f, "an explanation");
    Explanation e2 = Explanation.match(1f, "an explanation", Explanation.match(1f, "a subexplanation"));
    Explanation e25 = Explanation.match(1f, "an explanation",
        Explanation.match(1f, "a subexplanation", Explanation.match(1f, "a subsubexplanation")));
    Explanation e3 = Explanation.match(1f, "an explanation");
    Explanation e4 = Explanation.match(2f, "an explanation");
    Explanation e5 = Explanation.noMatch("an explanation");
    Explanation e6 = Explanation.noMatch("an explanation", Explanation.match(1f, "a subexplanation"));
    Explanation e7 = Explanation.noMatch("an explanation");
    Explanation e8 = Explanation.match(1f, "another explanation");

    assertEquals(e1, e3);
    assertFalse(e1.equals(e2));
    assertFalse(e2.equals(e25));
    assertFalse(e1.equals(e4));
    assertFalse(e1.equals(e5));
    assertEquals(e5, e7);
    assertFalse(e5.equals(e6));
    assertFalse(e1.equals(e8));

    assertEquals(e1.hashCode(), e3.hashCode());
    assertEquals(e5.hashCode(), e7.hashCode());

  }

}

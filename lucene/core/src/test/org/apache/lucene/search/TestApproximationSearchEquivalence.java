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


import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;

/**
 * Basic equivalence tests for approximations.
 */
public class TestApproximationSearchEquivalence extends SearchEquivalenceTestBase {
  
  public void testConjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq2.add(new RandomApproximationQuery(q2, random()), Occur.MUST);

    assertSameScores(bq1.build(), bq2.build());
  }

  public void testNestedConjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(bq1.build(), Occur.MUST);
    bq2.add(q3, Occur.MUST);
    
    BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.MUST);

    BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2.build(), bq4.build());
  }

  public void testDisjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.SHOULD);
    bq1.add(q2, Occur.SHOULD);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new RandomApproximationQuery(q1, random()), Occur.SHOULD);
    bq2.add(new RandomApproximationQuery(q2, random()), Occur.SHOULD);

    assertSameScores(bq1.build(), bq2.build());
  }

  public void testNestedDisjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.SHOULD);
    bq1.add(q2, Occur.SHOULD);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(bq1.build(), Occur.SHOULD);
    bq2.add(q3, Occur.SHOULD);

    BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.SHOULD);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.SHOULD);

    BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.SHOULD);
    bq4.add(q3, Occur.SHOULD);

    assertSameScores(bq2.build(), bq4.build());
  }

  public void testDisjunctionInConjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.SHOULD);
    bq1.add(q2, Occur.SHOULD);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(bq1.build(), Occur.MUST);
    bq2.add(q3, Occur.MUST);

    BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.SHOULD);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.SHOULD);

    BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2.build(), bq4.build());
  }

  public void testConjunctionInDisjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(bq1.build(), Occur.SHOULD);
    bq2.add(q3, Occur.SHOULD);

    BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.MUST);

    BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.SHOULD);
    bq4.add(q3, Occur.SHOULD);

    assertSameScores(bq2.build(), bq4.build());
  }

  public void testConstantScore() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(new ConstantScoreQuery(q1), Occur.MUST);
    bq1.add(new ConstantScoreQuery(q2), Occur.MUST);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new ConstantScoreQuery(new RandomApproximationQuery(q1, random())), Occur.MUST);
    bq2.add(new ConstantScoreQuery(new RandomApproximationQuery(q2, random())), Occur.MUST);

    assertSameScores(bq1.build(), bq2.build());
  }

  public void testExclusion() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST_NOT);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq2.add(new RandomApproximationQuery(q2, random()), Occur.MUST_NOT);

    assertSameScores(bq1.build(), bq2.build());
  }

  public void testNestedExclusion() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST_NOT);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(bq1.build(), Occur.MUST);
    bq2.add(q3, Occur.MUST);

    // Both req and excl have approximations
    BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.MUST_NOT);

    BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2.build(), bq4.build());

    // Only req has an approximation
    bq3 = new BooleanQuery.Builder();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq3.add(q2, Occur.MUST_NOT);

    bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2.build(), bq4.build());

    // Only excl has an approximation
    bq3 = new BooleanQuery.Builder();
    bq3.add(q1, Occur.MUST);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.MUST_NOT);

    bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2.build(), bq4.build());
  }

  public void testReqOpt() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);
    TermQuery q3 = new TermQuery(t3);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.SHOULD);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(bq1.build(), Occur.MUST);
    bq2.add(q3, Occur.MUST);
    
    BooleanQuery.Builder bq3 = new BooleanQuery.Builder();
    bq3.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq3.add(new RandomApproximationQuery(q2, random()), Occur.SHOULD);

    BooleanQuery.Builder bq4 = new BooleanQuery.Builder();
    bq4.add(bq3.build(), Occur.MUST);
    bq4.add(q3, Occur.MUST);

    assertSameScores(bq2.build(), bq4.build());
  }

}

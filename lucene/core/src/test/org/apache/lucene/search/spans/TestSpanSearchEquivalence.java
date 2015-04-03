package org.apache.lucene.search.spans;

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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearchEquivalenceTestBase;
import org.apache.lucene.search.TermQuery;

/**
 * Basic equivalence tests for span queries
 */
public class TestSpanSearchEquivalence extends SearchEquivalenceTestBase {
  
  // TODO: we could go a little crazy for a lot of these,
  // but these are just simple minimal cases in case something 
  // goes horribly wrong. Put more intense tests elsewhere.

  /** SpanTermQuery(A) = TermQuery(A) */
  public void testSpanTermVersusTerm() throws Exception {
    Term t1 = randomTerm();
    assertSameSet(new TermQuery(t1), new SpanTermQuery(t1));
  }
  
  /** SpanOrQuery(A, B) = (A B) */
  public void testSpanOrVersusBoolean() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery q1 = new BooleanQuery();
    q1.add(new TermQuery(t1), Occur.SHOULD);
    q1.add(new TermQuery(t2), Occur.SHOULD);
    SpanOrQuery q2 = new SpanOrQuery(new SpanTermQuery(t1), new SpanTermQuery(t2));
    assertSameSet(q1, q2);
  }
  
  /** SpanNotQuery(A, B) ⊆ SpanTermQuery(A) */
  public void testSpanNotVersusSpanTerm() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    assertSubsetOf(new SpanNotQuery(new SpanTermQuery(t1), new SpanTermQuery(t2)), new SpanTermQuery(t1));
  }
  
  /** SpanFirstQuery(A, 10) ⊆ SpanTermQuery(A) */
  public void testSpanFirstVersusSpanTerm() throws Exception {
    Term t1 = randomTerm();
    assertSubsetOf(new SpanFirstQuery(new SpanTermQuery(t1), 10), new SpanTermQuery(t1));
  }
  
  /** SpanNearQuery([A, B], 0, true) = "A B" */
  public void testSpanNearVersusPhrase() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    SpanNearQuery q1 = new SpanNearQuery(subquery, 0, true);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t1);
    q2.add(t2);
    assertSameSet(q1, q2);
  }
  
  /** SpanNearQuery([A, B], ∞, false) = +A +B */
  public void testSpanNearVersusBooleanAnd() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    SpanNearQuery q1 = new SpanNearQuery(subquery, Integer.MAX_VALUE, false);
    BooleanQuery q2 = new BooleanQuery();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSameSet(q1, q2);
  }
  
  /** SpanNearQuery([A B], 0, false) ⊆ SpanNearQuery([A B], 1, false) */
  public void testSpanNearVersusSloppySpanNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    SpanNearQuery q1 = new SpanNearQuery(subquery, 0, false);
    SpanNearQuery q2 = new SpanNearQuery(subquery, 1, false);
    assertSubsetOf(q1, q2);
  }
  
  /** SpanNearQuery([A B], 3, true) ⊆ SpanNearQuery([A B], 3, false) */
  public void testSpanNearInOrderVersusOutOfOrder() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    SpanNearQuery q1 = new SpanNearQuery(subquery, 3, true);
    SpanNearQuery q2 = new SpanNearQuery(subquery, 3, false);
    assertSubsetOf(q1, q2);
  }
  
  /** SpanNearQuery([A B], N, false) ⊆ SpanNearQuery([A B], N+1, false) */
  public void testSpanNearIncreasingSloppiness() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    for (int i = 0; i < 10; i++) {
      SpanNearQuery q1 = new SpanNearQuery(subquery, i, false);
      SpanNearQuery q2 = new SpanNearQuery(subquery, i+1, false);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanNearQuery([A B C], N, false) ⊆ SpanNearQuery([A B C], N+1, false) */
  public void testSpanNearIncreasingSloppiness3() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2), new SpanTermQuery(t3) };
    for (int i = 0; i < 10; i++) {
      SpanNearQuery q1 = new SpanNearQuery(subquery, i, false);
      SpanNearQuery q2 = new SpanNearQuery(subquery, i+1, false);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanNearQuery([A B], N, true) ⊆ SpanNearQuery([A B], N+1, true) */
  public void testSpanNearIncreasingOrderedSloppiness() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    for (int i = 0; i < 10; i++) {
      SpanNearQuery q1 = new SpanNearQuery(subquery, i, false);
      SpanNearQuery q2 = new SpanNearQuery(subquery, i+1, false);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanNearQuery([A B C], N, true) ⊆ SpanNearQuery([A B C], N+1, true) */
  public void testSpanNearIncreasingOrderedSloppiness3() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2), new SpanTermQuery(t3) };
    for (int i = 0; i < 10; i++) {
      SpanNearQuery q1 = new SpanNearQuery(subquery, i, true);
      SpanNearQuery q2 = new SpanNearQuery(subquery, i+1, true);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery(A, N) ⊆ TermQuery(A) */
  public void testSpanFirstTerm() throws Exception {
    Term t1 = randomTerm();
    for (int i = 0; i < 10; i++) {
      Query q1 = new SpanFirstQuery(new SpanTermQuery(t1), i);
      Query q2 = new TermQuery(t1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery(A, N) ⊆ SpanFirstQuery(A, N+1) */
  public void testSpanFirstTermIncreasing() throws Exception {
    Term t1 = randomTerm();
    for (int i = 0; i < 10; i++) {
      Query q1 = new SpanFirstQuery(new SpanTermQuery(t1), i);
      Query q2 = new SpanFirstQuery(new SpanTermQuery(t1), i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery(A, ∞) = TermQuery(A) */
  public void testSpanFirstTermEverything() throws Exception {
    Term t1 = randomTerm();
    Query q1 = new SpanFirstQuery(new SpanTermQuery(t1), Integer.MAX_VALUE);
    Query q2 = new TermQuery(t1);
    assertSameSet(q1, q2);
  }
  
  /** SpanFirstQuery([A B], N) ⊆ SpanNearQuery([A B]) */
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6393")
  public void testSpanFirstNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    SpanQuery nearQuery = new SpanNearQuery(subquery, 10, true);
    for (int i = 0; i < 10; i++) {
      Query q1 = new SpanFirstQuery(nearQuery, i);
      Query q2 = nearQuery;
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery([A B], N) ⊆ SpanFirstQuery([A B], N+1) */
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6393")
  public void testSpanFirstNearIncreasing() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    SpanQuery nearQuery = new SpanNearQuery(subquery, 10, true);
    for (int i = 0; i < 10; i++) {
      Query q1 = new SpanFirstQuery(nearQuery, i);
      Query q2 = new SpanFirstQuery(nearQuery, i+1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery([A B], ∞) = SpanNearQuery([A B]) */
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6393")
  public void testSpanFirstNearEverything() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { new SpanTermQuery(t1), new SpanTermQuery(t2) };
    SpanQuery nearQuery = new SpanNearQuery(subquery, 10, true);
    Query q1 = new SpanFirstQuery(nearQuery, Integer.MAX_VALUE);
    Query q2 = nearQuery;
    assertSameSet(q1, q2);
  }
}

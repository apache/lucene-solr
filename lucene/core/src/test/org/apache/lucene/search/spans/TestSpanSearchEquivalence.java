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
package org.apache.lucene.search.spans;


import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearchEquivalenceTestBase;
import org.apache.lucene.search.TermQuery;

import static org.apache.lucene.search.spans.SpanTestUtil.*;

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
    assertSameScores(new TermQuery(t1), spanQuery(new SpanTermQuery(t1)));
  }
  
  /** SpanOrQuery(A) = SpanTermQuery(A) */
  public void testSpanOrVersusTerm() throws Exception {
    Term t1 = randomTerm();
    SpanQuery term = spanQuery(new SpanTermQuery(t1));
    assertSameSet(spanQuery(new SpanOrQuery(term)), term);
  }
  
  /** SpanOrQuery(A, A) = SpanTermQuery(A) */
  public void testSpanOrDoubleVersusTerm() throws Exception {
    Term t1 = randomTerm();
    SpanQuery term = spanQuery(new SpanTermQuery(t1));
    assertSameSet(spanQuery(new SpanOrQuery(term, term)), term);
  }
  
  /** SpanOrQuery(A, B) = (A B) */
  public void testSpanOrVersusBooleanTerm() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery.Builder q1 = new BooleanQuery.Builder();
    q1.add(new TermQuery(t1), Occur.SHOULD);
    q1.add(new TermQuery(t2), Occur.SHOULD);
    SpanQuery q2 = spanQuery(new SpanOrQuery(spanQuery(new SpanTermQuery(t1)), spanQuery(new SpanTermQuery(t2))));
    assertSameSet(q1.build(), q2);
  }
  
  /** SpanOrQuery(SpanNearQuery[A B], SpanNearQuery[C D]) = (SpanNearQuery[A B], SpanNearQuery[C D]) */
  public void testSpanOrVersusBooleanNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    Term t4 = randomTerm();
    SpanQuery near1 = spanQuery(new SpanNearQuery(new SpanQuery[] { 
                                               spanQuery(new SpanTermQuery(t1)), 
                                               spanQuery(new SpanTermQuery(t2)) 
                                             }, 10, random().nextBoolean()));
    SpanQuery near2 = spanQuery(new SpanNearQuery(new SpanQuery[] { 
                                               spanQuery(new SpanTermQuery(t3)), 
                                               spanQuery(new SpanTermQuery(t4)) 
                                             }, 10, random().nextBoolean()));
    BooleanQuery.Builder q1 = new BooleanQuery.Builder();
    q1.add(near1, Occur.SHOULD);
    q1.add(near2, Occur.SHOULD);
    SpanQuery q2 = spanQuery(new SpanOrQuery(near1, near2));
    assertSameSet(q1.build(), q2);
  }
  
  /** SpanNotQuery(A, B) ⊆ SpanTermQuery(A) */
  public void testSpanNotVersusSpanTerm() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    assertSubsetOf(spanQuery(new SpanNotQuery(spanQuery(new SpanTermQuery(t1)), spanQuery(new SpanTermQuery(t2)))), 
                   spanQuery(new SpanTermQuery(t1)));
  }
  
  /** SpanNotQuery(A, [B C]) ⊆ SpanTermQuery(A) */
  public void testSpanNotNearVersusSpanTerm() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    SpanQuery near = spanQuery(new SpanNearQuery(new SpanQuery[] { 
                                              spanQuery(new SpanTermQuery(t2)), 
                                              spanQuery(new SpanTermQuery(t3)) 
                                            }, 10, random().nextBoolean()));
    assertSubsetOf(spanQuery(new SpanNotQuery(spanQuery(new SpanTermQuery(t1)), near)), spanQuery(new SpanTermQuery(t1)));
  }
  
  /** SpanNotQuery([A B], C) ⊆ SpanNearQuery([A B]) */
  public void testSpanNotVersusSpanNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    SpanQuery near = spanQuery(new SpanNearQuery(new SpanQuery[] { 
                                              spanQuery(new SpanTermQuery(t1)), 
                                              spanQuery(new SpanTermQuery(t2)) 
                                            }, 10, random().nextBoolean()));
    assertSubsetOf(spanQuery(new SpanNotQuery(near, spanQuery(new SpanTermQuery(t3)))), near);
  }
  
  /** SpanNotQuery([A B], [C D]) ⊆ SpanNearQuery([A B]) */
  public void testSpanNotNearVersusSpanNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    Term t4 = randomTerm();
    SpanQuery near1 = spanQuery(new SpanNearQuery(new SpanQuery[] { 
                                               spanQuery(new SpanTermQuery(t1)), 
                                               spanQuery(new SpanTermQuery(t2)) 
                                             }, 10, random().nextBoolean()));
    SpanQuery near2 = spanQuery(new SpanNearQuery(new SpanQuery[] { 
                                               spanQuery(new SpanTermQuery(t3)), 
                                               spanQuery(new SpanTermQuery(t4)) 
                                             }, 10, random().nextBoolean()));
    assertSubsetOf(spanQuery(new SpanNotQuery(near1, near2)), near1);
  }
  
  /** SpanFirstQuery(A, 10) ⊆ SpanTermQuery(A) */
  public void testSpanFirstVersusSpanTerm() throws Exception {
    Term t1 = randomTerm();
    assertSubsetOf(spanQuery(new SpanFirstQuery(spanQuery(new SpanTermQuery(t1)), 10)), 
                   spanQuery(new SpanTermQuery(t1)));
  }
  
  /** SpanNearQuery([A, B], 0, true) = "A B" */
  public void testSpanNearVersusPhrase() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, 0, true));
    PhraseQuery q2 = new PhraseQuery(t1.field(), t1.bytes(), t2.bytes());
    if (t1.equals(t2)) {
      assertSameSet(q1, q2);
    } else {
      assertSameScores(q1, q2);
    }
  }
  
  /** SpanNearQuery([A, B], ∞, false) = +A +B */
  public void testSpanNearVersusBooleanAnd() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, Integer.MAX_VALUE, false));
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSameSet(q1, q2.build());
  }
  
  /** SpanNearQuery([A B], 0, false) ⊆ SpanNearQuery([A B], 1, false) */
  public void testSpanNearVersusSloppySpanNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, 0, false));
    SpanQuery q2 = spanQuery(new SpanNearQuery(subquery, 1, false));
    assertSubsetOf(q1, q2);
  }
  
  /** SpanNearQuery([A B], 3, true) ⊆ SpanNearQuery([A B], 3, false) */
  public void testSpanNearInOrderVersusOutOfOrder() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, 3, true));
    SpanQuery q2 = spanQuery(new SpanNearQuery(subquery, 3, false));
    assertSubsetOf(q1, q2);
  }
  
  /** SpanNearQuery([A B], N, false) ⊆ SpanNearQuery([A B], N+1, false) */
  public void testSpanNearIncreasingSloppiness() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    for (int i = 0; i < 10; i++) {
      SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, i, false));
      SpanQuery q2 = spanQuery(new SpanNearQuery(subquery, i+1, false));
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanNearQuery([A B C], N, false) ⊆ SpanNearQuery([A B C], N+1, false) */
  public void testSpanNearIncreasingSloppiness3() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)), 
                             spanQuery(new SpanTermQuery(t3)) 
                           };
    for (int i = 0; i < 10; i++) {
      SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, i, false));
      SpanQuery q2 = spanQuery(new SpanNearQuery(subquery, i+1, false));
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanNearQuery([A B], N, true) ⊆ SpanNearQuery([A B], N+1, true) */
  public void testSpanNearIncreasingOrderedSloppiness() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    for (int i = 0; i < 10; i++) {
      SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, i, false));
      SpanQuery q2 = spanQuery(new SpanNearQuery(subquery, i+1, false));
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanNearQuery([A B C], N, true) ⊆ SpanNearQuery([A B C], N+1, true) */
  public void testSpanNearIncreasingOrderedSloppiness3() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    Term t3 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)), 
                             spanQuery(new SpanTermQuery(t3)) 
                           };
    for (int i = 0; i < 10; i++) {
      SpanQuery q1 = spanQuery(new SpanNearQuery(subquery, i, true));
      SpanQuery q2 = spanQuery(new SpanNearQuery(subquery, i+1, true));
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanPositionRangeQuery(A, M, N) ⊆ TermQuery(A) */
  public void testSpanRangeTerm() throws Exception {
    Term t1 = randomTerm();
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        Query q1 = spanQuery(new SpanPositionRangeQuery(spanQuery(new SpanTermQuery(t1)), i, i+j));
        Query q2 = new TermQuery(t1);
        assertSubsetOf(q1, q2);
      }
    }
  }
  
  /** SpanPositionRangeQuery(A, M, N) ⊆ SpanFirstQuery(A, M, N+1) */
  public void testSpanRangeTermIncreasingEnd() throws Exception {
    Term t1 = randomTerm();
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        Query q1 = spanQuery(new SpanPositionRangeQuery(spanQuery(new SpanTermQuery(t1)), i, i+j));
        Query q2 = spanQuery(new SpanPositionRangeQuery(spanQuery(new SpanTermQuery(t1)), i, i+j+1));
        assertSubsetOf(q1, q2);
      }
    }
  }
  
  /** SpanPositionRangeQuery(A, 0, ∞) = TermQuery(A) */
  public void testSpanRangeTermEverything() throws Exception {
    Term t1 = randomTerm();
    Query q1 = spanQuery(new SpanPositionRangeQuery(spanQuery(new SpanTermQuery(t1)), 0, Integer.MAX_VALUE));
    Query q2 = new TermQuery(t1);
    assertSameSet(q1, q2);
  }
  
  /** SpanPositionRangeQuery([A B], M, N) ⊆ SpanNearQuery([A B]) */
  public void testSpanRangeNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        Query q1 = spanQuery(new SpanPositionRangeQuery(nearQuery, i, i+j));
        Query q2 = nearQuery;
        assertSubsetOf(q1, q2);
      }
    }
  }
  
  /** SpanPositionRangeQuery([A B], M, N) ⊆ SpanFirstQuery([A B], M, N+1) */
  public void testSpanRangeNearIncreasingEnd() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        Query q1 = spanQuery(new SpanPositionRangeQuery(nearQuery, i, i+j));
        Query q2 = spanQuery(new SpanPositionRangeQuery(nearQuery, i, i+j+1));
        assertSubsetOf(q1, q2);
      }
    }
  }
  
  /** SpanPositionRangeQuery([A B], ∞) = SpanNearQuery([A B]) */
  public void testSpanRangeNearEverything() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    Query q1 = spanQuery(new SpanPositionRangeQuery(nearQuery, 0, Integer.MAX_VALUE));
    Query q2 = nearQuery;
    assertSameSet(q1, q2);
  }
  
  /** SpanFirstQuery(A, N) ⊆ TermQuery(A) */
  public void testSpanFirstTerm() throws Exception {
    Term t1 = randomTerm();
    for (int i = 0; i < 10; i++) {
      Query q1 = spanQuery(new SpanFirstQuery(spanQuery(new SpanTermQuery(t1)), i));
      Query q2 = new TermQuery(t1);
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery(A, N) ⊆ SpanFirstQuery(A, N+1) */
  public void testSpanFirstTermIncreasing() throws Exception {
    Term t1 = randomTerm();
    for (int i = 0; i < 10; i++) {
      Query q1 = spanQuery(new SpanFirstQuery(spanQuery(new SpanTermQuery(t1)), i));
      Query q2 = spanQuery(new SpanFirstQuery(spanQuery(new SpanTermQuery(t1)), i+1));
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery(A, ∞) = TermQuery(A) */
  public void testSpanFirstTermEverything() throws Exception {
    Term t1 = randomTerm();
    Query q1 = spanQuery(new SpanFirstQuery(spanQuery(new SpanTermQuery(t1)), Integer.MAX_VALUE));
    Query q2 = new TermQuery(t1);
    assertSameSet(q1, q2);
  }
  
  /** SpanFirstQuery([A B], N) ⊆ SpanNearQuery([A B]) */
  public void testSpanFirstNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    for (int i = 0; i < 10; i++) {
      Query q1 = spanQuery(new SpanFirstQuery(nearQuery, i));
      Query q2 = nearQuery;
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery([A B], N) ⊆ SpanFirstQuery([A B], N+1) */
  public void testSpanFirstNearIncreasing() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    for (int i = 0; i < 10; i++) {
      Query q1 = spanQuery(new SpanFirstQuery(nearQuery, i));
      Query q2 = spanQuery(new SpanFirstQuery(nearQuery, i+1));
      assertSubsetOf(q1, q2);
    }
  }
  
  /** SpanFirstQuery([A B], ∞) = SpanNearQuery([A B]) */
  public void testSpanFirstNearEverything() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
                             spanQuery(new SpanTermQuery(t1)), 
                             spanQuery(new SpanTermQuery(t2)) 
                           };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    Query q1 = spanQuery(new SpanFirstQuery(nearQuery, Integer.MAX_VALUE));
    Query q2 = nearQuery;
    assertSameSet(q1, q2);
  }
  
  /** SpanWithinQuery(A, B) ⊆ SpanNearQuery(A) */
  public void testSpanWithinVsNear() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
        spanQuery(new SpanTermQuery(t1)), 
        spanQuery(new SpanTermQuery(t2)) 
      };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    
    Term t3 = randomTerm();
    SpanQuery termQuery = spanQuery(new SpanTermQuery(t3));
    Query q1 = spanQuery(new SpanWithinQuery(nearQuery, termQuery));
    assertSubsetOf(q1, termQuery);
  }
  
  /** SpanWithinQuery(A, B) = SpanContainingQuery(A, B) */
  public void testSpanWithinVsContaining() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    SpanQuery subquery[] = new SpanQuery[] { 
        spanQuery(new SpanTermQuery(t1)), 
        spanQuery(new SpanTermQuery(t2)) 
      };
    SpanQuery nearQuery = spanQuery(new SpanNearQuery(subquery, 10, true));
    
    Term t3 = randomTerm();
    SpanQuery termQuery = spanQuery(new SpanTermQuery(t3));
    Query q1 = spanQuery(new SpanWithinQuery(nearQuery, termQuery));
    Query q2 = spanQuery(new SpanContainingQuery(nearQuery, termQuery));
    assertSameSet(q1, q2);
  }

  public void testSpanBoostQuerySimplification() throws Exception {
    float b1 = random().nextFloat() * 10;
    float b2 = random().nextFloat() * 10;
    Term term = randomTerm();

    Query q1 = new SpanBoostQuery(new SpanBoostQuery(new SpanTermQuery(term), b2), b1);
    // Use AssertingQuery to prevent BoostQuery from merging inner and outer boosts
    Query q2 = new SpanBoostQuery(new AssertingSpanQuery(new SpanBoostQuery(new SpanTermQuery(term), b2)), b1);

    assertSameScores(q1, q2);
  }
}

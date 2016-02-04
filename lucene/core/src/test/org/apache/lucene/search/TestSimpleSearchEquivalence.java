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
import org.apache.lucene.search.BooleanClause.Occur;


/**
 * Basic equivalence tests for core queries
 */
public class TestSimpleSearchEquivalence extends SearchEquivalenceTestBase {
  
  // TODO: we could go a little crazy for a lot of these,
  // but these are just simple minimal cases in case something 
  // goes horribly wrong. Put more intense tests elsewhere.
  
  /** A ⊆ (A B) */
  public void testTermVersusBooleanOr() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new TermQuery(t1), Occur.SHOULD);
    q2.add(new TermQuery(t2), Occur.SHOULD);
    assertSubsetOf(q1, q2.build());
  }
  
  /** A ⊆ (+A B) */
  public void testTermVersusBooleanReqOpt() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.SHOULD);
    assertSubsetOf(q1, q2.build());
  }
  
  /** (A -B) ⊆ A */
  public void testBooleanReqExclVersusTerm() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery.Builder q1 = new BooleanQuery.Builder();
    q1.add(new TermQuery(t1), Occur.MUST);
    q1.add(new TermQuery(t2), Occur.MUST_NOT);
    TermQuery q2 = new TermQuery(t1);
    assertSubsetOf(q1.build(), q2);
  }
  
  /** (+A +B) ⊆ (A B) */
  public void testBooleanAndVersusBooleanOr() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery.Builder q1 = new BooleanQuery.Builder();
    q1.add(new TermQuery(t1), Occur.SHOULD);
    q1.add(new TermQuery(t2), Occur.SHOULD);
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new TermQuery(t1), Occur.SHOULD);
    q2.add(new TermQuery(t2), Occur.SHOULD);
    assertSubsetOf(q1.build(), q2.build());
  }
  
  /** (A B) = (A | B) */
  public void testDisjunctionSumVersusDisjunctionMax() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery.Builder q1 = new BooleanQuery.Builder();
    q1.add(new TermQuery(t1), Occur.SHOULD);
    q1.add(new TermQuery(t2), Occur.SHOULD);
    DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(
        Arrays.<Query>asList(
            new TermQuery(t1),
            new TermQuery(t2)),
        0.5f);
    assertSameSet(q1.build(), q2);
  }
  
  /** "A B" ⊆ (+A +B) */
  public void testExactPhraseVersusBooleanAnd() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery(t1.field(), t1.bytes(), t2.bytes());
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSubsetOf(q1, q2.build());
  }
  
  /** same as above, with posincs */
  public void testExactPhraseVersusBooleanAndWithHoles() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(t1, 0);
    builder.add(t2, 2);
    PhraseQuery q1 = builder.build();
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSubsetOf(q1, q2.build());
  }
  
  /** "A B" ⊆ "A B"~1 */
  public void testPhraseVersusSloppyPhrase() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery(t1.field(), t1.bytes(), t2.bytes());
    PhraseQuery q2 = new PhraseQuery(1, t1.field(), t1.bytes(), t2.bytes());
    assertSubsetOf(q1, q2);
  }
  
  /** same as above, with posincs */
  public void testPhraseVersusSloppyPhraseWithHoles() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(t1, 0);
    builder.add(t2, 2);
    PhraseQuery q1 = builder.build();
    builder.setSlop(2);
    PhraseQuery q2 = builder.build();
    assertSubsetOf(q1, q2);
  }
  
  /** "A B" ⊆ "A (B C)" */
  public void testExactPhraseVersusMultiPhrase() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery(t1.field(), t1.bytes(), t2.bytes());
    Term t3 = randomTerm();
    MultiPhraseQuery q2 = new MultiPhraseQuery();
    q2.add(t1);
    q2.add(new Term[] { t2, t3 });
    assertSubsetOf(q1, q2);
  }
  
  /** same as above, with posincs */
  public void testExactPhraseVersusMultiPhraseWithHoles() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(t1, 0);
    builder.add(t2, 2);
    PhraseQuery q1 = builder.build();
    Term t3 = randomTerm();
    MultiPhraseQuery q2 = new MultiPhraseQuery();
    q2.add(t1);
    q2.add(new Term[] { t2, t3 }, 2);
    assertSubsetOf(q1, q2);
  }
  
  /** "A B"~∞ = +A +B if A != B */
  public void testSloppyPhraseVersusBooleanAnd() throws Exception {
    Term t1 = randomTerm();
    Term t2 = null;
    // semantics differ from SpanNear: SloppyPhrase handles repeats,
    // so we must ensure t1 != t2
    do {
      t2 = randomTerm();
    } while (t1.equals(t2));
    PhraseQuery q1 = new PhraseQuery(Integer.MAX_VALUE, t1.field(), t1.bytes(), t2.bytes());
    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSameSet(q1, q2.build());
  }

  /** Phrase positions are relative. */
  public void testPhraseRelativePositions() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery(t1.field(), t1.bytes(), t2.bytes());
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(t1, 10000);
    builder.add(t2, 10001);
    PhraseQuery q2 = builder.build();
    assertSameScores(q1, q2);
  }

  /** Sloppy-phrase positions are relative. */
  public void testSloppyPhraseRelativePositions() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery(2, t1.field(), t1.bytes(), t2.bytes());
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(t1, 10000);
    builder.add(t2, 10001);
    builder.setSlop(2);
    PhraseQuery q2 = builder.build();
    assertSameScores(q1, q2);
  }

  public void testBoostQuerySimplification() throws Exception {
    float b1 = random().nextFloat() * 10;
    float b2 = random().nextFloat() * 10;
    Term term = randomTerm();

    Query q1 = new BoostQuery(new BoostQuery(new TermQuery(term), b2), b1);
    // Use AssertingQuery to prevent BoostQuery from merging inner and outer boosts
    Query q2 = new BoostQuery(new AssertingQuery(random(), new BoostQuery(new TermQuery(term), b2)), b1);

    assertSameScores(q1, q2);
  }

  public void testBooleanBoostPropagation() throws Exception {
    float boost1 = random().nextFloat();
    Query tq = new BoostQuery(new TermQuery(randomTerm()), boost1);

    float boost2 = random().nextFloat();
    // Applying boost2 over the term or boolean query should have the same effect
    Query q1 = new BoostQuery(tq, boost2);
    Query q2 = new BooleanQuery.Builder()
      .add(tq, Occur.MUST)
      .add(tq, Occur.FILTER)
      .build();
    q2 = new BoostQuery(q2, boost2);

    assertSameScores(q1, q2);
  }
}

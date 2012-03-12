package org.apache.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;

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
    BooleanQuery q2 = new BooleanQuery();
    q2.add(new TermQuery(t1), Occur.SHOULD);
    q2.add(new TermQuery(t2), Occur.SHOULD);
    assertSubsetOf(q1, q2);
  }
  
  /** A ⊆ (+A B) */
  public void testTermVersusBooleanReqOpt() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    BooleanQuery q2 = new BooleanQuery();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.SHOULD);
    assertSubsetOf(q1, q2);
  }
  
  /** (A -B) ⊆ A */
  public void testBooleanReqExclVersusTerm() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery q1 = new BooleanQuery();
    q1.add(new TermQuery(t1), Occur.MUST);
    q1.add(new TermQuery(t2), Occur.MUST_NOT);
    TermQuery q2 = new TermQuery(t1);
    assertSubsetOf(q1, q2);
  }
  
  /** (+A +B) ⊆ (A B) */
  public void testBooleanAndVersusBooleanOr() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery q1 = new BooleanQuery();
    q1.add(new TermQuery(t1), Occur.SHOULD);
    q1.add(new TermQuery(t2), Occur.SHOULD);
    BooleanQuery q2 = new BooleanQuery();
    q2.add(new TermQuery(t1), Occur.SHOULD);
    q2.add(new TermQuery(t2), Occur.SHOULD);
    assertSubsetOf(q1, q2);
  }
  
  /** (A B) = (A | B) */
  public void testDisjunctionSumVersusDisjunctionMax() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    BooleanQuery q1 = new BooleanQuery();
    q1.add(new TermQuery(t1), Occur.SHOULD);
    q1.add(new TermQuery(t2), Occur.SHOULD);
    DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(0.5f);
    q2.add(new TermQuery(t1));
    q2.add(new TermQuery(t2));
    assertSameSet(q1, q2);
  }
  
  /** "A B" ⊆ (+A +B) */
  public void testExactPhraseVersusBooleanAnd() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2);
    BooleanQuery q2 = new BooleanQuery();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSubsetOf(q1, q2);
  }
  
  /** same as above, with posincs */
  public void testExactPhraseVersusBooleanAndWithHoles() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2, 2);
    BooleanQuery q2 = new BooleanQuery();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSubsetOf(q1, q2);
  }
  
  /** "A B" ⊆ "A B"~1 */
  public void testPhraseVersusSloppyPhrase() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t1);
    q2.add(t2);
    q2.setSlop(1);
    assertSubsetOf(q1, q2);
  }
  
  /** same as above, with posincs */
  public void testPhraseVersusSloppyPhraseWithHoles() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2, 2);
    PhraseQuery q2 = new PhraseQuery();
    q2.add(t1);
    q2.add(t2, 2);
    q2.setSlop(1);
    assertSubsetOf(q1, q2);
  }
  
  /** "A B" ⊆ "A (B C)" */
  public void testExactPhraseVersusMultiPhrase() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2);
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
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2, 2);
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
    PhraseQuery q1 = new PhraseQuery();
    q1.add(t1);
    q1.add(t2);
    q1.setSlop(Integer.MAX_VALUE);
    BooleanQuery q2 = new BooleanQuery();
    q2.add(new TermQuery(t1), Occur.MUST);
    q2.add(new TermQuery(t2), Occur.MUST);
    assertSameSet(q1, q2);
  }
}

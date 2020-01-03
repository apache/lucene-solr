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


import java.io.IOException;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/** Subclass of TermsEnum for enumerating all terms that are similar
 * to the specified filter term.
 *
 * <p>Term enumerations are always ordered by
 * {@link BytesRef#compareTo}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 */
public final class FuzzyTermsEnum extends TermsEnum {

  // NOTE: we can't subclass FilteredTermsEnum here because we need to sometimes change actualEnum:
  private TermsEnum actualEnum;

  private final AttributeSource atts;
  
  // We use this to communicate the score (boost) of the current matched term we are on back to
  // MultiTermQuery.TopTermsBlendedFreqScoringRewrite that is collecting the best (default 50) matched terms:
  private final BoostAttribute boostAtt;

  // MultiTermQuery.TopTermsBlendedFreqScoringRewrite tells us the worst boost still in its queue using this att,
  // which we use to know when we can reduce the automaton from ed=2 to ed=1, or ed=0 if only single top term is collected:
  private final MaxNonCompetitiveBoostAttribute maxBoostAtt;

  private final CompiledAutomaton[] automata;
  
  private float bottom;
  private BytesRef bottomTerm;

  private BytesRef queuedBottom;

  private final int termLength;

  // Maximum number of edits we will accept.  This is either 2 or 1 (or, degenerately, 0) passed by the user originally,
  // but as we collect terms, we can lower this (e.g. from 2 to 1) if we detect that the term queue is full, and all
  // collected terms are ed=1:
  private int maxEdits;

  private final Terms terms;
  private final Term term;

  /**
   * Constructor for enumeration of all terms from specified <code>reader</code> which share a prefix of
   * length <code>prefixLength</code> with <code>term</code> and which have at most {@code maxEdits} edits.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first
   * valid term if such a term exists.
   *
   * @param terms Delivers terms.
   * @param term Pattern term.
   * @param maxEdits Maximum edit distance.
   * @param prefixLength the length of the required common prefix
   * @param transpositions whether transpositions should count as a single edit
   * @throws IOException if there is a low-level IO error
   */
  public FuzzyTermsEnum(Terms terms, Term term, int maxEdits, int prefixLength, boolean transpositions) throws IOException {
    this(terms, term, stringToUTF32(term.text()), maxEdits, prefixLength, transpositions);
  }

  private FuzzyTermsEnum(Terms terms, Term term, int[] codePoints, int maxEdits, int prefixLength, boolean transpositions) throws IOException {
    this(terms, new AttributeSource(), term, codePoints.length, maxEdits,
        buildAutomata(term.text(), codePoints, prefixLength, transpositions, maxEdits));
  }

  /**
   * Constructor for enumeration of all terms from specified <code>reader</code> which share a prefix of
   * length <code>prefixLength</code> with <code>term</code> and which have at most {@code maxEdits} edits.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first 
   * valid term if such a term exists. 
   * 
   * @param terms Delivers terms.
   * @param atts {@link AttributeSource} created by the rewrite method of {@link MultiTermQuery}
   *              that contains information about competitive boosts during rewrite
   * @param term Pattern term.
   * @param maxEdits Maximum edit distance.
   * @param automata An array of levenshtein automata to match against terms,
   *                 see {@link #buildAutomata(String, int[], int, boolean, int)}
   * @throws IOException if there is a low-level IO error
   */
  public FuzzyTermsEnum(Terms terms, AttributeSource atts, Term term, int termLength,
      final int maxEdits, CompiledAutomaton[] automata) throws IOException {

    this.maxEdits = maxEdits;
    this.terms = terms;
    this.term = term;
    this.atts = atts;
    this.termLength = termLength;

    this.maxBoostAtt = atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);
    this.boostAtt = atts.addAttribute(BoostAttribute.class);

    this.automata = automata;

    bottom = maxBoostAtt.getMaxNonCompetitiveBoost();
    bottomTerm = maxBoostAtt.getCompetitiveTerm();
    bottomChanged(null);
  }

  /**
   * Sets the maximum non-competitive boost, which may allow switching to a
   * lower max-edit automaton at run time
   */
  public void setMaxNonCompetitiveBoost(float boost) {
    this.maxBoostAtt.setMaxNonCompetitiveBoost(boost);
  }

  /**
   * Gets the boost of the current term
   */
  public float getBoost() {
    return boostAtt.getBoost();
  }

  static CompiledAutomaton[] buildAutomata(String text, int[] termText, int prefixLength, boolean transpositions, int maxEdits) {
    CompiledAutomaton[] compiled = new CompiledAutomaton[maxEdits + 1];
    Automaton[] automata = buildAutomata(termText, prefixLength, transpositions, maxEdits);
    for (int i = 0; i <= maxEdits; i++) {
      try {
        compiled[i] = new CompiledAutomaton(automata[i], true, false);
      }
      catch (TooComplexToDeterminizeException e) {
        throw new FuzzyTermsException(text, e);
      }
    }
    return compiled;
  }

  static int[] stringToUTF32(String text) {
    int[] termText = new int[text.codePointCount(0, text.length())];
    for (int cp, i = 0, j = 0; i < text.length(); i += Character.charCount(cp)) {
      termText[j++] = cp = text.codePointAt(i);
    }
    return termText;
  }

  private static Automaton[] buildAutomata(int[] termText, int prefixLength, boolean transpositions, int maxEdits) {
    if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      throw new IllegalArgumentException("max edits must be 0.." + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + ", inclusive; got: " + maxEdits);
    }
    if (prefixLength < 0) {
      throw new IllegalArgumentException("prefixLength cannot be less than 0");
    }
    Automaton[] automata = new Automaton[maxEdits + 1];
    int termLength = termText.length;
    prefixLength = Math.min(prefixLength, termLength);
    String suffix = UnicodeUtil.newString(termText, prefixLength, termText.length - prefixLength);
    LevenshteinAutomata builder = new LevenshteinAutomata(suffix, transpositions);
    String prefix = UnicodeUtil.newString(termText, 0, prefixLength);
    for (int i = 0; i <= maxEdits; i++) {
      automata[i] = builder.toAutomaton(i, prefix);
    }
    return automata;
  }
  
  /**
   * return an automata-based enum for matching up to editDistance from
   * lastTerm, if possible
   */
  private TermsEnum getAutomatonEnum(int editDistance, BytesRef lastTerm) throws IOException {
    assert editDistance < automata.length;
    final CompiledAutomaton compiled = automata[editDistance];
    BytesRef initialSeekTerm;
    if (lastTerm == null) {
      // This is the first enum we are pulling:
      initialSeekTerm = null;
    } else {
      // We are pulling this enum (e.g., ed=1) after iterating for a while already (e.g., ed=2):
      initialSeekTerm = compiled.floor(lastTerm, new BytesRefBuilder());
    }
    return terms.intersect(compiled, initialSeekTerm);
  }

  /**
   * fired when the max non-competitive boost has changed. this is the hook to
   * swap in a smarter actualEnum.
   */
  private void bottomChanged(BytesRef lastTerm) throws IOException {
    int oldMaxEdits = maxEdits;
    
    // true if the last term encountered is lexicographically equal or after the bottom term in the PQ
    boolean termAfter = bottomTerm == null || (lastTerm != null && lastTerm.compareTo(bottomTerm) >= 0);

    // as long as the max non-competitive boost is >= the max boost
    // for some edit distance, keep dropping the max edit distance.
    while (maxEdits > 0) {
      float maxBoost = 1.0f - ((float) maxEdits / (float) termLength);
      if (bottom < maxBoost || (bottom == maxBoost && termAfter == false)) {
        break;
      }
      maxEdits--;
    }

    if (oldMaxEdits != maxEdits || lastTerm == null) {
      // This is a very powerful optimization: the maximum edit distance has changed.  This happens because we collect only the top scoring
      // N (= 50, by default) terms, and if e.g. maxEdits=2, and the queue is now full of matching terms, and we notice that the worst entry
      // in that queue is ed=1, then we can switch the automata here to ed=1 which is a big speedup.
      actualEnum = getAutomatonEnum(maxEdits, lastTerm);
    }
  }
  
  @Override
  public BytesRef next() throws IOException {

    if (queuedBottom != null) {
      bottomChanged(queuedBottom);
      queuedBottom = null;
    }
    

    BytesRef term;

    term = actualEnum.next();
    if (term == null) {
      // end
      return null;
    }

    int ed = maxEdits;
      
    // we know the outer DFA always matches.
    // now compute exact edit distance
    while (ed > 0) {
      if (matches(term, ed - 1)) {
        ed--;
      } else {
        break;
      }
    }
      
    if (ed == 0) { // exact match
      boostAtt.setBoost(1.0F);
    } else {
      final int codePointCount = UnicodeUtil.codePointCount(term);
      int minTermLength = Math.min(codePointCount, termLength);

      float similarity = 1.0f - (float) ed / (float) minTermLength;
      boostAtt.setBoost(similarity);
    }
      
    final float bottom = maxBoostAtt.getMaxNonCompetitiveBoost();
    final BytesRef bottomTerm = maxBoostAtt.getCompetitiveTerm();
    if (term != null && (bottom != this.bottom || bottomTerm != this.bottomTerm)) {
      this.bottom = bottom;
      this.bottomTerm = bottomTerm;
      // clone the term before potentially doing something with it
      // this is a rare but wonderful occurrence anyway

      // We must delay bottomChanged until the next next() call otherwise we mess up docFreq(), etc., for the current term:
      queuedBottom = BytesRef.deepCopyOf(term);
    }
    
    return term;
  }

  /** returns true if term is within k edits of the query term */
  private boolean matches(BytesRef termIn, int k) {
    return k == 0 ? termIn.equals(term.bytes()) : automata[k].runAutomaton.run(termIn.bytes, termIn.offset, termIn.length);
  }
  
  // proxy all other enum calls to the actual enum
  @Override
  public int docFreq() throws IOException {
    return actualEnum.docFreq();
  }

  @Override
  public long totalTermFreq() throws IOException {
    return actualEnum.totalTermFreq();
  }
  
  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    return actualEnum.postings(reuse, flags);
  }
  
  @Override
  public ImpactsEnum impacts(int flags) throws IOException {
    return actualEnum.impacts(flags);
  }
  
  @Override
  public void seekExact(BytesRef term, TermState state) throws IOException {
    actualEnum.seekExact(term, state);
  }
  
  @Override
  public TermState termState() throws IOException {
    return actualEnum.termState();
  }
  
  @Override
  public long ord() throws IOException {
    return actualEnum.ord();
  }

  @Override
  public AttributeSource attributes() {
    return atts;
  }

  @Override
  public boolean seekExact(BytesRef text) throws IOException {
    return actualEnum.seekExact(text);
  }

  @Override
  public SeekStatus seekCeil(BytesRef text) throws IOException {
    return actualEnum.seekCeil(text);
  }
  
  @Override
  public void seekExact(long ord) throws IOException {
    actualEnum.seekExact(ord);
  }
  
  @Override
  public BytesRef term() throws IOException {
    return actualEnum.term();
  }

  /**
   * Thrown to indicate that there was an issue creating a fuzzy query for a given term.
   * Typically occurs with terms longer than 220 UTF-8 characters,
   * but also possible with shorter terms consisting of UTF-32 code points.
   */
  public static class FuzzyTermsException extends RuntimeException {
    FuzzyTermsException(String term, Throwable cause) {
      super("Term too complex: " + term, cause);
    }
  }

}

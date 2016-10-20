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


import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

import java.io.IOException;
import java.util.Arrays;

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
  
  // We use this to communicate the score (boost) of the current matched term we are on back to
  // MultiTermQuery.TopTermsBlendedFreqScoringRewrite that is collecting the best (default 50) matched terms:
  private final BoostAttribute boostAtt;

  // MultiTermQuery.TopTermsBlendedFreqScoringRewrite tells us the worst boost still in its queue using this att,
  // which we use to know when we can reduce the automaton from ed=2 to ed=1, or ed=0 if only single top term is collected:
  private final MaxNonCompetitiveBoostAttribute maxBoostAtt;

  // We use this to share the pre-built (once for the query) Levenshtein automata across segments:
  private final LevenshteinAutomataAttribute dfaAtt;
  
  private float bottom;
  private BytesRef bottomTerm;
  private final CompiledAutomaton automata[];

  private BytesRef queuedBottom;

  final int termLength;

  // Maximum number of edits we will accept.  This is either 2 or 1 (or, degenerately, 0) passed by the user originally,
  // but as we collect terms, we can lower this (e.g. from 2 to 1) if we detect that the term queue is full, and all
  // collected terms are ed=1:
  private int maxEdits;

  final Terms terms;
  final Term term;
  final int termText[];
  final int realPrefixLength;

  // True (the default, in FuzzyQuery) if a transposition should count as a single edit:
  final boolean transpositions;
  
  /**
   * Constructor for enumeration of all terms from specified <code>reader</code> which share a prefix of
   * length <code>prefixLength</code> with <code>term</code> and which have at most {@code maxEdits} edits.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first 
   * valid term if such a term exists. 
   * 
   * @param terms Delivers terms.
   * @param atts {@link AttributeSource} created by the rewrite method of {@link MultiTermQuery}
   * thats contains information about competitive boosts during rewrite. It is also used
   * to cache DFAs between segment transitions.
   * @param term Pattern term.
   * @param maxEdits Maximum edit distance.
   * @param prefixLength Length of required common prefix. Default value is 0.
   * @throws IOException if there is a low-level IO error
   */
  public FuzzyTermsEnum(Terms terms, AttributeSource atts, Term term, 
      final int maxEdits, final int prefixLength, boolean transpositions) throws IOException {
    if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      throw new IllegalArgumentException("max edits must be 0.." + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + ", inclusive; got: " + maxEdits);
    }
    if (prefixLength < 0) {
      throw new IllegalArgumentException("prefixLength cannot be less than 0");
    }
    this.maxEdits = maxEdits;
    this.terms = terms;
    this.term = term;
    
    // convert the string into a utf32 int[] representation for fast comparisons
    final String utf16 = term.text();
    this.termText = new int[utf16.codePointCount(0, utf16.length())];
    for (int cp, i = 0, j = 0; i < utf16.length(); i += Character.charCount(cp)) {
      termText[j++] = cp = utf16.codePointAt(i);
    }
    this.termLength = termText.length;

    this.dfaAtt = atts.addAttribute(LevenshteinAutomataAttribute.class);
    this.maxBoostAtt = atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);

    // NOTE: boostAtt must pulled from attributes() not from atts!  This is because TopTermsRewrite looks for boostAtt from this TermsEnum's
    // private attributes() and not the global atts passed to us from MultiTermQuery:
    this.boostAtt = attributes().addAttribute(BoostAttribute.class);

    //The prefix could be longer than the word.
    //It's kind of silly though.  It means we must match the entire word.
    this.realPrefixLength = prefixLength > termLength ? termLength : prefixLength;
    this.transpositions = transpositions;

    CompiledAutomaton[] prevAutomata = dfaAtt.automata();
    if (prevAutomata == null) {
      prevAutomata = new CompiledAutomaton[maxEdits+1];

      LevenshteinAutomata builder = 
        new LevenshteinAutomata(UnicodeUtil.newString(termText, realPrefixLength, termText.length - realPrefixLength), transpositions);

      String prefix = UnicodeUtil.newString(termText, 0, realPrefixLength);
      for (int i = 0; i <= maxEdits; i++) {
        Automaton a = builder.toAutomaton(i, prefix);
        prevAutomata[i] = new CompiledAutomaton(a, true, false);
      }

      // first segment computes the automata, and we share with subsequent segments via this Attribute:
      dfaAtt.setAutomata(prevAutomata);
    }

    this.automata = prevAutomata;
    bottom = maxBoostAtt.getMaxNonCompetitiveBoost();
    bottomTerm = maxBoostAtt.getCompetitiveTerm();
    bottomChanged(null);
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
   * reuses compiled automata across different segments,
   * because they are independent of the index
   * @lucene.internal */
  public static interface LevenshteinAutomataAttribute extends Attribute {
    public CompiledAutomaton[] automata();
    public void setAutomata(CompiledAutomaton[] automata);
  }
    
  /** 
   * Stores compiled automata as a list (indexed by edit distance)
   * @lucene.internal */
  public static final class LevenshteinAutomataAttributeImpl extends AttributeImpl implements LevenshteinAutomataAttribute {
    private CompiledAutomaton[] automata;
      
    @Override
    public CompiledAutomaton[] automata() {
      return automata;
    }

    @Override
    public void setAutomata(CompiledAutomaton[] automata) {
      this.automata = automata;
    }

    @Override
    public void clear() {
      automata = null;
    }

    @Override
    public int hashCode() {
      if (automata == null) {
        return 0;
      } else {
        return automata.hashCode();
      }
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (!(other instanceof LevenshteinAutomataAttributeImpl))
        return false;
      return Arrays.equals(automata, ((LevenshteinAutomataAttributeImpl) other).automata);
    }

    @Override
    public void copyTo(AttributeImpl _target) {
      LevenshteinAutomataAttribute target = (LevenshteinAutomataAttribute) _target;
      if (automata == null) {
        target.setAutomata(null);
      } else {
        target.setAutomata(automata);
      }
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(LevenshteinAutomataAttribute.class, "automata", automata);
    }
  }
}

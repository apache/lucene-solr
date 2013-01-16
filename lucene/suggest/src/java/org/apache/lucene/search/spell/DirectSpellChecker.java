package org.apache.lucene.search.spell;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.FuzzyTermsEnum;
import org.apache.lucene.search.MaxNonCompetitiveBoostAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Locale;
import java.util.PriorityQueue;

/**
 * Simple automaton-based spellchecker.
 * <p>
 * Candidates are presented directly from the term dictionary, based on
 * Levenshtein distance. This is an alternative to {@link SpellChecker}
 * if you are using an edit-distance-like metric such as Levenshtein
 * or {@link JaroWinklerDistance}.
 * <p>
 * A practical benefit of this spellchecker is that it requires no additional
 * datastructures (neither in RAM nor on disk) to do its work.
 * 
 * @see LevenshteinAutomata
 * @see FuzzyTermsEnum
 * 
 * @lucene.experimental
 */
public class DirectSpellChecker {
  /** The default StringDistance, Damerau-Levenshtein distance implemented internally
   *  via {@link LevenshteinAutomata}.
   *  <p>
   *  Note: this is the fastest distance metric, because Damerau-Levenshtein is used
   *  to draw candidates from the term dictionary: this just re-uses the scoring.
   */
  public static final StringDistance INTERNAL_LEVENSHTEIN = new LuceneLevenshteinDistance();

  /** maximum edit distance for candidate terms */
  private int maxEdits = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
  /** minimum prefix for candidate terms */
  private int minPrefix = 1;
  /** maximum number of top-N inspections per suggestion */
  private int maxInspections = 5;
  /** minimum accuracy for a term to match */
  private float accuracy = SpellChecker.DEFAULT_ACCURACY;
  /** value in [0..1] (or absolute number >=1) representing the minimum
    * number of documents (of the total) where a term should appear. */
  private float thresholdFrequency = 0f;
  /** minimum length of a query word to return suggestions */
  private int minQueryLength = 4;
  /** value in [0..1] (or absolute number >=1) representing the maximum
   *  number of documents (of the total) a query term can appear in to
   *  be corrected. */
  private float maxQueryFrequency = 0.01f;
  /** true if the spellchecker should lowercase terms */
  private boolean lowerCaseTerms = true;
  /** the comparator to use */
  private Comparator<SuggestWord> comparator = SuggestWordQueue.DEFAULT_COMPARATOR;
  /** the string distance to use */
  private StringDistance distance = INTERNAL_LEVENSHTEIN;

  /** Creates a DirectSpellChecker with default configuration values */
  public DirectSpellChecker() {}

  /** Get the maximum number of Levenshtein edit-distances to draw
   *  candidate terms from. */  
  public int getMaxEdits() {
    return maxEdits;
  }

  /** Sets the maximum number of Levenshtein edit-distances to draw
   *  candidate terms from. This value can be 1 or 2. The default is 2.
   *  <p>
   *  Note: a large number of spelling errors occur with an edit distance
   *  of 1, by setting this value to 1 you can increase both performance
   *  and precision at the cost of recall.
   */
  public void setMaxEdits(int maxEdits) {
    if (maxEdits < 1 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE)
      throw new UnsupportedOperationException("Invalid maxEdits");
    this.maxEdits = maxEdits;
  }
  
  /**
   * Get the minimal number of characters that must match exactly
   */
  public int getMinPrefix() {
    return minPrefix;
  }
  
  /**
   * Sets the minimal number of initial characters (default: 1) 
   * that must match exactly.
   * <p>
   * This can improve both performance and accuracy of results,
   * as misspellings are commonly not the first character.
   */
  public void setMinPrefix(int minPrefix) {
    this.minPrefix = minPrefix;
  }
  
  /**
   * Get the maximum number of top-N inspections per suggestion
   */
  public int getMaxInspections() {
    return maxInspections;
  }

  /**
   * Set the maximum number of top-N inspections (default: 5) per suggestion.
   * <p>
   * Increasing this number can improve the accuracy of results, at the cost 
   * of performance.
   */
  public void setMaxInspections(int maxInspections) {
    this.maxInspections = maxInspections;
  }

  /**
   * Get the minimal accuracy from the StringDistance for a match
   */
  public float getAccuracy() {
    return accuracy;
  }

  /**
   * Set the minimal accuracy required (default: 0.5f) from a StringDistance 
   * for a suggestion match.
   */
  public void setAccuracy(float accuracy) {
    this.accuracy = accuracy;
  }

  /**
   * Get the minimal threshold of documents a term must appear for a match
   */
  public float getThresholdFrequency() {
    return thresholdFrequency;
  }

  /**
   * Set the minimal threshold of documents a term must appear for a match.
   * <p>
   * This can improve quality by only suggesting high-frequency terms. Note that
   * very high values might decrease performance slightly, by forcing the spellchecker
   * to draw more candidates from the term dictionary, but a practical value such
   * as <code>1</code> can be very useful towards improving quality.
   * <p>
   * This can be specified as a relative percentage of documents such as 0.5f,
   * or it can be specified as an absolute whole document frequency, such as 4f.
   * Absolute document frequencies may not be fractional.
   */
  public void setThresholdFrequency(float thresholdFrequency) {
    if (thresholdFrequency >= 1f && thresholdFrequency != (int) thresholdFrequency)
      throw new IllegalArgumentException("Fractional absolute document frequencies are not allowed");
    this.thresholdFrequency = thresholdFrequency;
  }

  /** Get the minimum length of a query term needed to return suggestions */
  public int getMinQueryLength() {
    return minQueryLength;
  }

  /** 
   * Set the minimum length of a query term (default: 4) needed to return suggestions. 
   * <p>
   * Very short query terms will often cause only bad suggestions with any distance
   * metric.
   */
  public void setMinQueryLength(int minQueryLength) {
    this.minQueryLength = minQueryLength;
  }

  /**
   * Get the maximum threshold of documents a query term can appear in order
   * to provide suggestions.
   */
  public float getMaxQueryFrequency() {
    return maxQueryFrequency;
  }

  /**
   * Set the maximum threshold (default: 0.01f) of documents a query term can 
   * appear in order to provide suggestions.
   * <p>
   * Very high-frequency terms are typically spelled correctly. Additionally,
   * this can increase performance as it will do no work for the common case
   * of correctly-spelled input terms.
   * <p>
   * This can be specified as a relative percentage of documents such as 0.5f,
   * or it can be specified as an absolute whole document frequency, such as 4f.
   * Absolute document frequencies may not be fractional.
   */
  public void setMaxQueryFrequency(float maxQueryFrequency) {
    if (maxQueryFrequency >= 1f && maxQueryFrequency != (int) maxQueryFrequency)
      throw new IllegalArgumentException("Fractional absolute document frequencies are not allowed");
    this.maxQueryFrequency = maxQueryFrequency;
  }

  /** true if the spellchecker should lowercase terms */
  public boolean getLowerCaseTerms() {
    return lowerCaseTerms;
  }
  
  /** 
   * True if the spellchecker should lowercase terms (default: true)
   * <p>
   * This is a convenience method, if your index field has more complicated
   * analysis (such as StandardTokenizer removing punctuation), its probably
   * better to turn this off, and instead run your query terms through your
   * Analyzer first.
   * <p>
   * If this option is not on, case differences count as an edit! 
   */
  public void setLowerCaseTerms(boolean lowerCaseTerms) {
    this.lowerCaseTerms = lowerCaseTerms;
  }
  
  /**
   * Get the current comparator in use.
   */
  public Comparator<SuggestWord> getComparator() {
    return comparator;
  }

  /**
   * Set the comparator for sorting suggestions.
   * The default is {@link SuggestWordQueue#DEFAULT_COMPARATOR}
   */
  public void setComparator(Comparator<SuggestWord> comparator) {
    this.comparator = comparator;
  }

  /**
   * Get the string distance metric in use.
   */
  public StringDistance getDistance() {
    return distance;
  }

  /**
   * Set the string distance metric.
   * The default is {@link #INTERNAL_LEVENSHTEIN}
   * <p>
   * Note: because this spellchecker draws its candidates from the term
   * dictionary using Damerau-Levenshtein, it works best with an edit-distance-like
   * string metric. If you use a different metric than the default,
   * you might want to consider increasing {@link #setMaxInspections(int)}
   * to draw more candidates for your metric to rank.
   */
  public void setDistance(StringDistance distance) {
    this.distance = distance;
  }

  /**
   * Calls {@link #suggestSimilar(Term, int, IndexReader, SuggestMode) 
   *       suggestSimilar(term, numSug, ir, SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX)}
   */
  public SuggestWord[] suggestSimilar(Term term, int numSug, IndexReader ir) 
     throws IOException {
    return suggestSimilar(term, numSug, ir, SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
  }
  
  /**
   * Calls {@link #suggestSimilar(Term, int, IndexReader, SuggestMode, float) 
   *       suggestSimilar(term, numSug, ir, suggestMode, this.accuracy)}
   * 
   */
  public SuggestWord[] suggestSimilar(Term term, int numSug, IndexReader ir, 
      SuggestMode suggestMode) throws IOException {
    return suggestSimilar(term, numSug, ir, suggestMode, this.accuracy);
  }
  
  /**
   * Suggest similar words.
   * 
   * <p>Unlike {@link SpellChecker}, the similarity used to fetch the most
   * relevant terms is an edit distance, therefore typically a low value
   * for numSug will work very well.
   * 
   * @param term Term you want to spell check on
   * @param numSug the maximum number of suggested words
   * @param ir IndexReader to find terms from
   * @param suggestMode specifies when to return suggested words
   * @param accuracy return only suggested words that match with this similarity
   * @return sorted list of the suggested words according to the comparator
   * @throws IOException If there is a low-level I/O error.
   */
  public SuggestWord[] suggestSimilar(Term term, int numSug, IndexReader ir, 
      SuggestMode suggestMode, float accuracy) throws IOException {
    final CharsRef spare = new CharsRef();
    String text = term.text();
    if (minQueryLength > 0 && text.codePointCount(0, text.length()) < minQueryLength)
      return new SuggestWord[0];
    
    if (lowerCaseTerms) {
      term = new Term(term.field(), text.toLowerCase(Locale.ROOT));
    }
    
    int docfreq = ir.docFreq(term);
    
    if (suggestMode==SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX && docfreq > 0) {
      return new SuggestWord[0];
    }
    
    int maxDoc = ir.maxDoc();
    
    if (maxQueryFrequency >= 1f && docfreq > maxQueryFrequency) {
      return new SuggestWord[0];
    } else if (docfreq > (int) Math.ceil(maxQueryFrequency * (float)maxDoc)) {
      return new SuggestWord[0];
    }
    
    if (suggestMode!=SuggestMode.SUGGEST_MORE_POPULAR) docfreq = 0;
    
    if (thresholdFrequency >= 1f) {
      docfreq = Math.max(docfreq, (int) thresholdFrequency);
    } else if (thresholdFrequency > 0f) {
      docfreq = Math.max(docfreq, (int)(thresholdFrequency * (float)maxDoc)-1);
    }
    
    Collection<ScoreTerm> terms = null;
    int inspections = numSug * maxInspections;
    
    // try ed=1 first, in case we get lucky
    terms = suggestSimilar(term, inspections, ir, docfreq, 1, accuracy, spare);
    if (maxEdits > 1 && terms.size() < inspections) {
      HashSet<ScoreTerm> moreTerms = new HashSet<ScoreTerm>();
      moreTerms.addAll(terms);
      moreTerms.addAll(suggestSimilar(term, inspections, ir, docfreq, maxEdits, accuracy, spare));
      terms = moreTerms;
    }
    
    // create the suggestword response, sort it, and trim it to size.
    
    SuggestWord suggestions[] = new SuggestWord[terms.size()];
    int index = suggestions.length - 1;
    for (ScoreTerm s : terms) {
      SuggestWord suggestion = new SuggestWord();
      if (s.termAsString == null) {
        UnicodeUtil.UTF8toUTF16(s.term, spare);
        s.termAsString = spare.toString();
      }
      suggestion.string = s.termAsString;
      suggestion.score = s.score;
      suggestion.freq = s.docfreq;
      suggestions[index--] = suggestion;
    }
    
    ArrayUtil.mergeSort(suggestions, Collections.reverseOrder(comparator));
    if (numSug < suggestions.length) {
      SuggestWord trimmed[] = new SuggestWord[numSug];
      System.arraycopy(suggestions, 0, trimmed, 0, numSug);
      suggestions = trimmed;
    }
    return suggestions;
  }

  /**
   * Provide spelling corrections based on several parameters.
   *
   * @param term The term to suggest spelling corrections for
   * @param numSug The maximum number of spelling corrections
   * @param ir The index reader to fetch the candidate spelling corrections from
   * @param docfreq The minimum document frequency a potential suggestion need to have in order to be included
   * @param editDistance The maximum edit distance candidates are allowed to have
   * @param accuracy The minimum accuracy a suggested spelling correction needs to have in order to be included
   * @param spare a chars scratch
   * @return a collection of spelling corrections sorted by <code>ScoreTerm</code>'s natural order.
   * @throws IOException If I/O related errors occur
   */
  protected Collection<ScoreTerm> suggestSimilar(Term term, int numSug, IndexReader ir, int docfreq, int editDistance,
                                                 float accuracy, final CharsRef spare) throws IOException {
    
    AttributeSource atts = new AttributeSource();
    MaxNonCompetitiveBoostAttribute maxBoostAtt =
      atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);
    Terms terms = MultiFields.getTerms(ir, term.field());
    if (terms == null) {
      return Collections.emptyList();
    }
    FuzzyTermsEnum e = new FuzzyTermsEnum(terms, atts, term, editDistance, Math.max(minPrefix, editDistance-1), true);
    final PriorityQueue<ScoreTerm> stQueue = new PriorityQueue<ScoreTerm>();
    
    BytesRef queryTerm = new BytesRef(term.text());
    BytesRef candidateTerm;
    ScoreTerm st = new ScoreTerm();
    BoostAttribute boostAtt =
      e.attributes().addAttribute(BoostAttribute.class);
    while ((candidateTerm = e.next()) != null) {
      final float boost = boostAtt.getBoost();
      // ignore uncompetitive hits
      if (stQueue.size() >= numSug && boost <= stQueue.peek().boost)
        continue;
      
      // ignore exact match of the same term
      if (queryTerm.bytesEquals(candidateTerm))
        continue;
      
      int df = e.docFreq();
      
      // check docFreq if required
      if (df <= docfreq)
        continue;
      
      final float score;
      final String termAsString;
      if (distance == INTERNAL_LEVENSHTEIN) {
        // delay creating strings until the end
        termAsString = null;
        // undo FuzzyTermsEnum's scale factor for a real scaled lev score
        score = boost / e.getScaleFactor() + e.getMinSimilarity();
      } else {
        UnicodeUtil.UTF8toUTF16(candidateTerm, spare);
        termAsString = spare.toString();
        score = distance.getDistance(term.text(), termAsString);
      }
      
      if (score < accuracy)
        continue;
      
      // add new entry in PQ
      st.term = BytesRef.deepCopyOf(candidateTerm);
      st.boost = boost;
      st.docfreq = df;
      st.termAsString = termAsString;
      st.score = score;
      stQueue.offer(st);
      // possibly drop entries from queue
      st = (stQueue.size() > numSug) ? stQueue.poll() : new ScoreTerm();
      maxBoostAtt.setMaxNonCompetitiveBoost((stQueue.size() >= numSug) ? stQueue.peek().boost : Float.NEGATIVE_INFINITY);
    }
      
    return stQueue;
  }

  /**
   * Holds a spelling correction for internal usage inside {@link DirectSpellChecker}.
   */
  protected static class ScoreTerm implements Comparable<ScoreTerm> {

    /**
     * The actual spellcheck correction.
     */
    public BytesRef term;

    /**
     * The boost representing the similarity from the FuzzyTermsEnum (internal similarity score)
     */
    public float boost;

    /**
     * The df of the spellcheck correction.
     */
    public int docfreq;

    /**
     * The spellcheck correction represented as string, can be <code>null</code>.
     */
    public String termAsString;

    /**
     * The similarity score.
     */
    public float score;

    /**
     * Constructor.
     */
    public ScoreTerm() {
    }

    @Override
    public int compareTo(ScoreTerm other) {
      if (term.bytesEquals(other.term))
        return 0; // consistent with equals
      if (this.boost == other.boost)
        return other.term.compareTo(this.term);
      else
        return Float.compare(this.boost, other.boost);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((term == null) ? 0 : term.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      ScoreTerm other = (ScoreTerm) obj;
      if (term == null) {
        if (other.term != null) return false;
      } else if (!term.bytesEquals(other.term)) return false;
      return true;
    }
  }
}

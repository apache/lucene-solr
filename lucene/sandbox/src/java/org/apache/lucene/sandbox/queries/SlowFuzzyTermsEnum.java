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
package org.apache.lucene.sandbox.queries;

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.FuzzyTermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;

/** Potentially slow fuzzy TermsEnum for enumerating all terms that are similar
 * to the specified filter term.
 * <p> If the minSimilarity or maxEdits is greater than the Automaton's
 * allowable range, this backs off to the classic (brute force)
 * fuzzy terms enum method by calling FuzzyTermsEnum's getAutomatonEnum.
 * </p>
 * <p>Term enumerations are always ordered by
 * {@link BytesRef#compareTo}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 * 
 * @deprecated Use {@link FuzzyTermsEnum} instead.
 */
@Deprecated
public final class SlowFuzzyTermsEnum extends FuzzyTermsEnum {
 
  public SlowFuzzyTermsEnum(Terms terms, AttributeSource atts, Term term,
      float minSimilarity, int prefixLength) throws IOException {
    super(terms, atts, term, minSimilarity, prefixLength, false);
  }
  
  @Override
  protected void maxEditDistanceChanged(BytesRef lastTerm, int maxEdits, boolean init)
      throws IOException {
    TermsEnum newEnum = getAutomatonEnum(maxEdits, lastTerm);
    if (newEnum != null) {
      setEnum(newEnum);
    } else if (init) {
      setEnum(new LinearFuzzyTermsEnum());      
    }
  }

  /**
   * Implement fuzzy enumeration with linear brute force.
   */
  private class LinearFuzzyTermsEnum extends FilteredTermsEnum {
    /* Allows us save time required to create a new array
     * every time similarity is called.
     */
    private int[] d;
    private int[] p;
    
    // this is the text, minus the prefix
    private final int[] text;
    
    private final BoostAttribute boostAtt =
      attributes().addAttribute(BoostAttribute.class);
    
    /**
     * Constructor for enumeration of all terms from specified <code>reader</code> which share a prefix of
     * length <code>prefixLength</code> with <code>term</code> and which have a fuzzy similarity &gt;
     * <code>minSimilarity</code>.
     * <p>
     * After calling the constructor the enumeration is already pointing to the first 
     * valid term if such a term exists.
     *
     * @throws IOException If there is a low-level I/O error.
     */
    public LinearFuzzyTermsEnum() throws IOException {
      super(terms.iterator());

      this.text = new int[termLength - realPrefixLength];
      System.arraycopy(termText, realPrefixLength, text, 0, text.length);
      final String prefix = UnicodeUtil.newString(termText, 0, realPrefixLength);
      prefixBytesRef = new BytesRef(prefix);
      this.d = new int[this.text.length + 1];
      this.p = new int[this.text.length + 1];
      
      setInitialSeekTerm(prefixBytesRef);
    }
    
    private final BytesRef prefixBytesRef;
    // used for unicode conversion from BytesRef byte[] to int[]
    private final IntsRefBuilder utf32 = new IntsRefBuilder();
    
    /**
     * <p>The termCompare method in FuzzyTermEnum uses Levenshtein distance to 
     * calculate the distance between the given term and the comparing term. 
     * </p>
     * <p>If the minSimilarity is &gt;= 1.0, this uses the maxEdits as the comparison.
     * Otherwise, this method uses the following logic to calculate similarity.
     * <pre>
     *   similarity = 1 - ((float)distance / (float) (prefixLength + Math.min(textlen, targetlen)));
     *   </pre>
     * where distance is the Levenshtein distance for the two words.
     * </p>
     * 
     */
    @Override
    protected final AcceptStatus accept(BytesRef term) {
      if (StringHelper.startsWith(term, prefixBytesRef)) {
        utf32.copyUTF8Bytes(term);
        final int distance = calcDistance(utf32.ints(), realPrefixLength, utf32.length() - realPrefixLength);
       
        //Integer.MIN_VALUE is the sentinel that Levenshtein stopped early
        if (distance == Integer.MIN_VALUE){
           return AcceptStatus.NO;
        }
        //no need to calc similarity, if raw is true and distance > maxEdits
        if (raw == true && distance > maxEdits){
              return AcceptStatus.NO;
        } 
        final float similarity = calcSimilarity(distance, (utf32.length() - realPrefixLength), text.length);
        
        //if raw is true, then distance must also be <= maxEdits by now
        //given the previous if statement
        if (raw == true ||
              (raw == false && similarity > minSimilarity)) {
          boostAtt.setBoost((similarity - minSimilarity) * scale_factor);
          return AcceptStatus.YES;
        } else {
           return AcceptStatus.NO;
        }
      } else {
        return AcceptStatus.END;
      }
    }
    
    /******************************
     * Compute Levenshtein distance
     ******************************/
    
    /**
     * <p>calcDistance returns the Levenshtein distance between the query term
     * and the target term.</p>
     * 
     * <p>Embedded within this algorithm is a fail-fast Levenshtein distance
     * algorithm.  The fail-fast algorithm differs from the standard Levenshtein
     * distance algorithm in that it is aborted if it is discovered that the
     * minimum distance between the words is greater than some threshold.

     * <p>Levenshtein distance (also known as edit distance) is a measure of similarity
     * between two strings where the distance is measured as the number of character
     * deletions, insertions or substitutions required to transform one string to
     * the other string.
     * @param target the target word or phrase
     * @param offset the offset at which to start the comparison
     * @param length the length of what's left of the string to compare
     * @return the number of edits or Integer.MIN_VALUE if the edit distance is
     * greater than maxDistance.
     */
    private final int calcDistance(final int[] target, int offset, int length) {
      final int m = length;
      final int n = text.length;
      if (n == 0)  {
        //we don't have anything to compare.  That means if we just add
        //the letters for m we get the new word
        return m;
      }
      if (m == 0) {
        return n;
      }
      
      final int maxDistance = calculateMaxDistance(m);
      
      if (maxDistance < Math.abs(m-n)) {
        //just adding the characters of m to n or vice-versa results in
        //too many edits
        //for example "pre" length is 3 and "prefixes" length is 8.  We can see that
        //given this optimal circumstance, the edit distance cannot be less than 5.
        //which is 8-3 or more precisely Math.abs(3-8).
        //if our maximum edit distance is 4, then we can discard this word
        //without looking at it.
        return Integer.MIN_VALUE;
      }
      
      // init matrix d
      for (int i = 0; i <=n; ++i) {
        p[i] = i;
      }
      
      // start computing edit distance
      for (int j = 1; j<=m; ++j) { // iterates through target
        int bestPossibleEditDistance = m;
        final int t_j = target[offset+j-1]; // jth character of t
        d[0] = j;

        for (int i=1; i<=n; ++i) { // iterates through text
          // minimum of cell to the left+1, to the top+1, diagonally left and up +(0|1)
          if (t_j != text[i-1]) {
            d[i] = Math.min(Math.min(d[i-1], p[i]),  p[i-1]) + 1;
          } else {
            d[i] = Math.min(Math.min(d[i-1]+1, p[i]+1),  p[i-1]);
          }
          bestPossibleEditDistance = Math.min(bestPossibleEditDistance, d[i]);
        }

        //After calculating row i, the best possible edit distance
        //can be found by found by finding the smallest value in a given column.
        //If the bestPossibleEditDistance is greater than the max distance, abort.

        if (j > maxDistance && bestPossibleEditDistance > maxDistance) {  //equal is okay, but not greater
          //the closest the target can be to the text is just too far away.
          //this target is leaving the party early.
          return Integer.MIN_VALUE;
        }

        // copy current distance counts to 'previous row' distance counts: swap p and d
        int _d[] = p;
        p = d;
        d = _d;
      }
      
      // our last action in the above loop was to switch d and p, so p now
      // actually has the most recent cost counts

      return p[n];
    }
    
    private float calcSimilarity(int edits, int m, int n){
      // this will return less than 0.0 when the edit distance is
      // greater than the number of characters in the shorter word.
      // but this was the formula that was previously used in FuzzyTermEnum,
      // so it has not been changed (even though minimumSimilarity must be
      // greater than 0.0)
      
      return 1.0f - ((float)edits / (float) (realPrefixLength + Math.min(n, m)));
    }
    
    /**
     * The max Distance is the maximum Levenshtein distance for the text
     * compared to some other value that results in score that is
     * better than the minimum similarity.
     * @param m the length of the "other value"
     * @return the maximum levenshtein distance that we care about
     */
    private int calculateMaxDistance(int m) {
      return raw ? maxEdits : Math.min(maxEdits, 
          (int)((1-minSimilarity) * (Math.min(text.length, m) + realPrefixLength)));
    }
  }
}

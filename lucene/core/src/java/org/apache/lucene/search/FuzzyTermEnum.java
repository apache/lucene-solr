package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/** Subclass of FilteredTermEnum for enumerating all terms that are similar
 * to the specified filter term.
 *
 * <p>Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 */
public final class FuzzyTermEnum extends FilteredTermEnum {

  /* Allows us save time required to create a new array
   * every time similarity is called.
   */
  private int[] p;
  private int[] d;

  private float similarity;
  private boolean endEnum = false;

  private Term searchTerm = null;
  private final String field;
  private final char[] text;
  private final String prefix;

  private final float minimumSimilarity;
  private final float scale_factor;

  /**
   * Creates a FuzzyTermEnum with an empty prefix and a minSimilarity of 0.5f.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first 
   * valid term if such a term exists. 
   * 
   * @param reader
   * @param term
   * @throws IOException
   * @see #FuzzyTermEnum(IndexReader, Term, float, int)
   */
  public FuzzyTermEnum(IndexReader reader, Term term) throws IOException {
    this(reader, term, FuzzyQuery.defaultMinSimilarity, FuzzyQuery.defaultPrefixLength);
  }
    
  /**
   * Creates a FuzzyTermEnum with an empty prefix.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first 
   * valid term if such a term exists. 
   * 
   * @param reader
   * @param term
   * @param minSimilarity
   * @throws IOException
   * @see #FuzzyTermEnum(IndexReader, Term, float, int)
   */
  public FuzzyTermEnum(IndexReader reader, Term term, float minSimilarity) throws IOException {
    this(reader, term, minSimilarity, FuzzyQuery.defaultPrefixLength);
  }
    
  /**
   * Constructor for enumeration of all terms from specified <code>reader</code> which share a prefix of
   * length <code>prefixLength</code> with <code>term</code> and which have a fuzzy similarity &gt;
   * <code>minSimilarity</code>.
   * <p>
   * After calling the constructor the enumeration is already pointing to the first 
   * valid term if such a term exists. 
   * 
   * @param reader Delivers terms.
   * @param term Pattern term.
   * @param minSimilarity Minimum required similarity for terms from the reader. Default value is 0.5f.
   * @param prefixLength Length of required common prefix. Default value is 0.
   * @throws IOException
   */
  public FuzzyTermEnum(IndexReader reader, Term term, final float minSimilarity, final int prefixLength) throws IOException {
    super();
    
    if (minSimilarity >= 1.0f)
      throw new IllegalArgumentException("minimumSimilarity cannot be greater than or equal to 1");
    else if (minSimilarity < 0.0f)
      throw new IllegalArgumentException("minimumSimilarity cannot be less than 0");
    if(prefixLength < 0)
      throw new IllegalArgumentException("prefixLength cannot be less than 0");

    this.minimumSimilarity = minSimilarity;
    this.scale_factor = 1.0f / (1.0f - minimumSimilarity);
    this.searchTerm = term;
    this.field = searchTerm.field();

    //The prefix could be longer than the word.
    //It's kind of silly though.  It means we must match the entire word.
    final int fullSearchTermLength = searchTerm.text().length();
    final int realPrefixLength = prefixLength > fullSearchTermLength ? fullSearchTermLength : prefixLength;

    this.text = searchTerm.text().substring(realPrefixLength).toCharArray();
    this.prefix = searchTerm.text().substring(0, realPrefixLength);

    this.p = new int[this.text.length+1];
    this.d = new int[this.text.length+1];

    setEnum(reader.terms(new Term(searchTerm.field(), prefix)));
  }

  /**
   * The termCompare method in FuzzyTermEnum uses Levenshtein distance to 
   * calculate the distance between the given term and the comparing term. 
   */
  @Override
  protected final boolean termCompare(Term term) {
    if (field == term.field() && term.text().startsWith(prefix)) {
        final String target = term.text().substring(prefix.length());
        this.similarity = similarity(target);
        return (similarity > minimumSimilarity);
    }
    endEnum = true;
    return false;
  }
  
  /** {@inheritDoc} */
  @Override
  public final float difference() {
    return (similarity - minimumSimilarity) * scale_factor;
  }
  
  /** {@inheritDoc} */
  @Override
  public final boolean endEnum() {
    return endEnum;
  }
  
  /******************************
   * Compute Levenshtein distance
   ******************************/
  
  /**
   * <p>Similarity returns a number that is 1.0f or less (including negative numbers)
   * based on how similar the Term is compared to a target term.  It returns
   * exactly 0.0f when
   * <pre>
   *    editDistance &gt; maximumEditDistance</pre>
   * Otherwise it returns:
   * <pre>
   *    1 - (editDistance / length)</pre>
   * where length is the length of the shortest term (text or target) including a
   * prefix that are identical and editDistance is the Levenshtein distance for
   * the two words.</p>
   *
   * <p>Embedded within this algorithm is a fail-fast Levenshtein distance
   * algorithm.  The fail-fast algorithm differs from the standard Levenshtein
   * distance algorithm in that it is aborted if it is discovered that the
   * minimum distance between the words is greater than some threshold.
   *
   * <p>To calculate the maximum distance threshold we use the following formula:
   * <pre>
   *     (1 - minimumSimilarity) * length</pre>
   * where length is the shortest term including any prefix that is not part of the
   * similarity comparison.  This formula was derived by solving for what maximum value
   * of distance returns false for the following statements:
   * <pre>
   *   similarity = 1 - ((float)distance / (float) (prefixLength + Math.min(textlen, targetlen)));
   *   return (similarity > minimumSimilarity);</pre>
   * where distance is the Levenshtein distance for the two words.
   * </p>
   * <p>Levenshtein distance (also known as edit distance) is a measure of similarity
   * between two strings where the distance is measured as the number of character
   * deletions, insertions or substitutions required to transform one string to
   * the other string.
   * @param target the target word or phrase
   * @return the similarity,  0.0 or less indicates that it matches less than the required
   * threshold and 1.0 indicates that the text and target are identical
   */
  private float similarity(final String target) {
    final int m = target.length();
    final int n = text.length;
    if (n == 0)  {
      //we don't have anything to compare.  That means if we just add
      //the letters for m we get the new word
      return prefix.length() == 0 ? 0.0f : 1.0f - ((float) m / prefix.length());
    }
    if (m == 0) {
      return prefix.length() == 0 ? 0.0f : 1.0f - ((float) n / prefix.length());
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
      return 0.0f;
    }

    // init matrix d
    for (int i = 0; i<=n; ++i) {
      p[i] = i;
    }

    // start computing edit distance
    for (int j = 1; j<=m; ++j) { // iterates through target
      int bestPossibleEditDistance = m;
      final char t_j = target.charAt(j-1); // jth character of t
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
        return 0.0f;
      }

      // copy current distance counts to 'previous row' distance counts: swap p and d
      int _d[] = p;
      p = d;
      d = _d;
    }

    // our last action in the above loop was to switch d and p, so p now
    // actually has the most recent cost counts

    // this will return less than 0.0 when the edit distance is
    // greater than the number of characters in the shorter word.
    // but this was the formula that was previously used in FuzzyTermEnum,
    // so it has not been changed (even though minimumSimilarity must be
    // greater than 0.0)
    return 1.0f - ((float)p[n] / (float) (prefix.length() + Math.min(n, m)));
  }

  /**
   * The max Distance is the maximum Levenshtein distance for the text
   * compared to some other value that results in score that is
   * better than the minimum similarity.
   * @param m the length of the "other value"
   * @return the maximum levenshtein distance that we care about
   */
  private int calculateMaxDistance(int m) {
    return (int) ((1-minimumSimilarity) * (Math.min(text.length, m) + prefix.length()));
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    p = d = null;
    searchTerm = null;
    super.close();  //call super.close() and let the garbage collector do its work.
  }
  
}

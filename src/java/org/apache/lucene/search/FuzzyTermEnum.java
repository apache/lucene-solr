package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/** Subclass of FilteredTermEnum for enumerating all terms that are similiar to the specified filter term.

  <p>Term enumerations are always ordered by Term.compareTo().  Each term in
  the enumeration is greater than all that precede it.  */
public final class FuzzyTermEnum extends FilteredTermEnum {
    float similarity;
    boolean endEnum = false;

    Term searchTerm = null;
    String field = "";
    String text = "";
    int textlen;
    String prefix = "";
    int prefixLength = 0;
    float minimumSimilarity;
    float scale_factor;
    
    
    /**
     * Empty prefix and minSimilarity of 0.5f are used.
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
     * This is the standard FuzzyTermEnum with an empty prefix.
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
     * 
     * @param reader Delivers terms.
     * @param term Pattern term.
     * @param minSimilarity Minimum required similarity for terms from the reader. Default value is 0.5f.
     * @param prefixLength Length of required common prefix. Default value is 0.
     * @throws IOException
     */
    public FuzzyTermEnum(IndexReader reader, Term term, float minSimilarity, int prefixLength) throws IOException {
        super();
        
        if (minimumSimilarity >= 1.0f)
          throw new IllegalArgumentException("minimumSimilarity >= 1");
        else if (minimumSimilarity < 0.0f)
          throw new IllegalArgumentException("minimumSimilarity < 0");
        
        minimumSimilarity = minSimilarity;
        scale_factor = 1.0f / (1.0f - minimumSimilarity);
        searchTerm = term;
        field = searchTerm.field();
        text = searchTerm.text();
        textlen = text.length();
        
        if(prefixLength < 0)
          throw new IllegalArgumentException("prefixLength < 0");
        
        if(prefixLength > textlen)
          prefixLength = textlen;
        
        this.prefixLength = prefixLength;
        prefix = text.substring(0, prefixLength);
        text = text.substring(prefixLength);
        textlen = text.length();
        
        setEnum(reader.terms(new Term(searchTerm.field(), prefix)));
    }
    
    /**
     The termCompare method in FuzzyTermEnum uses Levenshtein distance to 
     calculate the distance between the given term and the comparing term. 
     */
    protected final boolean termCompare(Term term) {
        String termText = term.text();
        if (field == term.field() && termText.startsWith(prefix)) {
            String target = termText.substring(prefixLength);
            int targetlen = target.length();
            int dist = editDistance(text, target, textlen, targetlen);
            similarity = 1 - ((float)dist / (float) (prefixLength + Math.min(textlen, targetlen)));
            return (similarity > minimumSimilarity);
        }
        endEnum = true;
        return false;
    }
    
    public final float difference() {
        return (float)((similarity - minimumSimilarity) * scale_factor);
    }
    
    public final boolean endEnum() {
        return endEnum;
    }
    
    /******************************
     * Compute Levenshtein distance
     ******************************/
    
    /**
     Finds and returns the smallest of three integers 
     */
    private static final int min(int a, int b, int c) {
        int t = (a < b) ? a : b;
        return (t < c) ? t : c;
    }
    
    /**
     * This static array saves us from the time required to create a new array
     * everytime editDistance is called.
     */
    private int e[][] = new int[1][1];
    
    /**
     Levenshtein distance also known as edit distance is a measure of similiarity
     between two strings where the distance is measured as the number of character 
     deletions, insertions or substitutions required to transform one string to 
     the other string. 
     <p>This method takes in four parameters; two strings and their respective 
     lengths to compute the Levenshtein distance between the two strings.
     The result is returned as an integer.
     */ 
    private final int editDistance(String s, String t, int n, int m) {
        if (e.length <= n || e[0].length <= m) {
            e = new int[Math.max(e.length, n+1)][Math.max(e[0].length, m+1)];
        }
        int d[][] = e; // matrix
        int i; // iterates through s
        int j; // iterates through t
        char s_i; // ith character of s
        
        if (n == 0) return m;
        if (m == 0) return n;
        
        // init matrix d
        for (i = 0; i <= n; i++) d[i][0] = i;
        for (j = 0; j <= m; j++) d[0][j] = j;
        
        // start computing edit distance
        for (i = 1; i <= n; i++) {
            s_i = s.charAt(i - 1);
            for (j = 1; j <= m; j++) {
                if (s_i != t.charAt(j-1))
                    d[i][j] = min(d[i-1][j], d[i][j-1], d[i-1][j-1])+1;
                else d[i][j] = min(d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1]);
            }
        }
        
        // we got the result!
        return d[n][m];
    }
    
  public void close() throws IOException {
      super.close();
      searchTerm = null;
      field = null;
      text = null;
  }
}

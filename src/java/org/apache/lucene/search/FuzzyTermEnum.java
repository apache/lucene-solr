package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

/** Subclass of FilteredTermEnum for enumerating all terms that are similiar to the specified filter term.

  <p>Term enumerations are always ordered by Term.compareTo().  Each term in
  the enumeration is greater than all that precede it.  */
final public class FuzzyTermEnum extends FilteredTermEnum {
    double distance;
    boolean fieldMatch = false;
    boolean endEnum = false;

    Term searchTerm = null;
    String field = "";
    String text = "";
    int textlen;
    
    public FuzzyTermEnum(IndexReader reader, Term term) throws IOException {
        super(reader, term);
        searchTerm = term;
        field = searchTerm.field();
        text = searchTerm.text();
        textlen = text.length();
        setEnum(reader.terms(new Term(searchTerm.field(), "")));
    }
    
    /**
     The termCompare method in FuzzyTermEnum uses Levenshtein distance to 
     calculate the distance between the given term and the comparing term. 
     */
    final protected boolean termCompare(Term term) {
        if (field == term.field()) {
            String target = term.text();
            int targetlen = target.length();
            int dist = editDistance(text, target, textlen, targetlen);
            distance = 1 - ((double)dist / (double)Math.min(textlen, targetlen));
            return (distance > FUZZY_THRESHOLD);
        }
        endEnum = true;
        return false;
    }
    
    final protected float difference() {
        return (float)((distance - FUZZY_THRESHOLD) * SCALE_FACTOR);
    }
    
    final public boolean endEnum() {
        return endEnum;
    }
    
    /******************************
     * Compute Levenshtein distance
     ******************************/
    
    public static final double FUZZY_THRESHOLD = 0.5;
    public static final double SCALE_FACTOR = 1.0f / (1.0f - FUZZY_THRESHOLD);
    
    /**
     Finds and returns the smallest of three integers 
     */
    private final static int min(int a, int b, int c) {
        int t = (a < b) ? a : b;
        return (t < c) ? t : c;
    }
    
    /**
     * This static array saves us from the time required to create a new array
     * everytime editDistance is called.
     */
    private int e[][] = new int[0][0];
    
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
            e = new int[Math.max(e.length, n+1)][Math.max(e.length, m+1)];
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

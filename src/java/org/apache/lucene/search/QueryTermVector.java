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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermFreqVector;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 *
 *
 **/
public class QueryTermVector implements TermFreqVector {
  private String [] terms = new String[0];
  private int [] termFreqs = new int[0];

  public String getField() { return null;  }

  /**
   * 
   * @param queryTerms The original list of terms from the query, can contain duplicates
   */ 
  public QueryTermVector(String [] queryTerms) {

    processTerms(queryTerms);
  }

  public QueryTermVector(String queryString, Analyzer analyzer) {    
    if (analyzer != null)
    {
      TokenStream stream = analyzer.tokenStream("", new StringReader(queryString));
      if (stream != null)
      {
        Token next = null;
        List terms = new ArrayList();
        try {
          while ((next = stream.next()) != null)
          {
            terms.add(next.termText());
          }
          processTerms((String[])terms.toArray(new String[terms.size()]));
        } catch (IOException e) {
        }
      }
    }                                                              
  }
  
  private void processTerms(String[] queryTerms) {
    if (queryTerms != null) {
      Arrays.sort(queryTerms);
      Map tmpSet = new HashMap(queryTerms.length);
      //filter out duplicates
      List tmpList = new ArrayList(queryTerms.length);
      List tmpFreqs = new ArrayList(queryTerms.length);
      int j = 0;
      for (int i = 0; i < queryTerms.length; i++) {
        String term = queryTerms[i];
        Integer position = (Integer)tmpSet.get(term);
        if (position == null) {
          tmpSet.put(term, new Integer(j++));
          tmpList.add(term);
          tmpFreqs.add(new Integer(1));
        }       
        else {
          Integer integer = (Integer)tmpFreqs.get(position.intValue());
          tmpFreqs.set(position.intValue(), new Integer(integer.intValue() + 1));          
        }
      }
      terms = (String[])tmpList.toArray(terms);
      //termFreqs = (int[])tmpFreqs.toArray(termFreqs);
      termFreqs = new int[tmpFreqs.size()];
      int i = 0;
      for (Iterator iter = tmpFreqs.iterator(); iter.hasNext();) {
        Integer integer = (Integer) iter.next();
        termFreqs[i++] = integer.intValue();
      }
    }
  }
  
  public final String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append('{');
        for (int i=0; i<terms.length; i++) {
            if (i>0) sb.append(", ");
            sb.append(terms[i]).append('/').append(termFreqs[i]);
        }
        sb.append('}');
        return sb.toString();
    }
  

  /** 
   * @return The number of terms in the term vector.
   */
  public int size() {
    return terms.length;
  }

  /** Returns an array of positions in which the term is found or null if no position information is
   * available or positions are not implemented.
   *  Terms are identified by the index at which its number appears in the
   *  term array obtained from <code>getTerms</code> method.
   */
  public int[] getTermPositions(int index) {
    return null;
  }

  /** 
   * @return An Array of term texts in ascending order.
   */
  public String[] getTerms() {
    return terms;
  }

  /** Array of term frequencies. Locations of the array correspond one to one
   *  to the term numbers in the array obtained from <code>getTermNumbers</code>
   *  method. Each location in the array contains the number of times this
   *  term occurs in the document or the document field.
   */
  public int[] getTermFrequencies() {
    return termFreqs;
  }

  /** Return a string representation of the vector, but use the provided IndexReader
   *  to obtain text for each term and include the text instead of term numbers.
   */
  public String toString(IndexReader ir) throws IOException {
    return toString();
  }

  /** Return an index in the term numbers array returned from <code>getTermNumbers</code>
   *  at which the term with the specified <code>termNumber</code> appears. If this
   *  term does not appear in the array, return -1.
   */
  public int indexOf(String term) {
    int res = Arrays.binarySearch(terms, term);
        return res >= 0 ? res : -1;
  }

  /** Just like <code>indexOf(int)</code> but searches for a number of terms
   *  at the same time. Returns an array that has the same size as the number
   *  of terms searched for, each slot containing the result of searching for
   *  that term number.
   *
   *  @param terms array containing terms to look for
   *  @param start index in the array where the list of terms starts
   *  @param len the number of terms in the list
   */
  public int[] indexesOf(String[] terms, int start, int len) {
    int res[] = new int[len];

    for (int i=0; i < len; i++) {
        res[i] = indexOf(terms[i]);
    }
    return res;                  
  }

}

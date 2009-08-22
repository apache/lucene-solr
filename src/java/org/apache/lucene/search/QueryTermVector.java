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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.TermFreqVector;

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
        List terms = new ArrayList();
        try {
          boolean hasMoreTokens = false;
          
          stream.reset(); 
          TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);

          hasMoreTokens = stream.incrementToken();
          while (hasMoreTokens) {
            terms.add(termAtt.term());
            hasMoreTokens = stream.incrementToken();
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
  

  public int size() {
    return terms.length;
  }

  public String[] getTerms() {
    return terms;
  }

  public int[] getTermFrequencies() {
    return termFreqs;
  }

  public int indexOf(String term) {
    int res = Arrays.binarySearch(terms, term);
        return res >= 0 ? res : -1;
  }

  public int[] indexesOf(String[] terms, int start, int len) {
    int res[] = new int[len];

    for (int i=0; i < len; i++) {
        res[i] = indexOf(terms[i]);
    }
    return res;                  
  }

}

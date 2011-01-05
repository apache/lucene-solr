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

import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 *
 *
 **/
public class QueryTermVector implements TermFreqVector {
  private BytesRef [] terms = new BytesRef[0];
  private int [] termFreqs = new int[0];

  public String getField() { return null;  }

  /**
   * 
   * @param queryTerms The original list of terms from the query, can contain duplicates
   */ 
  public QueryTermVector(BytesRef [] queryTerms) {

    processTerms(queryTerms);
  }

  public QueryTermVector(String queryString, Analyzer analyzer) {    
    if (analyzer != null)
    {
      TokenStream stream = analyzer.tokenStream("", new StringReader(queryString));
      if (stream != null)
      {
        List<BytesRef> terms = new ArrayList<BytesRef>();
        try {
          boolean hasMoreTokens = false;
          
          stream.reset(); 
          final TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

          hasMoreTokens = stream.incrementToken();
          while (hasMoreTokens) {
            BytesRef bytes = new BytesRef();
            termAtt.toBytesRef(bytes);
            terms.add(bytes);
            hasMoreTokens = stream.incrementToken();
          }
          processTerms(terms.toArray(new BytesRef[terms.size()]));
        } catch (IOException e) {
        }
      }
    }                                                              
  }
  
  private void processTerms(BytesRef[] queryTerms) {
    if (queryTerms != null) {
      ArrayUtil.quickSort(queryTerms);
      Map<BytesRef,Integer> tmpSet = new HashMap<BytesRef,Integer>(queryTerms.length);
      //filter out duplicates
      List<BytesRef> tmpList = new ArrayList<BytesRef>(queryTerms.length);
      List<Integer> tmpFreqs = new ArrayList<Integer>(queryTerms.length);
      int j = 0;
      for (int i = 0; i < queryTerms.length; i++) {
        BytesRef term = queryTerms[i];
        Integer position = tmpSet.get(term);
        if (position == null) {
          tmpSet.put(term, Integer.valueOf(j++));
          tmpList.add(term);
          tmpFreqs.add(Integer.valueOf(1));
        }       
        else {
          Integer integer = tmpFreqs.get(position.intValue());
          tmpFreqs.set(position.intValue(), Integer.valueOf(integer.intValue() + 1));          
        }
      }
      terms = tmpList.toArray(terms);
      //termFreqs = (int[])tmpFreqs.toArray(termFreqs);
      termFreqs = new int[tmpFreqs.size()];
      int i = 0;
      for (final Integer integer : tmpFreqs) {
        termFreqs[i++] = integer.intValue();
      }
    }
  }
  
  @Override
  public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (int i=0; i<terms.length; i++) {
            if (i>0) sb.append(", ");
            sb.append(terms[i].utf8ToString()).append('/').append(termFreqs[i]);
        }
        sb.append('}');
        return sb.toString();
    }
  

  public int size() {
    return terms.length;
  }

  public BytesRef[] getTerms() {
    return terms;
  }

  public int[] getTermFrequencies() {
    return termFreqs;
  }

  public int indexOf(BytesRef term) {
    int res = Arrays.binarySearch(terms, term);
        return res >= 0 ? res : -1;
  }

  public int[] indexesOf(BytesRef[] terms, int start, int len) {
    int res[] = new int[len];

    for (int i=0; i < len; i++) {
        res[i] = indexOf(terms[i]);
    }
    return res;                  
  }

}

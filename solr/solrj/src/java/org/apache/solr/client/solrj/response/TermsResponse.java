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
package org.apache.solr.client.solrj.response;

import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates responses from TermsComponent
 */
public class TermsResponse {
  private Map<String, List<Term>> termMap = new HashMap<>();
  
  public TermsResponse(NamedList<NamedList<Object>> termsInfo) {
    for (int i = 0; i < termsInfo.size(); i++) {
      String fieldName = termsInfo.getName(i);
      List<Term> itemList = new ArrayList<>();
      NamedList<Object> items = termsInfo.getVal(i);
      
      for (int j = 0; j < items.size(); j++) {
        String term = items.getName(j);
        Object val = items.getVal(j);
        Term t;
        if (val instanceof NamedList) {
          @SuppressWarnings("unchecked")
          NamedList<Number> termStats = (NamedList<Number>) val;
          t = new Term(term, termStats.get("df").longValue(), termStats.get("ttf").longValue());
        } else {
          t = new Term(term, ((Number) val).longValue());
        }
        itemList.add(t);
      }

      termMap.put(fieldName, itemList);
    }
  }

  /**
   * Get's the term list for a given field
   * 
   * @return the term list or null if no terms for the given field exist
   */
  public List<Term> getTerms(String field) {
    return termMap.get(field);
  }
  
  public Map<String, List<Term>> getTermMap() {
    return termMap;
  }

  public static class Term {
    private String term;
    private long frequency;
    private long totalTermFreq;

    public Term(String term, long frequency) {
      this(term, frequency, 0);
    }

    public Term(String term, long frequency, long totalTermFreq) {
      this.term = term;
      this.frequency = frequency;
      this.totalTermFreq = totalTermFreq;
    }

    public String getTerm() {
      return term;
    }

    public void setTerm(String term) {
      this.term = term;
    }
    
    public long getFrequency() {
      return frequency;
    }
    
    public void setFrequency(long frequency) {
      this.frequency = frequency;
    }

    public void addFrequency(long frequency) {
      this.frequency += frequency;
    }

    public long getTotalTermFreq() {
      return totalTermFreq;
    }

    public void setTotalTermFreq(long totalTermFreq) {
      this.totalTermFreq = totalTermFreq;
    }

    public void addTotalTermFreq(long totalTermFreq) {
      this.totalTermFreq += totalTermFreq;
    }
  }
}

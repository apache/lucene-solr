package org.apache.solr.client.solrj.response;
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

import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates responses from TermsComponent
 */
public class TermsResponse {
  private Map<String, List<Term>> termMap = new HashMap<String, List<Term>>();
  
  public TermsResponse(NamedList<NamedList<Number>> termsInfo) {
    for (int i = 0; i < termsInfo.size(); i++) {
      String fieldName = termsInfo.getName(i);
      List<Term> itemList = new ArrayList<Term>();
      NamedList<Number> items = termsInfo.getVal(i);
      
      for (int j = 0; j < items.size(); j++) {
        Term t = new Term(items.getName(j), items.getVal(j).longValue());
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

    public Term(String term, long frequency) {
      this.term = term;
      this.frequency = frequency;
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
  }
}

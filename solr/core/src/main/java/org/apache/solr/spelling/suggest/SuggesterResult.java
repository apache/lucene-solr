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
package org.apache.solr.spelling.suggest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.suggest.Lookup.LookupResult;

/** 
 * Encapsulates the results returned by the suggester in {@link SolrSuggester}
 * */
public class SuggesterResult {

  public SuggesterResult() {}
  
  /** token -> lookup results mapping*/
  private Map<String, Map<String, List<LookupResult>>> suggestionsMap = 
      new HashMap<>();

  /** Add suggestion results for <code>token</code> */
  public void add(String suggesterName, String token, List<LookupResult> results) {
    Map<String, List<LookupResult>> suggesterRes = this.suggestionsMap.get(suggesterName);
    if (suggesterRes == null) {
      this.suggestionsMap.put(suggesterName, new HashMap<String, List<LookupResult>>());
    }
    List<LookupResult> res = this.suggestionsMap.get(suggesterName).get(token);
    if (res == null) {
      res = results;
      this.suggestionsMap.get(suggesterName).put(token, res);
    }
  }
  
  /** 
   * Get a list of lookup result for a given <code>token</code>
   * null can be returned, if there are no lookup results
   * for the <code>token</code>
   * */
  public List<LookupResult> getLookupResult(String suggesterName, String token) {
    return (this.suggestionsMap.containsKey(suggesterName))
        ? this.suggestionsMap.get(suggesterName).get(token)
        : new ArrayList<LookupResult>();
  }
  
  /**
   * Get the set of tokens that are present in the
   * instance
   */
  public Set<String> getTokens(String suggesterName) {
    return (this.suggestionsMap.containsKey(suggesterName))
        ? this.suggestionsMap.get(suggesterName).keySet()
        : new HashSet<String>();
  }
  
  /**
   * Get the set of suggesterNames for which this
   * instance holds results
   */
  public Set<String> getSuggesterNames() {
    return this.suggestionsMap.keySet();
  }
}

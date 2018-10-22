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
package org.apache.solr.handler.component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.spell.SuggestWord;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.spelling.SpellCheckCollation;

public class SpellCheckMergeData {
  //original token -> corresponding Suggestion object (keep track of start,end)
  public Map<String, SpellCheckResponse.Suggestion> origVsSuggestion = new HashMap<>();
  // original token string -> summed up frequency
  public Map<String, Integer> origVsFreq = new HashMap<>();
  // original token string -> # of shards reporting it as misspelled
  public Map<String, Integer> origVsShards = new HashMap<>();
  // original token string -> set of alternatives
  // must preserve order because collation algorithm can only work in-order
  public Map<String, HashSet<String>> origVsSuggested = new LinkedHashMap<>();
  // alternative string -> corresponding SuggestWord object
  public Map<String, SuggestWord> suggestedVsWord = new HashMap<>();
  public Map<String, SpellCheckCollation> collations = new HashMap<>();
  //The original terms from the user's query.
  public Set<String> originalTerms = null;
  public int totalNumberShardResponses = 0;
  
  public boolean isOriginalToQuery(String term) {
    if(originalTerms==null) {
      return true;
    }
    return originalTerms.contains(term);
  }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.SolrSpellChecker;
import org.apache.solr.spelling.SpellingOptions;
import org.apache.solr.spelling.SpellingResult;
import org.apache.solr.spelling.Token;
/**
 * A Dummy SpellChecker for testing purposes
 *
 **/
public class DummyCustomParamSpellChecker extends SolrSpellChecker {

  @Override
  public void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException {

  }

  @Override
  public void build(SolrCore core, SolrIndexSearcher searcher) throws IOException {

  }

  @Override
  public SpellingResult getSuggestions(SpellingOptions options) throws IOException {

    SpellingResult result = new SpellingResult();
    //just spit back out the results

    // sort the keys to make ordering predictable
    Iterator<String> iterator = options.customParams.getParameterNamesIterator();
    List<String> lst = new ArrayList<>();
    while (iterator.hasNext()) {
      lst.add(iterator.next());
    }
    Collections.sort(lst);

    int i = 0;
    for (String name : lst) {
      String value = options.customParams.get(name);
      result.add(new Token(name, i, i+1),  Collections.singletonList(value));
      i += 2;
    }    
    return result;
  }
}

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
package org.apache.solr.spelling.suggest.fst;

import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.fst.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.spelling.suggest.LookupFactory;

/**
 * Factory for {@link WFSTCompletionLookup}
 * @lucene.experimental
 */
public class WFSTLookupFactory extends LookupFactory {
  /**
   * If <code>true</code>, exact suggestions are returned first, even if they are prefixes
   * of other strings in the automaton (possibly with larger weights). 
   */
  public static final String EXACT_MATCH_FIRST = "exactMatchFirst";
  
  /**
   * File name for the automaton.
   * 
   */
  private static final String FILENAME = "wfst.bin";

  @Override
  public Lookup create(@SuppressWarnings({"rawtypes"})NamedList params, SolrCore core) {
    boolean exactMatchFirst = params.get(EXACT_MATCH_FIRST) != null
    ? Boolean.valueOf(params.get(EXACT_MATCH_FIRST).toString())
    : true;

    return new WFSTCompletionLookup(getTempDir(), "suggester", exactMatchFirst);
  }

  @Override
  public String storeFileName() {
    return FILENAME;
  }
}

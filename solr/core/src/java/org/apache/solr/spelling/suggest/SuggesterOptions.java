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

import org.apache.lucene.util.CharsRef;

/**
 * Encapsulates the inputs required to be passed on to 
 * the underlying suggester in {@link SolrSuggester}
 **/
public class SuggesterOptions {
  
  /** The token to lookup */
  CharsRef token;
  
  /** Number of suggestions requested */
  int count;

  /** A Solr or Lucene query for filtering suggestions*/
  String contextFilterQuery;

  /** Are all terms required?*/
  boolean allTermsRequired;

  /** Highlight term in results?*/
  boolean highlight;

  public SuggesterOptions(CharsRef token, int count, String contextFilterQuery, boolean allTermsRequired, boolean highlight) {
    this.token = token;
    this.count = count;
    this.contextFilterQuery = contextFilterQuery;
    this.allTermsRequired = allTermsRequired;
    this.highlight = highlight;
  }
}

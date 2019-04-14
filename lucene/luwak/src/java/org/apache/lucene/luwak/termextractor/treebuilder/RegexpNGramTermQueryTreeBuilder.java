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

package org.apache.lucene.luwak.termextractor.treebuilder;

import org.apache.lucene.luwak.termextractor.QueryAnalyzer;
import org.apache.lucene.luwak.termextractor.querytree.QueryTree;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.luwak.termextractor.QueryTerm;
import org.apache.lucene.luwak.termextractor.QueryTreeBuilder;
import org.apache.lucene.luwak.termextractor.querytree.TermNode;

/**
 * Extracts longest exact substring from a regular expression, to then be matched
 * against ngrams from an input document.
 * <p>
 * N.B. the only regex chars dealt with currently are '.*' and '?'
 */
public class RegexpNGramTermQueryTreeBuilder extends QueryTreeBuilder<RegexpQuery> {

  private final String ngramSuffix;

  private final String wildcardToken;

  /**
   * Creates a RegexpNGramTermQueryTreeBuilder
   *
   * @param ngramSuffix   a string to identify terms as ngrams
   * @param wildcardToken a string to identify terms as wildcards
   */
  public RegexpNGramTermQueryTreeBuilder(String ngramSuffix, String wildcardToken) {
    super(RegexpQuery.class);
    this.ngramSuffix = ngramSuffix;
    this.wildcardToken = wildcardToken;
  }

  @Override
  public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, RegexpQuery query) {
    String regexp = parseOutRegexp(query.toString(""));
    String selected = selectLongestSubstring(regexp);
    QueryTerm term = new QueryTerm(query.getField(), selected + ngramSuffix, QueryTerm.Type.CUSTOM, wildcardToken);
    return new TermNode(term, weightor.weigh(term));
  }

  /**
   * The only way to extract a regexp from a RegexpQuery is by parsing it's
   * toString(String) output.  Which is a bit rubbish, really...
   *
   * @param queryRepresentation the toString() representation of a RegexpQuery
   * @return the regular expression for the query
   */
  public static String parseOutRegexp(String queryRepresentation) {
    int fieldSepPos = queryRepresentation.indexOf(":");
    int firstSlash = queryRepresentation.indexOf("/", fieldSepPos);
    int lastSlash = queryRepresentation.lastIndexOf("/");
    return queryRepresentation.substring(firstSlash + 1, lastSlash);
  }

  /**
   * Given a regular expression, parse out the longest static substring from it
   *
   * @param regexp the Regexp to parse
   * @return the longest static substring
   */
  public static String selectLongestSubstring(String regexp) {
    String selected = "";
    for (String substr : regexp.split("\\.|\\*|.\\?")) {
      if (substr.length() > selected.length())
        selected = substr;
    }
    return selected;
  }
}

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
package org.apache.lucene.queryparser.flexible.standard.processors;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.util.BytesRef;

/**
 * The {@link StandardSyntaxParser} creates {@link PrefixWildcardQueryNode} nodes which have values
 * containing the prefixed wildcard. However, Lucene {@link PrefixQuery} cannot contain the prefixed
 * wildcard. So, this processor basically removed the prefixed wildcard from the {@link
 * PrefixWildcardQueryNode} value.
 *
 * @see PrefixQuery
 * @see PrefixWildcardQueryNode
 */
public class WildcardQueryNodeProcessor extends QueryNodeProcessorImpl {

  private static final Pattern WILDCARD_PATTERN = Pattern.compile("(\\.)|([?*]+)");

  // because we call utf8ToString, this will only work with the default TermToBytesRefAttribute
  private static String analyzeWildcard(Analyzer a, String field, String wildcard) {
    // best effort to not pass the wildcard characters through #normalize
    Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(wildcard);
    StringBuilder sb = new StringBuilder();
    int last = 0;

    while (wildcardMatcher.find()) {
      // continue if escaped char
      if (wildcardMatcher.group(1) != null) {
        continue;
      }

      if (wildcardMatcher.start() > 0) {
        String chunk = wildcard.substring(last, wildcardMatcher.start());
        BytesRef normalized = a.normalize(field, chunk);
        sb.append(normalized.utf8ToString());
      }
      // append the wildcard character
      sb.append(wildcardMatcher.group(2));

      last = wildcardMatcher.end();
    }
    if (last < wildcard.length()) {
      String chunk = wildcard.substring(last);
      BytesRef normalized = a.normalize(field, chunk);
      sb.append(normalized.utf8ToString());
    }
    return sb.toString();
  }

  public WildcardQueryNodeProcessor() {
    // empty constructor
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    // the old Lucene Parser ignores FuzzyQueryNode that are also PrefixWildcardQueryNode or
    // WildcardQueryNode
    // we do the same here, also ignore empty terms
    if (node instanceof FieldQueryNode || node instanceof FuzzyQueryNode) {
      FieldQueryNode fqn = (FieldQueryNode) node;
      CharSequence text = fqn.getText();

      // do not process wildcards for TermRangeQueryNode children and
      // QuotedFieldQueryNode to reproduce the old parser behavior
      if (fqn.getParent() instanceof TermRangeQueryNode
          || fqn instanceof QuotedFieldQueryNode
          || text.length() <= 0) {
        // Ignore empty terms
        return node;
      }

      // Code below simulates the old lucene parser behavior for wildcards

      if (isWildcard(text)) {
        Analyzer analyzer = getQueryConfigHandler().get(ConfigurationKeys.ANALYZER);
        if (analyzer != null) {
          text = analyzeWildcard(analyzer, fqn.getFieldAsString(), text.toString());
        }
        if (isPrefixWildcard(text)) {
          return new PrefixWildcardQueryNode(fqn.getField(), text, fqn.getBegin(), fqn.getEnd());
        } else {
          return new WildcardQueryNode(fqn.getField(), text, fqn.getBegin(), fqn.getEnd());
        }
      }
    }

    return node;
  }

  private boolean isWildcard(CharSequence text) {
    if (text == null || text.length() <= 0) return false;

    // If a un-escaped '*' or '?' if found return true
    // start at the end since it's more common to put wildcards at the end
    for (int i = text.length() - 1; i >= 0; i--) {
      if ((text.charAt(i) == '*' || text.charAt(i) == '?')
          && !UnescapedCharSequence.wasEscaped(text, i)) {
        return true;
      }
    }

    return false;
  }

  private boolean isPrefixWildcard(CharSequence text) {
    if (text == null || text.length() <= 0 || !isWildcard(text)) return false;

    // Validate last character is a '*' and was not escaped
    // If single '*' is is a wildcard not prefix to simulate old queryparser
    if (text.charAt(text.length() - 1) != '*') return false;
    if (UnescapedCharSequence.wasEscaped(text, text.length() - 1)) return false;
    if (text.length() == 1) return false;

    // Only make a prefix if there is only one single star at the end and no '?' or '*' characters
    // If single wildcard return false to mimic old queryparser
    for (int i = 0; i < text.length(); i++) {
      if (text.charAt(i) == '?') return false;
      if (text.charAt(i) == '*' && !UnescapedCharSequence.wasEscaped(text, i)) {
        if (i == text.length() - 1) return true;
        else return false;
      }
    }

    return false;
  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {

    return children;
  }
}

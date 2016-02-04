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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.apache.lucene.search.PrefixQuery;

/**
 * The {@link StandardSyntaxParser} creates {@link PrefixWildcardQueryNode} nodes which
 * have values containing the prefixed wildcard. However, Lucene
 * {@link PrefixQuery} cannot contain the prefixed wildcard. So, this processor
 * basically removed the prefixed wildcard from the
 * {@link PrefixWildcardQueryNode} value.
 * 
 * @see PrefixQuery
 * @see PrefixWildcardQueryNode
 */
public class WildcardQueryNodeProcessor extends QueryNodeProcessorImpl {

  public WildcardQueryNodeProcessor() {
    // empty constructor
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    // the old Lucene Parser ignores FuzzyQueryNode that are also PrefixWildcardQueryNode or WildcardQueryNode
    // we do the same here, also ignore empty terms
    if (node instanceof FieldQueryNode || node instanceof FuzzyQueryNode) {      
      FieldQueryNode fqn = (FieldQueryNode) node;      
      CharSequence text = fqn.getText(); 
      
      // do not process wildcards for TermRangeQueryNode children and 
      // QuotedFieldQueryNode to reproduce the old parser behavior
      if (fqn.getParent() instanceof TermRangeQueryNode 
          || fqn instanceof QuotedFieldQueryNode 
          || text.length() <= 0){
        // Ignore empty terms
        return node;
      }
      
      // Code below simulates the old lucene parser behavior for wildcards
      
      if (isPrefixWildcard(text)) {        
        PrefixWildcardQueryNode prefixWildcardQN = new PrefixWildcardQueryNode(fqn);
        return prefixWildcardQN;
        
      } else if (isWildcard(text)){
        WildcardQueryNode wildcardQN = new WildcardQueryNode(fqn);
        return wildcardQN;
      }
             
    }

    return node;

  }

  private boolean isWildcard(CharSequence text) {
    if (text ==null || text.length() <= 0) return false;
    
    // If a un-escaped '*' or '?' if found return true
    // start at the end since it's more common to put wildcards at the end
    for(int i=text.length()-1; i>=0; i--){
      if ((text.charAt(i) == '*' || text.charAt(i) == '?') && !UnescapedCharSequence.wasEscaped(text, i)){
        return true;
      }
    }
    
    return false;
  }

  private boolean isPrefixWildcard(CharSequence text) {
    if (text == null || text.length() <= 0 || !isWildcard(text)) return false;
    
    // Validate last character is a '*' and was not escaped
    // If single '*' is is a wildcard not prefix to simulate old queryparser
    if (text.charAt(text.length()-1) != '*') return false;
    if (UnescapedCharSequence.wasEscaped(text, text.length()-1)) return false;
    if (text.length() == 1) return false;
      
    // Only make a prefix if there is only one single star at the end and no '?' or '*' characters
    // If single wildcard return false to mimic old queryparser
    for(int i=0; i<text.length(); i++){
      if (text.charAt(i) == '?') return false;
      if (text.charAt(i) == '*' && !UnescapedCharSequence.wasEscaped(text, i)){        
        if (i == text.length()-1) 
          return true;
        else 
          return false;
      }
    }
    
    return false;
  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    return node;

  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {

    return children;

  }

}

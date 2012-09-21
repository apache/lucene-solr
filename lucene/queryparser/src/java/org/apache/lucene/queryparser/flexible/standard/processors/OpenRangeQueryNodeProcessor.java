package org.apache.lucene.queryparser.flexible.standard.processors;

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

import java.util.List;

import org.apache.lucene.search.TermRangeQuery; // javadocs
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

/**
 * Processes {@link TermRangeQuery}s with open ranges.
 */
public class OpenRangeQueryNodeProcessor extends QueryNodeProcessorImpl {
  
  final public static String OPEN_RANGE_TOKEN = "*";
  
  public OpenRangeQueryNodeProcessor() {}
  
  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    
    if (node instanceof TermRangeQueryNode) {
      TermRangeQueryNode rangeNode = (TermRangeQueryNode) node;
      FieldQueryNode lowerNode = rangeNode.getLowerBound();
      FieldQueryNode upperNode = rangeNode.getUpperBound();
      CharSequence lowerText = lowerNode.getText();
      CharSequence upperText = upperNode.getText();
      
      if (OPEN_RANGE_TOKEN.equals(upperNode.getTextAsString())
          && (!(upperText instanceof UnescapedCharSequence) || !((UnescapedCharSequence) upperText)
              .wasEscaped(0))) {
        upperText = "";
      }
      
      if (OPEN_RANGE_TOKEN.equals(lowerNode.getTextAsString())
          && (!(lowerText instanceof UnescapedCharSequence) || !((UnescapedCharSequence) lowerText)
              .wasEscaped(0))) {
        lowerText = "";
      }
      
      lowerNode.setText(lowerText);
      upperNode.setText(upperText);
    }
    
    return node;
    
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

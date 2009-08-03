package org.apache.lucene.queryParser.original.processors;

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

import java.util.List;

import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryParser.original.parser.OriginalSyntaxParser;
import org.apache.lucene.search.PrefixQuery;

/**
 * The {@link OriginalSyntaxParser} creates {@link PrefixWildcardQueryNode} nodes which
 * have values containing the prefixed wildcard. However, Lucene
 * {@link PrefixQuery} cannot contain the prefixed wildcard. So, this processor
 * basically removed the prefixed wildcard from the
 * {@link PrefixWildcardQueryNode} value. <br/>
 * 
 * @see PrefixQuery
 * @see PrefixWildcardQueryNode
 */
public class PrefixWildcardQueryNodeProcessor extends QueryNodeProcessorImpl {

  public PrefixWildcardQueryNodeProcessor() {
    // empty constructor
  }

  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof PrefixWildcardQueryNode) {
      PrefixWildcardQueryNode prefixWildcardNode = (PrefixWildcardQueryNode) node;
      CharSequence text = prefixWildcardNode.getText();

      prefixWildcardNode.setText(text.subSequence(0, text.length() - 1));

    }

    return node;

  }

  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    return node;

  }

  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {

    return children;

  }

}

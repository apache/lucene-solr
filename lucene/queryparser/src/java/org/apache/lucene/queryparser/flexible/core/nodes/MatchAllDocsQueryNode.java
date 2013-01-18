package org.apache.lucene.queryparser.flexible.core.nodes;

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

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/**
 * A {@link MatchAllDocsQueryNode} indicates that a query node tree or subtree
 * will match all documents if executed in the index.
 */
public class MatchAllDocsQueryNode extends QueryNodeImpl {

  public MatchAllDocsQueryNode() {
    // empty constructor
  }

  @Override
  public String toString() {
    return "<matchAllDocs field='*' term='*'/>";
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    return "*:*";
  }

  @Override
  public MatchAllDocsQueryNode cloneTree() throws CloneNotSupportedException {
    MatchAllDocsQueryNode clone = (MatchAllDocsQueryNode) super.cloneTree();

    // nothing to clone

    return clone;
  }
}

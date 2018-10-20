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
package org.apache.lucene.queryparser.flexible.spans;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.search.spans.SpanQuery;

/**
 * Sets up a query tree builder to build a span query tree from a query node
 * tree.<br>
 * <br>
 * 
 * The defined map is:<br>
 * - every BooleanQueryNode instance is delegated to the SpanOrQueryNodeBuilder<br>
 * - every FieldQueryNode instance is delegated to the SpanTermQueryNodeBuilder <br>
 * 
 */
public class SpansQueryTreeBuilder extends QueryTreeBuilder implements
    StandardQueryBuilder {

  public SpansQueryTreeBuilder() {
    setBuilder(BooleanQueryNode.class, new SpanOrQueryNodeBuilder());
    setBuilder(FieldQueryNode.class, new SpanTermQueryNodeBuilder());

  }

  @Override
  public SpanQuery build(QueryNode queryTree) throws QueryNodeException {
    return (SpanQuery) super.build(queryTree);
  }

}

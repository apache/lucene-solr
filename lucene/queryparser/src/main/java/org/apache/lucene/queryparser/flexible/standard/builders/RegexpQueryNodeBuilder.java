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
package org.apache.lucene.queryparser.flexible.standard.builders;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.processors.MultiTermRewriteMethodProcessor;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.RegexpQuery;

/**
 * Builds a {@link RegexpQuery} object from a {@link RegexpQueryNode} object.
 */
public class RegexpQueryNodeBuilder implements StandardQueryBuilder {

  public RegexpQueryNodeBuilder() {
    // empty constructor
  }

  @Override
  public RegexpQuery build(QueryNode queryNode) throws QueryNodeException {
    RegexpQueryNode regexpNode = (RegexpQueryNode) queryNode;

    // TODO: make the maxStates configurable w/ a reasonable default (QueryParserBase uses 10000)
    RegexpQuery q = new RegexpQuery(new Term(regexpNode.getFieldAsString(),
        regexpNode.textToBytesRef()));

    MultiTermQuery.RewriteMethod method = (MultiTermQuery.RewriteMethod) queryNode
        .getTag(MultiTermRewriteMethodProcessor.TAG_ID);
    if (method != null) {
      q.setRewriteMethod(method);
    }

    return q;
  }

}

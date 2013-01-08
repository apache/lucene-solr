package org.apache.lucene.queryparser.flexible.standard.builders;

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

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.processors.MultiTermRewriteMethodProcessor;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;

/**
 * Builds a {@link PrefixQuery} object from a {@link PrefixWildcardQueryNode}
 * object.
 */
public class PrefixWildcardQueryNodeBuilder implements StandardQueryBuilder {

  public PrefixWildcardQueryNodeBuilder() {
    // empty constructor
  }

  @Override
  public PrefixQuery build(QueryNode queryNode) throws QueryNodeException {    

    PrefixWildcardQueryNode wildcardNode = (PrefixWildcardQueryNode) queryNode;

    String text = wildcardNode.getText().subSequence(0, wildcardNode.getText().length() - 1).toString();
    PrefixQuery q = new PrefixQuery(new Term(wildcardNode.getFieldAsString(), text));
    
    MultiTermQuery.RewriteMethod method = (MultiTermQuery.RewriteMethod)queryNode.getTag(MultiTermRewriteMethodProcessor.TAG_ID);
    if (method != null) {
      q.setRewriteMethod(method);
    }
    
    return q;
  }

}

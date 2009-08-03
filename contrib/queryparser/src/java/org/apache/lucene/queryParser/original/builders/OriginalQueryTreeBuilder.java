package org.apache.lucene.queryParser.original.builders;

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

import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryParser.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryParser.core.nodes.BoostQueryNode;
import org.apache.lucene.queryParser.core.nodes.FieldQueryNode;
import org.apache.lucene.queryParser.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryParser.core.nodes.GroupQueryNode;
import org.apache.lucene.queryParser.core.nodes.MatchAllDocsQueryNode;
import org.apache.lucene.queryParser.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryParser.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryParser.core.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.nodes.SlopQueryNode;
import org.apache.lucene.queryParser.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryParser.core.nodes.WildcardQueryNode;
import org.apache.lucene.queryParser.original.nodes.OriginalBooleanQueryNode;
import org.apache.lucene.queryParser.original.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryParser.original.nodes.RangeQueryNode;
import org.apache.lucene.queryParser.original.processors.OriginalQueryNodeProcessorPipeline;
import org.apache.lucene.search.Query;

/**
 * This query tree builder only defines the necessary map to build a
 * {@link Query} tree object. It should be used to generate a {@link Query} tree
 * object from a query node tree processed by a
 * {@link OriginalQueryNodeProcessorPipeline}. <br/>
 * 
 * @see QueryTreeBuilder
 * @see OriginalQueryNodeProcessorPipeline
 */
public class OriginalQueryTreeBuilder extends QueryTreeBuilder implements
    OriginalQueryBuilder {

  public OriginalQueryTreeBuilder() {
    setBuilder(GroupQueryNode.class, new GroupQueryNodeBuilder());
    setBuilder(FieldQueryNode.class, new FieldQueryNodeBuilder());
    setBuilder(BooleanQueryNode.class, new BooleanQueryNodeBuilder());
    setBuilder(FuzzyQueryNode.class, new FuzzyQueryNodeBuilder());
    setBuilder(BoostQueryNode.class, new BoostQueryNodeBuilder());
    setBuilder(ModifierQueryNode.class, new ModifierQueryNodeBuilder());
    setBuilder(WildcardQueryNode.class, new WildcardQueryNodeBuilder());
    setBuilder(TokenizedPhraseQueryNode.class, new PhraseQueryNodeBuilder());
    setBuilder(MatchNoDocsQueryNode.class, new MatchNoDocsQueryNodeBuilder());
    setBuilder(PrefixWildcardQueryNode.class,
        new PrefixWildcardQueryNodeBuilder());
    setBuilder(RangeQueryNode.class, new RangeQueryNodeBuilder());
    setBuilder(SlopQueryNode.class, new SlopQueryNodeBuilder());
    setBuilder(OriginalBooleanQueryNode.class,
        new OriginalBooleanQueryNodeBuilder());
    setBuilder(MultiPhraseQueryNode.class, new MultiPhraseQueryNodeBuilder());
    setBuilder(MatchAllDocsQueryNode.class, new MatchAllDocsQueryNodeBuilder());

  }

  public Query build(QueryNode queryNode) throws QueryNodeException {
    return (Query) super.build(queryNode);
  }

}

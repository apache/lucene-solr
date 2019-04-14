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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.termextractor.QueryAnalyzer;
import org.apache.lucene.luwak.termextractor.querytree.DisjunctionNode;
import org.apache.lucene.luwak.termextractor.querytree.QueryTree;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.luwak.termextractor.QueryTreeBuilder;
import org.apache.lucene.luwak.termextractor.querytree.TermNode;

public class TermInSetQueryTreeBuilder extends QueryTreeBuilder<TermInSetQuery> {

  public static final TermInSetQueryTreeBuilder INSTANCE = new TermInSetQueryTreeBuilder();

  private TermInSetQueryTreeBuilder() {
    super(TermInSetQuery.class);
  }

  @Override
  public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, TermInSetQuery query) {
    PrefixCodedTerms.TermIterator it = query.getTermData().iterator();
    List<QueryTree> terms = new ArrayList<>();
    BytesRef term;
    while ((term = it.next()) != null) {
      terms.add(new TermNode(new Term(it.field(), term), weightor));
    }
    return DisjunctionNode.build(terms);
  }
}

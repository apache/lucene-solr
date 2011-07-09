package org.apache.lucene.queryParser.standard.nodes;

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

import org.apache.lucene.queryParser.core.nodes.FieldQueryNode;

/**
 * This query node represents a range query.
 * 
 * @see org.apache.lucene.queryParser.standard.processors.ParametricRangeQueryNodeProcessor
 * @see org.apache.lucene.search.TermRangeQuery
 */
public class TermRangeQueryNode extends AbstractRangeQueryNode<FieldQueryNode> {
  
  public TermRangeQueryNode(FieldQueryNode lower, FieldQueryNode upper,
      boolean lowerInclusive, boolean upperInclusive) {
    setBounds(lower, upper, lowerInclusive, upperInclusive);
  }
  
}

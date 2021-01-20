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

/**
 * Query nodes commonly used by query parser implementations.
 *
 * <h2>Query Nodes</h2>
 *
 * <p>The package <code>org.apache.lucene.queryParser.nodes</code> contains all the basic query
 * nodes. The interface that represents a query node is {@link
 * org.apache.lucene.queryparser.flexible.core.nodes.QueryNode}.
 *
 * <p>{@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode}s are used by the text
 * parser to create a syntax tree. These nodes are designed to be used by UI or other text parsers.
 * The default Lucene text parser is {@link
 * org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser}, it implements
 * Lucene's standard syntax.
 *
 * <p>{@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} interface should be
 * implemented by all query nodes, the class {@link
 * org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl} implements {@link
 * org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} and is extended by all current query
 * node implementations.
 *
 * <p>A query node tree can be printed to the a stream, and it generates a pseudo XML representation
 * with all the nodes.
 *
 * <p>A query node tree can also generate a query string that can be parsed back by the original
 * text parser, at this point only the standard lucene syntax is supported.
 *
 * <p>Grouping nodes:
 *
 * <ul>
 *   <li>AndQueryNode - used for AND operator
 *   <li>AnyQueryNode - used for ANY operator
 *   <li>OrQueryNode - used for OR operator
 *   <li>BooleanQueryNode - used when no operator is specified
 *   <li>ModifierQueryNode - used for modifier operator
 *   <li>GroupQueryNode - used for parenthesis
 *   <li>BoostQueryNode - used for boost operator
 *   <li>SlopQueryNode - phrase slop
 *   <li>FuzzyQueryNode - fuzzy node
 *   <li>TermRangeQueryNode - used for parametric field:[low_value TO high_value]
 *   <li>ProximityQueryNode - used for proximity search
 *   <li>LegacyNumericRangeQueryNode - used for numeric range search
 *   <li>TokenizedPhraseQueryNode - used by tokenizers/lemmatizers/analyzers for phrases/autophrases
 * </ul>
 *
 * <p>Leaf Nodes:
 *
 * <ul>
 *   <li>FieldQueryNode - field/value node
 *   <li>LegacyNumericQueryNode - used for numeric search
 *   <li>PathQueryNode - {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} object
 *       used with path-like queries
 *   <li>OpaqueQueryNode - Used as for part of the query that can be parsed by other parsers.
 *       schema/value
 *   <li>PrefixWildcardQueryNode - non-phrase wildcard query
 *   <li>QuotedFieldQUeryNode - regular phrase node
 *   <li>WildcardQueryNode - non-phrase wildcard query
 * </ul>
 *
 * <p>Utility Nodes:
 *
 * <ul>
 *   <li>DeletedQueryNode - used by processors on optimizations
 *   <li>MatchAllDocsQueryNode - used by processors on optimizations
 *   <li>MatchNoDocsQueryNode - used by processors on optimizations
 *   <li>NoTokenFoundQueryNode - used by tokenizers/lemmatizers/analyzers
 * </ul>
 */
package org.apache.lucene.queryparser.flexible.core.nodes;

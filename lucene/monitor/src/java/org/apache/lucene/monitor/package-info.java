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
 *
 *
 * <h2>Monitoring framework</h2>
 *
 * This package contains classes to allow the monitoring of a stream of documents with a set of
 * queries.
 *
 * <p>To use, instantiate a {@link org.apache.lucene.monitor.Monitor} object, register queries with
 * it via {@link
 * org.apache.lucene.monitor.Monitor#register(org.apache.lucene.monitor.MonitorQuery...)}, and then
 * match documents against it either individually via {@link
 * org.apache.lucene.monitor.Monitor#match(org.apache.lucene.document.Document,
 * org.apache.lucene.monitor.MatcherFactory)} or in batches via {@link
 * org.apache.lucene.monitor.Monitor#match(org.apache.lucene.document.Document[],
 * org.apache.lucene.monitor.MatcherFactory)}
 *
 * <h3>Matcher types</h3>
 *
 * A number of matcher types are included:
 *
 * <ul>
 *   <li>{@link org.apache.lucene.monitor.QueryMatch#SIMPLE_MATCHER} &mdash; just returns the set of
 *       query ids that a Document has matched
 *   <li>{@link
 *       org.apache.lucene.monitor.ScoringMatch#matchWithSimilarity(org.apache.lucene.search.similarities.Similarity)}
 *       &mdash; returns the set of matching queries, with the score that each one records against a
 *       Document
 *   <li>{@link org.apache.lucene.monitor.ExplainingMatch#MATCHER &mdash; similar to ScoringMatch,
 *       but include the full Explanation}
 *   <li>{@link org.apache.lucene.monitor.HighlightsMatch#MATCHER &mdash; return the matching
 *       queries along with the matching terms for each query}
 * </ul>
 *
 * Matchers can be wrapped in {@link org.apache.lucene.monitor.PartitionMatcher} or {@link
 * org.apache.lucene.monitor.ParallelMatcher} to increase performance in low-concurrency systems.
 *
 * <h3>Pre-filtering of queries</h3>
 *
 * Monitoring is done efficiently by extracting minimal sets of terms from queries, and using these
 * to build a query index. When a document is passed to {@link
 * org.apache.lucene.monitor.Monitor#match(org.apache.lucene.document.Document,
 * org.apache.lucene.monitor.MatcherFactory)}, it is converted into a small index, and the terms
 * dictionary from that index is then used to build a disjunction query to run against the query
 * index. Queries that match this disjunction are then run against the document. In this way, the
 * Monitor can avoid running queries that have no chance of matching. The process of extracting
 * terms and building document disjunctions is handled by a {@link
 * org.apache.lucene.monitor.Presearcher}
 *
 * <p>In addition, extra per-field filtering can be specified by passing a set of keyword fields to
 * filter on. When queries are registered with the monitor, field-value pairs can be added as
 * optional metadata for each query, and these can then be used to restrict which queries a document
 * is checked against. For example, you can specify a language that each query should apply to, and
 * documents containing a value in their language field would only be checked against queries that
 * have that same value in their language metadata. Note that when matching documents in batches,
 * all documents in the batch must have the same values in their filter fields.
 *
 * <p>Query analysis uses the {@link org.apache.lucene.search.QueryVisitor} API to extract terms,
 * which will work for all basic term-based queries shipped with Lucene. The analyzer builds a
 * representation of the query called a {@link org.apache.lucene.monitor.QueryTree}, and then
 * selects a minimal set of terms, one of which must be present in a document for that document to
 * match. Individual terms are weighted using a {@link org.apache.lucene.monitor.TermWeightor},
 * which allows some selectivity when building the term set. For example, given a conjunction of
 * terms (a boolean query with several MUST clauses, or a phrase, span or interval query), we need
 * only extract one term. The TermWeightor can be configured in a number of ways; by default it will
 * weight longer terms more highly.
 *
 * <p>For query sets that contain many conjunctions, it can be useful to extract and index different
 * minimal term combinations. For example, a phrase query on 'the quick brown fox' could index both
 * 'quick' and 'brown', and avoid being run against documents that contain only one of these terms.
 * The {@link org.apache.lucene.monitor.MultipassTermFilteredPresearcher} allows this sort of
 * indexing, taking a minimum term weight so that very common terms such as 'the' can be avoided.
 *
 * <p>Custom Query implementations that are based on term matching, and that implement {@link
 * org.apache.lucene.search.Query#visit(org.apache.lucene.search.QueryVisitor)} will work with no
 * extra configuration; for more complicated custom queries, you can register a {@link
 * org.apache.lucene.monitor.CustomQueryHandler} with the presearcher. Included in this package is a
 * {@link org.apache.lucene.monitor.RegexpQueryHandler}, which gives an example of a different
 * method of indexing automaton-based queries by extracting fixed substrings from a regular
 * expression, and then using ngram filtering to build the document disjunction.
 *
 * <h3>Persistent query sets</h3>
 *
 * By default, {@link org.apache.lucene.monitor.Monitor} instances are ephemeral, storing their
 * query indexes in memory. To make a persistent monitor, build a {@link
 * org.apache.lucene.monitor.MonitorConfiguration} object and call {@link
 * org.apache.lucene.monitor.MonitorConfiguration#setIndexPath(java.nio.file.Path,
 * org.apache.lucene.monitor.MonitorQuerySerializer)} to tell the Monitor to store its query index
 * on disk. All queries registered with this Monitor will need to have a string representation that
 * is also stored, and can be re-parsed by the associated {@link
 * org.apache.lucene.monitor.MonitorQuerySerializer} when the index is loaded by a new Monitor
 * instance.
 */
package org.apache.lucene.monitor;

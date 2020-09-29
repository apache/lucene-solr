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
 * Parser designed for "advanced" users who are less interested in fine tuning relevancy, and more interested in finding
 * specific documents via complex queries. The intended audience is generally expected to be professionals using the
 * query interface on a daily basis, and therefore willing to learn a query syntax to improve their job performance.
 * This parser is also designed to be relatively safe for user input and as such does not allow LocalParams and
 * is typically configured to require at least 3 characters prefixes for prefix queries. It is derived from the
 * standard SolrQuery Parser but provides access to span query type functionality and relies solely on prefix
 * operators. It also contains an explicit "SHOULD" operator so that users are always able to escape the effects
 * of the default operator. All occurrence operators are also distributive across parenthesis though this applies
 * to the default operator too so one must take care with excess parenthesis. This parser is also designed to
 * give the token filters access to as much punctuation as possible to facilitate the use of
 * {@link org.apache.lucene.analysis.pattern.PatternTypingFilter}. Split on whitespace is always on, turning it off would make
 * the Paterns in the PatternTypingFilter more complicated and more expensive, so this has not been provided at this
 * time. Future work might reconsider this since it makes multi-word synonyms subject to "sausageization".
 *
 * <p>This package contains the following classes generated from QueryParser.jj by JavaCC:
 * <ul>
 *   <li>{@link org.apache.solr.aqp.ParseException} (*)</li>
 *   <li>{@link org.apache.solr.aqp.QueryParserTokenManager}</li>
 *   <li>{@link org.apache.solr.aqp.Token} (*)</li>
 *   <li>{@link org.apache.solr.aqp.TokenMgrError } (*)</li>
 * </ul>
 *
 * Of the above list files that are marked with {@code (*)} end with a comment like:
 * <pre>
 *   JavaCC - OriginalChecksum=e72ea0affab9dd0567059546b848e45c (do not edit this line)
 * </pre>
 * and will not be regenerated as long as that comment remains in the files, and have been edited. The comment
 * should not be removed and the files should not be regenerated. The remaining files get regenerated every time
 * the javacc-aqp task is run from the build.xml in solr-core. This also task contains several regular expressions that
 * eliminate code and unused imports that would cause precommit failures or ide warnings.
 *
 */
package org.apache.solr.aqp;



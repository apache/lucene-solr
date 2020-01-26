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
 * Pluggable term index / block terms dictionary implementations.
 * <p>
 * Structure similar to {@link org.apache.lucene.codecs.blockterms.VariableGapTermsIndexWriter}
 * with additional optimizations.
 * <p>
 *   <ul>
 *     <li>Designed to be extensible</li>
 *     <li>Reduced on-heap memory usage.</li>
 *     <li>Efficient to seek terms ({@link org.apache.lucene.search.TermQuery}, {@link org.apache.lucene.search.PhraseQuery})</li>
 *     <li>Quite efficient for {@link org.apache.lucene.search.PrefixQuery}</li>
 *     <li>Not efficient for spell-check and {@link org.apache.lucene.search.FuzzyQuery}, in this case prefer
 * {@link org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat}</li>
 *   </ul>
 */
package org.apache.lucene.codecs.uniformsplit;

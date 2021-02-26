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
 * Normalization of text before the tokenizer.
 *
 * <p>CharFilters are chainable filters that normalize text before tokenization and provide mappings
 * between normalized text offsets and the corresponding offset in the original text.
 *
 * <H2>CharFilter offset mappings</H2>
 *
 * <p>CharFilters modify an input stream via a series of substring replacements (including deletions
 * and insertions) to produce an output stream. There are three possible replacement cases: the
 * replacement string has the same length as the original substring; the replacement is shorter; and
 * the replacement is longer. In the latter two cases (when the replacement has a different length
 * than the original), one or more offset correction mappings are required.
 *
 * <p>When the replacement is shorter than the original (e.g. when the replacement is the empty
 * string), a single offset correction mapping should be added at the replacement's end offset in
 * the output stream. The <code>cumulativeDiff</code> parameter to the <code>addOffCorrectMapping()
 * </code> method will be the sum of all previous replacement offset adjustments, with the addition
 * of the difference between the lengths of the original substring and the replacement string (a
 * positive value).
 *
 * <p>When the replacement is longer than the original (e.g. when the original is the empty string),
 * you should add as many offset correction mappings as the difference between the lengths of the
 * replacement string and the original substring, starting at the end offset the original substring
 * would have had in the output stream. The <code>cumulativeDiff</code> parameter to the <code>
 * addOffCorrectMapping()</code> method will be the sum of all previous replacement offset
 * adjustments, with the addition of the difference between the lengths of the original substring
 * and the replacement string so far (a negative value).
 */
package org.apache.lucene.analysis.charfilter;

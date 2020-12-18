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
 * Fast, general-purpose grammar-based tokenizer {@link
 * org.apache.lucene.analysis.standard.StandardTokenizer} implements the Word Break rules from the
 * Unicode Text Segmentation algorithm, as specified in <a
 * href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a>. Unlike <code>
 * UAX29URLEmailTokenizer</code> from the analysis module, URLs and email addresses are <b>not</b>
 * tokenized as single tokens, but are instead split up into tokens according to the UAX#29 word
 * break rules. <br>
 * {@link org.apache.lucene.analysis.standard.StandardAnalyzer StandardAnalyzer} includes {@link
 * org.apache.lucene.analysis.standard.StandardTokenizer StandardTokenizer}, {@link
 * org.apache.lucene.analysis.LowerCaseFilter LowerCaseFilter} and {@link
 * org.apache.lucene.analysis.StopFilter StopFilter}.
 */
package org.apache.lucene.analysis.standard;

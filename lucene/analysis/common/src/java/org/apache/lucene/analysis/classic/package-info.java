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
 * Fast, general-purpose grammar-based tokenizers. {@link
 * org.apache.lucene.analysis.classic.ClassicTokenizer ClassicTokenizer}: this class was formerly
 * (prior to Lucene 3.1) named <code>StandardTokenizer</code>. (Its tokenization rules are not based
 * on the Unicode Text Segmentation algorithm.) {@link
 * org.apache.lucene.analysis.classic.ClassicAnalyzer ClassicAnalyzer} includes {@link
 * org.apache.lucene.analysis.classic.ClassicTokenizer ClassicTokenizer}, {@link
 * org.apache.lucene.analysis.LowerCaseFilter LowerCaseFilter} and {@link
 * org.apache.lucene.analysis.StopFilter StopFilter}.
 */
package org.apache.lucene.analysis.classic;

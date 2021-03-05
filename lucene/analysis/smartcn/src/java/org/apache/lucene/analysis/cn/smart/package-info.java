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
 * Analyzer for Simplified Chinese, which indexes words.
 *
 * @lucene.experimental <div> Three analyzers are provided for Chinese, each of which treats Chinese
 *     text in a different way.
 *     <ul>
 *       <li>StandardAnalyzer: Index unigrams (individual Chinese characters) as a token.
 *       <li>CJKAnalyzer (in the analyzers/cjk package): Index bigrams (overlapping groups of two
 *           adjacent Chinese characters) as tokens.
 *       <li>SmartChineseAnalyzer (in this package): Index words (attempt to segment Chinese text
 *           into words) as tokens.
 *     </ul>
 *     Example phrase： "我是中国人"
 *     <ol>
 *       <li>StandardAnalyzer: 我－是－中－国－人
 *       <li>CJKAnalyzer: 我是－是中－中国－国人
 *       <li>SmartChineseAnalyzer: 我－是－中国－人
 *     </ol>
 *     </div>
 */
package org.apache.lucene.analysis.cn.smart;

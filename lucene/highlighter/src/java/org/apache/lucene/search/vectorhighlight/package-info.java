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
 * Another highlighter implementation based on term vectors.
 *
 * <h2>Features</h2>
 *
 * <ul>
 *   <li>fast for large docs
 *   <li>support N-gram fields
 *   <li>support phrase-unit highlighting with slops
 *   <li>support multi-term (includes wildcard, range, regexp, etc) queries
 *   <li>highlight fields need to be stored with Positions and Offsets
 *   <li>take into account query boost and/or IDF-weight to score fragments
 *   <li>support colored highlight tags
 *   <li>pluggable FragListBuilder / FieldFragList
 *   <li>pluggable FragmentsBuilder
 * </ul>
 *
 * <h2>Algorithm</h2>
 *
 * <p>To explain the algorithm, let's use the following sample text (to be highlighted) and user
 * query:
 *
 * <table style="border: 1px solid">
 * <caption>sample document and query</caption>
 * <tr>
 * <td><b>Sample Text</b></td>
 * <td>Lucene is a search engine library.</td>
 * </tr>
 * <tr>
 * <td><b>User Query</b></td>
 * <td>Lucene^2 OR "search library"~1</td>
 * </tr>
 * </table>
 *
 * <p>The user query is a BooleanQuery that consists of TermQuery("Lucene") with boost of 2 and
 * PhraseQuery("search library") with slop of 1.
 *
 * <p>For your convenience, here is the offsets and positions info of the sample text.
 *
 * <pre>
 * +--------+-----------------------------------+
 * |        |          1111111111222222222233333|
 * |  offset|01234567890123456789012345678901234|
 * +--------+-----------------------------------+
 * |document|Lucene is a search engine library. |
 * +--------*-----------------------------------+
 * |position|0      1  2 3      4      5        |
 * +--------*-----------------------------------+
 * </pre>
 *
 * <h3>Step 1.</h3>
 *
 * <p>In Step 1, Fast Vector Highlighter generates {@link
 * org.apache.lucene.search.vectorhighlight.FieldQuery.QueryPhraseMap} from the user query. <code>
 * QueryPhraseMap</code> consists of the following members:
 *
 * <pre class="prettyprint">
 * public class QueryPhraseMap {
 *   boolean terminal;
 *   int slop;   // valid if terminal == true and phraseHighlight == true
 *   float boost;  // valid if terminal == true
 *   Map&lt;String, QueryPhraseMap&gt; subMap;
 * }
 * </pre>
 *
 * <p><code>QueryPhraseMap</code> has subMap. The key of the subMap is a term text in the user query
 * and the value is a subsequent <code>QueryPhraseMap</code>. If the query is a term (not phrase),
 * then the subsequent <code>QueryPhraseMap</code> is marked as terminal. If the query is a phrase,
 * then the subsequent <code>QueryPhraseMap</code> is not a terminal and it has the next term text
 * in the phrase.
 *
 * <p>From the sample user query, the following <code>QueryPhraseMap</code> will be generated:
 *
 * <pre>
 * QueryPhraseMap
 * +--------+-+  +-------+-+
 * |"Lucene"|o+-&gt;|boost=2|*|  * : terminal
 * +--------+-+  +-------+-+
 *
 * +--------+-+  +---------+-+  +-------+------+-+
 * |"search"|o+-&gt;|"library"|o+-&gt;|boost=1|slop=1|*|
 * +--------+-+  +---------+-+  +-------+------+-+
 * </pre>
 *
 * <h3>Step 2.</h3>
 *
 * <p>In Step 2, Fast Vector Highlighter generates {@link
 * org.apache.lucene.search.vectorhighlight.FieldTermStack}. Fast Vector Highlighter uses term
 * vector data (must be stored {@link
 * org.apache.lucene.document.FieldType#setStoreTermVectorOffsets(boolean)} and {@link
 * org.apache.lucene.document.FieldType#setStoreTermVectorPositions(boolean)}) to generate it.
 * <code>FieldTermStack</code> keeps the terms in the user query. Therefore, in this sample case,
 * Fast Vector Highlighter generates the following <code>FieldTermStack</code>:
 *
 * <pre>
 * FieldTermStack
 * +------------------+
 * |"Lucene"(0,6,0)   |
 * +------------------+
 * |"search"(12,18,3) |
 * +------------------+
 * |"library"(26,33,5)|
 * +------------------+
 * where : "termText"(startOffset,endOffset,position)
 * </pre>
 *
 * <h3>Step 3.</h3>
 *
 * <p>In Step 3, Fast Vector Highlighter generates {@link
 * org.apache.lucene.search.vectorhighlight.FieldPhraseList} by reference to <code>QueryPhraseMap
 * </code> and <code>FieldTermStack</code>.
 *
 * <pre>
 * FieldPhraseList
 * +----------------+-----------------+---+
 * |"Lucene"        |[(0,6)]          |w=2|
 * +----------------+-----------------+---+
 * |"search library"|[(12,18),(26,33)]|w=1|
 * +----------------+-----------------+---+
 * </pre>
 *
 * <p>The type of each entry is <code>WeightedPhraseInfo</code> that consists of an array of terms
 * offsets and weight.
 *
 * <h3>Step 4.</h3>
 *
 * <p>In Step 4, Fast Vector Highlighter creates <code>FieldFragList</code> by reference to <code>
 * FieldPhraseList</code>. In this sample case, the following <code>FieldFragList</code> will be
 * generated:
 *
 * <pre>
 * FieldFragList
 * +---------------------------------+
 * |"Lucene"[(0,6)]                  |
 * |"search library"[(12,18),(26,33)]|
 * |totalBoost=3                     |
 * +---------------------------------+
 * </pre>
 *
 * <p>The calculation for each <code>FieldFragList.WeightedFragInfo.totalBoost</code> (weight)
 * depends on the implementation of <code>FieldFragList.add( ... )</code>:
 *
 * <pre class="prettyprint">
 *   public void add( int startOffset, int endOffset, List&lt;WeightedPhraseInfo&gt; phraseInfoList ) {
 *     float totalBoost = 0;
 *     List&lt;SubInfo&gt; subInfos = new ArrayList&lt;SubInfo&gt;();
 *     for( WeightedPhraseInfo phraseInfo : phraseInfoList ){
 *       subInfos.add( new SubInfo( phraseInfo.getText(), phraseInfo.getTermsOffsets(), phraseInfo.getSeqnum() ) );
 *       totalBoost += phraseInfo.getBoost();
 *     }
 *     getFragInfos().add( new WeightedFragInfo( startOffset, endOffset, subInfos, totalBoost ) );
 *   }
 *
 * </pre>
 *
 * The used implementation of <code>FieldFragList</code> is noted in <code>
 * BaseFragListBuilder.createFieldFragList( ... )</code>:
 *
 * <pre class="prettyprint">
 *   public FieldFragList createFieldFragList( FieldPhraseList fieldPhraseList, int fragCharSize ){
 *     return createFieldFragList( fieldPhraseList, new SimpleFieldFragList( fragCharSize ), fragCharSize );
 *   }
 * </pre>
 *
 * <p>Currently there are basically to approaches available:
 *
 * <ul>
 *   <li><code>SimpleFragListBuilder using SimpleFieldFragList</code>:
 *       <i>sum-of-boosts</i>-approach. The totalBoost is calculated by summarizing the query-boosts
 *       per term. Per default a term is boosted by 1.0
 *   <li><code>WeightedFragListBuilder using WeightedFieldFragList</code>:
 *       <i>sum-of-distinct-weights</i>-approach. The totalBoost is calculated by summarizing the
 *       IDF-weights of distinct terms.
 * </ul>
 *
 * <p>Comparison of the two approaches:
 *
 * <table style="border: 1px solid">
 * <caption>
 *   query = das alte testament (The Old Testament)
 * </caption>
 * <tr><th>Terms in fragment</th><th>sum-of-distinct-weights</th><th>sum-of-boosts</th></tr>
 * <tr><td>das alte testament</td><td>5.339621</td><td>3.0</td></tr>
 * <tr><td>das alte testament</td><td>5.339621</td><td>3.0</td></tr>
 * <tr><td>das testament alte</td><td>5.339621</td><td>3.0</td></tr>
 * <tr><td>das alte testament</td><td>5.339621</td><td>3.0</td></tr>
 * <tr><td>das testament</td><td>2.9455688</td><td>2.0</td></tr>
 * <tr><td>das alte</td><td>2.4759595</td><td>2.0</td></tr>
 * <tr><td>das das das das</td><td>1.5015357</td><td>4.0</td></tr>
 * <tr><td>das das das</td><td>1.3003681</td><td>3.0</td></tr>
 * <tr><td>das das</td><td>1.061746</td><td>2.0</td></tr>
 * <tr><td>alte</td><td>1.0</td><td>1.0</td></tr>
 * <tr><td>alte</td><td>1.0</td><td>1.0</td></tr>
 * <tr><td>das</td><td>0.7507678</td><td>1.0</td></tr>
 * <tr><td>das</td><td>0.7507678</td><td>1.0</td></tr>
 * <tr><td>das</td><td>0.7507678</td><td>1.0</td></tr>
 * <tr><td>das</td><td>0.7507678</td><td>1.0</td></tr>
 * <tr><td>das</td><td>0.7507678</td><td>1.0</td></tr>
 * </table>
 *
 * <h3>Step 5.</h3>
 *
 * <p>In Step 5, by using <code>FieldFragList</code> and the field stored data, Fast Vector
 * Highlighter creates highlighted snippets!
 */
package org.apache.lucene.search.vectorhighlight;

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
 * Highlighting search terms.
 * <p>
 * The highlight package contains classes to provide "keyword in context" features
 * typically used to highlight search terms in the text of results pages.
 * The Highlighter class is the central component and can be used to extract the
 * most interesting sections of a piece of text and highlight them, with the help of
 * Fragmenter, fragment Scorer, and Formatter classes.
 * 
 * <h2>Example Usage</h2>
 *
 * <pre class="prettyprint">
 * //... Above, create documents with two fields, one with term vectors (tv) and one without (notv)
 * IndexSearcher searcher = new IndexSearcher(directory);
 * QueryParser parser = new QueryParser("notv", analyzer);
 * Query query = parser.parse("million");
 * 
 *   TopDocs hits = searcher.search(query, 10);
 * 
 *   SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
 *   Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(query));
 *   for (int i = 0; i &lt; 10; i++) {
 *     int id = hits.scoreDocs[i].doc;
 *     Document doc = searcher.doc(id);
 *     String text = doc.get("notv");
 *     TokenStream tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), id, "notv", analyzer);
 *     TextFragment[] frag = highlighter.getBestTextFragments(tokenStream, text, false, 10);//highlighter.getBestFragments(tokenStream, text, 3, "...");
 *     for (int j = 0; j &lt; frag.length; j++) {
 *       if ((frag[j] != null) &amp;&amp; (frag[j].getScore() &gt; 0)) {
 *         System.out.println((frag[j].toString()));
 *       }
 *     }
 *     //Term vector
 *     text = doc.get("tv");
 *     tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), hits.scoreDocs[i].doc, "tv", analyzer);
 *     frag = highlighter.getBestTextFragments(tokenStream, text, false, 10);
 *     for (int j = 0; j &lt; frag.length; j++) {
 *       if ((frag[j] != null) &amp;&amp; (frag[j].getScore() &gt; 0)) {
 *         System.out.println((frag[j].toString()));
 *       }
 *     }
 *     System.out.println("-------------");
 *   }
 * </pre>
 * 
 * <h2>New features 06/02/2005</h2>
 * 
 * This release adds options for encoding (thanks to Nicko Cadell).
 * An "Encoder" implementation such as the new SimpleHTMLEncoder class can be passed to the highlighter to encode
 * all those non-xhtml standard characters such as &amp; into legal values. This simple class may not suffice for
 * some languages -  Commons Lang has an implementation that could be used: escapeHtml(String) in
 * http://svn.apache.org/viewcvs.cgi/jakarta/commons/proper/lang/trunk/src/java/org/apache/commons/lang/StringEscapeUtils.java?rev=137958&amp;view=markup
 * 
 * <h2>New features 22/12/2004</h2>
 * 
 * This release adds some new capabilities:
 * <ol>
 *   <li>Faster highlighting using Term vector support</li>
 *   <li>New formatting options to use color intensity to show informational value</li>
 *   <li>Options for better summarization by using term IDF scores to influence fragment selection</li>
 * </ol>
 * 
 * <p>
 * The highlighter takes a TokenStream as input. Until now these streams have typically been produced
 * using an Analyzer but the new class TokenSources provides helper methods for obtaining TokenStreams from
 * the new TermVector position support (see latest CVS version).</p>
 * 
 * <p>The new class GradientFormatter can use a scale of colors to highlight terms according to their score.
 * A subtle use of color can help emphasise the reasons for matching (useful when doing "MoreLikeThis" queries and
 * you want to see what the basis of the similarities are).</p>
 * 
 * <p>The QueryScorer class has constructors that use an IndexReader to derive the IDF (inverse document frequency)
 * for each term in order to influence the score. This is useful for helping to extracting the most significant sections
 * of a document and in supplying scores used by the new GradientFormatter to color significant words more strongly.
 * The QueryScorer.getMaxTermWeight method is useful when passed to the GradientFormatter constructor to define the top score
 * which is associated with the top color.</p>
 */
package org.apache.lucene.search.highlight;

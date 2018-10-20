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
 *  Unicode collation support.
 *  <p>
 *  <code>Collation</code> converts each token into its binary <code>CollationKey</code> 
 *  using the provided <code>Collator</code>, allowing it to be stored as an index term.
 *  </p>
 * 
 * <h2>Use Cases</h2>
 * 
 * <ul>
 *   <li>
 *     Efficient sorting of terms in languages that use non-Unicode character 
 *     orderings.  (Lucene Sort using a Locale can be very slow.) 
 *   </li>
 *   <li>
 *     Efficient range queries over fields that contain terms in languages that 
 *     use non-Unicode character orderings.  (Range queries using a Locale can be
 *     very slow.)
 *   </li>
 *   <li>
 *     Effective Locale-specific normalization (case differences, diacritics, etc.).
 *     ({@link org.apache.lucene.analysis.LowerCaseFilter} and 
 *     {@link org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter} provide these services
 *     in a generic way that doesn't take into account locale-specific needs.)
 *   </li>
 * </ul>
 * 
 * <h2>Example Usages</h2>
 * 
 * <h3>Farsi Range Queries</h3>
 * <pre class="prettyprint">
 *   // "fa" Locale is not supported by Sun JDK 1.4 or 1.5
 *   Collator collator = Collator.getInstance(new Locale("ar"));
 *   CollationKeyAnalyzer analyzer = new CollationKeyAnalyzer(collator);
 *   Path dirPath = Files.createTempDirectory("tempIndex");
 *   Directory dir = FSDirectory.open(dirPath);
 *   IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
 *   Document doc = new Document();
 *   doc.add(new TextField("content", "\u0633\u0627\u0628", Field.Store.YES));
 *   writer.addDocument(doc);
 *   writer.close();
 *   IndexReader ir = DirectoryReader.open(dir);
 *   IndexSearcher is = new IndexSearcher(ir);
 * 
 *   QueryParser aqp = new QueryParser("content", analyzer);
 *   aqp.setAnalyzeRangeTerms(true);
 *     
 *   // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
 *   // orders the U+0698 character before the U+0633 character, so the single
 *   // indexed Term above should NOT be returned by a ConstantScoreRangeQuery
 *   // with a Farsi Collator (or an Arabic one for the case when Farsi is not
 *   // supported).
 *   ScoreDoc[] result
 *     = is.search(aqp.parse("[ \u062F TO \u0698 ]"), null, 1000).scoreDocs;
 *   assertEquals("The index Term should not be included.", 0, result.length);
 * </pre>
 * 
 * <h3>Danish Sorting</h3>
 * <pre class="prettyprint">
 *   Analyzer analyzer 
 *     = new CollationKeyAnalyzer(Collator.getInstance(new Locale("da", "dk")));
 *   Path dirPath = Files.createTempDirectory("tempIndex");
 *   Directory dir = FSDirectory.open(dirPath);
 *   IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
 *   String[] tracer = new String[] { "A", "B", "C", "D", "E" };
 *   String[] data = new String[] { "HAT", "HUT", "H\u00C5T", "H\u00D8T", "HOT" };
 *   String[] sortedTracerOrder = new String[] { "A", "E", "B", "D", "C" };
 *   for (int i = 0 ; i &lt; data.length ; ++i) {
 *     Document doc = new Document();
 *     doc.add(new StoredField("tracer", tracer[i]));
 *     doc.add(new TextField("contents", data[i], Field.Store.NO));
 *     writer.addDocument(doc);
 *   }
 *   writer.close();
 *   IndexReader ir = DirectoryReader.open(dir);
 *   IndexSearcher searcher = new IndexSearcher(ir);
 *   Sort sort = new Sort();
 *   sort.setSort(new SortField("contents", SortField.STRING));
 *   Query query = new MatchAllDocsQuery();
 *   ScoreDoc[] result = searcher.search(query, null, 1000, sort).scoreDocs;
 *   for (int i = 0 ; i &lt; result.length ; ++i) {
 *     Document doc = searcher.doc(result[i].doc);
 *     assertEquals(sortedTracerOrder[i], doc.getValues("tracer")[0]);
 *   }
 * </pre>
 * 
 * <h3>Turkish Case Normalization</h3>
 * <pre class="prettyprint">
 *   Collator collator = Collator.getInstance(new Locale("tr", "TR"));
 *   collator.setStrength(Collator.PRIMARY);
 *   Analyzer analyzer = new CollationKeyAnalyzer(collator);
 *   Path dirPath = Files.createTempDirectory("tempIndex");
 *   Directory dir = FSDirectory.open(dirPath);
 *   IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer));
 *   Document doc = new Document();
 *   doc.add(new TextField("contents", "DIGY", Field.Store.NO));
 *   writer.addDocument(doc);
 *   writer.close();
 *   IndexReader ir = DirectoryReader.open(dir);
 *   IndexSearcher is = new IndexSearcher(ir);
 *   QueryParser parser = new QueryParser("contents", analyzer);
 *   Query query = parser.parse("d\u0131gy");   // U+0131: dotless i
 *   ScoreDoc[] result = is.search(query, null, 1000).scoreDocs;
 *   assertEquals("The index Term should be included.", 1, result.length);
 * </pre>
 * 
 * <h2>Caveats and Comparisons</h2>
 * <p>
 *   <strong>WARNING:</strong> Make sure you use exactly the same 
 *   <code>Collator</code> at index and query time -- <code>CollationKey</code>s
 *   are only comparable when produced by
 *   the same <code>Collator</code>.  Since {@link java.text.RuleBasedCollator}s
 *   are not independently versioned, it is unsafe to search against stored
 *   <code>CollationKey</code>s unless the following are exactly the same (best 
 *   practice is to store this information with the index and check that they
 *   remain the same at query time):
 * </p>
 * <ol>
 *   <li>JVM vendor</li>
 *   <li>JVM version, including patch version</li>
 *   <li>
 *     The language (and country and variant, if specified) of the Locale
 *     used when constructing the collator via
 *     {@link java.text.Collator#getInstance(java.util.Locale)}.
 *   </li>
 *   <li>
 *     The collation strength used - see {@link java.text.Collator#setStrength(int)}
 *   </li>
 * </ol> 
 * <p>
 *   <code>ICUCollationKeyAnalyzer</code>, available in the <a href="{@docRoot}/../analyzers-icu/overview-summary.html">icu analysis module</a>,
 *   uses ICU4J's <code>Collator</code>, which 
 *   makes its version available, thus allowing collation to be versioned
 *   independently from the JVM.  <code>ICUCollationKeyAnalyzer</code> is also 
 *   significantly faster and generates significantly shorter keys than 
 *   <code>CollationKeyAnalyzer</code>.  See
 *   <a href="http://site.icu-project.org/charts/collation-icu4j-sun"
 *     >http://site.icu-project.org/charts/collation-icu4j-sun</a> for key
 *   generation timing and key length comparisons between ICU4J and
 *   <code>java.text.Collator</code> over several languages.
 * </p>
 * <p>
 *   <code>CollationKey</code>s generated by <code>java.text.Collator</code>s are 
 *   not compatible with those those generated by ICU Collators.  Specifically, if
 *   you use <code>CollationKeyAnalyzer</code> to generate index terms, do not use
 *   <code>ICUCollationKeyAnalyzer</code> on the query side, or vice versa.
 * </p>
 */
package org.apache.lucene.collation;

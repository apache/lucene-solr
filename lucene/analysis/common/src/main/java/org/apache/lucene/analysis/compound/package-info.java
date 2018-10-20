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
 * A filter that decomposes compound words you find in many Germanic
 * languages into the word parts. This example shows what it does:
 * <table border="1" summary="example input stream">
 *  <tr>
 *   <th>Input token stream</th>
 *  </tr>
 *  <tr>
 *   <td>Rindfleisch&uuml;berwachungsgesetz Drahtschere abba</td>
 *  </tr>
 * </table>
 * <br>
 * <table border="1" summary="example output stream">
 *  <tr>
 *   <th>Output token stream</th>
 *  </tr>
 *  <tr>
 *   <td>(Rindfleisch&uuml;berwachungsgesetz,0,29)</td>
 *  </tr>
 *  <tr>
 *   <td>(Rind,0,4,posIncr=0)</td>
 *  </tr>
 *  <tr>
 *   <td>(fleisch,4,11,posIncr=0)</td>
 *  </tr>
 *  <tr>
 *   <td>(&uuml;berwachung,11,22,posIncr=0)</td>
 *  </tr>
 *  <tr>
 *   <td>(gesetz,23,29,posIncr=0)</td>
 *  </tr>
 *  <tr>
 *   <td>(Drahtschere,30,41)</td>
 *  </tr>
 *  <tr>
 *   <td>(Draht,30,35,posIncr=0)</td>
 *  </tr>
 *  <tr>
 *   <td>(schere,35,41,posIncr=0)</td>
 *  </tr>
 *  <tr>
 *   <td>(abba,42,46)</td>
 *  </tr>
 * </table>
 * 
 * The input token is always preserved and the filters do not alter the case of word parts. There are two variants of the
 * filter available:
 * <ul>
 *  <li><i>HyphenationCompoundWordTokenFilter</i>: it uses a
 *  hyphenation grammar based approach to find potential word parts of a
 *  given word.</li>
 *  <li><i>DictionaryCompoundWordTokenFilter</i>: it uses a
 *  brute-force dictionary-only based approach to find the word parts of a given
 *  word.</li>
 * </ul>
 * 
 * <h3>Compound word token filters</h3>
 * <h4>HyphenationCompoundWordTokenFilter</h4>
 * The {@link
 * org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter
 * HyphenationCompoundWordTokenFilter} uses hyphenation grammars to find
 * potential subwords that a worth to check against the dictionary. It can be used
 * without a dictionary as well but then produces a lot of "nonword" tokens.
 * The quality of the output tokens is directly connected to the quality of the
 * grammar file you use. For languages like German they are quite good.
 * <h5>Grammar file</h5>
 * Unfortunately we cannot bundle the hyphenation grammar files with Lucene
 * because they do not use an ASF compatible license (they use the LaTeX
 * Project Public License instead). You can find the XML based grammar
 * files at the
 * <a href="http://offo.sourceforge.net/hyphenation/index.html">Objects
 * For Formatting Objects</a>
 * (OFFO) Sourceforge project (direct link to download the pattern files:
 * <a href="http://downloads.sourceforge.net/offo/offo-hyphenation.zip">http://downloads.sourceforge.net/offo/offo-hyphenation.zip</a>
 * ). The files you need are in the subfolder
 * <i>offo-hyphenation/hyph/</i>
 * .
 * <br>
 * Credits for the hyphenation code go to the
 * <a href="http://xmlgraphics.apache.org/fop/">Apache FOP project</a>
 * .
 * 
 * <h4>DictionaryCompoundWordTokenFilter</h4>
 * The {@link
 * org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilter
 * DictionaryCompoundWordTokenFilter} uses a dictionary-only approach to
 * find subwords in a compound word. It is much slower than the one that
 * uses the hyphenation grammars. You can use it as a first start to
 * see if your dictionary is good or not because it is much simpler in design.
 * 
 * <h3>Dictionary</h3>
 * The output quality of both token filters is directly connected to the
 * quality of the dictionary you use. They are language dependent of course.
 * You always should use a dictionary
 * that fits to the text you want to index. If you index medical text for
 * example then you should use a dictionary that contains medical words.
 * A good start for general text are the dictionaries you find at the
 * <a href="http://wiki.services.openoffice.org/wiki/Dictionaries">OpenOffice
 * dictionaries</a>
 * Wiki.
 * 
 * <h3>Which variant should I use?</h3>
 * This decision matrix should help you:
 * <table border="1" summary="comparison of dictionary and hyphenation based decompounding">
 *  <tr>
 *   <th>Token filter</th>
 *   <th>Output quality</th>
 *   <th>Performance</th>
 *  </tr>
 *  <tr>
 *   <td>HyphenationCompoundWordTokenFilter</td>
 *   <td>good if grammar file is good &ndash; acceptable otherwise</td>
 *   <td>fast</td>
 *  </tr>
 *  <tr>
 *   <td>DictionaryCompoundWordTokenFilter</td>
 *   <td>good</td>
 *   <td>slow</td>
 *  </tr>
 * </table>
 * <h3>Examples</h3>
 * <pre class="prettyprint">
 *   public void testHyphenationCompoundWordsDE() throws Exception {
 *     String[] dict = { "Rind", "Fleisch", "Draht", "Schere", "Gesetz",
 *         "Aufgabe", "&Uuml;berwachung" };
 * 
 *     Reader reader = new FileReader("de_DR.xml");
 * 
 *     HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter
 *         .getHyphenationTree(reader);
 * 
 *     HyphenationCompoundWordTokenFilter tf = new HyphenationCompoundWordTokenFilter(
 *         new WhitespaceTokenizer(new StringReader(
 *             "Rindfleisch&uuml;berwachungsgesetz Drahtschere abba")), hyphenator,
 *         dict, CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
 *         CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
 *         CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, false);
 *         
 *     CharTermAttribute t = tf.addAttribute(CharTermAttribute.class);
 *     while (tf.incrementToken()) {
 *        System.out.println(t);
 *     }
 *   }
 * 
 *   public void testHyphenationCompoundWordsWithoutDictionaryDE() throws Exception {
 *     Reader reader = new FileReader("de_DR.xml");
 * 
 *     HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter
 *         .getHyphenationTree(reader);
 * 
 *     HyphenationCompoundWordTokenFilter tf = new HyphenationCompoundWordTokenFilter(
 *         new WhitespaceTokenizer(new StringReader(
 *             "Rindfleisch&uuml;berwachungsgesetz Drahtschere abba")), hyphenator);
 *         
 *     CharTermAttribute t = tf.addAttribute(CharTermAttribute.class);
 *     while (tf.incrementToken()) {
 *        System.out.println(t);
 *     }
 *   }
 *   
 *   public void testDumbCompoundWordsSE() throws Exception {
 *     String[] dict = { "Bil", "D&ouml;rr", "Motor", "Tak", "Borr", "Slag", "Hammar",
 *         "Pelar", "Glas", "&Ouml;gon", "Fodral", "Bas", "Fiol", "Makare", "Ges&auml;ll",
 *         "Sko", "Vind", "Rute", "Torkare", "Blad" };
 * 
 *     DictionaryCompoundWordTokenFilter tf = new DictionaryCompoundWordTokenFilter(
 *         new WhitespaceTokenizer(
 *             new StringReader(
 *                 "Bild&ouml;rr Bilmotor Biltak Slagborr Hammarborr Pelarborr Glas&ouml;gonfodral Basfiolsfodral Basfiolsfodralmakareges&auml;ll Skomakare Vindrutetorkare Vindrutetorkarblad abba")),
 *         dict);
 *     CharTermAttribute t = tf.addAttribute(CharTermAttribute.class);
 *     while (tf.incrementToken()) {
 *        System.out.println(t);
 *     }
 *   }
 * </pre>
 */
package org.apache.lucene.analysis.compound;
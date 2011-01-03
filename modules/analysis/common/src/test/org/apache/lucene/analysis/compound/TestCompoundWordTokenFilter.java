package org.apache.lucene.analysis.compound;

/**
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

import java.io.StringReader;
import org.xml.sax.InputSource;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TestCompoundWordTokenFilter extends BaseTokenStreamTestCase {
  public void testHyphenationCompoundWordsDA() throws Exception {
    String[] dict = { "læse", "hest" };

    InputSource is = new InputSource(getClass().getResource("da_UTF8.xml").toExternalForm());
    HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter
        .getHyphenationTree(is);

    HyphenationCompoundWordTokenFilter tf = new HyphenationCompoundWordTokenFilter(TEST_VERSION_CURRENT, 
        new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(
            "min veninde som er lidt af en læsehest")), hyphenator,
        dict, CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, false);
    assertTokenStreamContents(tf, 
        new String[] { "min", "veninde", "som", "er", "lidt", "af", "en", "læsehest", "læse", "hest" },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 0, 0 }
    );
  }

  public void testHyphenationCompoundWordsDELongestMatch() throws Exception {
    String[] dict = { "basketball", "basket", "ball", "kurv" };

    InputSource is = new InputSource(getClass().getResource("da_UTF8.xml").toExternalForm());
    HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter
        .getHyphenationTree(is);

    // the word basket will not be added due to the longest match option
    HyphenationCompoundWordTokenFilter tf = new HyphenationCompoundWordTokenFilter(TEST_VERSION_CURRENT, 
        new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(
            "basketballkurv")), hyphenator, dict,
        CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE, 40, true);
    assertTokenStreamContents(tf, 
        new String[] { "basketballkurv", "basketball", "ball", "kurv" },
        new int[] { 1, 0, 0, 0 }
    );

  }

  /**
   * With hyphenation-only, you can get a lot of nonsense tokens.
   * This can be controlled with the min/max subword size.
   */
  public void testHyphenationOnly() throws Exception {
    InputSource is = new InputSource(getClass().getResource("da_UTF8.xml").toExternalForm());
    HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter
        .getHyphenationTree(is);
    
    HyphenationCompoundWordTokenFilter tf = new HyphenationCompoundWordTokenFilter(
        TEST_VERSION_CURRENT,
        new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("basketballkurv")),
        hyphenator,
        CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        2, 4);
    
    // min=2, max=4
    assertTokenStreamContents(tf,
        new String[] { "basketballkurv", "ba", "sket", "bal", "ball", "kurv" }
    );
    
    tf = new HyphenationCompoundWordTokenFilter(
        TEST_VERSION_CURRENT,
        new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("basketballkurv")),
        hyphenator,
        CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        4, 6);
    
    // min=4, max=6
    assertTokenStreamContents(tf,
        new String[] { "basketballkurv", "basket", "sket", "ball", "lkurv", "kurv" }
    );
    
    tf = new HyphenationCompoundWordTokenFilter(
        TEST_VERSION_CURRENT,
        new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("basketballkurv")),
        hyphenator,
        CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        4, 10);
    
    // min=4, max=10
    assertTokenStreamContents(tf,
        new String[] { "basketballkurv", "basket", "basketbal", "basketball", "sket", 
                       "sketbal", "sketball", "ball", "ballkurv", "lkurv", "kurv" }
    );
    
  }

  public void testDumbCompoundWordsSE() throws Exception {
    String[] dict = { "Bil", "Dörr", "Motor", "Tak", "Borr", "Slag", "Hammar",
        "Pelar", "Glas", "Ögon", "Fodral", "Bas", "Fiol", "Makare", "Gesäll",
        "Sko", "Vind", "Rute", "Torkare", "Blad" };

    DictionaryCompoundWordTokenFilter tf = new DictionaryCompoundWordTokenFilter(TEST_VERSION_CURRENT, 
        new WhitespaceTokenizer(TEST_VERSION_CURRENT, 
            new StringReader(
                "Bildörr Bilmotor Biltak Slagborr Hammarborr Pelarborr Glasögonfodral Basfiolsfodral Basfiolsfodralmakaregesäll Skomakare Vindrutetorkare Vindrutetorkarblad abba")),
        dict);

    assertTokenStreamContents(tf, new String[] { "Bildörr", "Bil", "dörr", "Bilmotor",
        "Bil", "motor", "Biltak", "Bil", "tak", "Slagborr", "Slag", "borr",
        "Hammarborr", "Hammar", "borr", "Pelarborr", "Pelar", "borr",
        "Glasögonfodral", "Glas", "ögon", "fodral", "Basfiolsfodral", "Bas",
        "fiol", "fodral", "Basfiolsfodralmakaregesäll", "Bas", "fiol",
        "fodral", "makare", "gesäll", "Skomakare", "Sko", "makare",
        "Vindrutetorkare", "Vind", "rute", "torkare", "Vindrutetorkarblad",
        "Vind", "rute", "blad", "abba" }, new int[] { 0, 0, 3, 8, 8, 11, 17,
        17, 20, 24, 24, 28, 33, 33, 39, 44, 44, 49, 54, 54, 58, 62, 69, 69, 72,
        77, 84, 84, 87, 92, 98, 104, 111, 111, 114, 121, 121, 125, 129, 137,
        137, 141, 151, 156 }, new int[] { 7, 3, 7, 16, 11, 16, 23, 20, 23, 32,
        28, 32, 43, 39, 43, 53, 49, 53, 68, 58, 62, 68, 83, 72, 76, 83, 110,
        87, 91, 98, 104, 110, 120, 114, 120, 136, 125, 129, 136, 155, 141, 145,
        155, 160 }, new int[] { 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1,
        0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 1,
        0, 0, 0, 1 });
  }

  public void testDumbCompoundWordsSELongestMatch() throws Exception {
    String[] dict = { "Bil", "Dörr", "Motor", "Tak", "Borr", "Slag", "Hammar",
        "Pelar", "Glas", "Ögon", "Fodral", "Bas", "Fiols", "Makare", "Gesäll",
        "Sko", "Vind", "Rute", "Torkare", "Blad", "Fiolsfodral" };

    DictionaryCompoundWordTokenFilter tf = new DictionaryCompoundWordTokenFilter(TEST_VERSION_CURRENT, 
        new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("Basfiolsfodralmakaregesäll")),
        dict, CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, true);

    assertTokenStreamContents(tf, new String[] { "Basfiolsfodralmakaregesäll", "Bas",
        "fiolsfodral", "fodral", "makare", "gesäll" }, new int[] { 0, 0, 3, 8,
        14, 20 }, new int[] { 26, 3, 14, 14, 20, 26 }, new int[] { 1, 0, 0, 0,
        0, 0 });
  }
  
  public void testReset() throws Exception {
    String[] dict = { "Rind", "Fleisch", "Draht", "Schere", "Gesetz",
        "Aufgabe", "Überwachung" };

    Tokenizer wsTokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(
        "Rindfleischüberwachungsgesetz"));
    DictionaryCompoundWordTokenFilter tf = new DictionaryCompoundWordTokenFilter(TEST_VERSION_CURRENT, 
        wsTokenizer, dict,
        CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, false);
    
    CharTermAttribute termAtt = tf.getAttribute(CharTermAttribute.class);
    assertTrue(tf.incrementToken());
    assertEquals("Rindfleischüberwachungsgesetz", termAtt.toString());
    assertTrue(tf.incrementToken());
    assertEquals("Rind", termAtt.toString());
    wsTokenizer.reset(new StringReader("Rindfleischüberwachungsgesetz"));
    tf.reset();
    assertTrue(tf.incrementToken());
    assertEquals("Rindfleischüberwachungsgesetz", termAtt.toString());
  }

}

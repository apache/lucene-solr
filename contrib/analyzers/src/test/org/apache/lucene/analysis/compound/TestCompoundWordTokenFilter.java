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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.compound.CompoundWordTokenFilterBase;
import org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;

import junit.framework.TestCase;

public class TestCompoundWordTokenFilter extends TestCase {
  private static String[] locations = {
      "http://dfn.dl.sourceforge.net/sourceforge/offo/offo-hyphenation.zip",
      "http://surfnet.dl.sourceforge.net/sourceforge/offo/offo-hyphenation.zip",
      "http://superb-west.dl.sourceforge.net/sourceforge/offo/offo-hyphenation.zip",
      "http://superb-east.dl.sourceforge.net/sourceforge/offo/offo-hyphenation.zip"};

  private byte[] patternsFileContent;

  protected void setUp() throws Exception {
    super.setUp();
    getHyphenationPatternFileContents();
  }

  public void testHyphenationCompoundWordsDE() throws Exception {
    String[] dict = { "Rind", "Fleisch", "Draht", "Schere", "Gesetz",
        "Aufgabe", "Überwachung" };

    Reader reader = getHyphenationReader("de_DR.xml");
    if (reader == null) {
      // we gracefully die if we have no reader
      return;
    }

    HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter
        .getHyphenationTree(reader);

    HyphenationCompoundWordTokenFilter tf = new HyphenationCompoundWordTokenFilter(
        new WhitespaceTokenizer(new StringReader(
            "Rindfleischüberwachungsgesetz Drahtschere abba")), hyphenator,
        dict, CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, false);
    assertFiltersTo(tf, new String[] { "Rindfleischüberwachungsgesetz", "Rind",
        "fleisch", "überwachung", "gesetz", "Drahtschere", "Draht", "schere",
        "abba" }, new int[] { 0, 0, 4, 11, 23, 30, 30, 35, 42 }, new int[] {
        29, 4, 11, 22, 29, 41, 35, 41, 46 }, new int[] { 1, 0, 0, 0, 0, 1, 0,
        0, 1 });
  }

  public void testHyphenationCompoundWordsDELongestMatch() throws Exception {
    String[] dict = { "Rind", "Fleisch", "Draht", "Schere", "Gesetz",
        "Aufgabe", "Überwachung", "Rindfleisch", "Überwachungsgesetz" };

    Reader reader = getHyphenationReader("de_DR.xml");
    if (reader == null) {
      // we gracefully die if we have no reader
      return;
    }

    HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter
        .getHyphenationTree(reader);

    HyphenationCompoundWordTokenFilter tf = new HyphenationCompoundWordTokenFilter(
        new WhitespaceTokenizer(new StringReader(
            "Rindfleischüberwachungsgesetz")), hyphenator, dict,
        CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE, 40, true);
    assertFiltersTo(tf, new String[] { "Rindfleischüberwachungsgesetz",
        "Rindfleisch", "fleisch", "überwachungsgesetz", "gesetz" }, new int[] {
        0, 0, 4, 11, 23 }, new int[] { 29, 11, 11, 29, 29 }, new int[] { 1, 0,
        0, 0, 0 });
  }

  public void testDumbCompoundWordsSE() throws Exception {
    String[] dict = { "Bil", "Dörr", "Motor", "Tak", "Borr", "Slag", "Hammar",
        "Pelar", "Glas", "Ögon", "Fodral", "Bas", "Fiol", "Makare", "Gesäll",
        "Sko", "Vind", "Rute", "Torkare", "Blad" };

    DictionaryCompoundWordTokenFilter tf = new DictionaryCompoundWordTokenFilter(
        new WhitespaceTokenizer(
            new StringReader(
                "Bildörr Bilmotor Biltak Slagborr Hammarborr Pelarborr Glasögonfodral Basfiolsfodral Basfiolsfodralmakaregesäll Skomakare Vindrutetorkare Vindrutetorkarblad abba")),
        dict);

    assertFiltersTo(tf, new String[] { "Bildörr", "Bil", "dörr", "Bilmotor",
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

    DictionaryCompoundWordTokenFilter tf = new DictionaryCompoundWordTokenFilter(
        new WhitespaceTokenizer(new StringReader("Basfiolsfodralmakaregesäll")),
        dict, CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
        CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, true);

    assertFiltersTo(tf, new String[] { "Basfiolsfodralmakaregesäll", "Bas",
        "fiolsfodral", "fodral", "makare", "gesäll" }, new int[] { 0, 0, 3, 8,
        14, 20 }, new int[] { 26, 3, 14, 14, 20, 26 }, new int[] { 1, 0, 0, 0,
        0, 0 });
  }

  private void assertFiltersTo(TokenFilter tf, String[] s, int[] startOffset,
      int[] endOffset, int[] posIncr) throws Exception {
    final Token reusableToken = new Token();
    for (int i = 0; i < s.length; ++i) {
      Token nextToken = tf.next(reusableToken);
      assertNotNull(nextToken);
      assertEquals(s[i], nextToken.term());
      assertEquals(startOffset[i], nextToken.startOffset());
      assertEquals(endOffset[i], nextToken.endOffset());
      assertEquals(posIncr[i], nextToken.getPositionIncrement());
    }
    assertNull(tf.next(reusableToken));
  }

  private void getHyphenationPatternFileContents() {
    try {
      List urls = new LinkedList(Arrays.asList(locations));
      Collections.shuffle(urls);
      URL url = new URL((String)urls.get(0));
      InputStream in = url.openStream();
      byte[] buffer = new byte[1024];
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      int count;

      while ((count = in.read(buffer)) != -1) {
        out.write(buffer, 0, count);
      }
      in.close();
      out.close();
      patternsFileContent = out.toByteArray();
    } catch (IOException e) {
      // we swallow all exceptions - the user might have no internet connection
    }
  }

  private Reader getHyphenationReader(String filename) throws Exception {
    if (patternsFileContent == null) {
      return null;
    }

    ZipInputStream zipstream = new ZipInputStream(new ByteArrayInputStream(
        patternsFileContent));

    ZipEntry entry;
    while ((entry = zipstream.getNextEntry()) != null) {
      if (entry.getName().equals("offo-hyphenation/hyph/" + filename)) {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream outstream = new ByteArrayOutputStream();
        int count;
        while ((count = zipstream.read(buffer)) != -1) {
          outstream.write(buffer, 0, count);
        }
        outstream.close();
        zipstream.close();
        return new StringReader(new String(outstream.toByteArray(),
            "ISO-8859-1"));
      }
    }
    // we never should get here
    return null;
  }
}

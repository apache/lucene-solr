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
package org.apache.lucene.analysis.icu;


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.icu.ICUTransformCharFilter.NormType;

import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UnicodeSet;


/**
 * Test the ICUTransformCharFilter with some basic examples.
 */
public class TestICUTransformCharFilter extends BaseTokenStreamTestCase {
  
  public void testBasicFunctionality() throws Exception {
    checkToken(Transliterator.getInstance("Traditional-Simplified"),
        "簡化字", "简化字"); 
    checkToken(Transliterator.getInstance("Katakana-Hiragana"),
        "ヒラガナ", "ひらがな");
    checkToken(Transliterator.getInstance("Fullwidth-Halfwidth"),
        "アルアノリウ", "ｱﾙｱﾉﾘｳ");
    checkToken(Transliterator.getInstance("Any-Latin"),
        "Αλφαβητικός Κατάλογος", "Alphabētikós Katálogos");
    checkToken(Transliterator.getInstance("NFD; [:Nonspacing Mark:] Remove"), 
        "Alphabētikós Katálogos", "Alphabetikos Katalogos");
    checkToken(Transliterator.getInstance("Han-Latin"),
        "中国", "zhōng guó");
  }

  public void testRollbackBuffer() throws Exception {
    checkToken(Transliterator.getInstance("Cyrillic-Latin"),
        "яяяяя", "âââââ", false); // final NFC transform applied
    checkToken(Transliterator.getInstance("Cyrillic-Latin"), 0, false,
        "яяяяя", "a\u0302a\u0302a\u0302a\u0302a\u0302", false); // final NFC transform never applied
    checkToken(Transliterator.getInstance("Cyrillic-Latin"), 0, false,
        "яяяяя", "âââââ", true); // final NFC transform disabled internally, applied externally
    checkToken(Transliterator.getInstance("Cyrillic-Latin"), 2, false,
        "яяяяя", "ââa\u0302a\u0302a\u0302", false);
    checkToken(Transliterator.getInstance("Cyrillic-Latin"), 4, false,
        "яяяяяяяяяя", "ââa\u0302a\u0302a\u0302a\u0302a\u0302a\u0302a\u0302a\u0302", false);
    checkToken(Transliterator.getInstance("Cyrillic-Latin"), 8, false,
        "яяяяяяяяяяяяяяяяяяяя", "ââââââa\u0302ââââa\u0302ââââa\u0302âââ", false);
    try {
      checkToken(Transliterator.getInstance("Cyrillic-Latin"), 8, true,
          "яяяяяяяяяяяяяяяяяяяя", "ââââââa\u0302ââââa\u0302ââââa\u0302âââ", false);
      fail("with failOnRollbackBufferOverflow=true, we expect to throw a RuntimeException");
    } catch (RuntimeException ex) {
      // this is expected.
    }
  }

  public void testCustomFunctionality() throws Exception {
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    checkToken(Transliterator.createFromRules("test", rules, Transliterator.FORWARD), "abacadaba", "bcbcbdbcb");
  }
  
  public void testCustomFunctionality2() throws Exception {
    String rules = "c { a > b; a > d;"; // convert a's to b's and b's to c's
    checkToken(Transliterator.createFromRules("test", rules, Transliterator.FORWARD), "caa", "cbd");
  }
  
  public void testOptimizer() throws Exception {
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    Transliterator custom = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    assertTrue(custom.getFilter() == null);
    new ICUTransformCharFilter(new StringReader(""), custom);
    assertTrue(custom.getFilter().equals(new UnicodeSet("[ab]")));
  }
  
  public void testOptimizer2() throws Exception {
    checkToken(Transliterator.getInstance("Traditional-Simplified; CaseFold"),
        "ABCDE", "abcde");
  }
  
  public void testOptimizerSurrogate() throws Exception {
    String rules = "\\U00020087 > x;"; // convert CJK UNIFIED IDEOGRAPH-20087 to an x
    Transliterator custom = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    assertTrue(custom.getFilter() == null);
    new ICUTransformCharFilter(new StringReader(""), custom);
    assertTrue(custom.getFilter().equals(new UnicodeSet("[\\U00020087]")));
  }

  private void checkToken(Transliterator transform, String input, String expected) throws IOException {
    checkToken(transform, ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY, false, input, expected);
  }

  private void checkToken(Transliterator transform, String input, String expected, boolean externalUnicodeNormalization) throws IOException {
    checkToken(transform, ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY, false, input, expected, externalUnicodeNormalization);
  }

  private void checkToken(Transliterator transform, int maxRollbackBufferCapacity, boolean failOnRollbackBufferOverflow, String input, String expected) throws IOException {
    checkToken(getTransliteratingFilter(transform, new StringReader(input), maxRollbackBufferCapacity, failOnRollbackBufferOverflow), expected);
  }

  private void checkToken(Transliterator transform, int maxRollbackBufferCapacity, boolean failOnRollbackBufferOverflow, String input, String expected, boolean externalUnicodeNormalization) throws IOException {
    checkToken(getTransliteratingFilter(transform, new StringReader(input), externalUnicodeNormalization, maxRollbackBufferCapacity, failOnRollbackBufferOverflow), expected);
  }

  private void checkToken(CharFilter input, String expected) throws IOException {
    final KeywordTokenizer input1 = new KeywordTokenizer();
    input1.setReader(input);
    assertTokenStreamContents(input1, new String[] { expected });
  }

  public void testRandomStringsLatinToKatakana() throws Exception {
    // this Transliterator often decreases character length wrt input
    testRandomStrings(Transliterator.getInstance("Latin-Katakana"));
  }

  public void testRandomStringsAnyToLatin() throws Exception {
    // this Transliterator often increases character length wrt input
    testRandomStrings(Transliterator.getInstance("Any-Latin"));
  }

  /** blast some random strings through the analyzer */
  private void testRandomStrings(final Transliterator transform) throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return super.initReader(fieldName, getTransliteratingFilter(transform, reader));
      }

      @Override
      protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        return super.initReaderForNormalization(fieldName, getTransliteratingFilter(transform, reader));
      }

    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }
  
  public void testEmptyTerm() throws IOException {
    final Transliterator transform = Transliterator.getInstance("Any-Latin");
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return super.initReader(fieldName, getTransliteratingFilter(transform, reader));
      }
      @Override
      protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        return super.initReaderForNormalization(fieldName, getTransliteratingFilter(transform, reader));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }

  private CharFilter getTransliteratingFilter(Transliterator t, Reader input) {
    return getTransliteratingFilter(t, input, random().nextBoolean());
  }

  private CharFilter getTransliteratingFilter(Transliterator transliterator, Reader r, boolean externalUnicodeNormalization) {
    return getTransliteratingFilter(transliterator, r, externalUnicodeNormalization, ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY, ICUTransformCharFilter.DEFAULT_FAIL_ON_ROLLBACK_BUFFER_OVERFLOW);
  }

  private CharFilter getTransliteratingFilter(Transliterator transliterator, Reader r, int maxRollbackBufferCapacity, boolean failOnRollbackBufferOverflow) {
    return getTransliteratingFilter(transliterator, r, random().nextBoolean(), maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
  }

  @SuppressWarnings("resource")
  private CharFilter getTransliteratingFilter(Transliterator transliterator, Reader r, boolean externalUnicodeNormalization, int maxRollbackBufferCapacity, boolean failOnRollbackBufferOverflow) {
    if (externalUnicodeNormalization) {
      Transliterator noNormTransliterator = ICUTransformCharFilter.withoutUnicodeNormalization(transliterator);
      if (noNormTransliterator == transliterator) {
        return new ICUTransformCharFilter(r, transliterator, maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
      }
      Transliterator[] originalElements = transliterator.getElements();
      NormType inputNormalization = ICUTransformCharFilter.unicodeNormalizationType(originalElements[0].getID());
      if (inputNormalization != null) {
        r = wrapWithNormalizer(r, inputNormalization);
      }
      CharFilter ret = new ICUTransformCharFilter(r, transliterator, maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
      NormType outputNormalization = ICUTransformCharFilter.unicodeNormalizationType(originalElements[originalElements.length - 1].getID());
      if (outputNormalization != null) {
        ret = wrapWithNormalizer(ret, outputNormalization);
      }
      return ret;
    } else {
      return new ICUTransformCharFilter(r, transliterator, maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
    }
  }

  private CharFilter wrapWithNormalizer(Reader r, NormType n) {
    switch (n) {
      case NFC:
        return new ICUNormalizer2CharFilter(r, Normalizer2.getNFCInstance());
      case NFD:
        return new ICUNormalizer2CharFilter(r, Normalizer2.getNFDInstance());
      case NFKC:
        return new ICUNormalizer2CharFilter(r, Normalizer2.getNFKCInstance());
      case NFKD:
        return new ICUNormalizer2CharFilter(r, Normalizer2.getNFKDInstance());
      default:
        throw new UnsupportedOperationException("test not yet able to compensate externally for normalization type \""+n+"\"");
    }
  }
}

package org.apache.lucene.analysis.icu;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.util.TestUtil;

import com.ibm.icu.text.Normalizer2;

public class TestICUNormalizer2CharFilter extends BaseTokenStreamTestCase {

  public void testNormalization() throws IOException {
    String input = "ʰ㌰゙5℃№㈱㌘，バッファーの正規化のテスト．㋐㋑㋒㋓㋔ｶｷｸｹｺｻﾞｼﾞｽﾞｾﾞｿﾞg̈각/각நிเกषिchkʷक्षि";
    Normalizer2 normalizer = Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE);
    String expectedOutput = normalizer.normalize(input);

    CharFilter reader = new ICUNormalizer2CharFilter(new StringReader(input), normalizer);
    char[] tempBuff = new char[10];
    StringBuilder output = new StringBuilder();
    while (true) {
      int length = reader.read(tempBuff);
      if (length == -1) {
        break;
      }
      output.append(tempBuff, 0, length);
      assertEquals(output.toString(), normalizer.normalize(input.substring(0, reader.correctOffset(output.length()))));
    }

    assertEquals(expectedOutput, output.toString());
  }

  public void testTokenStream() throws IOException {
    // '℃', '№', '㈱', '㌘', 'ｻ'+'<<', 'ｿ'+'<<', '㌰'+'<<'
    String input = "℃ № ㈱ ㌘ ｻﾞ ｿﾞ ㌰ﾞ";

    CharFilter reader = new ICUNormalizer2CharFilter(new StringReader(input),
      Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.COMPOSE));

    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenStream.setReader(reader);

    assertTokenStreamContents(tokenStream,
      new String[] {"°C", "No", "(株)", "グラム", "ザ", "ゾ", "ピゴ"},
      new int[] {0, 2, 4, 6, 8, 11, 14},
      new int[] {1, 3, 5, 7, 10, 13, 16},
      input.length());
  }

  public void testTokenStream2() throws IOException {
    // '㌰', '<<'゙, '5', '℃', '№', '㈱', '㌘', 'ｻ', '<<', 'ｿ', '<<'
    String input = "㌰゙5℃№㈱㌘ｻﾞｿﾞ";

    CharFilter reader = new ICUNormalizer2CharFilter(new StringReader(input),
      Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));

    Tokenizer tokenStream = new NGramTokenizer(TEST_VERSION_CURRENT, 1, 1);
    tokenStream.setReader(reader);

    assertTokenStreamContents(tokenStream,
      new String[] {"ピ", "ゴ", "5", "°", "c", "n", "o", "(", "株", ")", "グ", "ラ", "ム", "ザ", "ゾ"},
      new int[]{0, 1, 2, 3, 3, 4, 4, 5, 5, 5, 6, 6, 6, 7, 9},
      new int[]{1, 2, 3, 3, 4, 4, 5, 5, 5, 6, 6, 6, 7, 9, 11},
      input.length()
    );
  }
  
  public void testMassiveLigature() throws IOException {
    String input = "\uFDFA";

    CharFilter reader = new ICUNormalizer2CharFilter(new StringReader(input),
      Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));

    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenStream.setReader(reader);

    assertTokenStreamContents(tokenStream,
      new String[] {"صلى", "الله", "عليه", "وسلم"},
      new int[]{0, 0, 0, 0},
      new int[]{0, 0, 0, 1},
      input.length()
    );
  }

  public void doTestMode(final Normalizer2 normalizer, int maxLength, int iterations) throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.KEYWORD, false));
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new ICUNormalizer2CharFilter(reader, normalizer);
      }
    };

    for (int i = 0; i < iterations; i++) {
      String input = TestUtil.randomUnicodeString(random(), maxLength);
      if (input.length() == 0) {
        continue;
      }
      String normalized = normalizer.normalize(input);
      if (normalized.length() == 0) {
        continue; // MockTokenizer doesnt tokenize empty string...
      }
      checkOneTerm(a, input, normalized);
    }
  }

  public void testNFC() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfc", Normalizer2.Mode.COMPOSE), 20, RANDOM_MULTIPLIER*1000);
  }
  
  public void testNFCHuge() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfc", Normalizer2.Mode.COMPOSE), 8192, RANDOM_MULTIPLIER*100);
  }

  public void testNFD() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfc", Normalizer2.Mode.DECOMPOSE), 20, RANDOM_MULTIPLIER*1000);
  }
  
  public void testNFDHuge() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfc", Normalizer2.Mode.DECOMPOSE), 8192, RANDOM_MULTIPLIER*100);
  }

  public void testNFKC() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.COMPOSE), 20, RANDOM_MULTIPLIER*1000);
  }
  
  public void testNFKCHuge() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.COMPOSE), 8192, RANDOM_MULTIPLIER*100);
  }

  public void testNFKD() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.DECOMPOSE), 20, RANDOM_MULTIPLIER*1000);
  }
  
  public void testNFKDHuge() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.DECOMPOSE), 8192, RANDOM_MULTIPLIER*100);
  }

  public void testNFKC_CF() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE), 20, RANDOM_MULTIPLIER*1000);
  }
  
  public void testNFKC_CFHuge() throws Exception {
    doTestMode(Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE), 8192, RANDOM_MULTIPLIER*100);
  }

  public void testRandomStrings() throws IOException {
    // nfkc_cf
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new ICUNormalizer2CharFilter(reader, Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));
      }
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    // huge strings
    checkRandomData(random(), a, 100*RANDOM_MULTIPLIER, 8192);

    // nfkd
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new ICUNormalizer2CharFilter(reader, Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.DECOMPOSE));
      }
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    // huge strings
    checkRandomData(random(), a, 100*RANDOM_MULTIPLIER, 8192);
  }
  
  public void testCuriousString() throws Exception {
    String text = "\udb40\udc3d\uf273\ue960\u06c8\ud955\udc13\ub7fc\u0692 \u2089\u207b\u2073\u2075";
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new ICUNormalizer2CharFilter(reader, Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));
      }
    };
    for (int i = 0; i < 1000; i++) {
      checkAnalysisConsistency(random(), a, false, text);
    }
  }
}

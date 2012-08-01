package org.apache.lucene.analysis.snowball;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class TestSnowball extends BaseTokenStreamTestCase {

  public void testEnglish() throws Exception {
    Analyzer a = new SnowballAnalyzer(TEST_VERSION_CURRENT, "English");
    assertAnalyzesTo(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
  }
  
  public void testStopwords() throws Exception {
    Analyzer a = new SnowballAnalyzer(TEST_VERSION_CURRENT, "English",
        StandardAnalyzer.STOP_WORDS_SET);
    assertAnalyzesTo(a, "the quick brown fox jumped",
        new String[]{"quick", "brown", "fox", "jump"});
  }

  /**
   * Test english lowercasing. Test both cases (pre-3.1 and post-3.1) to ensure
   * we lowercase I correct for non-Turkish languages in either case.
   */
  public void testEnglishLowerCase() throws Exception {
    Analyzer a = new SnowballAnalyzer(TEST_VERSION_CURRENT, "English");
    assertAnalyzesTo(a, "cryogenic", new String[] { "cryogen" });
    assertAnalyzesTo(a, "CRYOGENIC", new String[] { "cryogen" });
    
    Analyzer b = new SnowballAnalyzer(Version.LUCENE_30, "English");
    assertAnalyzesTo(b, "cryogenic", new String[] { "cryogen" });
    assertAnalyzesTo(b, "CRYOGENIC", new String[] { "cryogen" });
  }
  
  /**
   * Test turkish lowercasing
   */
  public void testTurkish() throws Exception {
    Analyzer a = new SnowballAnalyzer(TEST_VERSION_CURRENT, "Turkish");

    assertAnalyzesTo(a, "ağacı", new String[] { "ağaç" });
    assertAnalyzesTo(a, "AĞACI", new String[] { "ağaç" });
  }
  
  /**
   * Test turkish lowercasing (old buggy behavior)
   * @deprecated (3.1) Remove this when support for 3.0 indexes is no longer required (5.0)
   */
  @Deprecated
  public void testTurkishBWComp() throws Exception {
    Analyzer a = new SnowballAnalyzer(Version.LUCENE_30, "Turkish");
    // AĞACI in turkish lowercases to ağacı, but with lowercase filter ağaci.
    // this fails due to wrong casing, because the stemmer
    // will only remove -ı, not -i
    assertAnalyzesTo(a, "ağacı", new String[] { "ağaç" });
    assertAnalyzesTo(a, "AĞACI", new String[] { "ağaci" });
  }

  
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new SnowballAnalyzer(TEST_VERSION_CURRENT, "English");
    assertAnalyzesToReuse(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
    assertAnalyzesToReuse(a, "she abhorred him",
        new String[]{"she", "abhor", "him"});
  }
  
  public void testFilterTokens() throws Exception {
    SnowballFilter filter = new SnowballFilter(new TestTokenStream(), "English");
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = filter.getAttribute(OffsetAttribute.class);
    TypeAttribute typeAtt = filter.getAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = filter.getAttribute(PayloadAttribute.class);
    PositionIncrementAttribute posIncAtt = filter.getAttribute(PositionIncrementAttribute.class);
    FlagsAttribute flagsAtt = filter.getAttribute(FlagsAttribute.class);
    
    filter.incrementToken();

    assertEquals("accent", termAtt.toString());
    assertEquals(2, offsetAtt.startOffset());
    assertEquals(7, offsetAtt.endOffset());
    assertEquals("wrd", typeAtt.type());
    assertEquals(3, posIncAtt.getPositionIncrement());
    assertEquals(77, flagsAtt.getFlags());
    assertEquals(new BytesRef(new byte[]{0,1,2,3}), payloadAtt.getPayload());
  }
  
  private final class TestTokenStream extends TokenStream {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
    
    TestTokenStream() {
      super();
    }
    
    @Override
    public boolean incrementToken() {
      clearAttributes();
      termAtt.setEmpty().append("accents");
      offsetAtt.setOffset(2, 7);
      typeAtt.setType("wrd");
      posIncAtt.setPositionIncrement(3);
      payloadAtt.setPayload(new BytesRef(new byte[]{0,1,2,3}));
      flagsAtt.setFlags(77);
      return true;
    }
  }
  
  /** for testing purposes ONLY */
  public static String SNOWBALL_LANGS[] = {
    "Armenian", "Basque", "Catalan", "Danish", "Dutch", "English",
    "Finnish", "French", "German2", "German", "Hungarian", "Irish",
    "Italian", "Kp", "Lovins", "Norwegian", "Porter", "Portuguese",
    "Romanian", "Russian", "Spanish", "Swedish", "Turkish"
  };
  
  public void testEmptyTerm() throws IOException {
    for (final String lang : SNOWBALL_LANGS) {
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
          Tokenizer tokenizer = new KeywordTokenizer(reader);
          return new TokenStreamComponents(tokenizer, new SnowballFilter(tokenizer, lang));
        }
      };
      checkOneTermReuse(a, "", "");
    }
  }
  
  public void testRandomStrings() throws IOException {
    for (String lang : SNOWBALL_LANGS) {
      checkRandomStrings(lang);
    }
  }
  
  public void checkRandomStrings(final String snowballLanguage) throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer t = new MockTokenizer(reader);
        return new TokenStreamComponents(t, new SnowballFilter(t, snowballLanguage));
      }  
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
  }
}
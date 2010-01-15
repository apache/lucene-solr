package org.apache.lucene.analysis.snowball;

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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.index.Payload;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;

public class TestSnowball extends BaseTokenStreamTestCase {

  public void testEnglish() throws Exception {
    Analyzer a = new SnowballAnalyzer(Version.LUCENE_CURRENT, "English");
    assertAnalyzesTo(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
  }
  
  public void testStopwords() throws Exception {
    Analyzer a = new SnowballAnalyzer(Version.LUCENE_CURRENT, "English",
        StandardAnalyzer.STOP_WORDS_SET);
    assertAnalyzesTo(a, "the quick brown fox jumped",
        new String[]{"quick", "brown", "fox", "jump"});
  }

  public void testReusableTokenStream() throws Exception {
    Analyzer a = new SnowballAnalyzer(Version.LUCENE_CURRENT, "English");
    assertAnalyzesToReuse(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
    assertAnalyzesToReuse(a, "she abhorred him",
        new String[]{"she", "abhor", "him"});
  }
  
  /**
   * subclass that acts just like whitespace analyzer for testing
   */
  private class SnowballSubclassAnalyzer extends SnowballAnalyzer {
    public SnowballSubclassAnalyzer(String name) {
      super(Version.LUCENE_CURRENT, name);
    }
    
    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new WhitespaceTokenizer(reader);
    }
  }
  
  public void testLUCENE1678BWComp() throws Exception {
    Analyzer a = new SnowballSubclassAnalyzer("English");
    assertAnalyzesToReuse(a, "he abhorred accents",
        new String[]{"he", "abhorred", "accents"});
  }
  
  public void testFilterTokens() throws Exception {
    SnowballFilter filter = new SnowballFilter(new TestTokenStream(), "English");
    TermAttribute termAtt = filter.getAttribute(TermAttribute.class);
    OffsetAttribute offsetAtt = filter.getAttribute(OffsetAttribute.class);
    TypeAttribute typeAtt = filter.getAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = filter.getAttribute(PayloadAttribute.class);
    PositionIncrementAttribute posIncAtt = filter.getAttribute(PositionIncrementAttribute.class);
    FlagsAttribute flagsAtt = filter.getAttribute(FlagsAttribute.class);
    
    filter.incrementToken();

    assertEquals("accent", termAtt.term());
    assertEquals(2, offsetAtt.startOffset());
    assertEquals(7, offsetAtt.endOffset());
    assertEquals("wrd", typeAtt.type());
    assertEquals(3, posIncAtt.getPositionIncrement());
    assertEquals(77, flagsAtt.getFlags());
    assertEquals(new Payload(new byte[]{0,1,2,3}), payloadAtt.getPayload());
  }
  
  private final class TestTokenStream extends TokenStream {
    private TermAttribute termAtt;
    private OffsetAttribute offsetAtt;
    private TypeAttribute typeAtt;
    private PayloadAttribute payloadAtt;
    private PositionIncrementAttribute posIncAtt;
    private FlagsAttribute flagsAtt;
    
    TestTokenStream() {
      super();
      termAtt = addAttribute(TermAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
      typeAtt = addAttribute(TypeAttribute.class);
      payloadAtt = addAttribute(PayloadAttribute.class);
      posIncAtt = addAttribute(PositionIncrementAttribute.class);
      flagsAtt = addAttribute(FlagsAttribute.class);
    }
    
    @Override
    public boolean incrementToken() {
      clearAttributes();
      termAtt.setTermBuffer("accents");
      offsetAtt.setOffset(2, 7);
      typeAtt.setType("wrd");
      posIncAtt.setPositionIncrement(3);
      payloadAtt.setPayload(new Payload(new byte[]{0,1,2,3}));
      flagsAtt.setFlags(77);
      return true;
    }
  }
}
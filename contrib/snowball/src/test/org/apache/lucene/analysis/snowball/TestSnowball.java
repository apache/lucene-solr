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

import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Payload;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class TestSnowball extends TestCase {

  public void assertAnalyzesTo(Analyzer a,
                               String input,
                               String[] output) throws Exception {
    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    TermAttribute termAtt = (TermAttribute) ts.getAttribute(TermAttribute.class);
    for (int i = 0; i < output.length; i++) {
      assertTrue(ts.incrementToken());
      assertEquals(output[i], termAtt.term());
    }
    assertFalse(ts.incrementToken());
    ts.close();
  }

  public void testEnglish() throws Exception {
    Analyzer a = new SnowballAnalyzer("English");
    assertAnalyzesTo(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
  }


  public void testFilterTokens() throws Exception {
    SnowballFilter filter = new SnowballFilter(new TestTokenStream(), "English");
    TermAttribute termAtt = (TermAttribute) filter.getAttribute(TermAttribute.class);
    OffsetAttribute offsetAtt = (OffsetAttribute) filter.getAttribute(OffsetAttribute.class);
    TypeAttribute typeAtt = (TypeAttribute) filter.getAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = (PayloadAttribute) filter.getAttribute(PayloadAttribute.class);
    PositionIncrementAttribute posIncAtt = (PositionIncrementAttribute) filter.getAttribute(PositionIncrementAttribute.class);
    FlagsAttribute flagsAtt = (FlagsAttribute) filter.getAttribute(FlagsAttribute.class);
    
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
      termAtt = (TermAttribute) addAttribute(TermAttribute.class);
      offsetAtt = (OffsetAttribute) addAttribute(OffsetAttribute.class);
      typeAtt = (TypeAttribute) addAttribute(TypeAttribute.class);
      payloadAtt = (PayloadAttribute) addAttribute(PayloadAttribute.class);
      posIncAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
      flagsAtt = (FlagsAttribute) addAttribute(FlagsAttribute.class);
    }
    
    public boolean incrementToken() {
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
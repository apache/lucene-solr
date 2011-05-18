package org.apache.lucene.analysis;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.English;
import java.io.IOException;
import java.io.StringReader;


/**
 * tests for the TestTeeSinkTokenFilter
 */
public class TestTeeSinkTokenFilter extends BaseTokenStreamTestCase {
  protected StringBuilder buffer1;
  protected StringBuilder buffer2;
  protected String[] tokens1;
  protected String[] tokens2;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    tokens1 = new String[]{"The", "quick", "Burgundy", "Fox", "jumped", "over", "the", "lazy", "Red", "Dogs"};
    tokens2 = new String[]{"The", "Lazy", "Dogs", "should", "stay", "on", "the", "porch"};
    buffer1 = new StringBuilder();

    for (int i = 0; i < tokens1.length; i++) {
      buffer1.append(tokens1[i]).append(' ');
    }
    buffer2 = new StringBuilder();
    for (int i = 0; i < tokens2.length; i++) {
      buffer2.append(tokens2[i]).append(' ');
    }
  }

  static final TeeSinkTokenFilter.SinkFilter theFilter = new TeeSinkTokenFilter.SinkFilter() {
    @Override
    public boolean accept(AttributeSource a) {
      CharTermAttribute termAtt = a.getAttribute(CharTermAttribute.class);
      return termAtt.toString().equalsIgnoreCase("The");
    }
  };

  static final TeeSinkTokenFilter.SinkFilter dogFilter = new TeeSinkTokenFilter.SinkFilter() {
    @Override
    public boolean accept(AttributeSource a) {
      CharTermAttribute termAtt = a.getAttribute(CharTermAttribute.class);
      return termAtt.toString().equalsIgnoreCase("Dogs");
    }
  };

  public void testGeneral() throws IOException {
    final TeeSinkTokenFilter source = new TeeSinkTokenFilter(new MockTokenizer(new StringReader(buffer1.toString()), MockTokenizer.WHITESPACE, false));
    final TokenStream sink1 = source.newSinkTokenStream();
    final TokenStream sink2 = source.newSinkTokenStream(theFilter);
    
    source.addAttribute(CheckClearAttributesAttribute.class);
    sink1.addAttribute(CheckClearAttributesAttribute.class);
    sink2.addAttribute(CheckClearAttributesAttribute.class);
    
    assertTokenStreamContents(source, tokens1);
    assertTokenStreamContents(sink1, tokens1);
    assertTokenStreamContents(sink2, new String[]{"The", "the"});
  }

  public void testMultipleSources() throws Exception {
    final TeeSinkTokenFilter tee1 = new TeeSinkTokenFilter(new MockTokenizer(new StringReader(buffer1.toString()), MockTokenizer.WHITESPACE, false));
    final TeeSinkTokenFilter.SinkTokenStream dogDetector = tee1.newSinkTokenStream(dogFilter);
    final TeeSinkTokenFilter.SinkTokenStream theDetector = tee1.newSinkTokenStream(theFilter);
    tee1.reset();
    final TokenStream source1 = new CachingTokenFilter(tee1);
    
    tee1.addAttribute(CheckClearAttributesAttribute.class);
    dogDetector.addAttribute(CheckClearAttributesAttribute.class);
    theDetector.addAttribute(CheckClearAttributesAttribute.class);

    final TeeSinkTokenFilter tee2 = new TeeSinkTokenFilter(new MockTokenizer(new StringReader(buffer2.toString()), MockTokenizer.WHITESPACE, false));
    tee2.addSinkTokenStream(dogDetector);
    tee2.addSinkTokenStream(theDetector);
    final TokenStream source2 = tee2;

    assertTokenStreamContents(source1, tokens1);
    assertTokenStreamContents(source2, tokens2);

    assertTokenStreamContents(theDetector, new String[]{"The", "the", "The", "the"});
    assertTokenStreamContents(dogDetector, new String[]{"Dogs", "Dogs"});
    
    source1.reset();
    TokenStream lowerCasing = new LowerCaseFilter(TEST_VERSION_CURRENT, source1);
    String[] lowerCaseTokens = new String[tokens1.length];
    for (int i = 0; i < tokens1.length; i++)
      lowerCaseTokens[i] = tokens1[i].toLowerCase();
    assertTokenStreamContents(lowerCasing, lowerCaseTokens);
  }

  /**
   * Not an explicit test, just useful to print out some info on performance
   *
   * @throws Exception
   */
  public void performance() throws Exception {
    int[] tokCount = {100, 500, 1000, 2000, 5000, 10000};
    int[] modCounts = {1, 2, 5, 10, 20, 50, 100, 200, 500};
    for (int k = 0; k < tokCount.length; k++) {
      StringBuilder buffer = new StringBuilder();
      System.out.println("-----Tokens: " + tokCount[k] + "-----");
      for (int i = 0; i < tokCount[k]; i++) {
        buffer.append(English.intToEnglish(i).toUpperCase()).append(' ');
      }
      //make sure we produce the same tokens
      TeeSinkTokenFilter teeStream = new TeeSinkTokenFilter(new StandardFilter(new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(buffer.toString()))));
      TokenStream sink = teeStream.newSinkTokenStream(new ModuloSinkFilter(100));
      teeStream.consumeAllTokens();
      TokenStream stream = new ModuloTokenFilter(new StandardFilter(new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(buffer.toString()))), 100);
      CharTermAttribute tfTok = stream.addAttribute(CharTermAttribute.class);
      CharTermAttribute sinkTok = sink.addAttribute(CharTermAttribute.class);
      for (int i=0; stream.incrementToken(); i++) {
        assertTrue(sink.incrementToken());
        assertTrue(tfTok + " is not equal to " + sinkTok + " at token: " + i, tfTok.equals(sinkTok) == true);
      }
      
      //simulate two fields, each being analyzed once, for 20 documents
      for (int j = 0; j < modCounts.length; j++) {
        int tfPos = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
          stream = new StandardFilter(new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(buffer.toString())));
          PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
          while (stream.incrementToken()) {
            tfPos += posIncrAtt.getPositionIncrement();
          }
          stream = new ModuloTokenFilter(new StandardFilter(new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(buffer.toString()))), modCounts[j]);
          posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
          while (stream.incrementToken()) {
            tfPos += posIncrAtt.getPositionIncrement();
          }
        }
        long finish = System.currentTimeMillis();
        System.out.println("ModCount: " + modCounts[j] + " Two fields took " + (finish - start) + " ms");
        int sinkPos = 0;
        //simulate one field with one sink
        start = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
          teeStream = new TeeSinkTokenFilter(new StandardFilter(new StandardTokenizer(TEST_VERSION_CURRENT, new StringReader(buffer.toString()))));
          sink = teeStream.newSinkTokenStream(new ModuloSinkFilter(modCounts[j]));
          PositionIncrementAttribute posIncrAtt = teeStream.getAttribute(PositionIncrementAttribute.class);
          while (teeStream.incrementToken()) {
            sinkPos += posIncrAtt.getPositionIncrement();
          }
          //System.out.println("Modulo--------");
          posIncrAtt = sink.getAttribute(PositionIncrementAttribute.class);
          while (sink.incrementToken()) {
            sinkPos += posIncrAtt.getPositionIncrement();
          }
        }
        finish = System.currentTimeMillis();
        System.out.println("ModCount: " + modCounts[j] + " Tee fields took " + (finish - start) + " ms");
        assertTrue(sinkPos + " does not equal: " + tfPos, sinkPos == tfPos);

      }
      System.out.println("- End Tokens: " + tokCount[k] + "-----");
    }

  }


  class ModuloTokenFilter extends TokenFilter {

    int modCount;

    ModuloTokenFilter(TokenStream input, int mc) {
      super(input);
      modCount = mc;
    }

    int count = 0;

    //return every 100 tokens
    @Override
    public boolean incrementToken() throws IOException {
      boolean hasNext;
      for (hasNext = input.incrementToken();
           hasNext && count % modCount != 0;
           hasNext = input.incrementToken()) {
        count++;
      }
      count++;
      return hasNext;
    }
  }

  class ModuloSinkFilter extends TeeSinkTokenFilter.SinkFilter {
    int count = 0;
    int modCount;

    ModuloSinkFilter(int mc) {
      modCount = mc;
    }

    @Override
    public boolean accept(AttributeSource a) {
      boolean b = (a != null && count % modCount == 0);
      count++;
      return b;
    }

  }
}


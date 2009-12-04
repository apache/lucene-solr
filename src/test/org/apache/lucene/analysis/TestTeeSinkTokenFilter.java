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
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.English;
import org.apache.lucene.util.Version;

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


  public TestTeeSinkTokenFilter(String s) {
    super(s);
  }

  @Override
  protected void setUp() throws Exception {
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
      TermAttribute termAtt = a.getAttribute(TermAttribute.class);
      return termAtt.term().equalsIgnoreCase("The");
    }
  };

  static final TeeSinkTokenFilter.SinkFilter dogFilter = new TeeSinkTokenFilter.SinkFilter() {
    @Override
    public boolean accept(AttributeSource a) {
      TermAttribute termAtt = a.getAttribute(TermAttribute.class);
      return termAtt.term().equalsIgnoreCase("Dogs");
    }
  };

  
  public void testGeneral() throws IOException {
    final TeeSinkTokenFilter source = new TeeSinkTokenFilter(new WhitespaceTokenizer(new StringReader(buffer1.toString())));
    final TokenStream sink1 = source.newSinkTokenStream();
    final TokenStream sink2 = source.newSinkTokenStream(theFilter);
    int i = 0;
    TermAttribute termAtt = source.getAttribute(TermAttribute.class);
    while (source.incrementToken()) {
      assertEquals(tokens1[i], termAtt.term());
      i++;
    }
    assertEquals(tokens1.length, i);
    
    i = 0;
    termAtt = sink1.getAttribute(TermAttribute.class);
    while (sink1.incrementToken()) {
      assertEquals(tokens1[i], termAtt.term());
      i++;
    }
    assertEquals(tokens1.length, i);
    
    i = 0;
    termAtt = sink2.getAttribute(TermAttribute.class);
    while (sink2.incrementToken()) {
      assertTrue(termAtt.term().equalsIgnoreCase("The"));
      i++;
    }
    assertEquals("there should be two times 'the' in the stream", 2, i);
  }

  public void testMultipleSources() throws Exception {
    final TeeSinkTokenFilter tee1 = new TeeSinkTokenFilter(new WhitespaceTokenizer(new StringReader(buffer1.toString())));
    final TeeSinkTokenFilter.SinkTokenStream dogDetector = tee1.newSinkTokenStream(dogFilter);
    final TeeSinkTokenFilter.SinkTokenStream theDetector = tee1.newSinkTokenStream(theFilter);
    final TokenStream source1 = new CachingTokenFilter(tee1);

    final TeeSinkTokenFilter tee2 = new TeeSinkTokenFilter(new WhitespaceTokenizer(new StringReader(buffer2.toString())));
    tee2.addSinkTokenStream(dogDetector);
    tee2.addSinkTokenStream(theDetector);
    final TokenStream source2 = tee2;

    int i = 0;
    TermAttribute termAtt = source1.getAttribute(TermAttribute.class);
    while (source1.incrementToken()) {
      assertEquals(tokens1[i], termAtt.term());
      i++;
    }
    assertEquals(tokens1.length, i);
    i = 0;
    termAtt = source2.getAttribute(TermAttribute.class);
    while (source2.incrementToken()) {
      assertEquals(tokens2[i], termAtt.term());
      i++;
    }
    assertEquals(tokens2.length, i);
    i = 0;
    termAtt = theDetector.getAttribute(TermAttribute.class);
    while (theDetector.incrementToken()) {
      assertTrue("'" + termAtt.term() + "' is not equal to 'The'", termAtt.term().equalsIgnoreCase("The"));
      i++;
    }
    assertEquals("there must be 4 times 'The' in the stream", 4, i);
    i = 0;
    termAtt = dogDetector.getAttribute(TermAttribute.class);
    while (dogDetector.incrementToken()) {
      assertTrue("'" + termAtt.term() + "' is not equal to 'Dogs'", termAtt.term().equalsIgnoreCase("Dogs"));
      i++;
    }
    assertEquals("there must be 2 times 'Dog' in the stream", 2, i);
    
    source1.reset();
    TokenStream lowerCasing = new LowerCaseFilter(Version.LUCENE_CURRENT, source1);
    i = 0;
    termAtt = lowerCasing.getAttribute(TermAttribute.class);
    while (lowerCasing.incrementToken()) {
      assertEquals(tokens1[i].toLowerCase(), termAtt.term());
      i++;
    }
    assertEquals(i, tokens1.length);
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
      TeeSinkTokenFilter teeStream = new TeeSinkTokenFilter(new StandardFilter(new StandardTokenizer(Version.LUCENE_CURRENT, new StringReader(buffer.toString()))));
      TokenStream sink = teeStream.newSinkTokenStream(new ModuloSinkFilter(100));
      teeStream.consumeAllTokens();
      TokenStream stream = new ModuloTokenFilter(new StandardFilter(new StandardTokenizer(Version.LUCENE_CURRENT, new StringReader(buffer.toString()))), 100);
      TermAttribute tfTok = stream.addAttribute(TermAttribute.class);
      TermAttribute sinkTok = sink.addAttribute(TermAttribute.class);
      for (int i=0; stream.incrementToken(); i++) {
        assertTrue(sink.incrementToken());
        assertTrue(tfTok + " is not equal to " + sinkTok + " at token: " + i, tfTok.equals(sinkTok) == true);
      }
      
      //simulate two fields, each being analyzed once, for 20 documents
      for (int j = 0; j < modCounts.length; j++) {
        int tfPos = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
          stream = new StandardFilter(new StandardTokenizer(Version.LUCENE_CURRENT, new StringReader(buffer.toString())));
          PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
          while (stream.incrementToken()) {
            tfPos += posIncrAtt.getPositionIncrement();
          }
          stream = new ModuloTokenFilter(new StandardFilter(new StandardTokenizer(Version.LUCENE_CURRENT, new StringReader(buffer.toString()))), modCounts[j]);
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
          teeStream = new TeeSinkTokenFilter(new StandardFilter(new StandardTokenizer(Version.LUCENE_CURRENT, new StringReader(buffer.toString()))));
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


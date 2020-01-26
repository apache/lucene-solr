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
package org.apache.lucene.analysis.util;


import java.io.IOException;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.IOUtils;

/** Basic tests for {@link SegmentingTokenizerBase} */
public class TestSegmentingTokenizerBase extends BaseTokenStreamTestCase {
  private Analyzer sentence, sentenceAndWord;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    sentence = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new WholeSentenceTokenizer());
      }
    };
    sentenceAndWord = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new SentenceAndWordTokenizer());
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(sentence, sentenceAndWord);
    super.tearDown();
  }

  
  /** Some simple examples, just outputting the whole sentence boundaries as "terms" */
  public void testBasics() throws IOException {
    assertAnalyzesTo(sentence, "The acronym for United States is U.S. but this doesn't end a sentence",
        new String[] { "The acronym for United States is U.S. but this doesn't end a sentence"}
    );
    assertAnalyzesTo(sentence, "He said, \"Are you going?\" John shook his head.",
        new String[] { "He said, \"Are you going?\" ", 
                       "John shook his head." }
    );
  }
  
  /** Test a subclass that sets some custom attribute values */
  public void testCustomAttributes() throws IOException {
    assertAnalyzesTo(sentenceAndWord, "He said, \"Are you going?\" John shook his head.",
        new String[] { "He", "said", "Are", "you", "going", "John", "shook", "his", "head" },
        new int[] { 0, 3, 10, 14, 18, 26, 31, 37, 41 },
        new int[] { 2, 7, 13, 17, 23, 30, 36, 40, 45 },
        new int[] { 1, 1,  1,  1,  1,  2,  1,  1,  1 }
    );
  }
  
  /** Tests tokenstream reuse */
  public void testReuse() throws IOException {
    assertAnalyzesTo(sentenceAndWord, "He said, \"Are you going?\"",
        new String[] { "He", "said", "Are", "you", "going" },
        new int[] { 0, 3, 10, 14, 18 },
        new int[] { 2, 7, 13, 17, 23 },
        new int[] { 1, 1,  1,  1,  1,}
    );
    assertAnalyzesTo(sentenceAndWord, "John shook his head.",
        new String[] { "John", "shook", "his", "head" },
        new int[] { 0,  5, 11, 15 },
        new int[] { 4, 10, 14, 19 },
        new int[] { 1,  1,  1,  1 }
    );
  }
  
  /** Tests TokenStream.end() */
  public void testEnd() throws IOException {
    // BaseTokenStreamTestCase asserts that end() is set to our StringReader's length for us here.
    // we add some junk whitespace to the end just to test it.
    assertAnalyzesTo(sentenceAndWord, "John shook his head          ",
        new String[] { "John", "shook", "his", "head" }
    );
    assertAnalyzesTo(sentenceAndWord, "John shook his head.          ",
        new String[] { "John", "shook", "his", "head" }
    );
  }
  
  /** Tests terms which span across boundaries */
  public void testHugeDoc() throws IOException {
    StringBuilder sb = new StringBuilder();
    char whitespace[] = new char[4094];
    Arrays.fill(whitespace, '\n');
    sb.append(whitespace);
    sb.append("testing 1234");
    String input = sb.toString();
    assertAnalyzesTo(sentenceAndWord, input, new String[] { "testing", "1234" });
  }
  
  /** Tests the handling of binary/malformed data */
  public void testHugeTerm() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10240; i++) {
      sb.append('a');
    }
    String input = sb.toString();
    char token[] = new char[1024];
    Arrays.fill(token, 'a');
    String expectedToken = new String(token);
    String expected[] = { 
        expectedToken, expectedToken, expectedToken, 
        expectedToken, expectedToken, expectedToken,
        expectedToken, expectedToken, expectedToken,
        expectedToken
    };
    assertAnalyzesTo(sentence, input, expected);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), sentence, 200*RANDOM_MULTIPLIER);
    checkRandomData(random(), sentenceAndWord, 200*RANDOM_MULTIPLIER);
  }

  // some tokenizers for testing
  
  /** silly tokenizer that just returns whole sentences as tokens */
  static class WholeSentenceTokenizer extends SegmentingTokenizerBase {
    int sentenceStart, sentenceEnd;
    boolean hasSentence;
    
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
    public WholeSentenceTokenizer() {
      super(newAttributeFactory(), BreakIterator.getSentenceInstance(Locale.ROOT));
    }

    @Override
    protected void setNextSentence(int sentenceStart, int sentenceEnd) {
      this.sentenceStart = sentenceStart;
      this.sentenceEnd = sentenceEnd;
      hasSentence = true;
    }

    @Override
    protected boolean incrementWord() {
      if (hasSentence) {
        hasSentence = false;
        clearAttributes();
        termAtt.copyBuffer(buffer, sentenceStart, sentenceEnd-sentenceStart);
        offsetAtt.setOffset(correctOffset(offset+sentenceStart), correctOffset(offset+sentenceEnd));
        return true;
      } else {
        return false;
      }
    }
  }
  
  /** 
   * simple tokenizer, that bumps posinc + 1 for tokens after a 
   * sentence boundary to inhibit phrase queries without slop.
   */
  static class SentenceAndWordTokenizer extends SegmentingTokenizerBase {
    int sentenceStart, sentenceEnd;
    int wordStart, wordEnd;
    int posBoost = -1; // initially set to -1 so the first word in the document doesn't get a pos boost
    
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    
    public SentenceAndWordTokenizer() {
      super(newAttributeFactory(), BreakIterator.getSentenceInstance(Locale.ROOT));
    }

    @Override
    protected void setNextSentence(int sentenceStart, int sentenceEnd) {
      this.wordStart = this.wordEnd = this.sentenceStart = sentenceStart;
      this.sentenceEnd = sentenceEnd;
      posBoost++;
    }
    
    @Override
    public void reset() throws IOException {
      super.reset();
      posBoost = -1;
    }

    @Override
    protected boolean incrementWord() {
      wordStart = wordEnd;
      while (wordStart < sentenceEnd) {
        if (Character.isLetterOrDigit(buffer[wordStart]))
          break;
        wordStart++;
      }
      
      if (wordStart == sentenceEnd) return false;
      
      wordEnd = wordStart+1;
      while (wordEnd < sentenceEnd && Character.isLetterOrDigit(buffer[wordEnd]))
        wordEnd++;
      
      clearAttributes();
      termAtt.copyBuffer(buffer, wordStart, wordEnd-wordStart);
      offsetAtt.setOffset(correctOffset(offset+wordStart), correctOffset(offset+wordEnd));
      posIncAtt.setPositionIncrement(posIncAtt.getPositionIncrement() + posBoost);
      posBoost = 0;
      return true;
    }
  }
}

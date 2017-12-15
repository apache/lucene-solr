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

package org.apache.lucene.analysis.opennlp;

import java.io.IOException;
import java.text.BreakIterator;
import java.text.CharacterIterator;

import org.apache.lucene.analysis.opennlp.tools.NLPSentenceDetectorOp;
import org.apache.lucene.analysis.opennlp.tools.OpenNLPOpsFactory;
import org.apache.lucene.analysis.util.CharArrayIterator;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.BeforeClass;

public class TestOpenNLPSentenceBreakIterator extends LuceneTestCase {

  private static final String TEXT
      //                                                                                                     111
      //           111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999000
      // 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012
      = "Sentence number 1 has 6 words. Sentence number 2, 5 words. And finally, sentence number 3 has 8 words.";
  private static final String[] SENTENCES = new String[] {
    "Sentence number 1 has 6 words. ", "Sentence number 2, 5 words. ", "And finally, sentence number 3 has 8 words." };
  private static final String PADDING = " Word. Word. ";
  private static final String sentenceModelFile = "en-test-sent.bin";


  @BeforeClass
  public static void populateCache() throws IOException {
    OpenNLPOpsFactory.getSentenceModel
        (sentenceModelFile, new ClasspathResourceLoader(TestOpenNLPSentenceBreakIterator.class));
  }

  public void testThreeSentences() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText(TEXT); // String is converted to StringCharacterIterator
    do3SentenceTest(bi);

    bi.setText(getCharArrayIterator(TEXT));
    do3SentenceTest(bi);
  }

  private CharacterIterator getCharArrayIterator(String text) {
    return getCharArrayIterator(text, 0, text.length());
  }

  private CharacterIterator getCharArrayIterator(String text, int start, int length) {
    CharArrayIterator charArrayIterator = new CharArrayIterator() {
      // Lie about all surrogates to the sentence tokenizer,
      // instead we treat them all as SContinue so we won't break around them.
      @Override
      protected char jreBugWorkaround(char ch) {
        return ch >= 0xD800 && ch <= 0xDFFF ? 0x002C : ch;
      }
    };
    charArrayIterator.setText(text.toCharArray(), start, length);
    return charArrayIterator;
  }

  private void do3SentenceTest(BreakIterator bi) {
    assertEquals(0, bi.current());
    assertEquals(0, bi.first());
    assertEquals(SENTENCES[0], TEXT.substring(bi.current(), bi.next()));
    assertEquals(SENTENCES[1], TEXT.substring(bi.current(), bi.next()));
    int current = bi.current();
    assertEquals(bi.getText().getEndIndex(), bi.next());
    int next = bi.current();
    assertEquals(SENTENCES[2], TEXT.substring(current, next));
    assertEquals(BreakIterator.DONE, bi.next());

    assertEquals(TEXT.length(), bi.last());
    int end = bi.current();
    assertEquals(SENTENCES[2], TEXT.substring(bi.previous(), end));
    end = bi.current();
    assertEquals(SENTENCES[1], TEXT.substring(bi.previous(), end));
    end = bi.current();
    assertEquals(SENTENCES[0], TEXT.substring(bi.previous(), end));
    assertEquals(BreakIterator.DONE, bi.previous());
    assertEquals(0, bi.current());

    assertEquals(59, bi.following(39));
    assertEquals(59, bi.following(31));
    assertEquals(31, bi.following(30));

    assertEquals(0, bi.preceding(57));
    assertEquals(0, bi.preceding(58));
    assertEquals(31, bi.preceding(59));

    assertEquals(0, bi.first());
    assertEquals(59, bi.next(2));
    assertEquals(0, bi.next(-2));
  }

  public void testSingleSentence() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText(getCharArrayIterator(SENTENCES[0]));
    test1Sentence(bi, SENTENCES[0]);
  }

  private void test1Sentence(BreakIterator bi, String text) {
    int start = bi.getText().getBeginIndex();
    assertEquals(start, bi.first());
    int current = bi.current();
    assertEquals(bi.getText().getEndIndex(), bi.next());
    int end = bi.current() - start;
    assertEquals(text, text.substring(current - start, end - start));

    assertEquals(text.length(), bi.last() - start);
    end = bi.current();
    bi.previous();
    assertEquals(BreakIterator.DONE, bi.previous());
    int previous = bi.current();
    assertEquals(text, text.substring(previous - start, end - start));
    assertEquals(start, bi.current());

    assertEquals(BreakIterator.DONE, bi.following(bi.last() / 2 + start));

    assertEquals(BreakIterator.DONE, bi.preceding(bi.last() / 2 + start));

    assertEquals(start, bi.first());
    assertEquals(BreakIterator.DONE, bi.next(13));
    assertEquals(BreakIterator.DONE, bi.next(-8));
  }

  public void testSliceEnd() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText(getCharArrayIterator(SENTENCES[0] + PADDING, 0, SENTENCES[0].length()));

    test1Sentence(bi, SENTENCES[0]);
  }

  public void testSliceStart() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText(getCharArrayIterator(PADDING + SENTENCES[0], PADDING.length(), SENTENCES[0].length()));
    test1Sentence(bi, SENTENCES[0]);
  }

  public void testSliceMiddle() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText(getCharArrayIterator(PADDING + SENTENCES[0] + PADDING, PADDING.length(), SENTENCES[0].length()));

    test1Sentence(bi, SENTENCES[0]);
  }

  /** the current position must be ignored, initial position is always first() */
  public void testFirstPosition() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText(getCharArrayIterator(SENTENCES[0]));
    assertEquals(SENTENCES[0].length(), bi.last()); // side-effect: set current position to last()
    test1Sentence(bi, SENTENCES[0]);
  }

  public void testWhitespaceOnly() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText("   \n \n\n\r\n\t  \n");
    test0Sentences(bi);
  }

  public void testEmptyString() throws Exception {
    NLPSentenceDetectorOp sentenceDetectorOp = OpenNLPOpsFactory.getSentenceDetector(sentenceModelFile);
    BreakIterator bi = new OpenNLPSentenceBreakIterator(sentenceDetectorOp);
    bi.setText("");
    test0Sentences(bi);
  }

  private void test0Sentences(BreakIterator bi) {
    assertEquals(0, bi.current());
    assertEquals(0, bi.first());
    assertEquals(BreakIterator.DONE, bi.next());
    assertEquals(0, bi.last());
    assertEquals(BreakIterator.DONE, bi.previous());
    assertEquals(BreakIterator.DONE, bi.following(0));
    assertEquals(BreakIterator.DONE, bi.preceding(0));
    assertEquals(0, bi.first());
    assertEquals(BreakIterator.DONE, bi.next(13));
    assertEquals(BreakIterator.DONE, bi.next(-8));
  }
}

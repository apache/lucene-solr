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

import java.text.BreakIterator;
import java.text.CharacterIterator;
import opennlp.tools.util.Span;
import org.apache.lucene.analysis.opennlp.tools.NLPSentenceDetectorOp;
import org.apache.lucene.analysis.util.CharArrayIterator;

/** A {@link BreakIterator} that splits sentences using an OpenNLP sentence chunking model. */
public final class OpenNLPSentenceBreakIterator extends BreakIterator {

  private CharacterIterator text;
  private int currentSentence;
  private int[] sentenceStarts;
  private NLPSentenceDetectorOp sentenceOp;

  public OpenNLPSentenceBreakIterator(NLPSentenceDetectorOp sentenceOp) {
    this.sentenceOp = sentenceOp;
  }

  @Override
  public int current() {
    return text.getIndex();
  }

  @Override
  public int first() {
    currentSentence = 0;
    text.setIndex(text.getBeginIndex());
    return current();
  }

  @Override
  public int last() {
    if (sentenceStarts.length > 0) {
      currentSentence = sentenceStarts.length - 1;
      text.setIndex(text.getEndIndex());
    } else { // there are no sentences; both the first and last positions are the begin index
      currentSentence = 0;
      text.setIndex(text.getBeginIndex());
    }
    return current();
  }

  @Override
  public int next() {
    if (text.getIndex() == text.getEndIndex() || 0 == sentenceStarts.length) {
      return DONE;
    } else if (currentSentence < sentenceStarts.length - 1) {
      text.setIndex(sentenceStarts[++currentSentence]);
      return current();
    } else {
      return last();
    }
  }

  @Override
  public int following(int pos) {
    if (pos < text.getBeginIndex() || pos > text.getEndIndex()) {
      throw new IllegalArgumentException("offset out of bounds");
    } else if (0 == sentenceStarts.length) {
      text.setIndex(text.getBeginIndex());
      return DONE;
    } else if (pos >= sentenceStarts[sentenceStarts.length - 1]) {
      // this conflicts with the javadocs, but matches actual behavior (Oracle has a bug in
      // something)
      // https://bugs.openjdk.java.net/browse/JDK-8015110
      text.setIndex(text.getEndIndex());
      currentSentence = sentenceStarts.length - 1;
      return DONE;
    } else { // there are at least two sentences
      currentSentence = (sentenceStarts.length - 1) / 2; // start search from the middle
      moveToSentenceAt(pos, 0, sentenceStarts.length - 2);
      text.setIndex(sentenceStarts[++currentSentence]);
      return current();
    }
  }

  /** Binary search over sentences */
  private void moveToSentenceAt(int pos, int minSentence, int maxSentence) {
    if (minSentence != maxSentence) {
      if (pos < sentenceStarts[currentSentence]) {
        int newMaxSentence = currentSentence - 1;
        currentSentence = minSentence + (currentSentence - minSentence) / 2;
        moveToSentenceAt(pos, minSentence, newMaxSentence);
      } else if (pos >= sentenceStarts[currentSentence + 1]) {
        int newMinSentence = currentSentence + 1;
        currentSentence = maxSentence - (maxSentence - currentSentence) / 2;
        moveToSentenceAt(pos, newMinSentence, maxSentence);
      }
    } else {
      assert currentSentence == minSentence;
      assert pos >= sentenceStarts[currentSentence];
      assert (currentSentence == sentenceStarts.length - 1 && pos <= text.getEndIndex())
          || pos < sentenceStarts[currentSentence + 1];
    }
    // we have arrived - nothing to do
  }

  @Override
  public int previous() {
    if (text.getIndex() == text.getBeginIndex()) {
      return DONE;
    } else {
      if (0 == sentenceStarts.length) {
        text.setIndex(text.getBeginIndex());
        return DONE;
      }
      if (text.getIndex() == text.getEndIndex()) {
        text.setIndex(sentenceStarts[currentSentence]);
      } else {
        text.setIndex(sentenceStarts[--currentSentence]);
      }
      return current();
    }
  }

  @Override
  public int preceding(int pos) {
    if (pos < text.getBeginIndex() || pos > text.getEndIndex()) {
      throw new IllegalArgumentException("offset out of bounds");
    } else if (0 == sentenceStarts.length) {
      text.setIndex(text.getBeginIndex());
      currentSentence = 0;
      return DONE;
    } else if (pos < sentenceStarts[0]) {
      // this conflicts with the javadocs, but matches actual behavior (Oracle has a bug in
      // something)
      // https://bugs.openjdk.java.net/browse/JDK-8015110
      text.setIndex(text.getBeginIndex());
      currentSentence = 0;
      return DONE;
    } else {
      currentSentence = sentenceStarts.length / 2; // start search from the middle
      moveToSentenceAt(pos, 0, sentenceStarts.length - 1);
      if (0 == currentSentence) {
        text.setIndex(text.getBeginIndex());
        return DONE;
      } else {
        text.setIndex(sentenceStarts[--currentSentence]);
        return current();
      }
    }
  }

  @Override
  public int next(int n) {
    currentSentence += n;
    if (n < 0) {
      if (text.getIndex() == text.getEndIndex()) {
        ++currentSentence;
      }
      if (currentSentence < 0) {
        currentSentence = 0;
        text.setIndex(text.getBeginIndex());
        return DONE;
      } else {
        text.setIndex(sentenceStarts[currentSentence]);
      }
    } else if (n > 0) {
      if (currentSentence >= sentenceStarts.length) {
        currentSentence = sentenceStarts.length - 1;
        text.setIndex(text.getEndIndex());
        return DONE;
      } else {
        text.setIndex(sentenceStarts[currentSentence]);
      }
    }
    return current();
  }

  @Override
  public CharacterIterator getText() {
    return text;
  }

  @Override
  public void setText(CharacterIterator newText) {
    text = newText;
    text.setIndex(text.getBeginIndex());
    currentSentence = 0;
    Span[] spans = sentenceOp.splitSentences(characterIteratorToString());
    sentenceStarts = new int[spans.length];
    for (int i = 0; i < spans.length; ++i) {
      // Adjust start positions to match those of the passed-in CharacterIterator
      sentenceStarts[i] = spans[i].getStart() + text.getBeginIndex();
    }
  }

  private String characterIteratorToString() {
    String fullText;
    if (text instanceof CharArrayIterator) {
      CharArrayIterator charArrayIterator = (CharArrayIterator) text;
      fullText =
          new String(
              charArrayIterator.getText(),
              charArrayIterator.getStart(),
              charArrayIterator.getLength());
    } else {
      // TODO: is there a better way to extract full text from arbitrary CharacterIterators?
      StringBuilder builder = new StringBuilder();
      for (char ch = text.first(); ch != CharacterIterator.DONE; ch = text.next()) {
        builder.append(ch);
      }
      fullText = builder.toString();
      text.setIndex(text.getBeginIndex());
    }
    return fullText;
  }
}

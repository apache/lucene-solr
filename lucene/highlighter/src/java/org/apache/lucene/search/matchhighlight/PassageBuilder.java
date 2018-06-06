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

package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.search.MatchesIterator;

public class PassageBuilder {

  private static final int PAD_WIDTH = 10;

  private final int snippetWidth;
  private final String markupStart = "<b>";
  private final String markupEnd = "</b>";

  private final BreakIterator wordBreakIterator = BreakIterator.getWordInstance(Locale.ROOT);

  private final List<String> passages = new ArrayList<>();

  private StringBuilder currentPassage;
  private int passageStart;
  private int passageEnd;

  public PassageBuilder(int snippetWidth) {
    this.snippetWidth = snippetWidth;
  }

  public Iterable<String> getTopPassages(int topN) {
    return passages.subList(0, topN);
  }

  public boolean build(String source, MatchesIterator mi, int offset) throws IOException {
    currentPassage = new StringBuilder();
    passageStart = passageEnd = 0;
    wordBreakIterator.setText(source);
    do {
      int startOffset = mi.startOffset() - offset;
      if (startOffset >= 0) {
        if (startOffset > source.length()) {
          finishPassage(source);
          return true;
        }
        int endOffset = mi.endOffset() - offset;
        if (currentPassage.length() == 0) {
          passageStart = startOffset;
          markup(source, startOffset, endOffset);
        }
        else {
          // subsequent passage
          // if total width of passage would exceed snippetWidth then finish this passage and start a new one
          if ((endOffset - passageStart + PAD_WIDTH * 2) > snippetWidth) {
            finishPassage(source);
            currentPassage = new StringBuilder();
            passageStart = startOffset;
            markup(source, startOffset, endOffset);
          }
          // otherwise we add to this passage
          else {
            currentPassage.append(source.substring(passageEnd, startOffset));
            markup(source, startOffset, endOffset);
          }
        }
      }
    } while (mi.next());
    finishPassage(source);
    return false;
  }

  private void markup(String source, int startOffset, int endOffset) {
    currentPassage.append(markupStart).append(source.substring(startOffset, endOffset)).append(markupEnd);
    passageEnd = endOffset;
  }

  private void finishPassage(String source) {
    int padWidth = Math.max(PAD_WIDTH, ((snippetWidth - (passageEnd - passageStart)) / 2));
    int start = Math.max(0, passageStart - padWidth);
    if (start != 0) {
      // we're not at the beginning of the source, so find an appropriate word boundary
      // to start on
      start = nextWordStartAfter(start, passageStart, source);
    }
    int endPadWidth = padWidth - (passageStart - start) + padWidth;
    int end = Math.min(source.length(), passageEnd + endPadWidth);
    if (end != source.length()) {
      end = wordBreakIterator.preceding(end);
    }
    String passage = source.substring(start, passageStart) + currentPassage.toString() + source.substring(passageEnd, end);
    passages.add(passage);
  }

  private int nextWordStartAfter(int position, int maxLength, String source) {
    int start = wordBreakIterator.following(position);
    while (start <= maxLength) {
      if (Character.isLetter(source.codePointAt(start))) {
        break;
      }
      start++;
    }
    return start;
  }

}

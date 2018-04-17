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

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.util.BytesRef;

public class SentencePassageBuilder implements PassageBuilder {

  private final BreakIterator breakIterator;
  private final String source;
  private final List<String> passages = new ArrayList<>();

  private StringBuilder currentPassage = new StringBuilder();
  private int matchStart = 0;
  private int matchEnd;

  public SentencePassageBuilder(String source) {
    this.breakIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
    this.breakIterator.setText(source);
    this.source = source;
    this.matchEnd = this.breakIterator.next();
  }

  @Override
  public void addMatch(BytesRef term, int startOffset, int endOffset) {
    if (startOffset > this.matchEnd) {
      finishPassage();
      while (matchEnd <= startOffset) {
        matchStart = matchEnd;
        matchEnd = breakIterator.next();
      }
    }
    currentPassage.append(this.source.substring(matchStart, startOffset));
    currentPassage.append("<b>");
    currentPassage.append(this.source.substring(startOffset, endOffset));
    currentPassage.append("</b>");
    matchStart = endOffset;
  }

  private void finishPassage() {
    if (currentPassage.length() == 0) {
      return;
    }
    currentPassage.append(this.source.substring(matchStart, matchEnd));
    passages.add(currentPassage.toString());
    currentPassage = new StringBuilder();
  }

  @Override
  public Iterable<String> getTopPassages(int topN) {
    finishPassage();
    return passages;
  }
}

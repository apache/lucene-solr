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
package org.apache.lucene.search.uhighlight;

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;

public class PassageStrategy {
  private final PassageScorer passageScorer;
  private final PassageFormatter passageFormatter;
  private final BreakIterator breakIterator;
  private final int maxNoHighlightPassages;

  public PassageStrategy(PassageScorer passageScorer, PassageFormatter passageFormatter, BreakIterator breakIterator, int maxNoHighlightPassages) {
    this.passageScorer = passageScorer;
    this.passageFormatter = passageFormatter;
    this.breakIterator = breakIterator;
    this.maxNoHighlightPassages = maxNoHighlightPassages;
  }

  public PassageScorer getPassageScorer() {
    return passageScorer;
  }

  public PassageFormatter getPassageFormatter() {
    return passageFormatter;
  }

  public BreakIterator getBreakIterator() {
    return breakIterator;
  }

  public int getMaxNoHighlightPassages() {
    return maxNoHighlightPassages;
  }

  public Passage[] getSummaryPassagesNoHighlight() {
    return getSummaryPassagesNoHighlight(maxNoHighlightPassages);
  }

  /**
   * Called to summarize a document when no highlights were found.
   * By default this just returns the first
   * {@code maxPassages} sentences; subclasses can override to customize.
   * The state of {@code bi} should be at the beginning.
   */
  public Passage[] getSummaryPassagesNoHighlight(int maxPassages) {
    assert breakIterator.current() == breakIterator.first();

    int finalMaxPassages = maxNoHighlightPassages == -1 ? maxPassages : maxNoHighlightPassages;
    // BreakIterator should be un-next'd:
    List<Passage> passages = new ArrayList<>(Math.min(finalMaxPassages, 10));
    int pos = breakIterator.current();
    assert pos == 0;
    while (passages.size() < finalMaxPassages) {
      int next = breakIterator.next();
      if (next == BreakIterator.DONE) {
        break;
      }
      Passage passage = new Passage();
      passage.score = Float.NaN;
      passage.startOffset = pos;
      passage.endOffset = next;
      passages.add(passage);
      pos = next;
    }

    return passages.toArray(new Passage[passages.size()]);
  }

}

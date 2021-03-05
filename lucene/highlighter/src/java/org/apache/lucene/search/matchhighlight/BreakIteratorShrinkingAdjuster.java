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
import java.util.Locale;

/**
 * A {@link PassageAdjuster} that adjusts the {@link Passage} range to word boundaries hinted by the
 * given {@link BreakIterator}.
 */
public class BreakIteratorShrinkingAdjuster implements PassageAdjuster {
  private final BreakIterator bi;
  private CharSequence value;

  public BreakIteratorShrinkingAdjuster() {
    this(BreakIterator.getWordInstance(Locale.ROOT));
  }

  public BreakIteratorShrinkingAdjuster(BreakIterator bi) {
    this.bi = bi;
  }

  @Override
  public void currentValue(CharSequence value) {
    this.value = value;
    bi.setText(new CharSequenceIterator(value));
  }

  @Override
  public OffsetRange adjust(Passage passage) {
    int from = passage.from;
    if (from > 0) {
      while (!bi.isBoundary(from)
          || (from < value.length() && Character.isWhitespace(value.charAt(from)))) {
        from = bi.following(from);
        if (from == BreakIterator.DONE) {
          from = passage.from;
          break;
        }
      }
      if (from == value.length()) {
        from = passage.from;
      }
    }

    int to = passage.to;
    if (to != value.length()) {
      while (!bi.isBoundary(to) || (to > 0 && Character.isWhitespace(value.charAt(to - 1)))) {
        to = bi.preceding(to);
        if (to == BreakIterator.DONE) {
          to = passage.to;
          break;
        }
      }
      if (to == 0) {
        to = passage.to;
      }
    }

    for (OffsetRange r : passage.markers) {
      from = Math.min(from, r.from);
      to = Math.max(to, r.to);
    }

    if (from > to) {
      from = to;
    }

    return new OffsetRange(from, to);
  }
}

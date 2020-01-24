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

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;

/**
 * Internal highlighter abstraction that operates on a per field basis.
 *
 * @lucene.internal
 */
public class FieldHighlighter {

  protected final String field;
  protected final FieldOffsetStrategy fieldOffsetStrategy;
  protected final BreakIterator breakIterator; // note: stateful!
  protected final PassageScorer passageScorer;
  protected final int maxPassages;
  protected final int maxNoHighlightPassages;
  protected final PassageFormatter passageFormatter;

  public FieldHighlighter(String field, FieldOffsetStrategy fieldOffsetStrategy, BreakIterator breakIterator,
                          PassageScorer passageScorer, int maxPassages, int maxNoHighlightPassages,
                          PassageFormatter passageFormatter) {
    this.field = field;
    this.fieldOffsetStrategy = fieldOffsetStrategy;
    this.breakIterator = breakIterator;
    this.passageScorer = passageScorer;
    this.maxPassages = maxPassages;
    this.maxNoHighlightPassages = maxNoHighlightPassages;
    this.passageFormatter = passageFormatter;
  }

  public String getField() {
    return field;
  }

  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return fieldOffsetStrategy.getOffsetSource();
  }

  /**
   * The primary method -- highlight this doc, assuming a specific field and given this content.
   */
  public Object highlightFieldForDoc(LeafReader reader, int docId, String content) throws IOException {
    // note: it'd be nice to accept a CharSequence for content, but we need a CharacterIterator impl for it.
    if (content.length() == 0) {
      return null; // nothing to do
    }

    breakIterator.setText(content);

    try (OffsetsEnum offsetsEnums = fieldOffsetStrategy.getOffsetsEnum(reader, docId, content)) {

      // Highlight the offsetsEnum list against the content to produce Passages.
      Passage[] passages = highlightOffsetsEnums(offsetsEnums);// and breakIterator & scorer

      // Format the resulting Passages.
      if (passages.length == 0) {
        // no passages were returned, so ask for a default summary
        passages = getSummaryPassagesNoHighlight(maxNoHighlightPassages == -1 ? maxPassages : maxNoHighlightPassages);
      }

      if (passages.length > 0) {
        return passageFormatter.format(passages, content);
      } else {
        return null;
      }
    }
  }

  /**
   * Called to summarize a document when no highlights were found.
   * By default this just returns the first
   * {@link #maxPassages} sentences; subclasses can override to customize.
   * The state of {@link #breakIterator} should be at the beginning.
   */
  protected Passage[] getSummaryPassagesNoHighlight(int maxPassages) {
    assert breakIterator.current() == breakIterator.first();

    List<Passage> passages = new ArrayList<>(Math.min(maxPassages, 10));
    int pos = breakIterator.current();
    assert pos == 0;
    while (passages.size() < maxPassages) {
      int next = breakIterator.next();
      if (next == BreakIterator.DONE) {
        break;
      }
      Passage passage = new Passage();
      passage.setStartOffset(pos);
      passage.setEndOffset(next);
      passages.add(passage);
      pos = next;
    }

    return passages.toArray(new Passage[passages.size()]);
  }

  // algorithm: treat sentence snippets as miniature documents
  // we can intersect these with the postings lists via BreakIterator.preceding(offset),s
  // score each sentence as norm(sentenceStartOffset) * sum(weight * tf(freq))
  protected Passage[] highlightOffsetsEnums(OffsetsEnum off)
      throws IOException {

    final int contentLength = this.breakIterator.getText().getEndIndex();

    if (off.nextPosition() == false) {
      return new Passage[0];
    }

    PriorityQueue<Passage> passageQueue = new PriorityQueue<>(Math.min(64, maxPassages + 1), (left, right) -> {
      if (left.getScore() < right.getScore()) {
        return -1;
      } else if (left.getScore() > right.getScore()) {
        return 1;
      } else {
        return left.getStartOffset() - right.getStartOffset();
      }
    });
    Passage passage = new Passage(); // the current passage in-progress.  Will either get reset or added to queue.
    int lastPassageEnd = 0;

    do {
      int start = off.startOffset();
      if (start == -1) {
        throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
      }
      int end = off.endOffset();
      if (start < contentLength && end > contentLength) {
        continue;
      }
      // See if this term should be part of a new passage.
      if (start >= passage.getEndOffset()) {
        passage = maybeAddPassage(passageQueue, passageScorer, passage, contentLength);
        // if we exceed limit, we are done
        if (start >= contentLength) {
          break;
        }
        // find fragment from the middle of the match, so the result's length may be closer to fragsize
        final int center = start + (end - start) / 2;
        // advance breakIterator
        passage.setStartOffset(Math.min(start, Math.max(this.breakIterator.preceding(Math.max(start + 1, center)), lastPassageEnd)));
        lastPassageEnd = Math.max(end, Math.min(this.breakIterator.following(Math.min(end - 1, center)), contentLength));
        passage.setEndOffset(lastPassageEnd);
      }
      // Add this term to the passage.
      BytesRef term = off.getTerm();// a reference; safe to refer to
      assert term != null;
      passage.addMatch(start, end, term, off.freq());
    } while (off.nextPosition());
    maybeAddPassage(passageQueue, passageScorer, passage, contentLength);

    Passage[] passages = passageQueue.toArray(new Passage[passageQueue.size()]);
    // sort in ascending order
    Arrays.sort(passages, Comparator.comparingInt(Passage::getStartOffset));
    return passages;
  }

  private Passage maybeAddPassage(PriorityQueue<Passage> passageQueue, PassageScorer scorer, Passage passage, int contentLength) {
    if (passage.getStartOffset() == -1) {
      // empty passage, we can ignore it
      return passage;
    }
    passage.setScore(scorer.score(passage, contentLength));
    // new sentence: first add 'passage' to queue
    if (passageQueue.size() == maxPassages && passage.getScore() < passageQueue.peek().getScore()) {
      passage.reset(); // can't compete, just reset it
    } else {
      passageQueue.offer(passage);
      if (passageQueue.size() > maxPassages) {
        passage = passageQueue.poll();
        passage.reset();
      } else {
        passage = new Passage();
      }
    }
    return passage;
  }

}

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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.similarities.Similarity;

final class ExactPhraseMatcher extends PhraseMatcher {

  private static class PostingsAndPosition {
    private final PostingsEnum postings;
    private final int offset;
    private int freq, upTo, pos;

    public PostingsAndPosition(PostingsEnum postings, int offset) {
      this.postings = postings;
      this.offset = offset;
    }
  }

  private final PostingsAndPosition[] postings;

  ExactPhraseMatcher(PhraseQuery.PostingsAndFreq[] postings, float matchCost) {
    super(approximation(postings), matchCost);
    List<PostingsAndPosition> postingsAndPositions = new ArrayList<>();
    for(PhraseQuery.PostingsAndFreq posting : postings) {
      postingsAndPositions.add(new PostingsAndPosition(posting.postings, posting.position));
    }
    this.postings = postingsAndPositions.toArray(new PostingsAndPosition[postingsAndPositions.size()]);
  }

  private static DocIdSetIterator approximation(PhraseQuery.PostingsAndFreq[] postings) {
    List<DocIdSetIterator> iterators = new ArrayList<>();
    for (PhraseQuery.PostingsAndFreq posting : postings) {
      iterators.add(posting.postings);
    }
    return ConjunctionDISI.intersectIterators(iterators);
  }

  @Override
  float maxFreq() {
    int minFreq = postings[0].freq;
    for (int i = 1; i < postings.length; i++) {
      minFreq = Math.min(minFreq, postings[i].freq);
    }
    return minFreq;
  }

  /** Advance the given pos enum to the first doc on or after {@code target}.
   *  Return {@code false} if the enum was exhausted before reaching
   *  {@code target} and {@code true} otherwise. */
  private static boolean advancePosition(PostingsAndPosition posting, int target) throws IOException {
    while (posting.pos < target) {
      if (posting.upTo == posting.freq) {
        return false;
      } else {
        posting.pos = posting.postings.nextPosition();
        posting.upTo += 1;
      }
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    for (PostingsAndPosition posting : postings) {
      posting.freq = posting.postings.freq();
      posting.pos = -1;
      posting.upTo = 0;
    }
  }

  @Override
  public boolean nextMatch() throws IOException {
    final PostingsAndPosition lead = postings[0];
    if (lead.upTo < lead.freq) {
      lead.pos = lead.postings.nextPosition();
      lead.upTo += 1;
    }
    else {
      return false;
    }
    advanceHead:
    while (true) {
      final int phrasePos = lead.pos - lead.offset;
      for (int j = 1; j < postings.length; ++j) {
        final PostingsAndPosition posting = postings[j];
        final int expectedPos = phrasePos + posting.offset;

        // advance up to the same position as the lead
        if (advancePosition(posting, expectedPos) == false) {
          break advanceHead;
        }

        if (posting.pos != expectedPos) { // we advanced too far
          if (advancePosition(lead, posting.pos - posting.offset + lead.offset)) {
            continue advanceHead;
          } else {
            break advanceHead;
          }
        }
      }
      return true;
    }
    return false;
  }

  @Override
  float sloppyWeight(Similarity.SimScorer simScorer) {
    return 1;
  }

  @Override
  public int startPosition() {
    return postings[0].pos;
  }

  @Override
  public int endPosition() {
    return postings[postings.length - 1].pos;
  }

  @Override
  public int startOffset() throws IOException {
    return postings[0].postings.startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    return postings[postings.length - 1].postings.endOffset();
  }

}

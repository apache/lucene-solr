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

final class ExactPhraseScorer extends Scorer {

  private static class PostingsAndPosition {
    private final PostingsEnum postings;
    private final int offset;
    private int freq, upTo, pos;

    public PostingsAndPosition(PostingsEnum postings, int offset) {
      this.postings = postings;
      this.offset = offset;
    }
  }

  private final DocIdSetIterator conjunction;
  private final PostingsAndPosition[] postings;

  private int freq;

  private final LeafSimScorer docScorer;
  private final boolean needsScores, needsTotalHitCount;
  private float matchCost;
  private float minCompetitiveScore;

  ExactPhraseScorer(Weight weight, PhraseQuery.PostingsAndFreq[] postings,
                    LeafSimScorer docScorer, ScoreMode scoreMode,
                    float matchCost) throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.needsScores = scoreMode.needsScores();
    this.needsTotalHitCount = scoreMode != ScoreMode.TOP_SCORES;

    List<DocIdSetIterator> iterators = new ArrayList<>();
    List<PostingsAndPosition> postingsAndPositions = new ArrayList<>();
    for(PhraseQuery.PostingsAndFreq posting : postings) {
      iterators.add(posting.postings);
      postingsAndPositions.add(new PostingsAndPosition(posting.postings, posting.position));
    }
    conjunction = ConjunctionDISI.intersectIterators(iterators);
    assert TwoPhaseIterator.unwrap(conjunction) == null;
    this.postings = postingsAndPositions.toArray(new PostingsAndPosition[postingsAndPositions.size()]);
    this.matchCost = matchCost;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    minCompetitiveScore = minScore;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return new TwoPhaseIterator(conjunction) {
      @Override
      public boolean matches() throws IOException {
        if (needsTotalHitCount == false && minCompetitiveScore > 0) {
          int minFreq = postings[0].postings.freq();
          for (int i = 1; i < postings.length; ++i) {
            minFreq = Math.min(postings[i].postings.freq(), minFreq);
          }
          if (docScorer.score(docID(), minFreq) < minCompetitiveScore) {
            // The maximum score we could get is less than the min competitive score
            return false;
          }
        }
        return phraseFreq() > 0;
      }

      @Override
      public float matchCost() {
        return matchCost;
      }
    };
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public String toString() {
    return "ExactPhraseScorer(" + weight + ")";
  }

  final int freq() {
    return freq;
  }

  @Override
  public int docID() {
    return conjunction.docID();
  }

  @Override
  public float score() throws IOException {
    return docScorer.score(docID(), freq);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return docScorer.maxScore();
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

  private int phraseFreq() throws IOException {
    // reset state
    final PostingsAndPosition[] postings = this.postings;
    for (PostingsAndPosition posting : postings) {
      posting.freq = posting.postings.freq();
      posting.pos = posting.postings.nextPosition();
      posting.upTo = 1;
    }

    int freq = 0;
    final PostingsAndPosition lead = postings[0];

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

      freq += 1;
      if (needsScores == false) {
        break;
      }

      if (lead.upTo == lead.freq) {
        break;
      }
      lead.pos = lead.postings.nextPosition();
      lead.upTo += 1;
    }

    return this.freq = freq;
  }

}

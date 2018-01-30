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


import java.util.Arrays;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Represents a passage (typically a sentence of the document).
 * <p>
 * A passage contains {@link #getNumMatches} highlights from the query,
 * and the offsets and query terms that correspond with each match.
 *
 * @lucene.experimental
 */
public class Passage {
  private int startOffset = -1;
  private int endOffset = -1;
  private float score = 0.0f;

  private int[] matchStarts = new int[8];
  private int[] matchEnds = new int[8];
  private BytesRef[] matchTerms = new BytesRef[8];
  private int numMatches = 0;
  private final BytesRefHash termsHash = new BytesRefHash();
  private int[] termFreqsInDoc = new int[8];
  private int[] termFreqsInPassage = new int[8];

  /** @lucene.internal */
  public void addMatch(int startOffset, int endOffset, BytesRef term, int termFreqInDoc) {
    assert startOffset >= this.startOffset && startOffset <= this.endOffset;
    if (numMatches == matchStarts.length) {
      int newLength = ArrayUtil.oversize(numMatches + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      int newMatchStarts[] = new int[newLength];
      int newMatchEnds[] = new int[newLength];
      BytesRef newMatchTerms[] = new BytesRef[newLength];
      System.arraycopy(matchStarts, 0, newMatchStarts, 0, numMatches);
      System.arraycopy(matchEnds, 0, newMatchEnds, 0, numMatches);
      System.arraycopy(matchTerms, 0, newMatchTerms, 0, numMatches);
      matchStarts = newMatchStarts;
      matchEnds = newMatchEnds;
      matchTerms = newMatchTerms;
    }
    assert matchStarts.length == matchEnds.length && matchEnds.length == matchTerms.length;
    matchStarts[numMatches] = startOffset;
    matchEnds[numMatches] = endOffset;
    matchTerms[numMatches] = term;
    numMatches++;
    int termIndex = termsHash.add(term);
    if (termIndex >= termFreqsInDoc.length) {
      int newLength = ArrayUtil.oversize(termFreqsInDoc.length + 1, Integer.BYTES);
      int newTermFreqsInDoc[] = new int[newLength];
      int newTermFreqsInPassage[] = new int[newLength];
      System.arraycopy(termFreqsInDoc, 0, newTermFreqsInDoc, 0, termFreqsInDoc.length);
      System.arraycopy(termFreqsInPassage, 0, newTermFreqsInPassage, 0, termFreqsInDoc.length);
      Arrays.fill(newTermFreqsInDoc, termFreqsInDoc.length, newTermFreqsInDoc.length - 1, 0);
      Arrays.fill(newTermFreqsInPassage, termFreqsInPassage.length, newTermFreqsInPassage.length - 1, 0);
      termFreqsInDoc = newTermFreqsInDoc;
      termFreqsInPassage = newTermFreqsInPassage;
    }
    if (termIndex < 0) {
      termIndex = -(termIndex + 1);
    }
    else {
      termFreqsInDoc[termIndex] = termFreqInDoc;
    }
    termFreqsInPassage[termIndex]++;
  }

  /** @lucene.internal */
  public void reset() {
    startOffset = endOffset = -1;
    score = 0.0f;
    numMatches = 0;
    Arrays.fill(termFreqsInPassage, 0);
    termsHash.clear();
    termsHash.reinit();
  }

  /** For debugging.  ex: Passage[0-22]{yin[0-3],yang[4-8],yin[10-13]}score=2.4964213 */
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Passage[").append(startOffset).append('-').append(endOffset).append(']');
    buf.append('{');
    for (int i = 0; i < numMatches; i++) {
      if (i != 0) {
        buf.append(',');
      }
      buf.append(matchTerms[i].utf8ToString());
      buf.append('[').append(matchStarts[i] - startOffset).append('-').append(matchEnds[i] - startOffset).append(']');
    }
    buf.append('}');
    buf.append("score=").append(score);
    return buf.toString();
  }

  /**
   * Start offset of this passage.
   *
   * @return start index (inclusive) of the passage in the
   * original content: always &gt;= 0.
   */
  public int getStartOffset() {
    return startOffset;
  }

  /**
   * End offset of this passage.
   *
   * @return end index (exclusive) of the passage in the
   * original content: always &gt;= {@link #getStartOffset()}
   */
  public int getEndOffset() {
    return endOffset;
  }

  public int getLength() {
    return endOffset - startOffset;
  }

  /**
   * Passage's score.
   */
  public float getScore() {
    return score;
  }

  public void setScore(PassageScorer scorer, int contentLength) {
    score = 0;
    for (int termIndex = 0; termIndex < termsHash.size(); termIndex++) {
      score += scorer.tf(termFreqsInPassage[termIndex], getLength()) * scorer.weight(contentLength, termFreqsInDoc[termIndex]);
    }
    score *= scorer.norm(startOffset);
  }

  /**
   * Number of term matches available in
   * {@link #getMatchStarts}, {@link #getMatchEnds},
   * {@link #getMatchTerms}
   */
  public int getNumMatches() {
    return numMatches;
  }

  /**
   * Start offsets of the term matches, in increasing order.
   * <p>
   * Only {@link #getNumMatches} are valid. Note that these
   * offsets are absolute (not relative to {@link #getStartOffset()}).
   */
  public int[] getMatchStarts() {
    return matchStarts;
  }

  /**
   * End offsets of the term matches, corresponding with {@link #getMatchStarts}.
   * <p>
   * Only {@link #getNumMatches} are valid. Note that its possible that an end offset
   * could exceed beyond the bounds of the passage ({@link #getEndOffset()}), if the
   * Analyzer produced a term which spans a passage boundary.
   */
  public int[] getMatchEnds() {
    return matchEnds;
  }

  /**
   * BytesRef (term text) of the matches, corresponding with {@link #getMatchStarts()}.
   * <p>
   * Only {@link #getNumMatches()} are valid.
   */
  public BytesRef[] getMatchTerms() {
    return matchTerms;
  }

  /** @lucene.internal */
  public void setStartOffset(int startOffset) {
    this.startOffset = startOffset;
  }

  /** @lucene.internal */
  public void setEndOffset(int endOffset) {
    assert startOffset <= endOffset;
    this.endOffset = endOffset;
  }

}

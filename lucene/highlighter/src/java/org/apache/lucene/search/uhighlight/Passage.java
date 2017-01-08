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


import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
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

  /** @lucene.internal */
  public void addMatch(int startOffset, int endOffset, BytesRef term) {
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
  }

  /** @lucene.internal */
  public void sort() {
    final int starts[] = matchStarts;
    final int ends[] = matchEnds;
    final BytesRef terms[] = matchTerms;
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        int temp = starts[i];
        starts[i] = starts[j];
        starts[j] = temp;

        temp = ends[i];
        ends[i] = ends[j];
        ends[j] = temp;

        BytesRef tempTerm = terms[i];
        terms[i] = terms[j];
        terms[j] = tempTerm;
      }

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(starts[i], starts[j]);
      }

    }.sort(0, numMatches);
  }

  /** @lucene.internal */
  public void reset() {
    startOffset = endOffset = -1;
    score = 0.0f;
    numMatches = 0;
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

  /**
   * Passage's score.
   */
  public float getScore() {
    return score;
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

  /** @lucene.internal */
  public void setScore(float score) {
    this.score = score;
  }
}

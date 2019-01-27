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
  private int[] matchTermFreqInDoc = new int[8];
  private int numMatches = 0;

  /** @lucene.internal */
  public void addMatch(int startOffset, int endOffset, BytesRef term, int termFreqInDoc) {
    assert startOffset >= this.startOffset && startOffset <= this.endOffset;
    if (numMatches == matchStarts.length) {
      int newLength = ArrayUtil.oversize(numMatches + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      int newMatchStarts[] = new int[newLength];
      int newMatchEnds[] = new int[newLength];
      int newMatchTermFreqInDoc[] = new int[newLength];
      BytesRef newMatchTerms[] = new BytesRef[newLength];
      System.arraycopy(matchStarts, 0, newMatchStarts, 0, numMatches);
      System.arraycopy(matchEnds, 0, newMatchEnds, 0, numMatches);
      System.arraycopy(matchTerms, 0, newMatchTerms, 0, numMatches);
      System.arraycopy(matchTermFreqInDoc, 0, newMatchTermFreqInDoc, 0, numMatches);
      matchStarts = newMatchStarts;
      matchEnds = newMatchEnds;
      matchTerms = newMatchTerms;
      matchTermFreqInDoc = newMatchTermFreqInDoc;
    }
    assert matchStarts.length == matchEnds.length && matchEnds.length == matchTerms.length;
    matchStarts[numMatches] = startOffset;
    matchEnds[numMatches] = endOffset;
    matchTerms[numMatches] = term;
    matchTermFreqInDoc[numMatches] = termFreqInDoc;
    numMatches++;
  }

  /** @lucene.internal */
  public void reset() {
    startOffset = endOffset = -1;
    score = 0.0f;
    numMatches = 0;
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

  public void setScore(float score) {
    this.score = score;
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
   * BytesRef (term text) of the matches, corresponding with {@link #getMatchStarts()}.  The primary purpose of this
   * method is to expose the number of unique terms per passage for use in passage scoring.
   * The actual term byte content is not well defined by this highlighter, and thus use of it is more subject to
   * change.
   * <p>
   * The term might be simply the analyzed term at this position.
   * Depending on the highlighter's configuration, the match term may be a phrase (instead of a word), and in such
   * a case might be a series of space-separated analyzed terms.
   * If the match is from a {@link org.apache.lucene.search.MultiTermQuery} then the match term may be the toString() of
   * that query.
   * <p>
   * Only {@link #getNumMatches()} are valid.
   */
  public BytesRef[] getMatchTerms() {
    return matchTerms;
  }

  public int[] getMatchTermFreqsInDoc() {
    return matchTermFreqInDoc;
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

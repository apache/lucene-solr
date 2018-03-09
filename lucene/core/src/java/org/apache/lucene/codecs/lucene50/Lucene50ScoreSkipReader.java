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
package org.apache.lucene.codecs.lucene50;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;

final class Lucene50ScoreSkipReader extends Lucene50SkipReader {

  private final SimScorer scorer;
  private final float[] maxScore;
  private final byte[][] impacts;
  private final int[] impactsLength;
  private final float globalMaxScore;
  private final ByteArrayDataInput badi = new ByteArrayDataInput();

  public Lucene50ScoreSkipReader(int version, IndexInput skipStream, int maxSkipLevels,
      boolean hasPos, boolean hasOffsets, boolean hasPayloads, SimScorer scorer) {
    super(version, skipStream, maxSkipLevels, hasPos, hasOffsets, hasPayloads);
    if (version < Lucene50PostingsFormat.VERSION_IMPACT_SKIP_DATA) {
      throw new IllegalStateException("Cannot skip based on scores if impacts are not indexed");
    }
    this.scorer = Objects.requireNonNull(scorer);
    this.maxScore = new float[maxSkipLevels];
    this.impacts = new byte[maxSkipLevels][];
    Arrays.fill(impacts, new byte[0]);
    this.impactsLength = new int[maxSkipLevels];
    this.globalMaxScore = scorer.score(Float.MAX_VALUE, 1);
  }

  @Override
  public void init(long skipPointer, long docBasePointer, long posBasePointer, long payBasePointer, int df) throws IOException {
    super.init(skipPointer, docBasePointer, posBasePointer, payBasePointer, df);
    Arrays.fill(impactsLength, 0);
    Arrays.fill(maxScore, globalMaxScore);
  }

  /** Upper bound of scores up to {@code upTo} included. */
  public float getMaxScore(int upTo) throws IOException {
    for (int level = 0; level < numberOfSkipLevels; ++level) {
      if (upTo <= skipDoc[level]) {
        return maxScore(level);
      }
    }
    return globalMaxScore;
  }

  private float maxScore(int level) throws IOException {
    assert level < numberOfSkipLevels;
    if (impactsLength[level] > 0) {
      badi.reset(impacts[level], 0, impactsLength[level]);
      maxScore[level] = readImpacts(badi, scorer);
      impactsLength[level] = 0;
    }
    return maxScore[level];
  }

  @Override
  protected void readImpacts(int level, IndexInput skipStream) throws IOException {
    int length = skipStream.readVInt();
    if (impacts[level].length < length) {
      impacts[level] = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
    }
    skipStream.readBytes(impacts[level], 0, length);
    impactsLength[level] = length;
  }

  static float readImpacts(ByteArrayDataInput in, SimScorer scorer) throws IOException {
    int freq = 0;
    long norm = 0;
    float maxScore = 0;
    while (in.getPosition() < in.length()) {
      int freqDelta = in.readVInt();
      if ((freqDelta & 0x01) != 0) {
        freq += 1 + (freqDelta >>> 1);
        norm += 1 + in.readZLong();
      } else {
        freq += 1 + (freqDelta >>> 1);
        norm++;
      }
      maxScore = Math.max(maxScore, scorer.score(freq, norm));
    }
    return maxScore;
  }

}

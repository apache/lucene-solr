package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.search.posfilter.Interval;
import org.apache.lucene.util.BytesRef;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public abstract class PhraseScorer extends Scorer {

  protected PhraseScorer(Weight weight) {
    super(weight);
  }

  protected int freq = -1;
  protected Interval[] positionCache = new Interval[4];
  private int currentPos = -1;

  @Override
  public final int freq() throws IOException {
    if (freq == -1) {
      cachePositions();
    }
    return freq;
  }

  private void cachePositions() throws IOException {
    assert freq == -1;
    int f = 0;
    while (doNextPosition() != NO_MORE_POSITIONS) {
      if (f >= positionCache.length) {
        Interval[] newCache = new Interval[positionCache.length * 2];
        System.arraycopy(positionCache, 0, newCache, 0, positionCache.length);
        positionCache = newCache;
      }
      positionCache[f] = new Interval(this);
      f++;
    }
    this.freq = f;
  }

  @Override
  public final int nextPosition() throws IOException {
    if (freq == -1)
      return doNextPosition();
    currentPos++;
    if (currentPos >= freq)
      return NO_MORE_POSITIONS;
    return positionCache[currentPos].begin;
  }

  @Override
  public final int startPosition() throws IOException {
    if (freq == -1)
      return doStartPosition();
    return positionCache[currentPos].begin;
  }

  @Override
  public final int endPosition() throws IOException {
    if (freq == -1)
      return doEndPosition();
    return positionCache[currentPos].end;
  }

  @Override
  public final int startOffset() throws IOException {
    if (freq == -1)
      return doStartOffset();
    return positionCache[currentPos].offsetBegin;
  }

  @Override
  public final int endOffset() throws IOException {
    if (freq == -1)
      return doEndOffset();
    return positionCache[currentPos].offsetEnd;
  }

  @Override
  public final BytesRef getPayload() throws IOException {
    return null;  // TODO - how to deal with payloads on intervals?
  }

  protected abstract int doNextPosition() throws IOException;

  protected abstract int doStartPosition() throws IOException;

  protected abstract int doEndPosition() throws IOException;

  protected abstract int doStartOffset() throws IOException;

  protected abstract int doEndOffset() throws IOException;
}

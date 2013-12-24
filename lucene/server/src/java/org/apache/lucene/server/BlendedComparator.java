package org.apache.lucene.server;

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

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.Scorer;

/** Blends score with another source; this sorts in reverse
 *  float order, i.e. higher scores are better. */

abstract class BlendedComparator extends FieldComparator<Float> {

  private final float[] scores;
  private float bottom;
  protected Scorer scorer;

  public BlendedComparator(int numHits) {
    scores = new float[numHits];
  }

  @Override
  public int compare(int slot1, int slot2) {
    return Float.compare(scores[slot2], scores[slot1]);
  }

  @Override
  public int compareBottom(int doc) {
    return Float.compare(getScore(doc), bottom);
  }

  @Override
  public void copy(int slot, int doc) throws IOException {
    scores[slot] = getScore(doc);
  }

  @Override
  public FieldComparator<Float> setNextReader(AtomicReaderContext context) throws IOException {
    return this;
  }

  @Override
  public void setBottom(final int bottom) {
    this.bottom = scores[bottom];
  }

  @Override
  public void setScorer(Scorer scorer) {
    // wrap with a ScoreCachingWrappingScorer so that successive calls to
    // score() will not incur score computation over and
    // over again.
    if (!(scorer instanceof ScoreCachingWrappingScorer)) {
      this.scorer = new ScoreCachingWrappingScorer(scorer);
    } else {
      this.scorer = scorer;
    }
  }

  @Override
  public Float value(int slot) {
    return Float.valueOf(scores[slot]);
  }

  // Override because we sort reverse of natural Float order:
  @Override
  public int compareValues(Float first, Float second) {
    // Reversed intentionally because relevance by default
    // sorts descending:
    return second.compareTo(first);
  }

  @Override
  public int compareDocToValue(int doc, Float valueObj) throws IOException {
    return Float.compare(valueObj.floatValue(), getScore(doc));
  }

  protected abstract float getScore(int doc);
}

package org.apache.lucene.search;

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
import java.util.Random;

import org.apache.lucene.index.AtomicReaderContext;

/** Randomize collection order. Don't forget to call {@link #flush()} when
 *  collection is finished to collect buffered documents. */
final class RandomOrderCollector extends Collector {

  final Random random;
  final Collector in;
  Scorer scorer;
  FakeScorer fakeScorer;

  int buffered;
  final int bufferSize;
  final int[] docIDs;
  final float[] scores;
  final int[] freqs;

  RandomOrderCollector(Random random, Collector in) {
    if (!in.acceptsDocsOutOfOrder()) {
      throw new IllegalArgumentException();
    }
    this.in = in;
    this.random = random;
    bufferSize = 1 + random.nextInt(100);
    docIDs = new int[bufferSize];
    scores = new float[bufferSize];
    freqs = new int[bufferSize];
    buffered = 0;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    fakeScorer = new FakeScorer();
    in.setScorer(fakeScorer);
  }

  private void shuffle() {
    for (int i = buffered - 1; i > 0; --i) {
      final int other = random.nextInt(i + 1);

      final int tmpDoc = docIDs[i];
      docIDs[i] = docIDs[other];
      docIDs[other] = tmpDoc;

      final float tmpScore = scores[i];
      scores[i] = scores[other];
      scores[other] = tmpScore;

      final int tmpFreq = freqs[i];
      freqs[i] = freqs[other];
      freqs[other] = tmpFreq;
    }
  }

  public void flush() throws IOException {
    shuffle();
    for (int i = 0; i < buffered; ++i) {
      fakeScorer.doc = docIDs[i];
      fakeScorer.freq = freqs[i];
      fakeScorer.score = scores[i];
      in.collect(fakeScorer.doc);
    }
    buffered = 0;
  }

  @Override
  public void collect(int doc) throws IOException {
    docIDs[buffered] = doc;
    scores[buffered] = scorer.score();
    try {
      freqs[buffered] = scorer.freq();
    } catch (UnsupportedOperationException e) {
      freqs[buffered] = -1;
    }
    if (++buffered == bufferSize) {
      flush();
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return in.acceptsDocsOutOfOrder();
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

}
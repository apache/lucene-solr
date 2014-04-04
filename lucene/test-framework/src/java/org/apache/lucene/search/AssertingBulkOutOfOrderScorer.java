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
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.WeakHashMap;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.util.VirtualMethod;

/** A crazy {@link BulkScorer} that wraps a {@link Scorer}
 *  but shuffles the order of the collected documents. */
public class AssertingBulkOutOfOrderScorer extends BulkScorer {

  final Random random;
  final Scorer scorer;

  public AssertingBulkOutOfOrderScorer(Random random, Scorer scorer) {
    this.random = random;
    this.scorer = scorer;
  }

  private void shuffle(int[] docIDs, float[] scores, int[] freqs, int size) {
    for (int i = size - 1; i > 0; --i) {
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

  private static void flush(int[] docIDs, float[] scores, int[] freqs, int size,
      FakeScorer scorer, LeafCollector collector) throws IOException {
    for (int i = 0; i < size; ++i) {
      scorer.doc = docIDs[i];
      scorer.freq = freqs[i];
      scorer.score = scores[i];
      collector.collect(scorer.doc);
    }
  }

  @Override
  public boolean score(LeafCollector collector, int max) throws IOException {
    if (scorer.docID() == -1) {
      scorer.nextDoc();
    }

    FakeScorer fake = new FakeScorer();
    collector.setScorer(fake);

    final int bufferSize = 1 + random.nextInt(100);
    final int[] docIDs = new int[bufferSize];
    final float[] scores = new float[bufferSize];
    final int[] freqs = new int[bufferSize];

    int buffered = 0;
    int doc = scorer.docID();
    while (doc < max) {
      docIDs[buffered] = doc;
      scores[buffered] = scorer.score();
      freqs[buffered] = scorer.freq();

      if (++buffered == bufferSize) {
        shuffle(docIDs, scores, freqs, buffered);
        flush(docIDs, scores, freqs, buffered, fake, collector);
        buffered = 0;
      }
      doc = scorer.nextDoc();
    }

    shuffle(docIDs, scores, freqs, buffered);
    flush(docIDs, scores, freqs, buffered, fake, collector);

    return doc != Scorer.NO_MORE_DOCS;
  }

  @Override
  public String toString() {
    return "AssertingBulkOutOfOrderScorer(" + scorer + ")";
  }
}

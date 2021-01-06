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
package org.apache.lucene.sandbox.search;

import static org.apache.lucene.sandbox.search.BM25FQuery.FieldAndWeight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.SmallFloat;

/** Copy of {@link LeafSimScorer} that sums document's norms from multiple fields. */
final class MultiNormsLeafSimScorer {
  /** Cache of decoded norms. */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  private final SimScorer scorer;
  private final NumericDocValues norms;

  /** Sole constructor: Score documents of {@code reader} with {@code scorer}. */
  MultiNormsLeafSimScorer(
      SimScorer scorer,
      LeafReader reader,
      Collection<FieldAndWeight> normFields,
      boolean needsScores)
      throws IOException {
    this.scorer = Objects.requireNonNull(scorer);
    if (needsScores) {
      final List<NumericDocValues> normsList = new ArrayList<>();
      final List<Float> weightList = new ArrayList<>();
      for (FieldAndWeight field : normFields) {
        NumericDocValues norms = reader.getNormValues(field.field);
        if (norms != null) {
          normsList.add(norms);
          weightList.add(field.weight);
        }
      }
      if (normsList.isEmpty()) {
        norms = null;
      } else if (normsList.size() == 1) {
        norms = normsList.get(0);
      } else {
        final NumericDocValues[] normsArr = normsList.toArray(new NumericDocValues[0]);
        final float[] weightArr = new float[normsList.size()];
        for (int i = 0; i < weightList.size(); i++) {
          weightArr[i] = weightList.get(i);
        }
        norms = new MultiFieldNormValues(normsArr, weightArr);
      }
    } else {
      norms = null;
    }
  }

  private long getNormValue(int doc) throws IOException {
    if (norms != null) {
      boolean found = norms.advanceExact(doc);
      assert found;
      return norms.longValue();
    } else {
      return 1L; // default norm
    }
  }

  /**
   * Score the provided document assuming the given term document frequency. This method must be
   * called on non-decreasing sequences of doc ids.
   *
   * @see SimScorer#score(float, long)
   */
  public float score(int doc, float freq) throws IOException {
    return scorer.score(freq, getNormValue(doc));
  }

  /**
   * Explain the score for the provided document assuming the given term document frequency. This
   * method must be called on non-decreasing sequences of doc ids.
   *
   * @see SimScorer#explain(Explanation, long)
   */
  public Explanation explain(int doc, Explanation freqExpl) throws IOException {
    return scorer.explain(freqExpl, getNormValue(doc));
  }

  private static class MultiFieldNormValues extends NumericDocValues {
    private final NumericDocValues[] normsArr;
    private final float[] weightArr;
    private long current;
    private int docID = -1;

    MultiFieldNormValues(NumericDocValues[] normsArr, float[] weightArr) {
      this.normsArr = normsArr;
      this.weightArr = weightArr;
    }

    @Override
    public long longValue() {
      return current;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      float normValue = 0;
      for (int i = 0; i < normsArr.length; i++) {
        boolean found = normsArr[i].advanceExact(target);
        assert found;
        normValue +=
            weightArr[i] * LENGTH_TABLE[Byte.toUnsignedInt((byte) normsArr[i].longValue())];
      }
      current = SmallFloat.intToByte4(Math.round(normValue));
      return true;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }
  }
}

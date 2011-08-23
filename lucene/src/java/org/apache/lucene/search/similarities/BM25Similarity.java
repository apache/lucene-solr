package org.apache.lucene.search.similarities;

/**
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

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.TermContext;

/**
 * BM25 Similarity. Introduced in Stephen E. Robertson, Steve Walker,
 * Susan Jones, Micheline Hancock-Beaulieu, and Mike Gatford. Okapi at TREC-3.
 * In Proceedings of the Third Text REtrieval Conference (TREC 1994).
 * Gaithersburg, USA, November 1994.
 */
public class BM25Similarity extends Similarity {
  private final float k1;
  private final float b;
  
  public BM25Similarity(float k1, float b) {
    this.k1 = k1;
    this.b  = b;
  }
  
  /** Sets the default values for BM25:
   * <ul>
   *   <li>{@code k1 = 1.2},
   *   <li>{@code b = 0.75}.</li>
   * </ul>
   */
  public BM25Similarity() {
    this.k1 = 1.2f;
    this.b  = 0.75f;
  }
  
  /** Implemented as <code>log(1 + (numDocs - docFreq + 0.5)/(docFreq + 0.5))</code>. */
  public float idf(int docFreq, int numDocs) {
    return (float) Math.log(1 + ((numDocs - docFreq + 0.5D)/(docFreq + 0.5D)));
  }
  
  /** Implemented as <code>1 / (distance + 1)</code>. */
  public float sloppyFreq(int distance) {
    return 1.0f / (distance + 1);
  }
  
  /** The default implementation returns <code>1</code> */
  public float scorePayload(int doc, int start, int end, BytesRef payload) {
    return 1;
  }
  
  /** return avg doc length across the field (or 1 if the codec does not store sumTotalTermFreq) */
  public float avgFieldLength(IndexSearcher searcher, String field) throws IOException {
    long sumTotalTermFreq = MultiFields.getTerms(searcher.getIndexReader(), field).getSumTotalTermFreq();
    long maxdoc = searcher.getIndexReader().maxDoc();
    return sumTotalTermFreq == -1 ? 1f : (float) (sumTotalTermFreq / (double) maxdoc);
  }

  @Override
  public byte computeNorm(FieldInvertState state) {
    final int numTerms = state.getLength() - state.getNumOverlap();
    return encodeNormValue(state.getBoost() / (float) Math.sqrt(numTerms));
  }
  
  public float decodeNormValue(byte b) {
    return NORM_TABLE[b & 0xFF];
  }

  public byte encodeNormValue(float f) {
    return SmallFloat.floatToByte315(f);
  }
  
  /** Cache of decoded bytes. */
  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      float f = SmallFloat.byte315ToFloat((byte)i);
      NORM_TABLE[i] = 1.0f / (f*f);
    }
  }

  @Override
  public Stats computeStats(IndexSearcher searcher, String fieldName, float queryBoost, TermContext... termStats) throws IOException {
    float value = 0.0f;
    final int max = searcher.maxDoc();
    
    for (final TermContext stat : termStats ) {
      value += idf(stat.docFreq(), max);
    }
    
    return new BM25Stats(value, queryBoost, avgFieldLength(searcher, fieldName));
  }

  @Override
  public final ExactDocScorer exactDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException {
    return new ExactBM25DocScorer((BM25Stats) stats, context.reader.norms(fieldName));
  }

  @Override
  public final SloppyDocScorer sloppyDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException {
    return new SloppyBM25DocScorer((BM25Stats) stats, context.reader.norms(fieldName));
  }
  
  private class ExactBM25DocScorer extends ExactDocScorer {
    private final float weightValue;
    private final byte[] norms;
    private final float avgdl;
    
    ExactBM25DocScorer(BM25Stats stats, byte norms[]) {
      this.weightValue = stats.weight;
      this.avgdl = stats.avgdl;
      this.norms = norms;
    }
    
    // todo: optimize
    @Override
    public float score(int doc, int freq) {
      // if there are no norms, we act as if b=0
      float norm = norms == null ? k1 : k1 * ((1 - b) + b * (decodeNormValue(norms[doc])) / (avgdl));
      return weightValue * (freq * (k1 + 1)) / (freq + norm);
    }
  }
  
  private class SloppyBM25DocScorer extends SloppyDocScorer {
    private final float weightValue;
    private final byte[] norms;
    private final float avgdl;
    
    SloppyBM25DocScorer(BM25Stats stats, byte norms[]) {
      this.weightValue = stats.weight;
      this.avgdl = stats.avgdl;
      this.norms = norms;
    }
    
    // todo: optimize
    @Override
    public float score(int doc, float freq) {
      // if there are no norms, we act as if b=0
      float norm = norms == null ? k1 : k1 * ((1 - b) + b * (decodeNormValue(norms[doc])) / (avgdl));
      return weightValue * (freq * (k1 + 1)) / (freq + norm);
    }

    @Override
    public float computeSlopFactor(int distance) {
      return sloppyFreq(distance);
    }

    @Override
    public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
      return scorePayload(doc, start, end, payload);
    }
  }
  
  /** Collection statistics for the BM25 model. */
  private static class BM25Stats extends Stats {
    /** BM25's idf */
    private final float idf;
    /** The average document length. */
    private final float avgdl;
    /** query's inner boost */
    private final float queryBoost;
    /** weight (idf * boost) */
    private float weight;

    BM25Stats(float idf, float queryBoost, float avgdl) {
      this.idf = idf;
      this.queryBoost = queryBoost;
      this.avgdl = avgdl;
    }

    @Override
    public float getValueForNormalization() {
      // we return a TF-IDF like normalization to be nice, but we don't actually normalize ourselves.
      final float queryWeight = idf * queryBoost;
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      // we don't normalize with queryNorm at all, we just capture the top-level boost
      this.weight = idf * queryBoost * topLevelBoost;
    } 
  }
}

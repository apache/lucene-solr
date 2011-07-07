package org.apache.lucene.search;

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
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.TermContext;
import org.apache.lucene.util.SmallFloat;

/**
 * BM25 Similarity.
 */
public class MockBM25Similarity extends Similarity {
  // TODO: the norm table can probably be per-sim so you can configure these
  // its also pretty nice that we don't bake the parameter into the index... you can tune it at runtime.
  private static final float k1 = 2f;
  private static final float b = 0.75f;
  
  /**
   * Our normalization is k1 * ((1 - b) + b * numTerms / avgNumTerms)
   * currently we put doclen into the boost byte (divided by boost) for simple quantization
   * our decoder precomputes the full formula into the norm table
   * 
   * this is pretty crappy for doc/field boosting, but with a static schema you can boost per-field
   * in your sim anyway (sorta dumb to bake into the index)
   */
  @Override
  public byte computeNorm(FieldInvertState state) {
    final int numTerms = state.getLength() - state.getNumOverlap();
    return encodeNormValue(numTerms / state.getBoost());
  }
  
  /** Cache of decoded bytes. */
  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte)i);
    }
  }
  
  public float decodeNormValue(byte b) {
    return NORM_TABLE[b & 0xFF];
  }

  public byte encodeNormValue(float f) {
    return SmallFloat.floatToByte315(f);
  }

  @Override
  public float sloppyFreq(int distance) {
    return 1.0f / (distance + 1);
  }

  // weight for a term as log(1 + ((n - dfj + 0.5F)/(dfj + 0.5F)))
  // TODO: are we summing this in the right place for phrase estimation????
  @Override
  public Stats computeStats(IndexSearcher searcher, String fieldName, float queryBoost, TermContext... termStats) throws IOException {
    float value = 0.0f;
    final StringBuilder exp = new StringBuilder();

    final int max = searcher.maxDoc();
    
    for (final TermContext stat : termStats ) {
      final int dfj = stat.docFreq();
      value += Math.log(1 + ((max - dfj + 0.5F)/(dfj + 0.5F)));
      exp.append(" ");
      exp.append(dfj);
    }
    
    return new BM25Stats(value, queryBoost, avgDocumentLength(searcher, fieldName));
  }

  @Override
  public ExactDocScorer exactDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException {
    return new ExactBM25DocScorer((BM25Stats) stats, context.reader.norms(fieldName));
  }

  @Override
  public SloppyDocScorer sloppyDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException {
    return new SloppyBM25DocScorer((BM25Stats) stats, context.reader.norms(fieldName));
  }
  
  /** return avg doc length across the field (or 1 if the codec does not store sumTotalTermFreq) */
  private float avgDocumentLength(IndexSearcher searcher, String field) throws IOException {
    if (!searcher.reader.hasNorms(field)) {
      return 0f;
    } else {
      long sumTotalTermFreq = MultiFields.getTerms(searcher.reader, field).getSumTotalTermFreq();
      long maxdoc = searcher.reader.maxDoc();
      return sumTotalTermFreq == -1 ? 1f : (float) (sumTotalTermFreq / (double) maxdoc);
    }
  }

  private class ExactBM25DocScorer extends ExactDocScorer {
    private final float weightValue;
    private final byte[] norms;
    private final float avgdl;
    
    ExactBM25DocScorer(BM25Stats stats, byte norms[]) {
      // we incorporate boost here up front... maybe we should multiply by tf instead?
      this.weightValue = stats.idf * stats.queryBoost * stats.topLevelBoost;
      this.avgdl = stats.avgdl;
      this.norms = norms;
    }
    
    // todo: optimize
    @Override
    public float score(int doc, int freq) {
      float norm = norms == null ? 0 : k1 * ((1 - b) + b * (decodeNormValue(norms[doc])) / (avgdl));
      return weightValue * (freq * (k1 + 1)) / (freq + norm);
    }
  }
  
  private class SloppyBM25DocScorer extends SloppyDocScorer {
    private final float weightValue;
    private final byte[] norms;
    private final float avgdl;
    
    SloppyBM25DocScorer(BM25Stats stats, byte norms[]) {
      // we incorporate boost here up front... maybe we should multiply by tf instead?
      this.weightValue = stats.idf * stats.queryBoost * stats.topLevelBoost;
      this.avgdl = stats.avgdl;
      this.norms = norms;
    }
    
    // todo: optimize
    @Override
    public float score(int doc, float freq) {
      float norm = norms == null ? 0 : k1 * ((1 - b) + b * (decodeNormValue(norms[doc])) / (avgdl));
      return weightValue * (freq * (k1 + 1)) / (freq + norm);
    }
  }
  
  /** Collection statistics for the BM25 model. */
  public static class BM25Stats extends Stats {
    /** BM25's idf */
    private final float idf;
    /** The average document length. */
    private final float avgdl;
    /** query's inner boost */
    private final float queryBoost;
    /** any outer query's boost */
    private float topLevelBoost;

    public BM25Stats(float idf, float queryBoost, float avgdl) {
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
      this.topLevelBoost = topLevelBoost;
    } 
  }
}

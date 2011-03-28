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
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Explanation.IDFExplanation;
import org.apache.lucene.util.ReaderUtil;
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
  public float computeNorm(FieldInvertState state) {
    final int numTerms = state.getLength() - state.getNumOverlap();
    return numTerms / state.getBoost();
  }
  
  /** Cache of decoded bytes. */
  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte)i);
    }
  }
  
  @Override
  public float decodeNormValue(byte b) {
    return NORM_TABLE[b & 0xFF];
  }

  @Override
  public byte encodeNormValue(float f) {
    return SmallFloat.floatToByte315(f);
  }

  @Override
  public float sloppyFreq(int distance) {
    return 1.0f / (distance + 1);
  }

  // weight for a term as log(1 + ((n - dfj + 0.5F)/(dfj + 0.5F)))
  // nocommit: nuke IDFExplanation!
  // nocommit: are we summing this in the right place for phrase estimation????
  @Override
  public IDFExplanation computeWeight(IndexSearcher searcher, String fieldName, TermContext... termStats) throws IOException {
    float value = 0.0f;
    final StringBuilder exp = new StringBuilder();

    final int max = searcher.maxDoc();
    
    for (final TermContext stat : termStats ) {
      final int dfj = stat.docFreq();
      value += Math.log(1 + ((max - dfj + 0.5F)/(dfj + 0.5F)));
      exp.append(" ");
      exp.append(dfj);
    }
    
    final float idfValue = value;
    return new IDFExplanation() {
      @Override
      public float getIdf() {
        return idfValue;
      }
      @Override
      public String explain() {
        return exp.toString();
      }
    };
  }

  @Override
  public ExactDocScorer exactDocScorer(Weight weight, String fieldName, AtomicReaderContext context) throws IOException {
    byte[] norms = context.reader.norms(fieldName);
    float avgdl = norms == null ? 0f : avgDocumentLength(fieldName, context);
    return new ExactBM25DocScorer((float) Math.sqrt(weight.getValue()), norms, avgdl);
  }

  @Override
  public SloppyDocScorer sloppyDocScorer(Weight weight, String fieldName, AtomicReaderContext context) throws IOException {
    byte[] norms = context.reader.norms(fieldName);
    float avgdl = norms == null ? 0f : avgDocumentLength(fieldName, context);
    return new SloppyBM25DocScorer((float) Math.sqrt(weight.getValue()), norms, avgdl);
  }
  
  private float avgDocumentLength(String field, ReaderContext context) throws IOException {
    // nocommit: crap that we calc this over and over redundantly for each segment (we should just do it once in the weight, once its generalized)
    context = ReaderUtil.getTopLevelContext(context);
    long normsum = context.reader.getSumOfNorms(field);
    long maxdoc = context.reader.maxDoc();
    int avgnorm = (int) (normsum / (double) maxdoc);
    return decodeNormValue((byte)avgnorm);
  }

  private class ExactBM25DocScorer extends ExactDocScorer {
    private final float weightValue;
    private final byte[] norms;
    private final float avgdl;
    
    ExactBM25DocScorer(float weightValue, byte norms[], float avgdl) {
      this.weightValue = weightValue;
      this.norms = norms;
      this.avgdl = avgdl;
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
    
    SloppyBM25DocScorer(float weightValue, byte norms[], float avgdl) {
      this.weightValue = weightValue;
      this.norms = norms;
      this.avgdl = avgdl;
    }
    
    // todo: optimize
    @Override
    public float score(int doc, float freq) {
      float norm = norms == null ? 0 : k1 * ((1 - b) + b * (decodeNormValue(norms[doc])) / (avgdl));
      return weightValue * (freq * (k1 + 1)) / (freq + norm);
    }
  }
}

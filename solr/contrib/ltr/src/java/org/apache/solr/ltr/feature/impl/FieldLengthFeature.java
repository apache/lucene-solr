package org.apache.solr.ltr.feature.impl;

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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.SmallFloat;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.ranking.FeatureScorer;
import org.apache.solr.ltr.ranking.FeatureWeight;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.NamedParams;

public class FieldLengthFeature extends Feature {
  String field;

  /** Cache of decoded bytes. */

  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte) i);

    }

  }

  /**
   * Decodes the norm value, assuming it is a single byte.
   *
   */

  private final float decodeNorm(long norm) {
    return NORM_TABLE[(int) (norm & 0xFF)]; // & 0xFF maps negative bytes to
    // positive above 127
  }

  public FieldLengthFeature() {

  }

  public void init(String name, NamedParams params, int id)
      throws FeatureException {
    super.init(name, params, id);
    if (!params.containsKey("field")) {
      throw new FeatureException("missing param field");
    }
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    this.field = (String) params.get("field");
    return new FieldLengthFeatureWeight(searcher, name, params, norm, id);
  }

  @Override
  public String toString(String f) {
    return "FieldLengthFeature [field:" + field + "]";

  }

  public class FieldLengthFeatureWeight extends FeatureWeight {

    public FieldLengthFeatureWeight(IndexSearcher searcher, String name,
        NamedParams params, Normalizer norm, int id) {
      super(FieldLengthFeature.this, searcher, name, params, norm, id);
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      return new FieldLengthFeatureScorer(this, context);

    }

    public class FieldLengthFeatureScorer extends FeatureScorer {

      LeafReaderContext context = null;
      NumericDocValues norms = null;
      DocIdSetIterator itr;

      public FieldLengthFeatureScorer(FeatureWeight weight,
          LeafReaderContext context) throws IOException {
        super(weight);
        this.context = context;
        this.itr = new MatchAllIterator();
        norms = context.reader().getNormValues(field);

        // In the constructor, docId is -1, so using 0 as default lookup
        IndexableField idxF = searcher.doc(0).getField(field);
        if (idxF.fieldType().omitNorms()) throw new IOException(
            "FieldLengthFeatures can't be used if omitNorms is enabled (field="
                + field + ")");

      }

      @Override
      public float score() throws IOException {

        long l = norms.get(itr.docID());
        float norm = decodeNorm(l);
        float numTerms = (float) Math.pow(1f / norm, 2);

        return numTerms;
      }

      @Override
      public String toString() {
        return "FieldLengthFeature [name=" + name + " field=" + field + "]";
      }

      @Override
      public int docID() {
        return itr.docID();
      }

      @Override
      public DocIdSetIterator iterator() {
        return itr;
      }

    }
  }

}

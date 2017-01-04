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
package org.apache.solr.ltr.feature;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SmallFloat;
import org.apache.solr.request.SolrQueryRequest;
/**
 * This feature returns the length of a field (in terms) for the current document.
 * Example configuration:
 * <pre>{
  "name":  "titleLength",
  "class": "org.apache.solr.ltr.feature.FieldLengthFeature",
  "params": {
      "field": "title"
  }
}</pre>
 * Note: since this feature relies on norms values that are stored in a single byte
 * the value of the feature could have a lightly different value.
 * (see also {@link org.apache.lucene.search.similarities.ClassicSimilarity})
 **/
public class FieldLengthFeature extends Feature {

  private String field;

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>(1, 1.0f);
    params.put("field", field);
    return params;
  }

  @Override
  protected void validate() throws FeatureException {
    if (field == null || field.isEmpty()) {
      throw new FeatureException(getClass().getSimpleName()+
          ": field must be provided");
    }
  }

  /** Cache of decoded bytes. */

  private static final float[] NORM_TABLE = new float[256];

  static {
    NORM_TABLE[0] = 0;
    for (int i = 1; i < 256; i++) {
      float norm = SmallFloat.byte315ToFloat((byte) i);
      NORM_TABLE[i] = 1.0f / (norm * norm);
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

  public FieldLengthFeature(String name, Map<String,Object> params) {
    super(name, params);
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores,
      SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi)
          throws IOException {

    return new FieldLengthFeatureWeight(searcher, request, originalQuery, efi);
  }


  public class FieldLengthFeatureWeight extends FeatureWeight {

    public FieldLengthFeatureWeight(IndexSearcher searcher,
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) {
      super(FieldLengthFeature.this, searcher, request, originalQuery, efi);
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      NumericDocValues norms = context.reader().getNormValues(field);
      if (norms == null){
        return new ValueFeatureScorer(this, 0f,
            DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
      }
      return new FieldLengthFeatureScorer(this, norms);
    }

    public class FieldLengthFeatureScorer extends FeatureScorer {

      NumericDocValues norms = null;

      public FieldLengthFeatureScorer(FeatureWeight weight,
          NumericDocValues norms) throws IOException {
        super(weight, norms);
        this.norms = norms;

        // In the constructor, docId is -1, so using 0 as default lookup
        final IndexableField idxF = searcher.doc(0).getField(field);
        if (idxF.fieldType().omitNorms()) {
          throw new IOException(
              "FieldLengthFeatures can't be used if omitNorms is enabled (field="
                  + field + ")");
        }
      }

      @Override
      public float score() throws IOException {

        final long l = norms.longValue();
        final float numTerms = decodeNorm(l);
        return numTerms;
      }
    }
  }

}

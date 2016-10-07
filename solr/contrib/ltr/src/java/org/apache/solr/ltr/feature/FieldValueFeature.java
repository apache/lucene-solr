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
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;

import com.google.common.collect.Sets;
/**
 * This feature returns the value of a field in the current document
 * Example configuration:
 * <pre>{
  "name":  "rawHits",
  "class": "org.apache.solr.ltr.feature.FieldValueFeature",
  "params": {
      "field": "hits"
  }
}</pre>
 */
public class FieldValueFeature extends Feature {

  private String field;
  private Set<String> fieldAsSet;

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
    fieldAsSet = Sets.newHashSet(field);
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>(1, 1.0f);
    params.put("field", field);
    return params;
  }

  public FieldValueFeature(String name, Map<String,Object> params) {
    super(name, params);
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, 
      SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi)
      throws IOException {
    return new FieldValueFeatureWeight(searcher, request, originalQuery, efi);
  }

  public class FieldValueFeatureWeight extends FeatureWeight {

    public FieldValueFeatureWeight(IndexSearcher searcher, 
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) {
      super(FieldValueFeature.this, searcher, request, originalQuery, efi);
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      return new FieldValueFeatureScorer(this, context,
          DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
    }

    public class FieldValueFeatureScorer extends FeatureScorer {

      LeafReaderContext context = null;

      public FieldValueFeatureScorer(FeatureWeight weight,
          LeafReaderContext context, DocIdSetIterator itr) {
        super(weight, itr);
        this.context = context;
      }

      @Override
      public float score() throws IOException {

        try {
          final Document document = context.reader().document(itr.docID(),
              fieldAsSet);
          final IndexableField indexableField = document.getField(field);
          if (indexableField == null) {
            return getDefaultValue();
          }
          final Number number = indexableField.numericValue();
          if (number != null) {
            return number.floatValue();
          } else {
            final String string = indexableField.stringValue();
            // boolean values in the index are encoded with the
            // chars T/F
            if (string.equals("T")) {
              return 1;
            }
            if (string.equals("F")) {
              return 0;
            }
          }
        } catch (final IOException e) {
          throw new FeatureException(
              e.toString() + ": " +
              "Unable to extract feature for "
              + name, e);
        }
        return getDefaultValue();
      }
    }
  }
}

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.ranking.FeatureScorer;
import org.apache.solr.ltr.ranking.FeatureWeight;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.NamedParams;

public class ValueFeature extends Feature {

  protected float configValue = -1f;
  protected String configValueStr = null;

  public ValueFeature() {}

  @Override
  public void init(String name, NamedParams params, int id)
      throws FeatureException {
    super.init(name, params, id);
    Object paramValue = params.get("value");
    if (paramValue == null) {
      throw new FeatureException("Missing the field 'value' in params for "
          + this);
    }

    if (paramValue instanceof String) {
      this.configValueStr = (String) paramValue;
      if (this.configValueStr.trim().isEmpty()) {
        throw new FeatureException("Empty field 'value' in params for " + this);
      }
    } else {
      try {
        this.configValue = NamedParams.convertToFloat(paramValue);
      } catch (NumberFormatException e) {
        throw new FeatureException("Invalid type for 'value' in params for "
            + this);
      }
    }
  }

  @Override
  public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    return new ValueFeatureWeight(searcher, name, params, norm, id);
  }

  public class ValueFeatureWeight extends FeatureWeight {

    protected float featureValue;

    public ValueFeatureWeight(IndexSearcher searcher, String name,
        NamedParams params, Normalizer norm, int id) {
      super(ValueFeature.this, searcher, name, params, norm, id);
    }

    @Override
    public void process() throws IOException {
      // Value replace from external feature info if applicable. Each request
      // can change the
      // value if it is using ${myExternalValue} for the configValueStr,
      // otherwise use the
      // constant value provided in the config.
      if (configValueStr != null) {
        featureValue = Float.parseFloat(macroExpander.expand(configValueStr));
      } else {
        featureValue = configValue;
      }
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {
      return new ValueFeatureScorer(this, featureValue, "ValueFeature");
    }

    /**
     * Default FeatureScorer class that returns the score passed in. Can be used
     * as a simple ValueFeature, or to return a default scorer in case an
     * underlying feature's scorer is null.
     */
    public class ValueFeatureScorer extends FeatureScorer {

      float constScore;
      String featureType;
      DocIdSetIterator itr;

      public ValueFeatureScorer(FeatureWeight weight, float constScore,
          String featureType) {
        super(weight);
        this.constScore = constScore;
        this.featureType = featureType;
        this.itr = new MatchAllIterator();
      }

      @Override
      public float score() {
        return constScore;
      }

      @Override
      public String toString() {
        return featureType + " [name=" + name + " value=" + constScore + "]";
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

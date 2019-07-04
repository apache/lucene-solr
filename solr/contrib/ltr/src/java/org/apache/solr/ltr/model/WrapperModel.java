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

package org.apache.solr.ltr.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.Normalizer;

/**
 * A scoring model that wraps the other model.
 *
 * <p>This model loads a model from an external resource during the initialization.
 * The way of fetching the wrapped model is depended on
 * the implementation of {@link WrapperModel#fetchModelMap()}.
 *
 * <p>This model doesn't hold the actual parameters of the wrapped model,
 * thus it can manage large models which are difficult to upload to ZooKeeper.
 *
 * <p>Example configuration:
 * <pre>{
    "class": "...",
    "name": "myModelName",
    "params": {
        ...
    }
 }</pre>
 *
 * <p>NOTE: no "features" are configured in the wrapper model
 * because the wrapped model's features will be used instead.
 * Also note that if a "store" is configured for the wrapper
 * model then it must match the "store" of the wrapped model.
 */
public abstract class WrapperModel extends AdapterModel {

  protected LTRScoringModel model;

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((model == null) ? 0 : model.hashCode());
    result = prime * result + ((solrResourceLoader == null) ? 0 : solrResourceLoader.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    WrapperModel other = (WrapperModel) obj;
    if (model == null) {
      if (other.model != null) return false;
    } else if (!model.equals(other.model)) return false;
    if (solrResourceLoader == null) {
      if (other.solrResourceLoader != null) return false;
    } else if (!solrResourceLoader.equals(other.solrResourceLoader)) return false;
    return true;
  }

  public WrapperModel(String name, List<Feature> features, List<Normalizer> norms, String featureStoreName,
                      List<Feature> allFeatures, Map<String, Object> params) {
    super(name, features, norms, featureStoreName, allFeatures, params);
  }

  @Override
  protected void validate() throws ModelException {
    if (!features.isEmpty()) {
      throw new ModelException("features must be empty for the wrapper model " + name);
    }
    if (!norms.isEmpty()) {
      throw new ModelException("norms must be empty for the wrapper model " + name);
    }

    if (model != null) {
      super.validate();
      model.validate();
      // check feature store names match
      final String wrappedFeatureStoreName = model.getFeatureStoreName();
      if (wrappedFeatureStoreName == null || !wrappedFeatureStoreName.equals(this.getFeatureStoreName())) {
        throw new ModelException("wrapper feature store name ("+this.getFeatureStoreName() +")"
            + " must match the "
            + "wrapped feature store name ("+wrappedFeatureStoreName+")");
      }
    }
  }

  public void updateModel(LTRScoringModel model) {
    this.model = model;
    validate();
  }

  /*
   * The child classes must implement how to fetch the definition of the wrapped model.
   */
  public abstract Map<String, Object> fetchModelMap() throws ModelException;

  @Override
  public List<Normalizer> getNorms() {
    return model.getNorms();
  }

  @Override
  public List<Feature> getFeatures() {
    return model.getFeatures();
  }

  @Override
  public Collection<Feature> getAllFeatures() {
    return model.getAllFeatures();
  }

  @Override
  public long ramBytesUsed() {
    return model.ramBytesUsed();
  }

  @Override
  public float score(float[] modelFeatureValuesNormalized) {
    return model.score(modelFeatureValuesNormalized);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc, float finalScore,
                             List<Explanation> featureExplanations) {
    return model.explain(context, doc, finalScore, featureExplanations);
  }

  @Override
  public void normalizeFeaturesInPlace(float[] modelFeatureValues) {
    model.normalizeFeaturesInPlace(modelFeatureValues);
  }

  @Override
  public Explanation getNormalizerExplanation(Explanation e, int idx) {
    return model.getNormalizerExplanation(e, idx);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("(name=").append(getName());
    sb.append(",model=(").append(model.toString()).append(")");

    return sb.toString();
  }

}

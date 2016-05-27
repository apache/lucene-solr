package org.apache.solr.ltr.feature;

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

import java.util.Collection;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.util.NamedParams;

/**
 * Contains all the data needed for loading a model.
 */
// FIXME: Rename to something like RankingAlgorithm or ScoringAlgorithm
public abstract class ModelMetadata {

  private String name;
  private String type;
  private String featureStoreName;
  private List<Feature> features;
  private Collection<Feature> allFeatures;
  private NamedParams params;

  public ModelMetadata(String name, String type, List<Feature> features,
      String featureStoreName, Collection<Feature> allFeatures,
      NamedParams params) {
    this.name = name;
    this.type = type;
    this.features = features;
    this.featureStoreName = featureStoreName;
    this.allFeatures = allFeatures;
    this.params = params;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * @return the features
   */
  public List<Feature> getFeatures() {
    return features;
  }

  public NamedParams getParams() {
    return params;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((features == null) ? 0 : features.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((params == null) ? 0 : params.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ModelMetadata other = (ModelMetadata) obj;
    if (features == null) {
      if (other.features != null) return false;
    } else if (!features.equals(other.features)) return false;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    if (params == null) {
      if (other.params != null) return false;
    } else if (!params.equals(other.params)) return false;
    if (type == null) {
      if (other.type != null) return false;
    } else if (!type.equals(other.type)) return false;
    return true;
  }

  public boolean hasParams() {
    return !(params == null || params.isEmpty());
  }

  public Collection<Feature> getAllFeatures() {
    return allFeatures;
  }

  public String getFeatureStoreName() {
    return featureStoreName;
  }

  /**
   * Given a list of normalized values for all features a scoring algorithm
   * cares about, calculate and return a score.
   *
   * @param modelFeatureValuesNormalized
   *          List of normalized feature values. Each feature is identified by
   *          its id, which is the index in the array
   * @return The final score for a document
   */
  public abstract float score(float[] modelFeatureValuesNormalized);

  /**
   * Similar to the score() function, except it returns an explanation of how
   * the features were used to calculate the score.
   *
   * @param context
   *          Context the document is in
   * @param doc
   *          Document to explain
   * @param finalScore
   *          Original score
   * @param featureExplanations
   *          Explanations for each feature calculation
   * @return Explanation for the scoring of a doument
   */
  public abstract Explanation explain(LeafReaderContext context, int doc,
      float finalScore, List<Explanation> featureExplanations);

}

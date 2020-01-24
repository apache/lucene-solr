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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.util.SolrPluginUtils;

/**
 * A scoring model computes scores that can be used to rerank documents.
 * <p>
 * A scoring model consists of
 * <ul>
 * <li> a list of features ({@link Feature}) and
 * <li> a list of normalizers ({@link Normalizer}) plus
 * <li> parameters or configuration to represent the scoring algorithm.
 * </ul>
 * <p>
 * Example configuration (snippet):
 * <pre>{
   "class" : "...",
   "name" : "myModelName",
   "features" : [
       {
         "name" : "isBook"
       },
       {
         "name" : "originalScore",
         "norm": {
             "class" : "org.apache.solr.ltr.norm.StandardNormalizer",
             "params" : { "avg":"100", "std":"10" }
         }
       },
       {
         "name" : "price",
         "norm": {
             "class" : "org.apache.solr.ltr.norm.MinMaxNormalizer",
             "params" : { "min":"0", "max":"1000" }
         }
       }
   ],
   "params" : {
       ...
   }
}</pre>
 * <p>
 * {@link LTRScoringModel} is an abstract class and concrete classes must
 * implement the {@link #score(float[])} and
 * {@link #explain(LeafReaderContext, int, float, List)} methods.
 */
public abstract class LTRScoringModel implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(LTRScoringModel.class);

  protected final String name;
  private final String featureStoreName;
  protected final List<Feature> features;
  private final List<Feature> allFeatures;
  private final Map<String,Object> params;
  protected final List<Normalizer> norms;
  private Integer hashCode; // cached since it shouldn't actually change after construction

  public static LTRScoringModel getInstance(SolrResourceLoader solrResourceLoader,
      String className, String name, List<Feature> features,
      List<Normalizer> norms,
      String featureStoreName, List<Feature> allFeatures,
      Map<String,Object> params) throws ModelException {
    final LTRScoringModel model;
    try {
      // create an instance of the model
      model = solrResourceLoader.newInstance(
          className,
          LTRScoringModel.class,
          new String[0], // no sub packages
          new Class[] { String.class, List.class, List.class, String.class, List.class, Map.class },
          new Object[] { name, features, norms, featureStoreName, allFeatures, params });
      if (params != null) {
        SolrPluginUtils.invokeSetters(model, params.entrySet());
      }
    } catch (final Exception e) {
      throw new ModelException("Model type does not exist " + className, e);
    }
    model.validate();
    return model;
  }

  public LTRScoringModel(String name, List<Feature> features,
      List<Normalizer> norms,
      String featureStoreName, List<Feature> allFeatures,
      Map<String,Object> params) {
    this.name = name;
    this.features = features != null ? Collections.unmodifiableList(new ArrayList<>(features)) : null;
    this.featureStoreName = featureStoreName;
    this.allFeatures = allFeatures != null ? Collections.unmodifiableList(new ArrayList<>(allFeatures)) : null;
    this.params = params != null ? Collections.unmodifiableMap(new LinkedHashMap<>(params)) : null;
    this.norms = norms != null ? Collections.unmodifiableList(new ArrayList<>(norms)) : null;
  }

  /**
   * Validate that settings make sense and throws
   * {@link ModelException} if they do not make sense.
   */
  protected void validate() throws ModelException {
    final List<Feature> features = getFeatures();
    final List<Normalizer> norms = getNorms();
    if (features.isEmpty()) {
      throw new ModelException("no features declared for model "+name);
    }
    final HashSet<String> featureNames = new HashSet<>();
    for (final Feature feature : features) {
      final String featureName = feature.getName();
      if (!featureNames.add(featureName)) {
        throw new ModelException("duplicated feature "+featureName+" in model "+name);
      }
    }
    if (features.size() != norms.size()) {
      throw new ModelException("counted "+features.size()+" features and "+norms.size()+" norms in model "+name);
    }
  }

  /**
   * @return the norms
   */
  public List<Normalizer> getNorms() {
    return norms;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the features
   */
  public List<Feature> getFeatures() {
    return features;
  }

  public Map<String,Object> getParams() {
    return params;
  }

  @Override
  public int hashCode() {
    if(hashCode == null) {
      hashCode = calculateHashCode();
    }
    return hashCode;
  }

  final private int calculateHashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Objects.hashCode(features);
    result = (prime * result) + Objects.hashCode(name);
    result = (prime * result) + Objects.hashCode(params);
    result = (prime * result) + Objects.hashCode(norms);
    result = (prime * result) + Objects.hashCode(featureStoreName);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final LTRScoringModel other = (LTRScoringModel) obj;
    if (features == null) {
      if (other.features != null) {
        return false;
      }
    } else if (!features.equals(other.features)) {
      return false;
    }
    if (norms == null) {
      if (other.norms != null) {
        return false;
      }
    } else if (!norms.equals(other.norms)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (params == null) {
      if (other.params != null) {
        return false;
      }
    } else if (!params.equals(other.params)) {
      return false;
    }
    if (featureStoreName == null) {
      if (other.featureStoreName != null) {
        return false;
      }
    } else if (!featureStoreName.equals(other.featureStoreName)) {
      return false;
    }


    return true;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(allFeatures, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.sizeOfObject(features, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.sizeOfObject(featureStoreName) +
        RamUsageEstimator.sizeOfObject(name) +
        RamUsageEstimator.sizeOfObject(norms) +
        RamUsageEstimator.sizeOfObject(params);
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
   * @return Explanation for the scoring of a document
   */
  public abstract Explanation explain(LeafReaderContext context, int doc,
      float finalScore, List<Explanation> featureExplanations);

  @Override
  public String toString() {
    return  getClass().getSimpleName() + "(name="+getName()+")";
  }

  /**
   * Goes through all the stored feature values, and calculates the normalized
   * values for all the features that will be used for scoring.
   */
  public void normalizeFeaturesInPlace(float[] modelFeatureValues) {
    float[] modelFeatureValuesNormalized = modelFeatureValues;
    if (modelFeatureValues.length != norms.size()) {
      throw new FeatureException("Must have normalizer for every feature");
    }
    for(int idx = 0; idx < modelFeatureValuesNormalized.length; ++idx) {
      modelFeatureValuesNormalized[idx] =
          norms.get(idx).normalize(modelFeatureValuesNormalized[idx]);
    }
  }

  public Explanation getNormalizerExplanation(Explanation e, int idx) {
    Normalizer n = norms.get(idx);
    if (n != IdentityNormalizer.INSTANCE) {
      return n.explain(e);
    }
    return e;
  }

}

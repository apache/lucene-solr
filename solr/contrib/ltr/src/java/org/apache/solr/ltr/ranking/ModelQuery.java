package org.apache.solr.ltr.ranking;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Scorer.ChildScorer;
import org.apache.solr.ltr.feature.ModelMetadata;
import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.feature.norm.impl.IdentityNormalizer;
import org.apache.solr.ltr.log.FeatureLogger;
import org.apache.solr.request.SolrQueryRequest;

/**
 * The ranking query that is run, reranking results using the ModelMetadata
 * algorithm
 */
public class ModelQuery extends Query {

  // contains a description of the model
  protected ModelMetadata meta;
  // feature logger to output the features.
  private FeatureLogger fl = null;
  // Map of external parameters, such as query intent, that can be used by
  // features
  protected Map<String,String> efi;
  // Original solr query used to fetch matching documents
  protected Query originalQuery;
  // Original solr request
  protected SolrQueryRequest request;

  public ModelQuery(ModelMetadata meta) {
    this.meta = meta;
  }

  public ModelMetadata getMetadata() {
    return meta;
  }

  public void setFeatureLogger(FeatureLogger fl) {
    this.fl = fl;
  }

  public FeatureLogger getFeatureLogger() {
    return this.fl;
  }

  public Collection<Feature> getAllFeatures() {
    return meta.getAllFeatures();
  }

  public void setOriginalQuery(Query mainQuery) {
    this.originalQuery = mainQuery;
  }

  public void setExternalFeatureInfo(Map<String,String> externalFeatureInfo) {
    this.efi = externalFeatureInfo;
  }

  public void setRequest(SolrQueryRequest request) {
    this.request = request;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((meta == null) ? 0 : meta.hashCode());
    result = prime * result
        + ((originalQuery == null) ? 0 : originalQuery.hashCode());
    result = prime * result + ((efi == null) ? 0 : originalQuery.hashCode());
    result = prime * result + this.toString().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    ModelQuery other = (ModelQuery) obj;
    if (meta == null) {
      if (other.meta != null) return false;
    } else if (!meta.equals(other.meta)) return false;
    if (originalQuery == null) {
      if (other.originalQuery != null) return false;
    } else if (!originalQuery.equals(other.originalQuery)) return false;
    return true;
  }

  public SolrQueryRequest getRequest() {
    return request;
  }

  public List<Feature> getFeatures() {
    return meta.getFeatures();
  }

  @Override
  public ModelWeight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    Collection<Feature> features = this.getAllFeatures();
    List<Feature> modelFeatures = this.getFeatures();

    return new ModelWeight(searcher, getWeights(modelFeatures, searcher,
        needsScores), getWeights(features, searcher, needsScores));
  }

  private FeatureWeight[] getWeights(Collection<Feature> features,
      IndexSearcher searcher, boolean needsScores) throws IOException {
    FeatureWeight[] arr = new FeatureWeight[features.size()];
    int i = 0;
    SolrQueryRequest req = this.getRequest();
    // since the feature store is a linkedhashmap order is preserved
    for (Feature f : features) {
      FeatureWeight fw = f.createWeight(searcher, needsScores);
      fw.setRequest(req);
      fw.setOriginalQuery(originalQuery);
      fw.setExternalFeatureInfo(efi);
      fw.process();
      arr[i] = fw;
      ++i;
    }
    return arr;
  }

  @Override
  public String toString(String field) {
    return field;
  }

  public class ModelWeight extends Weight {

    IndexSearcher searcher;

    // List of the model's features used for scoring. This is a subset of the
    // features used for logging.
    FeatureWeight[] modelFeatures;
    float[] modelFeatureValuesNormalized;

    // List of all the feature values, used for both scoring and logging
    FeatureWeight[] allFeatureWeights;
    float[] allFeatureValues;
    String[] allFeatureNames;
    boolean[] allFeaturesUsed;

    public ModelWeight(IndexSearcher searcher, FeatureWeight[] modelFeatures,
        FeatureWeight[] allFeatures) {
      super(ModelQuery.this);
      this.searcher = searcher;
      this.allFeatureWeights = allFeatures;
      this.modelFeatures = modelFeatures;
      this.modelFeatureValuesNormalized = new float[modelFeatures.length];
      this.allFeatureValues = new float[allFeatures.length];
      this.allFeatureNames = new String[allFeatures.length];
      this.allFeaturesUsed = new boolean[allFeatures.length];

      for (int i = 0; i < allFeatures.length; ++i) {
        allFeatureNames[i] = allFeatures[i].getName();
      }
    }

    /**
     * Goes through all the stored feature values, and calculates the normalized
     * values for all the features that will be used for scoring.
     */
    public void normalize() {
      int pos = 0;
      for (FeatureWeight feature : modelFeatures) {
        int featureId = feature.getId();
        if (allFeaturesUsed[featureId]) {
          Normalizer norm = feature.getNorm();
          modelFeatureValuesNormalized[pos] = norm
              .normalize(allFeatureValues[featureId]);
        } else {
          modelFeatureValuesNormalized[pos] = feature.getDefaultValue();
        }
        pos++;
      }
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc)
        throws IOException {
      // FIXME: This explain doens't skip null scorers like the scorer()
      // function
      Explanation[] explanations = new Explanation[allFeatureValues.length];
      int index = 0;
      for (FeatureWeight feature : allFeatureWeights) {
        explanations[index++] = feature.explain(context, doc);
      }

      List<Explanation> featureExplanations = new ArrayList<>();
      for (FeatureWeight f : modelFeatures) {
        Normalizer n = f.getNorm();
        Explanation e = explanations[f.id];
        if (n != IdentityNormalizer.INSTANCE) e = n.explain(e);
        featureExplanations.add(e);
      }
      // TODO this calls twice the scorers, could be optimized.
      ModelScorer bs = scorer(context);
      bs.iterator().advance(doc);

      float finalScore = bs.score();

      return meta.explain(context, doc, finalScore, featureExplanations);

    }

    @Override
    public float getValueForNormalization() throws IOException {
      return 1;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      for (FeatureWeight feature : allFeatureWeights) {
        feature.normalize(norm, topLevelBoost);
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (FeatureWeight feature : allFeatureWeights) {
        feature.extractTerms(terms);
      }
    }

    protected void reset() {
      for (int i = 0, len = allFeaturesUsed.length; i < len; i++) {
        allFeaturesUsed[i] = false;
      }
    }

    @Override
    public ModelScorer scorer(LeafReaderContext context) throws IOException {
      List<FeatureScorer> featureScorers = new ArrayList<FeatureScorer>(
          allFeatureWeights.length);
      for (int i = 0; i < allFeatureWeights.length; i++) {
        FeatureScorer scorer = allFeatureWeights[i].scorer(context);
        if (scorer != null) {
          featureScorers.add(allFeatureWeights[i].scorer(context));
        }
      }

      // Always return a ModelScorer, even if no features match, because we
      // always need to call
      // score on the model for every document, since 0 features matching could
      // return a
      // non 0 score for a given model.
      return new ModelScorer(this, featureScorers);
    }

    public class ModelScorer extends Scorer {
      protected HashMap<String,Object> docInfo;
      protected Scorer featureTraversalScorer;

      public ModelScorer(Weight weight, List<FeatureScorer> featureScorers) {
        super(weight);
        docInfo = new HashMap<String,Object>();
        for (FeatureScorer subSocer : featureScorers) {
          subSocer.setDocInfo(docInfo);
        }

        if (featureScorers.size() <= 1) { // TODO: Allow the use of dense
                                          // features in other cases
          featureTraversalScorer = new DenseModelScorer(weight, featureScorers);
        } else {
          featureTraversalScorer = new SparseModelScorer(weight, featureScorers);
        }
      }

      @Override
      public Collection<ChildScorer> getChildren() {
        return featureTraversalScorer.getChildren();
      }

      public void setDocInfoParam(String key, Object value) {
        docInfo.put(key, value);
      }

      @Override
      public int docID() {
        return featureTraversalScorer.docID();
      }

      @Override
      public float score() throws IOException {
        return featureTraversalScorer.score();
      }

      @Override
      public int freq() throws IOException {
        return featureTraversalScorer.freq();
      }

      @Override
      public DocIdSetIterator iterator() {
        return featureTraversalScorer.iterator();
      }

      public class SparseModelScorer extends Scorer {
        protected DisiPriorityQueue subScorers;
        protected ModelQuerySparseIterator itr;

        protected int targetDoc = -1;
        protected int activeDoc = -1;

        protected SparseModelScorer(Weight weight,
            List<FeatureScorer> featureScorers) {
          super(weight);
          if (featureScorers.size() <= 1) {
            throw new IllegalArgumentException(
                "There must be at least 2 subScorers");
          }
          this.subScorers = new DisiPriorityQueue(featureScorers.size());
          for (Scorer scorer : featureScorers) {
            final DisiWrapper w = new DisiWrapper(scorer);
            this.subScorers.add(w);
          }

          itr = new ModelQuerySparseIterator(this.subScorers);
        }

        @Override
        public int docID() {
          return itr.docID();
        }

        @Override
        public float score() throws IOException {
          DisiWrapper topList = subScorers.topList();
          // If target doc we wanted to advance to matches the actual doc
          // the underlying features advanced to, perform the feature
          // calculations,
          // otherwise just continue with the model's scoring process with empty
          // features.
          reset();
          if (activeDoc == targetDoc) {
            for (DisiWrapper w = topList; w != null; w = w.next) {
              Scorer subScorer = w.scorer;
              int featureId = ((FeatureWeight) subScorer.getWeight()).getId();
              allFeaturesUsed[featureId] = true;
              allFeatureValues[featureId] = subScorer.score();
            }
          }
          normalize();
          return meta.score(modelFeatureValuesNormalized);
        }

        @Override
        public int freq() throws IOException {
          DisiWrapper subMatches = subScorers.topList();
          int freq = 1;
          for (DisiWrapper w = subMatches.next; w != null; w = w.next) {
            freq += 1;
          }
          return freq;
        }

        @Override
        public DocIdSetIterator iterator() {
          return itr;
        }

        @Override
        public final Collection<ChildScorer> getChildren() {
          ArrayList<ChildScorer> children = new ArrayList<>();
          for (DisiWrapper scorer : subScorers) {
            children.add(new ChildScorer(scorer.scorer, "SHOULD"));
          }
          return children;
        }

        protected class ModelQuerySparseIterator extends
            DisjunctionDISIApproximation {

          public ModelQuerySparseIterator(DisiPriorityQueue subIterators) {
            super(subIterators);
          }

          @Override
          public final int nextDoc() throws IOException {
            if (activeDoc == targetDoc) {
              activeDoc = super.nextDoc();
            } else if (activeDoc < targetDoc) {
              activeDoc = super.advance(targetDoc + 1);
            }
            return ++targetDoc;
          }

          @Override
          public final int advance(int target) throws IOException {
            // If target doc we wanted to advance to matches the actual doc
            // the underlying features advanced to, perform the feature
            // calculations,
            // otherwise just continue with the model's scoring process with
            // empty features.
            if (activeDoc < target) activeDoc = super.advance(target);
            targetDoc = target;
            return targetDoc;
          }
        }

      }

      public class DenseModelScorer extends Scorer {
        int activeDoc = -1; // The doc that our scorer's are actually at
        int targetDoc = -1; // The doc we were most recently told to go to
        int freq = -1;
        List<FeatureScorer> featureScorers;

        protected DenseModelScorer(Weight weight,
            List<FeatureScorer> featureScorers) {
          super(weight);
          this.featureScorers = featureScorers;
        }

        @Override
        public int docID() {
          return targetDoc;
        }

        @Override
        public float score() throws IOException {
          reset();
          freq = 0;
          if (targetDoc == activeDoc) {
            for (Scorer scorer : featureScorers) {
              if (scorer.docID() == activeDoc) {
                freq++;
                int featureId = ((FeatureWeight) scorer.getWeight()).getId();
                allFeaturesUsed[featureId] = true;
                allFeatureValues[featureId] = scorer.score();
              }
            }
          }
          normalize();
          return meta.score(modelFeatureValuesNormalized);
        }

        @Override
        public final Collection<ChildScorer> getChildren() {
          ArrayList<ChildScorer> children = new ArrayList<>();
          for (Scorer scorer : featureScorers) {
            children.add(new ChildScorer(scorer, "SHOULD"));
          }
          return children;
        }

        @Override
        public int freq() throws IOException {
          return freq;
        }

        @Override
        public DocIdSetIterator iterator() {
          return new DenseIterator();
        }

        class DenseIterator extends DocIdSetIterator {

          @Override
          public int docID() {
            return targetDoc;
          }

          @Override
          public int nextDoc() throws IOException {
            if (activeDoc <= targetDoc) {
              activeDoc = NO_MORE_DOCS;
              for (Scorer scorer : featureScorers) {
                if (scorer.docID() != NO_MORE_DOCS) {
                  activeDoc = Math.min(activeDoc, scorer.iterator().nextDoc());
                }
              }
            }
            return ++targetDoc;
          }

          @Override
          public int advance(int target) throws IOException {
            if (activeDoc < target) {
              activeDoc = NO_MORE_DOCS;
              for (Scorer scorer : featureScorers) {
                if (scorer.docID() != NO_MORE_DOCS) {
                  activeDoc = Math.min(activeDoc,
                      scorer.iterator().advance(target));
                }
              }
            }
            targetDoc = target;
            return target;
          }

          @Override
          public long cost() {
            long sum = 0;
            for (int i = 0; i < featureScorers.size(); i++) {
              sum += featureScorers.get(i).iterator().cost();
            }
            return sum;
          }

        }
      }
    }
  }

}

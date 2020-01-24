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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.DocInfo;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.macro.MacroExpander;
import org.apache.solr.util.SolrPluginUtils;

/**
 * A recipe for computing a feature.  Subclass this for specialized feature calculations.
 * <p>
 * A feature consists of
 * <ul>
 * <li> a name as the identifier
 * <li> parameters to represent the specific feature
 * </ul>
 * <p>
 * Example configuration (snippet):
 * <pre>{
   "class" : "...",
   "name" : "myFeature",
   "params" : {
       ...
   }
}</pre>
 * <p>
 * {@link Feature} is an abstract class and concrete classes should implement
 * the {@link #validate()} function, and must implement the {@link #paramsToMap()}
 * and createWeight() methods.
 */
public abstract class Feature extends Query implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(Feature.class);

  final protected String name;
  private int index = -1;
  private float defaultValue = 0.0f;
  private Object defaultValueObject = null;

  final private Map<String,Object> params;

  public static Feature getInstance(SolrResourceLoader solrResourceLoader,
      String className, String name, Map<String,Object> params) {
    final Feature f = solrResourceLoader.newInstance(
        className,
        Feature.class,
        new String[0], // no sub packages
        new Class[] { String.class, Map.class },
        new Object[] { name, params });
    if (params != null) {
      SolrPluginUtils.invokeSetters(f, params.entrySet());
    }
    f.validate();
    return f;
  }

  public Feature(String name, Map<String,Object> params) {
    this.name = name;
    this.params = params;
  }

  /**
   * As part of creation of a feature instance, this function confirms
   * that the feature parameters are valid.
   *
   * @throws FeatureException
   *             Feature Exception
   */
  protected abstract void validate() throws FeatureException;

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder(64); // default initialCapacity of 16 won't be enough
    sb.append(getClass().getSimpleName());
    sb.append(" [name=").append(name);
    final LinkedHashMap<String,Object> params = paramsToMap();
    if (params != null) {
      sb.append(", params=").append(params);
    }
    sb.append(']');
    return sb.toString();
  }

  public abstract FeatureWeight createWeight(IndexSearcher searcher,
      boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) throws IOException;

  public float getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(Object obj){
    this.defaultValueObject = obj;
    if (obj instanceof String) {
      defaultValue = Float.parseFloat((String)obj);
    } else if (obj instanceof Double) {
      defaultValue = ((Double) obj).floatValue();
    } else if (obj instanceof Float) {
      defaultValue = ((Float) obj).floatValue();
    } else if (obj instanceof Integer) {
      defaultValue = ((Integer) obj).floatValue();
    } else if (obj instanceof Long) {
      defaultValue = ((Long) obj).floatValue();
    } else {
      throw new FeatureException("Invalid type for 'defaultValue' in params for " + this);
    }
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = (prime * result) + index;
    result = (prime * result) + ((name == null) ? 0 : name.hashCode());
    result = (prime * result) + ((params == null) ? 0 : params.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) &&  equalsTo(getClass().cast(o));
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(name) +
        RamUsageEstimator.sizeOfObject(params);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  private boolean equalsTo(Feature other) {
    if (index != other.index) {
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
    return true;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the id
   */
  public int getIndex() {
    return index;
  }

  /**
   * @param index
   *          Unique ID for this feature. Similar to feature name, except it can
   *          be used to directly access the feature in the global list of
   *          features.
   */
  public void setIndex(int index) {
    this.index = index;
  }

  public abstract LinkedHashMap<String,Object> paramsToMap();

  protected LinkedHashMap<String,Object> defaultParamsToMap() {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>();
    if (defaultValueObject != null) {
      params.put("defaultValue", defaultValueObject);
    }
    return params;
  }

  /**
   * Weight for a feature
   **/
  public abstract class FeatureWeight extends Weight {

    final protected IndexSearcher searcher;
    final protected SolrQueryRequest request;
    final protected Map<String,String[]> efi;
    final protected MacroExpander macroExpander;
    final protected Query originalQuery;

    /**
     * Initialize a feature without the normalizer from the feature file. This is
     * called on initial construction since multiple models share the same
     * features, but have different normalizers. A concrete model's feature is
     * copied through featForNewModel().
     *
     * @param q
     *          Solr query associated with this FeatureWeight
     * @param searcher
     *          Solr searcher available for features if they need them
     */
    public FeatureWeight(Query q, IndexSearcher searcher,
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) {
      super(q);
      this.searcher = searcher;
      this.request = request;
      this.originalQuery = originalQuery;
      this.efi = efi;
      macroExpander = new MacroExpander(efi,true);
    }

    public String getName() {
      return Feature.this.getName();
    }

    public int getIndex() {
      return Feature.this.getIndex();
    }

    public float getDefaultValue() {
      return Feature.this.getDefaultValue();
    }

    @Override
    public abstract FeatureScorer scorer(LeafReaderContext context)
        throws IOException;

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc)
        throws IOException {
      final FeatureScorer r = scorer(context);
      float score = getDefaultValue();
      if (r != null) {
        r.iterator().advance(doc);
        if (r.docID() == doc) {
          score = r.score();
        }
        return Explanation.match(score, toString());
      }else{
        return Explanation.match(score, "The feature has no value");
      }
    }

    /**
     * Used in the FeatureWeight's explain. Each feature should implement this
     * returning properties of the specific scorer useful for an explain. For
     * example "MyCustomClassFeature [name=" + name + "myVariable:" + myVariable +
     * "]";  If not provided, a default implementation will return basic feature
     * properties, which might not include query time specific values.
     */
    @Override
    public String toString() {
      return Feature.this.toString();
    }

    /**
     * A 'recipe' for computing a feature
     */
    public abstract class FeatureScorer extends Scorer {

      final protected String name;
      private DocInfo docInfo;
      final protected DocIdSetIterator itr;

      public FeatureScorer(Feature.FeatureWeight weight,
          DocIdSetIterator itr) {
        super(weight);
        this.itr = itr;
        name = weight.getName();
        docInfo = null;
      }

      @Override
      public abstract float score() throws IOException;

      /**
       * Used to provide context from initial score steps to later reranking steps.
       */
      public void setDocInfo(DocInfo docInfo) {
        this.docInfo = docInfo;
      }

      public DocInfo getDocInfo() {
        return docInfo;
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

    /**
     * Default FeatureScorer class that returns the score passed in. Can be used
     * as a simple ValueFeature, or to return a default scorer in case an
     * underlying feature's scorer is null.
     */
    public class ValueFeatureScorer extends FeatureScorer {
      float constScore;

      public ValueFeatureScorer(FeatureWeight weight, float constScore,
          DocIdSetIterator itr) {
        super(weight,itr);
        this.constScore = constScore;
      }

      @Override
      public float score() {
        return constScore;
      }

      @Override
      public float getMaxScore(int upTo) throws IOException {
        return constScore;
      }
    }

  }

}

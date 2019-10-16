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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * {@link Field} that can be used to store static scoring factors into
 * documents. This is mostly inspired from the work from Nick Craswell,
 * Stephen Robertson, Hugo Zaragoza and Michael Taylor. Relevance weighting
 * for query independent evidence. Proceedings of the 28th annual international
 * ACM SIGIR conference on Research and development in information retrieval.
 * August 15-19, 2005, Salvador, Brazil.
 * <p>
 * Feature values are internally encoded as term frequencies. Putting
 * feature queries as
 * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses of a
 * {@link BooleanQuery} allows to combine query-dependent scores (eg. BM25)
 * with query-independent scores using a linear combination. The fact that
 * feature values are stored as frequencies also allows search logic to
 * efficiently skip documents that can't be competitive when total hit counts
 * are not requested. This makes it a compelling option compared to storing
 * such factors eg. in a doc-value field.
 * <p>
 * This field may only store factors that are positively correlated with the
 * final score, like pagerank. In case of factors that are inversely correlated
 * with the score like url length, the inverse of the scoring factor should be
 * stored, ie. {@code 1/urlLength}.
 * <p>
 * This field only considers the top 9 significant bits for storage efficiency
 * which allows to store them on 16 bits internally. In practice this limitation
 * means that values are stored with a relative precision of
 * 2<sup>-8</sup> = 0.00390625.
 * <p>
 * Given a scoring factor {@code S > 0} and its weight {@code w > 0}, there
 * are three ways that S can be turned into a score:
 * <ul>
 *   <li>{@link #newLogQuery w * log(a + S)}, with a &ge; 1. This function
 *       usually makes sense because the distribution of scoring factors
 *       often follows a power law. This is typically the case for pagerank for
 *       instance. However the paper suggested that the {@code satu} and
 *       {@code sigm} functions give even better results.
 *   <li>{@link #newSaturationQuery satu(S) = w * S / (S + k)}, with k &gt; 0. This
 *       function is similar to the one used by {@link BM25Similarity} in order
 *       to incorporate term frequency into the final score and produces values
 *       between 0 and 1. A value of 0.5 is obtained when S and k are equal.
 *   <li>{@link #newSigmoidQuery sigm(S) = w * S<sup>a</sup> / (S<sup>a</sup> + k<sup>a</sup>)},
 *       with k &gt; 0, a &gt; 0. This function provided even better results
 *       than the two above but is also harder to tune due to the fact it has
 *       2 parameters. Like with {@code satu}, values are in the 0..1 range and
 *       0.5 is obtained when S and k are equal.
 * </ul>
 * <p>
 * The constants in the above formulas typically need training in order to
 * compute optimal values. If you don't know where to start, the
 * {@link #newSaturationQuery(String, String)} method uses
 * {@code 1f} as a weight and tries to guess a sensible value for the
 * {@code pivot} parameter of the saturation function based on index
 * statistics, which shouldn't perform too bad. Here is an example, assuming
 * that documents have a {@link FeatureField} called 'features' with values for
 * the 'pagerank' feature.
 * <pre class="prettyprint">
 * Query query = new BooleanQuery.Builder()
 *     .add(new TermQuery(new Term("body", "apache")), Occur.SHOULD)
 *     .add(new TermQuery(new Term("body", "lucene")), Occur.SHOULD)
 *     .build();
 * Query boost = FeatureField.newSaturationQuery("features", "pagerank");
 * Query boostedQuery = new BooleanQuery.Builder()
 *     .add(query, Occur.MUST)
 *     .add(boost, Occur.SHOULD)
 *     .build();
 * TopDocs topDocs = searcher.search(boostedQuery, 10);
 * </pre>
 * @lucene.experimental
 */
public final class FeatureField extends Field {

  private static final FieldType FIELD_TYPE = new FieldType();
  static {
    FIELD_TYPE.setTokenized(false);
    FIELD_TYPE.setOmitNorms(true);
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
  }

  private float featureValue;

  /**
   * Create a feature.
   * @param fieldName The name of the field to store the information into. All features may be stored in the same field.
   * @param featureName The name of the feature, eg. 'pagerank`. It will be indexed as a term.
   * @param featureValue The value of the feature, must be a positive, finite, normal float.
   */
  public FeatureField(String fieldName, String featureName, float featureValue) {
    super(fieldName, featureName, FIELD_TYPE);
    setFeatureValue(featureValue);
  }

  /**
   * Update the feature value of this field.
   */
  public void setFeatureValue(float featureValue) {
    if (Float.isFinite(featureValue) == false) {
      throw new IllegalArgumentException("featureValue must be finite, got: " + featureValue +
          " for feature " + fieldsData + " on field " + name);
    }
    if (featureValue < Float.MIN_NORMAL) {
      throw new IllegalArgumentException("featureValue must be a positive normal float, got: " +
          featureValue + " for feature " + fieldsData + " on field " + name +
          " which is less than the minimum positive normal float: " + Float.MIN_NORMAL);
    }
    this.featureValue = featureValue;
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
    FeatureTokenStream stream;
    if (reuse instanceof FeatureTokenStream) {
      stream = (FeatureTokenStream) reuse;
    } else {
      stream = new FeatureTokenStream();
    }

    int freqBits = Float.floatToIntBits(featureValue);
    stream.setValues((String) fieldsData, freqBits >>> 15);
    return stream;
  }

  private static final class FeatureTokenStream extends TokenStream {
    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final TermFrequencyAttribute freqAttribute = addAttribute(TermFrequencyAttribute.class);
    private boolean used = true;
    private String value = null;
    private int freq = 0;

    private FeatureTokenStream() {
    }

    /** Sets the values */
    void setValues(String value, int freq) {
      this.value = value;
      this.freq = freq;
    }

    @Override
    public boolean incrementToken() {
      if (used) {
        return false;
      }
      clearAttributes();
      termAttribute.append(value);
      freqAttribute.setTermFrequency(freq);
      used = true;
      return true;
    }

    @Override
    public void reset() {
      used = false;
    }

    @Override
    public void close() {
      value = null;
    }
  }

  static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

  static float decodeFeatureValue(float freq) {
    if (freq > MAX_FREQ) {
      // This is never used in practice but callers of the SimScorer API might
      // occasionally call it on eg. Float.MAX_VALUE to compute the max score
      // so we need to be consistent.
      return Float.MAX_VALUE;
    }
    int tf = (int) freq; // lossless
    int featureBits = tf << 15;
    return Float.intBitsToFloat(featureBits);
  }

  static abstract class FeatureFunction {
    abstract SimScorer scorer(float w);
    abstract Explanation explain(String field, String feature, float w, int freq);
    FeatureFunction rewrite(IndexReader reader) throws IOException { return this; }
  }

  static final class LogFunction extends FeatureFunction {

    private final float scalingFactor;

    LogFunction(float a) {
      this.scalingFactor = a;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      LogFunction that = (LogFunction) obj;
      return scalingFactor == that.scalingFactor;
    }

    @Override
    public int hashCode() {
      return Float.hashCode(scalingFactor);
    }

    @Override
    public String toString() {
      return "LogFunction(scalingFactor=" + scalingFactor + ")";
    }

    @Override
    SimScorer scorer(float weight) {
      return new SimScorer() {
        @Override
        public float score(float freq, long norm) {
          return (float) (weight * Math.log(scalingFactor + decodeFeatureValue(freq)));
        }
      };
    }

    @Override
    Explanation explain(String field, String feature, float w, int freq) {
      float featureValue = decodeFeatureValue(freq);
      float score = scorer(w).score(freq, 1L);
      return Explanation.match(score,
          "Log function on the " + field + " field for the " + feature + " feature, computed as w * log(a + S) from:",
          Explanation.match(w, "w, weight of this function"),
          Explanation.match(scalingFactor, "a, scaling factor"),
          Explanation.match(featureValue, "S, feature value"));
    }
  }

  static final class SaturationFunction extends FeatureFunction {

    private final String field, feature;
    private final Float pivot;

    SaturationFunction(String field, String feature, Float pivot) {
      this.field = field;
      this.feature = feature;
      this.pivot = pivot;
    }

    @Override
    public FeatureFunction rewrite(IndexReader reader) throws IOException {
      if (pivot != null) {
        return super.rewrite(reader);
      }
      float newPivot = computePivotFeatureValue(reader, field, feature);
      return new SaturationFunction(field, feature, newPivot);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      SaturationFunction that = (SaturationFunction) obj;
      return Objects.equals(field, that.field) &&
          Objects.equals(feature, that.feature) &&
          Objects.equals(pivot, that.pivot);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, feature, pivot);
    }

    @Override
    public String toString() {
      return "SaturationFunction(pivot=" + pivot + ")";
    }

    @Override
    SimScorer scorer(float weight) {
      if (pivot == null) {
        throw new IllegalStateException("Rewrite first");
      }
      final float pivot = this.pivot; // unbox
      return new SimScorer() {
        @Override
        public float score(float freq, long norm) {
          float f = decodeFeatureValue(freq);
          // should be f / (f + k) but we rewrite it to
          // 1 - k / (f + k) to make sure it doesn't decrease
          // with f in spite of rounding
          return weight * (1 - pivot / (f + pivot));
        }
      };
    }

    @Override
    Explanation explain(String field, String feature, float weight, int freq) {
      float featureValue = decodeFeatureValue(freq);
      float score = scorer(weight).score(freq, 1L);
      return Explanation.match(score,
          "Saturation function on the " + field + " field for the " + feature + " feature, computed as w * S / (S + k) from:",
          Explanation.match(weight, "w, weight of this function"),
          Explanation.match(pivot, "k, pivot feature value that would give a score contribution equal to w/2"),
          Explanation.match(featureValue, "S, feature value"));
    }
  }

  static final class SigmoidFunction extends FeatureFunction {

    private final float pivot, a;
    private final double pivotPa;

    SigmoidFunction(float pivot, float a) {
      this.pivot = pivot;
      this.a = a;
      this.pivotPa = Math.pow(pivot, a);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      SigmoidFunction that = (SigmoidFunction) obj;
      return pivot == that.pivot
          && a == that.a;
    }

    @Override
    public int hashCode() {
      int h = Float.hashCode(pivot);
      h = 31 * h + Float.hashCode(a);
      return h;
    }

    @Override
    public String toString() {
      return "SigmoidFunction(pivot=" + pivot + ", a=" + a + ")";
    }

    @Override
    SimScorer scorer(float weight) {
      return new SimScorer() {
        @Override
        public float score(float freq, long norm) {
          float f = decodeFeatureValue(freq);
          // should be f^a / (f^a + k^a) but we rewrite it to
          // 1 - k^a / (f + k^a) to make sure it doesn't decrease
          // with f in spite of rounding
          return (float) (weight * (1 - pivotPa / (Math.pow(f, a) + pivotPa)));
        }
      };
    }

    @Override
    Explanation explain(String field, String feature, float weight, int freq) {
      float featureValue = decodeFeatureValue(freq);
      float score = scorer(weight).score(freq, 1L);
      return Explanation.match(score,
          "Sigmoid function on the " + field + " field for the " + feature + " feature, computed as w * S^a / (S^a + k^a) from:",
          Explanation.match(weight, "w, weight of this function"),
          Explanation.match(pivot, "k, pivot feature value that would give a score contribution equal to w/2"),
          Explanation.match(pivot, "a, exponent, higher values make the function grow slower before k and faster after k"),
          Explanation.match(featureValue, "S, feature value"));
    }
  }

  /**
   * Given that IDFs are logs, similarities that incorporate term freq and
   * document length in sane (ie. saturated) ways should have their score
   * bounded by a log. So we reject weights that are too high as it would mean
   * that this clause would completely dominate ranking, removing the need for
   * query-dependent scores.
   */
  private static final float MAX_WEIGHT = Long.SIZE;

  /**
   * Return a new {@link Query} that will score documents as
   * {@code weight * Math.log(scalingFactor + S)} where S is the value of the static feature.
   * @param fieldName     field that stores features
   * @param featureName   name of the feature
   * @param weight        weight to give to this feature, must be in (0,64]
   * @param scalingFactor scaling factor applied before taking the logarithm, must be in [1, +Infinity)
   * @throws IllegalArgumentException if weight is not in (0,64] or scalingFactor is not in [1, +Infinity)
   */
  public static Query newLogQuery(String fieldName, String featureName, float weight, float scalingFactor) {
    if (weight <= 0 || weight > MAX_WEIGHT) {
      throw new IllegalArgumentException("weight must be in (0, " + MAX_WEIGHT + "], got: " + weight);
    }
    if (scalingFactor < 1 || Float.isFinite(scalingFactor) == false) {
      throw new IllegalArgumentException("scalingFactor must be >= 1, got: " + scalingFactor);
    }
    Query q = new FeatureQuery(fieldName, featureName, new LogFunction(scalingFactor));
    if (weight != 1f) {
      q = new BoostQuery(q, weight);
    }
    return q;
  }

  /**
   * Return a new {@link Query} that will score documents as
   * {@code weight * S / (S + pivot)} where S is the value of the static feature.
   * @param fieldName   field that stores features
   * @param featureName name of the feature
   * @param weight      weight to give to this feature, must be in (0,64]
   * @param pivot       feature value that would give a score contribution equal to weight/2, must be in (0, +Infinity)
   * @throws IllegalArgumentException if weight is not in (0,64] or pivot is not in (0, +Infinity)
   */
  public static Query newSaturationQuery(String fieldName, String featureName, float weight, float pivot) {
    return newSaturationQuery(fieldName, featureName, weight, Float.valueOf(pivot));
  }

  /**
   * Same as {@link #newSaturationQuery(String, String, float, float)} but
   * {@code 1f} is used as a weight and a reasonably good default pivot value
   * is computed based on index statistics and is approximately equal to the
   * geometric mean of all values that exist in the index.
   * @param fieldName   field that stores features
   * @param featureName name of the feature
   * @throws IllegalArgumentException if weight is not in (0,64] or pivot is not in (0, +Infinity)
   */
  public static Query newSaturationQuery(String fieldName, String featureName) {
    return newSaturationQuery(fieldName, featureName, 1f, null);
  }

  private static Query newSaturationQuery(String fieldName, String featureName, float weight, Float pivot) {
    if (weight <= 0 || weight > MAX_WEIGHT) {
      throw new IllegalArgumentException("weight must be in (0, " + MAX_WEIGHT + "], got: " + weight);
    }
    if (pivot != null && (pivot <= 0 || Float.isFinite(pivot) == false)) {
      throw new IllegalArgumentException("pivot must be > 0, got: " + pivot);
    }
    Query q = new FeatureQuery(fieldName, featureName, new SaturationFunction(fieldName, featureName, pivot));
    if (weight != 1f) {
      q = new BoostQuery(q, weight);
    }
    return q;
  }

  /**
   * Return a new {@link Query} that will score documents as
   * {@code weight * S^a / (S^a + pivot^a)} where S is the value of the static feature.
   * @param fieldName   field that stores features
   * @param featureName name of the feature
   * @param weight      weight to give to this feature, must be in (0,64]
   * @param pivot       feature value that would give a score contribution equal to weight/2, must be in (0, +Infinity)
   * @param exp         exponent, higher values make the function grow slower before 'pivot' and faster after 'pivot', must be in (0, +Infinity)
   * @throws IllegalArgumentException if w is not in (0,64] or either k or a are not in (0, +Infinity)
   */
  public static Query newSigmoidQuery(String fieldName, String featureName, float weight, float pivot, float exp) {
    if (weight <= 0 || weight > MAX_WEIGHT) {
      throw new IllegalArgumentException("weight must be in (0, " + MAX_WEIGHT + "], got: " + weight);
    }
    if (pivot <= 0 || Float.isFinite(pivot) == false) {
      throw new IllegalArgumentException("pivot must be > 0, got: " + pivot);
    }
    if (exp <= 0 || Float.isFinite(exp) == false) {
      throw new IllegalArgumentException("exp must be > 0, got: " + exp);
    }
    Query q = new FeatureQuery(fieldName, featureName, new SigmoidFunction(pivot, exp));
    if (weight != 1f) {
      q = new BoostQuery(q, weight);
    }
    return q;
  }

  /**
   * Compute a feature value that may be used as the {@code pivot} parameter of
   * the {@link #newSaturationQuery(String, String, float, float)} and
   * {@link #newSigmoidQuery(String, String, float, float, float)} factory
   * methods. The implementation takes the average of the int bits of the float
   * representation in practice before converting it back to a float. Given that
   * floats store the exponent in the higher bits, it means that the result will
   * be an approximation of the geometric mean of all feature values.
   * @param reader       the {@link IndexReader} to search against
   * @param featureField the field that stores features
   * @param featureName  the name of the feature
   */
  static float computePivotFeatureValue(IndexReader reader, String featureField, String featureName) throws IOException {
    Term term = new Term(featureField, featureName);
    TermStates states = TermStates.build(reader.getContext(), term, true);
    if (states.docFreq() == 0) {
      // avoid division by 0
      // The return value doesn't matter much here, the term doesn't exist,
      // it will never be used for scoring. Just Make sure to return a legal
      // value.
      return 1;
    }
    float avgFreq = (float) ((double) states.totalTermFreq() / states.docFreq());
    return decodeFeatureValue(avgFreq);
  }

  /**
   * Creates a SortField for sorting by the value of a feature.
   * <p>
   * This sort orders documents by descending value of a feature. The value returned in {@link FieldDoc} for
   * the hits contains a Float instance with the feature value.
   * <p>
   * If a document is missing the field, then it is treated as having a vaue of <code>0.0f</code>.
   * <p>
   * 
   * @param field field name. Must not be null.
   * @param featureName feature name. Must not be null.
   * @return SortField ordering documents by the value of the feature
   * @throws NullPointerException if {@code field} or {@code featureName} is null.
   */
  public static SortField newFeatureSort(String field, String featureName) {
    return new FeatureSortField(field, featureName);
  }
  
  /**
   * Creates a {@link DoubleValuesSource} instance which can be used to read the values of a feature from the a 
   * {@link FeatureField} for documents.
   * 
   * @param field field name. Must not be null.
   * @param featureName feature name. Must not be null.
   * @return a {@link DoubleValuesSource} which can be used to access the values of the feature for documents
   * @throws NullPointerException if {@code field} or {@code featureName} is null.
   */
  public static DoubleValuesSource newDoubleValues(String field, String featureName) {
    return new FeatureDoubleValuesSource(field, featureName);
  }
}

package org.apache.lucene.search.function;

/**
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
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

/**
 * Query that sets document score as a programmatic function of (up to) two (sub) scores.
 * <ol>
 *    <li>the score of its subQuery (any query)</li>
 *    <li>(optional) the score of its ValueSourtceQuery,
 *        for most simple/convineient use case this query would be a 
 *        {@link org.apache.lucene.search.function.FieldScoreQuery FieldScoreQuery}</li>
 * </ol>
 * Subclasses can modify the computation by overriding {@link #customScore(int, float, float)}.
 * 
 * <p><font color="#FF0000">
 * WARNING: The status of the <b>search.function</b> package is experimental. 
 * The APIs introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 */
public class CustomScoreQuery extends Query {

  private Query subQuery;
  private ValueSourceQuery valSrcQuery; // optional, can be null
  private boolean strict = false; // if true, valueSource part of query does not take part in weights normalization.  
  
  /**
   * Create a CustomScoreQuery over input subQuery.
   * @param subQuery the sub query whose scored is being customed. Must not be null. 
   */
  public CustomScoreQuery(Query subQuery) {
    this(subQuery,null);
  }

  /**
   * Create a CustomScoreQuery over input subQuery and a {@link ValueSourceQuery}.
   * @param subQuery the sub query whose score is being customed. Must not be null.
   * @param valSrcQuery a value source query whose scores are used in the custom score
   * computation. For most simple/convineient use case this would be a 
   * {@link org.apache.lucene.search.function.FieldScoreQuery FieldScoreQuery}.
   * This parameter is optional - it can be null.
   */
  public CustomScoreQuery(Query subQuery, ValueSourceQuery valSrcQuery) {
    super();
    this.subQuery = subQuery;
    this.valSrcQuery = valSrcQuery;
    if (subQuery == null) throw new IllegalArgumentException("<subqyery> must not be null!");
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader) */
  public Query rewrite(IndexReader reader) throws IOException {
    subQuery = subQuery.rewrite(reader);
    if (valSrcQuery!=null) {
      valSrcQuery = (ValueSourceQuery) valSrcQuery.rewrite(reader);
    }
    return this;
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#extractTerms(java.util.Set) */
  public void extractTerms(Set terms) {
    subQuery.extractTerms(terms);
    if (valSrcQuery!=null) {
      valSrcQuery.extractTerms(terms);
    }
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#clone() */
  public Object clone() {
    CustomScoreQuery clone = (CustomScoreQuery)super.clone();
    clone.subQuery = (Query) subQuery.clone();
    if (valSrcQuery!=null) {
      clone.valSrcQuery = (ValueSourceQuery) valSrcQuery.clone();
    }
    return clone;
  }

  /* (non-Javadoc) @see org.apache.lucene.search.Query#toString(java.lang.String) */
  public String toString(String field) {
    StringBuffer sb = new StringBuffer(name()).append("(");
    sb.append(subQuery.toString(field));
    if (valSrcQuery!=null) {
      sb.append(", ").append(valSrcQuery.toString(field));
    }
    sb.append(")");
    sb.append(strict?" STRICT" : "");
    return sb.toString() + ToStringUtils.boost(getBoost());
  }

  /** Returns true if <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) {
      return false;
    }
    CustomScoreQuery other = (CustomScoreQuery)o;
    return this.getBoost() == other.getBoost()
           && this.subQuery.equals(other.subQuery)
           && (this.valSrcQuery==null ? other.valSrcQuery==null 
               : this.valSrcQuery.equals(other.valSrcQuery));
  }

  /** Returns a hash code value for this object. */
  public int hashCode() {
    int valSrcHash = valSrcQuery==null ? 0 : valSrcQuery.hashCode();
    return (getClass().hashCode() + subQuery.hashCode() + valSrcHash) ^ Float.floatToIntBits(getBoost());
  }  
  
  /**
   * Compute a custom score by the subQuery score and the ValueSourceQuery score.
   * <p> 
   * Subclasses can override this method to modify the custom score.
   * <p>
   * The default computation herein is:
   * <pre>
   *     ModifiedScore = valSrcScore * subQueryScore.
   * </pre>
   * 
   * @param doc id of scored doc. 
   * @param subQueryScore score of that doc by the subQuery.
   * @param valSrcScore score of that doc by the ValueSourceQuery.
   * @return custom score.
   */
  public float customScore(int doc, float subQueryScore, float valSrcScore) {
    return valSrcScore * subQueryScore;
  }

  /**
   * Explain the custom score.
   * Whenever overriding {@link #customScore(int, float, float)}, 
   * this method should also be overriden to provide the correct explanation
   * for the part of the custom scoring. 
   * @param doc doc being explained.
   * @param subQueryExpl explanation for the sub-query part.
   * @param valSrcExpl explanation for the value source part.
   * @return an explanation for the custom score
   */
  public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpl) {
    float valSrcScore = valSrcExpl==null ? 1 : valSrcExpl.getValue();
    Explanation exp = new Explanation( valSrcScore * subQueryExpl.getValue(), "custom score: product of:");
    exp.addDetail(subQueryExpl);
    if (valSrcExpl != null) {
      exp.addDetail(valSrcExpl);
    }
    return exp;
  }
  //=========================== W E I G H T ============================
  
  private class CustomWeight implements Weight {
    Searcher searcher;
    Weight subQueryWeight;
    Weight valSrcWeight; // optional
    boolean qStrict;

    public CustomWeight(Searcher searcher) throws IOException {
      this.searcher = searcher;
      this.subQueryWeight = subQuery.weight(searcher); 
      if (valSrcQuery!=null) {
        this.valSrcWeight = valSrcQuery.createWeight(searcher);
      }
      this.qStrict = strict;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#getQuery() */
    public Query getQuery() {
      return CustomScoreQuery.this;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#getValue() */
    public float getValue() {
      return getBoost();
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#sumOfSquaredWeights() */
    public float sumOfSquaredWeights() throws IOException {
      float sum = subQueryWeight.sumOfSquaredWeights();
      if (valSrcWeight!=null) {
        if (qStrict) {
          valSrcWeight.sumOfSquaredWeights(); // do not include ValueSource part in the query normalization
        } else {
          sum += valSrcWeight.sumOfSquaredWeights();
        }
      }
      sum *= getBoost() * getBoost(); // boost each sub-weight
      return sum ;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#normalize(float) */
    public void normalize(float norm) {
      norm *= getBoost(); // incorporate boost
      subQueryWeight.normalize(norm);
      if (valSrcWeight!=null) {
        if (qStrict) {
          valSrcWeight.normalize(1); // do not normalize the ValueSource part
        } else {
          valSrcWeight.normalize(norm);
        }
      }
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#scorer(org.apache.lucene.index.IndexReader) */
    public Scorer scorer(IndexReader reader) throws IOException {
      Scorer subQueryScorer = subQueryWeight.scorer(reader);
      Scorer valSrcScorer = (valSrcWeight==null ? null : valSrcWeight.scorer(reader));
      return new CustomScorer(getSimilarity(searcher), reader, this, subQueryScorer, valSrcScorer);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#explain(org.apache.lucene.index.IndexReader, int) */
    public Explanation explain(IndexReader reader, int doc) throws IOException {
      return scorer(reader).explain(doc);
    }
  }


  //=========================== S C O R E R ============================
  
  /**
   * A scorer that applies a (callback) function on scores of the subQuery.
   */
  private class CustomScorer extends Scorer {
    private final CustomWeight weight;
    private final float qWeight;
    private Scorer subQueryScorer;
    private Scorer valSrcScorer; // optional
    private IndexReader reader;

    // constructor
    private CustomScorer(Similarity similarity, IndexReader reader, CustomWeight w,
        Scorer subQueryScorer, Scorer valSrcScorer) throws IOException {
      super(similarity);
      this.weight = w;
      this.qWeight = w.getValue();
      this.subQueryScorer = subQueryScorer;
      this.valSrcScorer = valSrcScorer;
      this.reader = reader;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#next() */
    public boolean next() throws IOException {
      boolean hasNext = subQueryScorer.next();
      if (valSrcScorer!=null && hasNext) {
        valSrcScorer.skipTo(subQueryScorer.doc());
      }
      return hasNext;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#doc() */
    public int doc() {
      return subQueryScorer.doc();
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#score() */
    public float score() throws IOException {
      float valSrcScore = (valSrcScorer==null ? 1 : valSrcScorer.score());
      return qWeight * customScore(subQueryScorer.doc(), subQueryScorer.score(), valSrcScore);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#skipTo(int) */
    public boolean skipTo(int target) throws IOException {
      boolean hasNext = subQueryScorer.skipTo(target);
      if (valSrcScorer!=null && hasNext) {
        valSrcScorer.skipTo(subQueryScorer.doc());
      }
      return hasNext;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#explain(int) */
    public Explanation explain(int doc) throws IOException {
      Explanation subQueryExpl = weight.subQueryWeight.explain(reader,doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }
      // match
      Explanation valSrcExpl = valSrcScorer==null ? null : valSrcScorer.explain(doc);
      Explanation customExp = customExplain(doc,subQueryExpl,valSrcExpl);
      float sc = qWeight * customExp.getValue();
      Explanation res = new ComplexExplanation(
        true, sc, CustomScoreQuery.this.toString() + ", product of:");
      res.addDetail(customExp);
      res.addDetail(new Explanation(qWeight, "queryBoost")); // actually using the q boost as q weight (== weight value)
      return res;
    }
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#createWeight(org.apache.lucene.search.Searcher) */
  protected Weight createWeight(Searcher searcher) throws IOException {
    return new CustomWeight(searcher);
  }

  /**
   * Checks if this is strict custom scoring.
   * In strict custom scoring, the ValueSource part of does not participate in weight normalization.
   * This may be useful when one wants full control over how scores are modified, and does 
   * not care about normalizing by the ValueSource part.
   * One particular case where this is useful if for testing this query.   
   * <P>
   * Note: only has effect when the ValueSource part is not null.
   */
  public boolean isStrict() {
    return strict;
  }

  /**
   * Set the strict mode of this query. 
   * @param strict The strict mode to set.
   * @see #isStrict()
   */
  public void setStrict(boolean strict) {
    this.strict = strict;
  }

  /**
   * A short name of this query, used in {@link #toString(String)}.
   */
  public String name() {
    return "custom";
  }

}

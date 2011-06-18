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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldCache; // for javadocs

/**
 * An instance of this subclass should be returned by
 * {@link CustomScoreQuery#getCustomScoreProvider}, if you want
 * to modify the custom score calculation of a {@link CustomScoreQuery}.
 * <p>Since Lucene 2.9, queries operate on each segment of an index separately,
 * so the protected {@link #reader} field can be used to resolve doc IDs,
 * as the supplied <code>doc</code> ID is per-segment and without knowledge
 * of the IndexReader you cannot access the document or {@link FieldCache}.
 * 
 * @lucene.experimental
 * @since 2.9.2
 */
public class CustomScoreProvider {

  protected final AtomicReaderContext context;

  /**
   * Creates a new instance of the provider class for the given {@link IndexReader}.
   */
  public CustomScoreProvider(AtomicReaderContext context) {
    this.context = context;
  }

  /**
   * Compute a custom score by the subQuery score and a number of 
   * {@link ValueSourceQuery} scores.
   * <p> 
   * Subclasses can override this method to modify the custom score.  
   * <p>
   * If your custom scoring is different than the default herein you 
   * should override at least one of the two customScore() methods.
   * If the number of ValueSourceQueries is always &lt; 2 it is 
   * sufficient to override the other 
   * {@link #customScore(int, float, float) customScore()} 
   * method, which is simpler. 
   * <p>
   * The default computation herein is a multiplication of given scores:
   * <pre>
   *     ModifiedScore = valSrcScore * valSrcScores[0] * valSrcScores[1] * ...
   * </pre>
   * 
   * @param doc id of scored doc. 
   * @param subQueryScore score of that doc by the subQuery.
   * @param valSrcScores scores of that doc by the ValueSourceQuery.
   * @return custom score.
   */
  public float customScore(int doc, float subQueryScore, float valSrcScores[]) throws IOException {
    if (valSrcScores.length == 1) {
      return customScore(doc, subQueryScore, valSrcScores[0]);
    }
    if (valSrcScores.length == 0) {
      return customScore(doc, subQueryScore, 1);
    }
    float score = subQueryScore;
    for(int i = 0; i < valSrcScores.length; i++) {
      score *= valSrcScores[i];
    }
    return score;
  }

  /**
   * Compute a custom score by the subQuery score and the ValueSourceQuery score.
   * <p> 
   * Subclasses can override this method to modify the custom score.
   * <p>
   * If your custom scoring is different than the default herein you 
   * should override at least one of the two customScore() methods.
   * If the number of ValueSourceQueries is always &lt; 2 it is 
   * sufficient to override this customScore() method, which is simpler. 
   * <p>
   * The default computation herein is a multiplication of the two scores:
   * <pre>
   *     ModifiedScore = subQueryScore * valSrcScore
   * </pre>
   *
   * @param doc id of scored doc. 
   * @param subQueryScore score of that doc by the subQuery.
   * @param valSrcScore score of that doc by the ValueSourceQuery.
   * @return custom score.
   */
  public float customScore(int doc, float subQueryScore, float valSrcScore) throws IOException {
    return subQueryScore * valSrcScore;
  }

  /**
   * Explain the custom score.
   * Whenever overriding {@link #customScore(int, float, float[])}, 
   * this method should also be overridden to provide the correct explanation
   * for the part of the custom scoring.
   *  
   * @param doc doc being explained.
   * @param subQueryExpl explanation for the sub-query part.
   * @param valSrcExpls explanation for the value source part.
   * @return an explanation for the custom score
   */
  public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpls[]) throws IOException {
    if (valSrcExpls.length == 1) {
      return customExplain(doc, subQueryExpl, valSrcExpls[0]);
    }
    if (valSrcExpls.length == 0) {
      return subQueryExpl;
    }
    float valSrcScore = 1;
    for (int i = 0; i < valSrcExpls.length; i++) {
      valSrcScore *= valSrcExpls[i].getValue();
    }
    Explanation exp = new Explanation( valSrcScore * subQueryExpl.getValue(), "custom score: product of:");
    exp.addDetail(subQueryExpl);
    for (int i = 0; i < valSrcExpls.length; i++) {
      exp.addDetail(valSrcExpls[i]);
    }
    return exp;
  }
  
  /**
   * Explain the custom score.
   * Whenever overriding {@link #customScore(int, float, float)}, 
   * this method should also be overridden to provide the correct explanation
   * for the part of the custom scoring.
   *  
   * @param doc doc being explained.
   * @param subQueryExpl explanation for the sub-query part.
   * @param valSrcExpl explanation for the value source part.
   * @return an explanation for the custom score
   */
  public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpl) throws IOException {
    float valSrcScore = 1;
    if (valSrcExpl != null) {
      valSrcScore *= valSrcExpl.getValue();
    }
    Explanation exp = new Explanation( valSrcScore * subQueryExpl.getValue(), "custom score: product of:");
    exp.addDetail(subQueryExpl);
    exp.addDetail(valSrcExpl);
    return exp;
  }

}

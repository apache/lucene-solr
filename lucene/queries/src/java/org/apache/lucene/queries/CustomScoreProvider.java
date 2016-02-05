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
package org.apache.lucene.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader; // for javadocs
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.search.Explanation;

/**
 * An instance of this subclass should be returned by
 * {@link CustomScoreQuery#getCustomScoreProvider}, if you want
 * to modify the custom score calculation of a {@link CustomScoreQuery}.
 * <p>Since Lucene 2.9, queries operate on each segment of an index separately,
 * so the protected {@link #context} field can be used to resolve doc IDs,
 * as the supplied <code>doc</code> ID is per-segment and without knowledge
 * of the IndexReader you cannot access the document or DocValues.
 * 
 * @lucene.experimental
 * @since 2.9.2
 */
public class CustomScoreProvider {

  protected final LeafReaderContext context;

  /**
   * Creates a new instance of the provider class for the given {@link IndexReader}.
   */
  public CustomScoreProvider(LeafReaderContext context) {
    this.context = context;
  }

  /**
   * Compute a custom score by the subQuery score and a number of 
   * {@link org.apache.lucene.queries.function.FunctionQuery} scores.
   * <p> 
   * Subclasses can override this method to modify the custom score.  
   * <p>
   * If your custom scoring is different than the default herein you 
   * should override at least one of the two customScore() methods.
   * If the number of {@link FunctionQuery function queries} is always &lt; 2 it is 
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
   * @param valSrcScores scores of that doc by the {@link FunctionQuery}.
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
    for (float valSrcScore : valSrcScores) {
      score *= valSrcScore;
    }
    return score;
  }

  /**
   * Compute a custom score by the subQuery score and the {@link FunctionQuery} score.
   * <p> 
   * Subclasses can override this method to modify the custom score.
   * <p>
   * If your custom scoring is different than the default herein you 
   * should override at least one of the two customScore() methods.
   * If the number of {@link FunctionQuery function queries} is always &lt; 2 it is 
   * sufficient to override this customScore() method, which is simpler. 
   * <p>
   * The default computation herein is a multiplication of the two scores:
   * <pre>
   *     ModifiedScore = subQueryScore * valSrcScore
   * </pre>
   *
   * @param doc id of scored doc. 
   * @param subQueryScore score of that doc by the subQuery.
   * @param valSrcScore score of that doc by the {@link FunctionQuery}.
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
    for (Explanation valSrcExpl : valSrcExpls) {
      valSrcScore *= valSrcExpl.getValue();
    }
    
    List<Explanation> subs = new ArrayList<>();
    subs.add(subQueryExpl);
    for (Explanation valSrcExpl : valSrcExpls) {
      subs.add(valSrcExpl);
    }
    return Explanation.match(valSrcScore * subQueryExpl.getValue(), "custom score: product of:", subs);
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
    return Explanation.match(valSrcScore * subQueryExpl.getValue(), "custom score: product of:", subQueryExpl, valSrcExpl);
  }

}

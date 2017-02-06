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

package org.apache.lucene.queries.mlt.query;

import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.PriorityQueue;

/**
 * This class has the responsibility of building the More Like This Boolean Query.
 * It takes in inpout the interesting terms and build the term queries based on the score of each.
 *
 * Query time boosting is supported.
 * If enabled each term will be boosted by its score.
 */
public class MoreLikeThisQueryBuilder {

  private MoreLikeThisParameters parameters;

  public MoreLikeThisQueryBuilder(MoreLikeThisParameters params) {
    this.parameters = params;
  }

  public BooleanQuery createQuery(PriorityQueue<ScoredTerm> interestingTerms) {
    BooleanQuery.Builder moreLikeThisQuery = new BooleanQuery.Builder();
    ScoredTerm interestingTerm;
    float minScore = -1;

    while ((interestingTerm = interestingTerms.pop()) != null) {
      Query interestingTermQuery = new TermQuery(new Term(interestingTerm.field, interestingTerm.term));

      if (parameters.isBoostEnabled()) {
        float currentScore = (interestingTerm.score);
        if (minScore == -1) {
          float fieldBoost = 1.0f;
          Map<String, Float> fieldToQueryTimeBoostFactor = parameters.getFieldToQueryTimeBoostFactor();
          if(fieldToQueryTimeBoostFactor!=null && fieldToQueryTimeBoostFactor.get(interestingTerm.field)!=null){
            fieldBoost = fieldToQueryTimeBoostFactor.get(interestingTerm.field);
          }
          minScore = currentScore/fieldBoost; // boost was already applied when finindg interesting terms
        }
        interestingTermQuery = new BoostQuery(interestingTermQuery, currentScore / minScore);
      }

      try {
        moreLikeThisQuery.add(interestingTermQuery, BooleanClause.Occur.SHOULD);
      }
      catch (BooleanQuery.TooManyClauses ignore) {
        break;
      }
    }
    return moreLikeThisQuery.build();
  }

  public MoreLikeThisParameters getParameters() {
    return parameters;
  }

  public void setParameters(MoreLikeThisParameters parameters) {
    this.parameters = parameters;
  }
}

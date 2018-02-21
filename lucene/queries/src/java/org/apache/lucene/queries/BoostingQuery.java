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
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * The BoostingQuery class can be used to effectively demote results that match a given query. 
 * Unlike the "NOT" clause, this still selects documents that contain undesirable terms, 
 * but reduces their overall score:
 *
 *     Query balancedQuery = new BoostingQuery(positiveQuery, negativeQuery, 0.01f);
 * In this scenario the positiveQuery contains the mandatory, desirable criteria which is used to 
 * select all matching documents, and the negativeQuery contains the undesirable elements which 
 * are simply used to lessen the scores. Documents that match the negativeQuery have their score 
 * multiplied by the supplied "boost" parameter, so this should be less than 1 to achieve a 
 * demoting effect
 * 
 * This code was originally made available here: 
 *   <a href="http://marc.theaimsgroup.com/?l=lucene-user&amp;m=108058407130459&amp;w=2">http://marc.theaimsgroup.com/?l=lucene-user&amp;m=108058407130459&amp;w=2</a>
 * and is documented here: http://wiki.apache.org/lucene-java/CommunityContributions
 *
 * @deprecated Use {@link org.apache.lucene.queries.function.FunctionScoreQuery#boostByQuery(Query, Query, float)}
 */
@Deprecated
public class BoostingQuery extends Query {
    private final float contextBoost;                     // the amount to boost by
    private final Query match;                            // query to match
    private final Query context;                          // boost when matches too

    public BoostingQuery(Query match, Query context, float boost) {
      this.match = match;
      this.context = context; // ignore context-only matches
      this.contextBoost = boost;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      Query matchRewritten = match.rewrite(reader);
      Query contextRewritten = context.rewrite(reader);
      if (match != matchRewritten || context != contextRewritten) {
        return new BoostingQuery(matchRewritten, contextRewritten, contextBoost);
      }
      return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      if (needsScores == false) {
        return match.createWeight(searcher, needsScores, boost);
      }
      final Weight matchWeight = searcher.createWeight(match, needsScores, boost);
      final Weight contextWeight = searcher.createWeight(context, false, boost);
      return new Weight(this) {

        @Override
        public void extractTerms(Set<Term> terms) {
          matchWeight.extractTerms(terms);
          if (boost >= 1) {
            contextWeight.extractTerms(terms);
          }
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
          final Explanation matchExplanation = matchWeight.explain(context, doc);
          final Explanation contextExplanation = contextWeight.explain(context, doc);
          if (matchExplanation.isMatch() == false || contextExplanation.isMatch() == false) {
            return matchExplanation;
          }
          return Explanation.match(matchExplanation.getValue() * contextBoost, "product of:",
              matchExplanation,
              Explanation.match(contextBoost, "boost"));
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final Scorer matchScorer = matchWeight.scorer(context);
          if (matchScorer == null) {
            return null;
          }
          final Scorer contextScorer = contextWeight.scorer(context);
          if (contextScorer == null) {
            return matchScorer;
          }
          TwoPhaseIterator contextTwoPhase = contextScorer.twoPhaseIterator();
          DocIdSetIterator contextApproximation = contextTwoPhase == null
              ? contextScorer.iterator()
              : contextTwoPhase.approximation();
          return new FilterScorer(matchScorer) {
            @Override
            public float score() throws IOException {
              if (contextApproximation.docID() < docID()) {
                contextApproximation.advance(docID());
              }
              assert contextApproximation.docID() >= docID();
              float score = super.score();
              if (contextApproximation.docID() == docID()
                  && (contextTwoPhase == null || contextTwoPhase.matches())) {
                score *= contextBoost;
              }
              return score;
            }
          };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return matchWeight.isCacheable(ctx) && contextWeight.isCacheable(ctx);
        }

      };
    }

    public Query getMatch() {
      return match;
    }

    public Query getContext() {
      return context;
    }

    public float getBoost() {
      return contextBoost;
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + Objects.hash(match, context, contextBoost);
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(BoostingQuery other) {
      return match.equals(other.match)
          && context.equals(other.context)
          && Float.floatToIntBits(contextBoost) == Float.floatToIntBits(other.contextBoost);
    }

    @Override
    public String toString(String field) {
      return match.toString(field) + "/" + context.toString(field);
    }
  }

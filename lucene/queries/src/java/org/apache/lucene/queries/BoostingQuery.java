package org.apache.lucene.queries;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;

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
 * This code was originally made available here: [WWW] http://marc.theaimsgroup.com/?l=lucene-user&m=108058407130459&w=2
 * and is documented here: http://wiki.apache.org/lucene-java/CommunityContributions
 */
public class BoostingQuery extends Query {
    private final float boost;                            // the amount to boost by
    private final Query match;                            // query to match
    private final Query context;                          // boost when matches too

    public BoostingQuery(Query match, Query context, float boost) {
      this.match = match;
      this.context = context.clone();        // clone before boost
      this.boost = boost;
      this.context.setBoost(0.0f);                      // ignore context-only matches
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      BooleanQuery result = new BooleanQuery() {
        @Override
        public Weight createWeight(IndexSearcher searcher) throws IOException {
          return new BooleanWeight(searcher, false) {

            @Override
            public float coord(int overlap, int max) {
              switch (overlap) {

              case 1:                               // matched only one clause
                return 1.0f;                        // use the score as-is

              case 2:                               // matched both clauses
                return boost;                       // multiply by boost

              default:
                return 0.0f;
                
              }
            }
          };
        }
      };

      result.add(match, BooleanClause.Occur.MUST);
      result.add(context, BooleanClause.Occur.SHOULD);

      return result;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + Float.floatToIntBits(boost);
      result = prime * result + ((context == null) ? 0 : context.hashCode());
      result = prime * result + ((match == null) ? 0 : match.hashCode());
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
      
      if (!super.equals(obj)) {
        return false;
      }

      BoostingQuery other = (BoostingQuery) obj;
      if (Float.floatToIntBits(boost) != Float.floatToIntBits(other.boost)) {
        return false;
      }
      
      if (context == null) {
        if (other.context != null) {
          return false;
        }
      } else if (!context.equals(other.context)) {
        return false;
      }
      
      if (match == null) {
        if (other.match != null) {
          return false;
        }
      } else if (!match.equals(other.match)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString(String field) {
      return match.toString(field) + "/" + context.toString(field);
    }
  }

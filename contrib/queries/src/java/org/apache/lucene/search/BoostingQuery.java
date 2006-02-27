package org.apache.lucene.search;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
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
 * and is documented here: http://wiki.apache.org/jakarta-lucene/CommunityContributions
 */
public class BoostingQuery extends Query {
    private float boost;                            // the amount to boost by
    private Query match;                            // query to match
    private Query context;                          // boost when matches too

    public BoostingQuery(Query match, Query context, float boost) {
      this.match = match;
      this.context = (Query)context.clone();        // clone before boost
      this.boost = boost;

      context.setBoost(0.0f);                      // ignore context-only matches
    }

    public Query rewrite(IndexReader reader) throws IOException {
      BooleanQuery result = new BooleanQuery() {

        public Similarity getSimilarity(Searcher searcher) {
          return new DefaultSimilarity() {

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

    public String toString(String field) {
      return match.toString(field) + "/" + context.toString(field);
    }
  }
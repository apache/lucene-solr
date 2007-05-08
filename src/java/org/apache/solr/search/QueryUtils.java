package org.apache.solr.search;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchAllDocsQuery;

import java.util.List;
import java.util.Arrays;

/**
 * @author yonik
 * @version $Id$
 */
public class QueryUtils {

  /** return true if this query has no positive components */
  static boolean isNegative(Query q) {
    if (!(q instanceof BooleanQuery)) return false;
    BooleanQuery bq = (BooleanQuery)q;
    List<BooleanClause> clauses = bq.clauses();
    if (clauses.size()==0) return false;
    for (BooleanClause clause : clauses) {
      if (!clause.isProhibited()) return false;
    }
    return true;
  }

  /** Returns the original query if it was already a positive query, otherwise
   * return the negative of the query (i.e., a positive query).
   * <p>
   * Example: both id:10 and id:-10 will return id:10
   * <p>
   * The caller can tell the sign of the original by a reference comparison between
   * the original and returned query.
   * @param q
   * @return
   */
  static Query getAbs(Query q) {
    if (!(q instanceof BooleanQuery)) return q;
    BooleanQuery bq = (BooleanQuery)q;

    List<BooleanClause> clauses = bq.clauses();
    if (clauses.size()==0) return q;


    for (BooleanClause clause : clauses) {
      if (!clause.isProhibited()) return q;
    }

    if (clauses.size()==1) {
      // if only one clause, dispense with the wrapping BooleanQuery
      Query negClause = clauses.get(0).getQuery();
      // we shouldn't need to worry about adjusting the boosts since the negative
      // clause would have never been selected in a positive query, and hence would
      // not contribute to a score.
      return negClause;
    } else {
      BooleanQuery newBq = new BooleanQuery(bq.isCoordDisabled());
      newBq.setBoost(bq.getBoost());
      // ignore minNrShouldMatch... it doesn't make sense for a negative query

      // the inverse of -a -b is a OR b
      for (BooleanClause clause : clauses) {
        newBq.add(clause.getQuery(), BooleanClause.Occur.SHOULD);
      }
      return newBq;
    }


    /*** TODO: use after next lucene update
    List <BooleanClause> clauses = (List <BooleanClause>)bq.clauses();
    // A single filtered out stopword currently causes a BooleanQuery with
    // zero clauses.
    if (clauses.size()==0) return q;

    for (BooleanClause clause: clauses) {
      if (!clause.isProhibited()) return q;
    }

    if (clauses.size()==1) {
      // if only one clause, dispense with the wrapping BooleanQuery
      Query negClause = clauses.get(0).getQuery();
      // we shouldn't need to worry about adjusting the boosts since the negative
      // clause would have never been selected in a positive query, and hence the
      // boost is meaningless.
      return negClause;
    } else {
      BooleanQuery newBq = new BooleanQuery(bq.isCoordDisabled());
      newBq.setBoost(bq.getBoost());
      // ignore minNrShouldMatch... it doesn't make sense for a negative query

      // the inverse of -a -b is a b
      for (BooleanClause clause: clauses) {
        newBq.add(clause.getQuery(), BooleanClause.Occur.SHOULD);
      }
      return newBq;
    }
    ***/
  }

  /** Makes negative queries suitable for querying by
   * lucene.
   */
  static Query makeQueryable(Query q) {
    return isNegative(q) ? fixNegativeQuery(q) : q;
  }

  /** Fixes a negative query by adding a MatchAllDocs query clause.
   * The query passed in *must* be a negative query.
   */
  static Query fixNegativeQuery(Query q) {
    BooleanQuery newBq = (BooleanQuery)q.clone();
    newBq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    return newBq;    
  }

}

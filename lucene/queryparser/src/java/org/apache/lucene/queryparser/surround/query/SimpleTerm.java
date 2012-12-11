package org.apache.lucene.queryparser.surround.query;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

/**
 * Base class for queries that expand to sets of simple terms.
 */
public abstract class SimpleTerm
  extends SrndQuery
  implements DistanceSubQuery, Comparable<SimpleTerm>
{
  public SimpleTerm(boolean q) {quoted = q;}
  
  private boolean quoted;
  boolean isQuoted() {return quoted;}
  
  public String getQuote() {return "\"";}
  public String getFieldOperator() {return "/";}
  
  public abstract String toStringUnquoted();

  /** @deprecated (March 2011) Not normally used, to be removed from Lucene 4.0.
   *   This class implementing Comparable is to be removed at the same time.
   */
  @Override
  @Deprecated
  public int compareTo(SimpleTerm ost) {
    /* for ordering terms and prefixes before using an index, not used */
    return this.toStringUnquoted().compareTo( ost.toStringUnquoted());
  }
  
  protected void suffixToString(StringBuilder r) {} /* override for prefix query */
  
  @Override
  public String toString() {
    StringBuilder r = new StringBuilder();
    if (isQuoted()) {
      r.append(getQuote());
    }
    r.append(toStringUnquoted());
    if (isQuoted()) {
      r.append(getQuote());
    }
    suffixToString(r);
    weightToString(r);
    return r.toString();
  }
  
  public abstract void visitMatchingTerms(
                            IndexReader reader,
                            String fieldName,
                            MatchingTermVisitor mtv) throws IOException;
  
  /**
   * Callback to visit each matching term during "rewrite"
   * in {@link #visitMatchingTerm(Term)}
   */
  public interface MatchingTermVisitor {
    void visitMatchingTerm(Term t)throws IOException;
  }

  @Override
  public String distanceSubQueryNotAllowed() {return null;}
  
  @Override
  public void addSpanQueries(final SpanNearClauseFactory sncf) throws IOException {
    visitMatchingTerms(
          sncf.getIndexReader(),
          sncf.getFieldName(),
          new MatchingTermVisitor() {
            @Override
            public void visitMatchingTerm(Term term) throws IOException {
              sncf.addTermWeighted(term, getWeight());
            }
          });
  }

  @Override
  public Query makeLuceneQueryFieldNoBoost(final String fieldName, final BasicQueryFactory qf) {
    return new SimpleTermRewriteQuery(this, fieldName, qf);
  }
}




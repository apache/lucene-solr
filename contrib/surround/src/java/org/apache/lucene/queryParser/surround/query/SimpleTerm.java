package org.apache.lucene.queryParser.surround.query;
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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

public abstract class SimpleTerm
  extends SrndQuery
  implements DistanceSubQuery, Comparable
{
  public SimpleTerm(boolean q) {quoted = q;}
  
  private boolean quoted;
  boolean isQuoted() {return quoted;}
  
  public String getQuote() {return "\"";}
  public String getFieldOperator() {return "/";}
  
  public abstract String toStringUnquoted();
  
  public int compareTo(Object o) {
    /* for ordering terms and prefixes before using an index, not used */
    SimpleTerm ost = (SimpleTerm) o;
    return this.toStringUnquoted().compareTo( ost.toStringUnquoted());
  }
  
  protected void suffixToString(StringBuffer r) {;} /* override for prefix query */
  
  public String toString() {
    StringBuffer r = new StringBuffer();
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
  
  public interface MatchingTermVisitor {
    void visitMatchingTerm(Term t)throws IOException;
  }

  public String distanceSubQueryNotAllowed() {return null;}

  
  public Query makeLuceneQueryFieldNoBoost(final String fieldName, final BasicQueryFactory qf) {
    return new Query() {
      public String toString(String fn) {
        return getClass().toString() + " " + fieldName + " (" + fn + "?)";
      }
      
      public Query rewrite(IndexReader reader) throws IOException {
        final List luceneSubQueries = new ArrayList();
        visitMatchingTerms( reader, fieldName,
            new MatchingTermVisitor() {
              public void visitMatchingTerm(Term term) throws IOException {
                luceneSubQueries.add(qf.newTermQuery(term));
              }
            });
        return  (luceneSubQueries.size() == 0) ? SrndQuery.theEmptyLcnQuery
              : (luceneSubQueries.size() == 1) ? (Query) luceneSubQueries.get(0)
              : SrndBooleanQuery.makeBooleanQuery(
                  /* luceneSubQueries all have default weight */
                  luceneSubQueries, BooleanClause.Occur.SHOULD); /* OR the subquery terms */ 
      }
    };
  }
    
  public void addSpanQueries(final SpanNearClauseFactory sncf) throws IOException {
    visitMatchingTerms(
          sncf.getIndexReader(),
          sncf.getFieldName(),
          new MatchingTermVisitor() {
            public void visitMatchingTerm(Term term) throws IOException {
              sncf.addTermWeighted(term, getWeight());
            }
          });
  }
}




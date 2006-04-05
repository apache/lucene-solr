package org.apache.lucene.queryParser.surround.query;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
SpanNearClauseFactory:

Operations:

- create for a field name and an indexreader.

- add a weighted Term
  this should add a corresponding SpanTermQuery, or
  increase the weight of an existing one.
  
- add a weighted subquery SpanNearQuery 

- create a clause for SpanNearQuery from the things added above.
  For this, create an array of SpanQuery's from the added ones.
  The clause normally is a SpanOrQuery over the added subquery SpanNearQuery
  the SpanTermQuery's for the added Term's
*/

/* When  it is necessary to suppress double subqueries as much as possible:
   hashCode() and equals() on unweighted SpanQuery are needed (possibly via getTerms(),
   the terms are individually hashable).
   Idem SpanNearQuery: hash on the subqueries and the slop.
   Evt. merge SpanNearQuery's by adding the weights of the corresponding subqueries.
 */
 
/* To be determined:
   Are SpanQuery weights handled correctly during search by Lucene?
   Should the resulting SpanOrQuery be sorted?
   Could other SpanQueries be added for use in this factory:
   - SpanOrQuery: in principle yes, but it only has access to it's terms
                  via getTerms(); are the corresponding weights available?
   - SpanFirstQuery: treat similar to subquery SpanNearQuery. (ok?)
   - SpanNotQuery: treat similar to subquery SpanNearQuery. (ok?)
 */

import java.util.HashMap;
import java.util.Iterator;

import java.util.Comparator;
import java.util.Arrays;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import org.apache.lucene.search.Query;

import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;


public class SpanNearClauseFactory {
  public SpanNearClauseFactory(IndexReader reader, String fieldName, BasicQueryFactory qf) {
    this.reader = reader;
    this.fieldName = fieldName;
    this.weightBySpanQuery = new HashMap(); 
    this.qf = qf;
  }
  private IndexReader reader;
  private String fieldName;
  private HashMap weightBySpanQuery;
  private BasicQueryFactory qf;
  
  public IndexReader getIndexReader() {return reader;}
  
  public String getFieldName() {return fieldName;}

  public BasicQueryFactory getBasicQueryFactory() {return qf;}
  
  public TermEnum getTermEnum(String termText) throws IOException {
    return getIndexReader().terms(new Term(getFieldName(), termText));
  }
  
  public int size() {return weightBySpanQuery.size();}
  
  public void clear() {weightBySpanQuery.clear();}

  protected void addSpanQueryWeighted(SpanQuery sq, float weight) {
    Float w = (Float) weightBySpanQuery.get(sq);
    if (w != null)
      w = new Float(w.floatValue() + weight);
    else
      w = new Float(weight);
    weightBySpanQuery.put(sq, w); 
  }
  
  public void addTermWeighted(Term t, float weight) throws IOException {   
    SpanTermQuery stq = qf.newSpanTermQuery(t);
    /* CHECKME: wrap in Hashable...? */
    addSpanQueryWeighted(stq, weight);
  }
  
  public void addSpanNearQuery(Query q) {
    if (q == SrndQuery.theEmptyLcnQuery)
      return;
    if (! (q instanceof SpanNearQuery))
      throw new AssertionError("Expected SpanNearQuery: " + q.toString(getFieldName()));
    /* CHECKME: wrap in Hashable...? */
    addSpanQueryWeighted((SpanNearQuery)q, q.getBoost());
  }
  
  public SpanQuery makeSpanNearClause() {
    SpanQuery [] spanQueries = new SpanQuery[size()];
    Iterator sqi = weightBySpanQuery.keySet().iterator();
    int i = 0;
    while (sqi.hasNext()) {
      SpanQuery sq = (SpanQuery) sqi.next();
      sq.setBoost(((Float)weightBySpanQuery.get(sq)).floatValue());
      spanQueries[i++] = sq;
    }
    
    if (spanQueries.length == 1)
      return spanQueries[0];
    else
      return new SpanOrQuery(spanQueries);
  }
}


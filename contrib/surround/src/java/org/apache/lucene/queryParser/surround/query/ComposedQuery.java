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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public abstract class ComposedQuery extends SrndQuery { 
  
  public ComposedQuery(List qs, boolean operatorInfix, String opName) {
    recompose(qs);
    this.operatorInfix = operatorInfix;
    this.opName = opName;
  }
  
  protected void recompose(List queries) {
    if (queries.size() < 2) throw new AssertionError("Too few subqueries"); 
    this.queries = queries;
  }
  
  private String opName;
  public String getOperatorName() {return opName;}
  
  private List queries;
  
  public Iterator getSubQueriesIterator() {return queries.listIterator();}

  public int getNrSubQueries() {return queries.size();}
  
  public SrndQuery getSubQuery(int qn) {return (SrndQuery) queries.get(qn);}

  private boolean operatorInfix; 
  public boolean isOperatorInfix() { return operatorInfix; } /* else prefix operator */
  
  public List makeLuceneSubQueriesField(String fn, BasicQueryFactory qf) {
    List luceneSubQueries = new ArrayList();
    Iterator sqi = getSubQueriesIterator();
    while (sqi.hasNext()) {
      luceneSubQueries.add( ((SrndQuery) sqi.next()).makeLuceneQueryField(fn, qf));
    }
    return luceneSubQueries;
  }

  public String toString() {
    StringBuffer r = new StringBuffer();
    if (isOperatorInfix()) {
      infixToString(r);
    } else {
      prefixToString(r);
    }
    weightToString(r);
    return r.toString();
  }

  /* Override for different spacing */
  protected String getPrefixSeparator() { return ", ";}
  protected String getBracketOpen() { return "(";}
  protected String getBracketClose() { return ")";}
  
  protected void infixToString(StringBuffer r) {
    /* Brackets are possibly redundant in the result. */
    Iterator sqi = getSubQueriesIterator();
    r.append(getBracketOpen());
    if (sqi.hasNext()) {
      r.append(sqi.next().toString());
      while (sqi.hasNext()) {
        r.append(" ");
        r.append(getOperatorName()); /* infix operator */
        r.append(" ");
        r.append(sqi.next().toString());
      }
    }
    r.append(getBracketClose());
  }

  protected void prefixToString(StringBuffer r) {
    Iterator sqi = getSubQueriesIterator();
    r.append(getOperatorName()); /* prefix operator */
    r.append(getBracketOpen());
    if (sqi.hasNext()) {
      r.append(sqi.next().toString());
      while (sqi.hasNext()) {
        r.append(getPrefixSeparator());
        r.append(sqi.next().toString());
      }
    }
    r.append(getBracketClose());
  }
  
  
  public boolean isFieldsSubQueryAcceptable() {
    /* at least one subquery should be acceptable */
    Iterator sqi = getSubQueriesIterator();
    while (sqi.hasNext()) {
      if (((SrndQuery) sqi.next()).isFieldsSubQueryAcceptable()) {
        return true;
      }
    }
    return false;
  }
}


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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/** Lowest level base class for surround queries */
public abstract class SrndQuery implements Cloneable {
  public SrndQuery() {}
  
  private float weight = (float) 1.0;
  private boolean weighted = false;

  public void setWeight(float w) {
    weight = w; /* as parsed from the query text */
    weighted = true;
  } 
  public boolean isWeighted() {return weighted;}
  public float getWeight() { return weight; }
  public String getWeightString() {return Float.toString(getWeight());}

  public String getWeightOperator() {return "^";}

  protected void weightToString(StringBuilder r) { /* append the weight part of a query */
    if (isWeighted()) {
      r.append(getWeightOperator());
      r.append(getWeightString());
    }
  }
  
  public Query makeLuceneQueryField(String fieldName, BasicQueryFactory qf){
    Query q = makeLuceneQueryFieldNoBoost(fieldName, qf);
    if (isWeighted()) {
      q.setBoost(getWeight() * q.getBoost()); /* weight may be at any level in a SrndQuery */
    }
    return q;
  }
  
  public abstract Query makeLuceneQueryFieldNoBoost(String fieldName, BasicQueryFactory qf);
  
  /** This method is used by {@link #hashCode()} and {@link #equals(Object)},
   *  see LUCENE-2945.
   */
  @Override
  public abstract String toString();
  
  public boolean isFieldsSubQueryAcceptable() {return true;}
    
  @Override
  public SrndQuery clone() {
    try {
      return (SrndQuery)super.clone();
    } catch (CloneNotSupportedException cns) {
      throw new Error(cns);
    }
  }

  /** For subclasses of {@link SrndQuery} within the package
   *  {@link org.apache.lucene.queryparser.surround.query}
   *  it is not necessary to override this method,
   *  @see #toString()
   */
  @Override
  public int hashCode() {
    return getClass().hashCode() ^ toString().hashCode();
  }

  /** For subclasses of {@link SrndQuery} within the package
   *  {@link org.apache.lucene.queryparser.surround.query}
   *  it is not necessary to override this method,
   *  @see #toString()
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (! getClass().equals(obj.getClass()))
      return false;
    return toString().equals(obj.toString());
  }

  /** An empty Lucene query */
  public final static Query theEmptyLcnQuery = new BooleanQuery() { /* no changes allowed */
    @Override
    public void setBoost(float boost) {
      throw new UnsupportedOperationException();
    }
    @Override
    public void add(BooleanClause clause) {
      throw new UnsupportedOperationException();
    }
    @Override
    public void add(Query query, BooleanClause.Occur occur) {
      throw new UnsupportedOperationException();
    }
  };
}


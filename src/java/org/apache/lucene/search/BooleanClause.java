package org.apache.lucene.search;

import org.apache.lucene.util.Parameter;

/**
 * Copyright 2004 The Apache Software Foundation
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

/** A clause in a BooleanQuery. */
public class BooleanClause implements java.io.Serializable {
  
  public static final class Occur extends Parameter implements java.io.Serializable {
    
    private Occur(String name) {
      // typesafe enum pattern, no public constructor
      super(name);
    }
   
    /** Use this operator for terms that <i>must</i> appear in the matching documents. */
    public static final Occur MUST = new Occur("MUST");
    /** Use this operator for terms of which <i>should</i> appear in the 
     * matching documents. For a BooleanQuery with two <code>SHOULD</code> 
     * subqueries, at least one of the queries must appear in the matching documents. */
    public static final Occur SHOULD = new Occur("SHOULD");
    /** Use this operator for terms that <i>must not</i> appear in the matching documents.
     * Note that it is not possible to search for queries that only consist
     * of a <code>MUST_NOT</code> query. */
    public static final Occur MUST_NOT = new Occur("MUST_NOT");
    
  }

  /** The query whose matching documents are combined by the boolean query.
   *     @deprecated use {@link #setQuery(Query)} instead */
  public Query query;    // TODO: decrease visibility for Lucene 2.0

  /** If true, documents documents which <i>do not</i>
    match this sub-query will <i>not</i> match the boolean query.
    @deprecated use {@link #setOccur(BooleanClause.Occur)} instead */
  public boolean required = false;  // TODO: decrease visibility for Lucene 2.0
  
  /** If true, documents documents which <i>do</i>
    match this sub-query will <i>not</i> match the boolean query.
    @deprecated use {@link #setOccur(BooleanClause.Occur)} instead */
  public boolean prohibited = false;  // TODO: decrease visibility for Lucene 2.0

  private Occur occur = Occur.SHOULD;

  /** Constructs a BooleanClause with query <code>q</code>, required
   * <code>r</code> and prohibited <code>p</code>.
   * @deprecated use BooleanClause(Query, Occur) instead
   * <ul>
   *  <li>For BooleanClause(query, true, false) use BooleanClause(query, BooleanClause.Occur.MUST)
   *  <li>For BooleanClause(query, false, false) use BooleanClause(query, BooleanClause.Occur.SHOULD)
   *  <li>For BooleanClause(query, false, true) use BooleanClause(query, BooleanClause.Occur.MUST_NOT)
   * </ul>
   */ 
  public BooleanClause(Query q, boolean r, boolean p) {
    // TODO: remove for Lucene 2.0
    query = q;
    required = r;
    prohibited = p;
    if (required) {
      if (prohibited) {
        // prohibited && required doesn't make sense, but we want the old behaviour:
        occur = Occur.MUST_NOT;
      } else {
         occur = Occur.MUST;
      }
    } else {
      if (prohibited) {
         occur = Occur.MUST_NOT;
      } else {
         occur = Occur.SHOULD;
      }
    }
  }

  /** Constructs a BooleanClause.
  */ 
  public BooleanClause(Query query, Occur occur) {
    this.query = query;
    this.occur = occur;
    setFields(occur);
  }

  public Occur getOccur() {
    return occur;
  }

  public void setOccur(Occur occur) {
    this.occur = occur;
    setFields(occur);
  }

  public Query getQuery() {
    return query;
  }

  public void setQuery(Query query) {
    this.query = query;
  }
  
  public boolean isProhibited() {
    return prohibited;
  }

  public boolean isRequired() {
    return required;
  }

  private void setFields(Occur occur) {
    if (occur == Occur.MUST) {
      required = true;
      prohibited = false;
    } else if (occur == Occur.SHOULD) {
      required = false;
      prohibited = false;
    } else if (occur == Occur.MUST_NOT) {
      required = false;
      prohibited = true;
    } else {
      throw new IllegalArgumentException("Unknown operator " + occur);
    }
  }

  /** Returns true iff <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (!(o instanceof BooleanClause))
      return false;
    BooleanClause other = (BooleanClause)o;
    return this.query.equals(other.query)
      && (this.required == other.required)
      && (this.prohibited == other.prohibited);
  }

  /** Returns a hash code value for this object.*/
  public int hashCode() {
    return query.hashCode() ^ (this.required?1:0) ^ (this.prohibited?2:0);
  }

}

package org.apache.lucene.search;

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
  /** The query whose matching documents are combined by the boolean query. */
  public Query query;
  /** If true, documents documents which <i>do not</i>
    match this sub-query will <i>not</i> match the boolean query. */
  public boolean required = false;
  /** If true, documents documents which <i>do</i>
    match this sub-query will <i>not</i> match the boolean query. */
  public boolean prohibited = false;
  
  /** Constructs a BooleanClause with query <code>q</code>, required
    <code>r</code> and prohibited <code>p</code>. */ 
  public BooleanClause(Query q, boolean r, boolean p) {
    query = q;
    required = r;
    prohibited = p;
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

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

package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ConstantScoreQuery;

import java.io.IOException;

/**
 * @author yonik
 * @version $Id$
 */
public class ConstantScorePrefixQuery extends Query {
  private final Term prefix;

  public ConstantScorePrefixQuery(Term prefix) {
    this.prefix = prefix;
  }

  /** Returns the prefix  for this query */
  public Term getPrefix() { return prefix; }

  public Query rewrite(IndexReader reader) throws IOException {
    // TODO: if number of terms are low enough, rewrite to a BooleanQuery
    // for potentially faster execution.
    // TODO: cache the bitset somewhere instead of regenerating it
    Query q = new ConstantScoreQuery(new PrefixFilter(prefix));
    q.setBoost(getBoost());
    return q;
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field)
  {
    StringBuffer buffer = new StringBuffer();
    if (!prefix.field().equals(field)) {
      buffer.append(prefix.field());
      buffer.append(":");
    }
    buffer.append(prefix.text());
    buffer.append('*');
    if (getBoost() != 1.0f) {
      buffer.append("^");
      buffer.append(Float.toString(getBoost()));
    }
    return buffer.toString();
  }

    /** Returns true if <code>o</code> is equal to this. */
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ConstantScorePrefixQuery)) return false;
      ConstantScorePrefixQuery other = (ConstantScorePrefixQuery) o;
      return this.prefix.equals(other.prefix) && this.getBoost()==other.getBoost();
    }

    /** Returns a hash code value for this object.*/
    public int hashCode() {
      int h = prefix.hashCode() ^ Float.floatToIntBits(getBoost());
      h ^= (h << 14) | (h >>> 19);  // reversible (1 to 1) transformation unique to ConstantScorePrefixQuery
      return h;
    }

}

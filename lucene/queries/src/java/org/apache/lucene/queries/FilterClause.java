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
package org.apache.lucene.queries;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Filter;

/**
 * A Filter that wrapped with an indication of how that filter
 * is used when composed with another filter.
 * (Follows the boolean logic in BooleanClause for composition 
 * of queries.)
 */
public final class FilterClause {

  private final Occur occur;
  private final Filter filter;

  /**
   * Create a new FilterClause
   * @param filter A Filter object containing a BitSet
   * @param occur A parameter implementation indicating SHOULD, MUST or MUST NOT
   */

  public FilterClause(Filter filter, Occur occur) {
    this.occur = occur;
    this.filter = filter;
  }

  /**
   * Returns this FilterClause's filter
   * @return A Filter object
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * Returns this FilterClause's occur parameter
   * @return An Occur object
   */
  public Occur getOccur() {
    return occur;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (o == null || !(o instanceof FilterClause))
      return false;
    final FilterClause other = (FilterClause)o;
    return this.filter.equals(other.filter)
      && this.occur == other.occur;
  }

  @Override
  public int hashCode() {
    return filter.hashCode() ^ occur.hashCode();
  }

  public String toString(String field) {
    return occur.toString() + filter.toString(field);
  }

  @Override
  public String toString() {
    return toString("");
  }

}

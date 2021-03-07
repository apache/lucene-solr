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
package org.apache.solr.search;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TwoPhaseIterator;

/** The ExtendedQuery interface provides extra metadata to a query.
 *  Implementations of ExtendedQuery must also extend Query.
 */
public interface ExtendedQuery {
  /** Should this query be cached in the query cache or filter cache. */
  public boolean getCache();

  public void setCache(boolean cache);

  /**
   * Returns the cost of this query, used to order checking of filters that are not cached. If
   * getCache()==false &amp;&amp; getCost()&gt;=100 &amp;&amp; this instanceof PostFilter, then the
   * PostFilter interface will be used for filtering.  Otherwise, for smaller costs, this cost will
   * be used for {@link TwoPhaseIterator#matchCost()}.
   */
  public int getCost();

  public void setCost(int cost);

  /**
   * Returns this Query, applying {@link QueryUtils#makeQueryable(Query)} and maybe wrapping it to
   * apply the {@link #getCost()} if it's above zero. Subclasses may customize this if the query
   * internally applies the configurable cost on the underlying {@link
   * TwoPhaseIterator#matchCost()}.
   */
  public default Query getCostAppliedQuery() {
    Query me = QueryUtils.makeQueryable((Query) this);
    if (getCost() <= 0) {
      return me;
    }
    return new MatchCostQuery(me, getCost());
  }
}

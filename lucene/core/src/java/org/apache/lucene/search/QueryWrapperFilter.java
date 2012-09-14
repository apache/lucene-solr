package org.apache.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.Bits;

/** 
 * Constrains search results to only match those which also match a provided
 * query.  
 *
 * <p> This could be used, for example, with a {@link TermRangeQuery} on a suitably
 * formatted date field to implement date filtering.  One could re-use a single
 * QueryFilter that matches, e.g., only documents modified within the last
 * week.  The QueryFilter and TermRangeQuery would only need to be reconstructed
 * once per day.
 */
public class QueryWrapperFilter extends Filter {
  private final Query query;

  /** Constructs a filter which only matches documents matching
   * <code>query</code>.
   */
  public QueryWrapperFilter(Query query) {
    if (query == null)
      throw new NullPointerException("Query may not be null");
    this.query = query;
  }
  
  /** returns the inner Query */
  public final Query getQuery() {
    return query;
  }

  @Override
  public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    // get a private context that is used to rewrite, createWeight and score eventually
    final AtomicReaderContext privateContext = context.reader().getContext();
    final Weight weight = new IndexSearcher(privateContext).createNormalizedWeight(query);
    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return weight.scorer(privateContext, true, false, acceptDocs);
      }
      @Override
      public boolean isCacheable() { return false; }
    };
  }

  @Override
  public String toString() {
    return "QueryWrapperFilter(" + query + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QueryWrapperFilter))
      return false;
    return this.query.equals(((QueryWrapperFilter)o).query);
  }

  @Override
  public int hashCode() {
    return query.hashCode() ^ 0x923F64B9;
  }
}

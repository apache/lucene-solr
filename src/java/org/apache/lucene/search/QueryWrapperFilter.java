package org.apache.lucene.search;

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
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.OpenBitSet;

/** 
 * Constrains search results to only match those which also match a provided
 * query.  
 *
 * <p> This could be used, for example, with a {@link RangeQuery} on a suitably
 * formatted date field to implement date filtering.  One could re-use a single
 * QueryFilter that matches, e.g., only documents modified within the last
 * week.  The QueryFilter and RangeQuery would only need to be reconstructed
 * once per day.
 *
 * @version $Id:$
 */
public class QueryWrapperFilter extends Filter {
  private Query query;

  /** Constructs a filter which only matches documents matching
   * <code>query</code>.
   */
  public QueryWrapperFilter(Query query) {
    this.query = query;
  }

  /**
   * @deprecated Use {@link #getDocIdSet(IndexReader)} instead.
   */
  public BitSet bits(IndexReader reader) throws IOException {
    final BitSet bits = new BitSet(reader.maxDoc());

    new IndexSearcher(reader).search(query, new HitCollector() {
      public final void collect(int doc, float score) {
        bits.set(doc);  // set bit for hit
      }
    });
    return bits;
  }
  
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    final OpenBitSet bits = new OpenBitSet(reader.maxDoc());

    new IndexSearcher(reader).search(query, new HitCollector() {
      public final void collect(int doc, float score) {
        bits.set(doc);  // set bit for hit
      }
    });
    return bits;
  }

  public String toString() {
    return "QueryWrapperFilter(" + query + ")";
  }

  public boolean equals(Object o) {
    if (!(o instanceof QueryWrapperFilter))
      return false;
    return this.query.equals(((QueryWrapperFilter)o).query);
  }

  public int hashCode() {
    return query.hashCode() ^ 0x923F64B9;
  }
}

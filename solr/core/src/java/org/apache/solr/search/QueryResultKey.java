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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import java.io.IOException;
import java.util.List;

/** A hash key encapsulating a query, a list of filters, and a sort
 *
 */
public final class QueryResultKey {
  final Query query;
  final Sort sort;
  final SortField[] sfields;
  final List<Query> filters;
  final int nc_flags;  // non-comparable flags... ignored by hashCode and equals

  private final int hc;  // cached hashCode

  private static SortField[] defaultSort = new SortField[0];


  public QueryResultKey(Query query, List<Query> filters, Sort sort, int nc_flags) throws IOException {
    this.query = query;
    this.sort = sort;
    this.filters = filters;
    this.nc_flags = nc_flags;

    int h = query.hashCode();

    if (filters != null) h ^= filters.hashCode();

    sfields = (this.sort !=null) ? this.sort.getSort() : defaultSort;
    for (SortField sf : sfields) {
      h = h*29 + sf.hashCode();
    }

    hc = h;
  }

  @Override
  public int hashCode() {
    return hc;
  }

  @Override
  public boolean equals(Object o) {
    if (o==this) return true;
    if (!(o instanceof QueryResultKey)) return false;
    QueryResultKey other = (QueryResultKey)o;

    // fast check of the whole hash code... most hash tables will only use
    // some of the bits, so if this is a hash collision, it's still likely
    // that the full cached hash code will be different.
    if (this.hc != other.hc) return false;

    // check for the thing most likely to be different (and the fastest things)
    // first.
    if (this.sfields.length != other.sfields.length) return false;
    if (!this.query.equals(other.query)) return false;
    if (!isEqual(this.filters, other.filters)) return false;

    for (int i=0; i<sfields.length; i++) {
      SortField sf1 = this.sfields[i];
      SortField sf2 = other.sfields[i];
      if (!sf1.equals(sf2)) return false;
    }

    return true;
  }


  private static boolean isEqual(Object o1, Object o2) {
    if (o1==o2) return true;  // takes care of identity and null cases
    if (o1==null || o2==null) return false;
    return o1.equals(o2);
  }
}

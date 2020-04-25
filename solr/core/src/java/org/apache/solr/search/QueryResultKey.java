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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.List;
import java.util.ArrayList;

/** A hash key encapsulating a query, a list of filters, and a sort
 *
 */
public final class QueryResultKey implements Accountable {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(QueryResultKey.class);
  private static final long BASE_SF_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SortField.class);

  final Query query;
  final Sort sort;
  final SortField[] sfields;
  final List<Query> filters;
  final int nc_flags;  // non-comparable flags... ignored by hashCode and equals

  private final int hc;  // cached hashCode
  private final long ramBytesUsed; // cached

  private static SortField[] defaultSort = new SortField[0];


  public QueryResultKey(Query query, List<Query> filters, Sort sort, int nc_flags) {
    this.query = query;
    this.sort = sort;
    this.filters = filters;
    this.nc_flags = nc_flags;

    int h = query.hashCode();

    if (filters != null) {
      for (Query filt : filters)
        // NOTE: simple summation used here so keys with the same filters but in
        // different orders get the same hashCode
        h += filt.hashCode();
    }

    sfields = (this.sort !=null) ? this.sort.getSort() : defaultSort;
    long ramSfields = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    for (SortField sf : sfields) {
      h = h*29 + sf.hashCode();
      ramSfields += BASE_SF_RAM_BYTES_USED + RamUsageEstimator.sizeOfObject(sf.getField());
    }

    hc = h;

    ramBytesUsed =
        BASE_RAM_BYTES_USED +
        ramSfields +
        RamUsageEstimator.sizeOfObject(query, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.sizeOfObject(filters, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED);
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
    if (!unorderedCompare(this.filters, other.filters)) return false;

    for (int i=0; i<sfields.length; i++) {
      SortField sf1 = this.sfields[i];
      SortField sf2 = other.sfields[i];
      if (!sf1.equals(sf2)) return false;
    }

    return true;
  }

  /** 
   * compares the two lists of queries in an unordered manner such that this method 
   * returns true if the 2 lists are the same size, and contain the same elements.
   *
   * This method should only be used if the lists come from QueryResultKeys which have 
   * already been found to have equal hashCodes, since the unordered comparison aspects 
   * of the logic are not cheap.
   * 
   * @return true if the lists of equivalent other then the ordering
   */
  private static boolean unorderedCompare(List<Query> fqList1, List<Query> fqList2) {
    // Do fast version first, expecting that filters are usually in the same order
    //
    // Fall back to unordered compare logic on the first non-equal elements.
    // The slower unorderedCompare should pretty much never be called if filter 
    // lists are generally ordered consistently
    if (fqList1 == fqList2) return true;  // takes care of identity and null cases
    if (fqList1 == null || fqList2 == null) return false;
    int sz = fqList1.size();
    if (sz != fqList2.size()) return false;

    for (int i = 0; i < sz; i++) {
      if (!fqList1.get(i).equals(fqList2.get(i))) {
        return unorderedCompare(fqList1, fqList2, i);
      }
    }
    return true;
  }


  /** 
   * Does an unordered comparison of the elements of two lists of queries starting at 
   * the specified start index.
   * 
   * This method should only be called on lists which are the same size, and where 
   * all items with an index less then the specified start index are the same.
   *
   * @return true if the list items after start are equivalent other then the ordering
   */
  private static boolean unorderedCompare(List<Query> fqList1, List<Query> fqList2, int start) {
    assert null != fqList1;
    assert null != fqList2;

    final int sz = fqList1.size();
    assert fqList2.size() == sz;

    // SOLR-5618: if we had a guarantee that the lists never contained any duplicates,
    // this logic could be a lot simpler 
    //
    // (And of course: if the SolrIndexSearcher / QueryCommmand was ever changed to
    // sort the filter query list, then this whole method could be eliminated).

    final ArrayList<Query> set2 = new ArrayList<>(fqList2.subList(start, sz));
    for (int i = start; i < sz; i++) {
      Query q1 = fqList1.get(i);
      if ( ! set2.remove(q1) ) {
        return false;
      }
    }
    return set2.isEmpty();
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }
}

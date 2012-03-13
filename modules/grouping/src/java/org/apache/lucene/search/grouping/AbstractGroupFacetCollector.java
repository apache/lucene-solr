package org.apache.lucene.search.grouping;

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

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;

/**
 * Base class for computing grouped facets.
 *
 * @lucene.experimental
 */
public abstract class AbstractGroupFacetCollector extends Collector {

  protected final String groupField;
  protected final String facetField;
  protected final BytesRef facetPrefix;

  protected AbstractGroupFacetCollector(String groupField, String facetField, BytesRef facetPrefix) {
    this.groupField = groupField;
    this.facetField = facetField;
    this.facetPrefix = facetPrefix;
  }

  /**
   * Returns grouped facet results that were computed over zero or more segments.
   * Grouped facet counts are merged from zero or more segment results.
   *
   * @param size The total number of facets to include. This is typically offset + limit
   * @param minCount The minimum count a facet entry should have to be included in the grouped facet result
   * @param orderByCount Whether to sort the facet entries by facet entry count. If <code>false</code> then the facets
   *                     are sorted lexicographically in ascending order.
   * @return grouped facet results
   * @throws IOException If I/O related errors occur during merging segment grouped facet counts.
   */
  public abstract GroupedFacetResult mergeSegmentResults(int size, int minCount, boolean orderByCount) throws IOException;

  public void setScorer(Scorer scorer) throws IOException {
  }

  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  /**
   * The grouped facet result. Containing grouped facet entries, total count and total missing count.
   */
  public static class GroupedFacetResult {

    private final static Comparator<FacetEntry> orderByCountAndValue = new Comparator<FacetEntry>() {

      public int compare(FacetEntry a, FacetEntry b) {
        int cmp = b.count - a.count; // Highest count first!
        if (cmp != 0) {
          return cmp;
        }
        return a.value.compareTo(b.value);
      }

    };

    private final static Comparator<FacetEntry> orderByValue = new Comparator<FacetEntry>() {

      public int compare(FacetEntry a, FacetEntry b) {
        return a.value.compareTo(b.value);
      }

    };

    private final int maxSize;
    private final NavigableSet<FacetEntry> facetEntries;
    private final int totalMissingCount;
    private final int totalCount;

    private int currentMin;

    public GroupedFacetResult(int size, int minCount, boolean orderByCount, int totalCount, int totalMissingCount) {
      this.facetEntries = new TreeSet<FacetEntry>(orderByCount ? orderByCountAndValue : orderByValue);
      this.totalMissingCount = totalMissingCount;
      this.totalCount = totalCount;
      maxSize = size;
      currentMin = minCount;
    }

    public void addFacetCount(BytesRef facetValue, int count) {
      if (count < currentMin) {
        return;
      }

      FacetEntry facetEntry = new FacetEntry(facetValue, count);
      if (facetEntries.size() == maxSize) {
        if (facetEntries.higher(facetEntry) == null) {
          return;
        }
        facetEntries.pollLast();
      }
      facetEntries.add(facetEntry);

      if (facetEntries.size() == maxSize) {
        currentMin = facetEntries.last().count;
      }
    }

    /**
     * Returns a list of facet entries to be rendered based on the specified offset and limit.
     * The facet entries are retrieved from the facet entries collected during merging.
     *
     * @param offset The offset in the collected facet entries during merging
     * @param limit The number of facets to return starting from the offset.
     * @return a list of facet entries to be rendered based on the specified offset and limit
     */
    public List<FacetEntry> getFacetEntries(int offset, int limit) {
      List<FacetEntry> entries = new LinkedList<FacetEntry>();
      limit += offset;

      int i = 0;
      for (FacetEntry facetEntry : facetEntries) {
        if (i < offset) {
          i++;
          continue;
        }
        if (i++ >= limit) {
          break;
        }
        entries.add(facetEntry);
      }
      return entries;
    }

    /**
     * Returns the sum of all facet entries counts.
     *
     * @return the sum of all facet entries counts
     */
    public int getTotalCount() {
      return totalCount;
    }

    /**
     * Returns the number of groups that didn't have a facet value.
     *
     * @return the number of groups that didn't have a facet value
     */
    public int getTotalMissingCount() {
      return totalMissingCount;
    }
  }

  /**
   * Represents a facet entry with a value and a count.
   */
  public static class FacetEntry {

    private final BytesRef value;
    private final int count;

    public FacetEntry(BytesRef value, int count) {
      this.value = value;
      this.count = count;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FacetEntry that = (FacetEntry) o;

      if (count != that.count) return false;
      if (!value.equals(that.value)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = value.hashCode();
      result = 31 * result + count;
      return result;
    }

    @Override
    public String toString() {
      return "FacetEntry{" +
          "value=" + value.utf8ToString() +
          ", count=" + count +
          '}';
    }

    /**
     * @return The value of this facet entry
     */
    public BytesRef getValue() {
      return value;
    }

    /**
     * @return The count (number of groups) of this facet entry.
     */
    public int getCount() {
      return count;
    }
  }

}

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
package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * Base class for computing grouped facets.
 *
 * @lucene.experimental
 */
public abstract class GroupFacetCollector extends SimpleCollector {

  protected final String groupField;
  protected final String facetField;
  protected final BytesRef facetPrefix;
  protected final List<SegmentResult> segmentResults;

  protected int[] segmentFacetCounts;
  protected int segmentTotalCount;
  protected int startFacetOrd;
  protected int endFacetOrd;

  protected GroupFacetCollector(String groupField, String facetField, BytesRef facetPrefix) {
    this.groupField = groupField;
    this.facetField = facetField;
    this.facetPrefix = facetPrefix;
    segmentResults = new ArrayList<>();
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
  public GroupedFacetResult mergeSegmentResults(int size, int minCount, boolean orderByCount) throws IOException {
    if (segmentFacetCounts != null) {
      segmentResults.add(createSegmentResult());
      segmentFacetCounts = null; // reset
    }

    int totalCount = 0;
    int missingCount = 0;
    SegmentResultPriorityQueue segments = new SegmentResultPriorityQueue(segmentResults.size());
    for (SegmentResult segmentResult : segmentResults) {
      missingCount += segmentResult.missing;
      if (segmentResult.mergePos >= segmentResult.maxTermPos) {
        continue;
      }
      totalCount += segmentResult.total;
      segments.add(segmentResult);
    }

    GroupedFacetResult facetResult = new GroupedFacetResult(size, minCount, orderByCount, totalCount, missingCount);
    while (segments.size() > 0) {
      SegmentResult segmentResult = segments.top();
      BytesRef currentFacetValue = BytesRef.deepCopyOf(segmentResult.mergeTerm);
      int count = 0;

      do {
        count += segmentResult.counts[segmentResult.mergePos++];
        if (segmentResult.mergePos < segmentResult.maxTermPos) {
          segmentResult.nextTerm();
          segmentResult = segments.updateTop();
        } else {
          segments.pop();
          segmentResult = segments.top();
          if (segmentResult == null) {
            break;
          }
        }
      } while (currentFacetValue.equals(segmentResult.mergeTerm));
      facetResult.addFacetCount(currentFacetValue, count);
    }
    return facetResult;
  }

  protected abstract SegmentResult createSegmentResult() throws IOException;

  @Override
  public void setScorer(Scorable scorer) throws IOException {
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  /**
   * The grouped facet result. Containing grouped facet entries, total count and total missing count.
   */
  public static class GroupedFacetResult {

    private final static Comparator<FacetEntry> orderByCountAndValue = new Comparator<FacetEntry>() {

      @Override
      public int compare(FacetEntry a, FacetEntry b) {
        int cmp = b.count - a.count; // Highest count first!
        if (cmp != 0) {
          return cmp;
        }
        return a.value.compareTo(b.value);
      }

    };

    private final static Comparator<FacetEntry> orderByValue = new Comparator<FacetEntry>() {

      @Override
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
      this.facetEntries = new TreeSet<>(orderByCount ? orderByCountAndValue : orderByValue);
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
      List<FacetEntry> entries = new LinkedList<>();

      int skipped = 0;
      int included = 0;
      for (FacetEntry facetEntry : facetEntries) {
        if (skipped < offset) {
          skipped++;
          continue;
        }
        if (included++ >= limit) {
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

  /**
   * Contains the local grouped segment counts for a particular segment.
   * Each <code>SegmentResult</code> must be added together.
   */
  protected abstract static class SegmentResult {

    protected final int[] counts;
    protected final int total;
    protected final int missing;
    protected final int maxTermPos;

    protected BytesRef mergeTerm;
    protected int mergePos;

    protected SegmentResult(int[] counts, int total, int missing, int maxTermPos) {
      this.counts = counts;
      this.total = total;
      this.missing = missing;
      this.maxTermPos = maxTermPos;
    }

    /**
     * Go to next term in this <code>SegmentResult</code> in order to retrieve the grouped facet counts.
     *
     * @throws IOException If I/O related errors occur
     */
    protected abstract void nextTerm() throws IOException;

  }

  private static class SegmentResultPriorityQueue extends PriorityQueue<SegmentResult> {

    SegmentResultPriorityQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(SegmentResult a, SegmentResult b) {
      return a.mergeTerm.compareTo(b.mergeTerm) < 0;
    }
  }

}

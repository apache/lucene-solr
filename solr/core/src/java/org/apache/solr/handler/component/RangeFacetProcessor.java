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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams.FacetRangeMethod;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.IntervalFacets;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SyntaxError;

/**
 * Processor for Range Facets
 */
public class RangeFacetProcessor extends SimpleFacets {

  public RangeFacetProcessor(SolrQueryRequest req, DocSet docs, SolrParams params, ResponseBuilder rb) {
    super(req, docs, params, rb);
  }

  /**
   * Returns a list of value constraints and the associated facet
   * counts for each facet numerical field, range, and interval
   * specified in the SolrParams
   *
   * @see org.apache.solr.common.params.FacetParams#FACET_RANGE
   */
  public NamedList<Object> getFacetRangeCounts() throws IOException, SyntaxError {
    final NamedList<Object> resOuter = new SimpleOrderedMap<>();

    List<RangeFacetRequest> rangeFacetRequests = Collections.emptyList();
    try {
      FacetComponent.FacetContext facetContext = FacetComponent.FacetContext.getFacetContext(req);
      rangeFacetRequests = facetContext.getAllRangeFacetRequests();
    } catch (IllegalStateException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to compute facet ranges, facet context is not set");
    }

    if (rangeFacetRequests.isEmpty()) return resOuter;
    for (RangeFacetRequest rangeFacetRequest : rangeFacetRequests) {
      getFacetRangeCounts(rangeFacetRequest, resOuter);
    }

    return resOuter;
  }

  /**
   * Returns a list of value constraints and the associated facet counts
   * for each facet range specified by the given {@link RangeFacetRequest}
   */
  public void getFacetRangeCounts(RangeFacetRequest rangeFacetRequest, NamedList<Object> resOuter)
      throws IOException, SyntaxError {

    final IndexSchema schema = searcher.getSchema();

    final String key = rangeFacetRequest.getKey();
    final String f = rangeFacetRequest.facetOn;
    FacetRangeMethod method = rangeFacetRequest.getMethod();

    final SchemaField sf = schema.getField(f);
    final FieldType ft = sf.getType();

    if (method.equals(FacetRangeMethod.DV)) {
      assert ft instanceof TrieField || ft.isPointField();
      resOuter.add(key, getFacetRangeCountsDocValues(rangeFacetRequest));
    } else {
      resOuter.add(key, getFacetRangeCounts(rangeFacetRequest));
    }
  }

  private <T extends Comparable<T>> NamedList getFacetRangeCounts(final RangeFacetRequest rfr)
      throws IOException, SyntaxError {

    final NamedList<Object> res = new SimpleOrderedMap<>();
    final NamedList<Integer> counts = new NamedList<>();
    res.add("counts", counts);

    // explicitly return the gap.
    res.add("gap", rfr.getGapObj());

    DocSet docSet = computeDocSet(docsOrig, rfr.getExcludeTags());

    for (RangeFacetRequest.FacetRange range : rfr.getFacetRanges()) {
      if (range.other != null) {
        // these are added to top-level NamedList
        // and we always include them regardless of mincount
        res.add(range.other.toString(), rangeCount(docSet, rfr, range));
      } else {
        final int count = rangeCount(docSet, rfr, range);
        if (count >= rfr.getMinCount()) {
          counts.add(range.lower, count);
        }
      }
    }

    // explicitly return the start and end so all the counts
    // (including before/after/between) are meaningful - even if mincount
    // has removed the neighboring ranges
    res.add("start", rfr.getStartObj());
    res.add("end", rfr.getEndObj());

    return res;
  }

  private <T extends Comparable<T>> NamedList<Object> getFacetRangeCountsDocValues(RangeFacetRequest rfr)
      throws IOException, SyntaxError {

    SchemaField sf = rfr.getSchemaField();
    final NamedList<Object> res = new SimpleOrderedMap<>();
    final NamedList<Integer> counts = new NamedList<>();
    res.add("counts", counts);

    ArrayList<IntervalFacets.FacetInterval> intervals = new ArrayList<>();

    // explicitly return the gap.  compute this early so we are more
    // likely to catch parse errors before attempting math
    res.add("gap", rfr.getGapObj());

    final int minCount = rfr.getMinCount();

    boolean includeBefore = false;
    boolean includeBetween = false;
    boolean includeAfter = false;

    Set<FacetRangeOther> others = rfr.getOthers();
    // Intervals must be in order (see IntervalFacets.getSortedIntervals), if "BEFORE" or
    // "BETWEEN" are set, they must be added first
    // no matter what other values are listed, we don't do
    // anything if "none" is specified.
    if (!others.contains(FacetRangeOther.NONE)) {
      if (others.contains(FacetRangeOther.ALL) || others.contains(FacetRangeOther.BEFORE)) {
        // We'll add an interval later in this position
        intervals.add(null);
        includeBefore = true;
      }

      if (others.contains(FacetRangeOther.ALL) || others.contains(FacetRangeOther.BETWEEN)) {
        // We'll add an interval later in this position
        intervals.add(null);
        includeBetween = true;
      }

      if (others.contains(FacetRangeOther.ALL) || others.contains(FacetRangeOther.AFTER)) {
        includeAfter = true;
      }
    }

    IntervalFacets.FacetInterval after = null;

    for (RangeFacetRequest.FacetRange range : rfr.getFacetRanges()) {
      if (range.other != null) {
        switch (range.other) {
          case BEFORE:
            assert range.lower == null;
            intervals.set(0, new IntervalFacets.FacetInterval(sf, "*", range.upper, range.includeLower,
                range.includeUpper, FacetRangeOther.BEFORE.toString()));
            break;
          case AFTER:
            assert range.upper == null;
            after = new IntervalFacets.FacetInterval(sf, range.lower, "*",
                range.includeLower, range.includeUpper, FacetRangeOther.AFTER.toString());
            break;
          case BETWEEN:
            intervals.set(includeBefore ? 1 : 0, new IntervalFacets.FacetInterval(sf, range.lower, range.upper,
                range.includeLower, range.includeUpper, FacetRangeOther.BETWEEN.toString()));
            break;
          case ALL:
          case NONE:
            break;
        }
      } else {
        intervals.add(new IntervalFacets.FacetInterval(sf, range.lower, range.upper, range.includeLower, range.includeUpper, range.lower));
      }
    }

    if (includeAfter) {
      assert after != null;
      intervals.add(after);
    }

    IntervalFacets.FacetInterval[] intervalsArray = intervals.toArray(new IntervalFacets.FacetInterval[intervals.size()]);
    // don't use the ArrayList anymore
    intervals = null;

    new IntervalFacets(sf, searcher, computeDocSet(docsOrig, rfr.getExcludeTags()), intervalsArray);

    int intervalIndex = 0;
    int lastIntervalIndex = intervalsArray.length - 1;
    // if the user requested "BEFORE", it will be the first of the intervals. Needs to be added to the
    // response named list instead of with the counts
    if (includeBefore) {
      res.add(intervalsArray[intervalIndex].getKey(), intervalsArray[intervalIndex].getCount());
      intervalIndex++;
    }

    // if the user requested "BETWEEN", it will be the first or second of the intervals (depending on if
    // "BEFORE" was also requested). Needs to be added to the response named list instead of with the counts
    if (includeBetween) {
      res.add(intervalsArray[intervalIndex].getKey(), intervalsArray[intervalIndex].getCount());
      intervalIndex++;
    }

    // if the user requested "AFTER", it will be the last of the intervals.
    // Needs to be added to the response named list instead of with the counts
    if (includeAfter) {
      res.add(intervalsArray[lastIntervalIndex].getKey(), intervalsArray[lastIntervalIndex].getCount());
      lastIntervalIndex--;
    }
    // now add all other intervals to the counts NL
    while (intervalIndex <= lastIntervalIndex) {
      IntervalFacets.FacetInterval interval = intervalsArray[intervalIndex];
      if (interval.getCount() >= minCount) {
        counts.add(interval.getKey(), interval.getCount());
      }
      intervalIndex++;
    }

    res.add("start", rfr.getStartObj());
    res.add("end", rfr.getEndObj());
    return res;
  }

  /**
   * Macro for getting the numDocs of range over docs
   *
   * @see org.apache.solr.search.SolrIndexSearcher#numDocs
   * @see org.apache.lucene.search.TermRangeQuery
   */
  protected int rangeCount(DocSet subset, RangeFacetRequest rfr, RangeFacetRequest.FacetRange fr) throws IOException, SyntaxError {
    SchemaField schemaField = rfr.getSchemaField();
    Query rangeQ = schemaField.getType().getRangeQuery(null, schemaField, fr.lower, fr.upper, fr.includeLower, fr.includeUpper);
    if (rfr.isGroupFacet()) {
      return getGroupedFacetQueryCount(rangeQ, subset);
    } else {
      return searcher.numDocs(rangeQ, subset);
    }
  }

}


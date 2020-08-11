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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.PivotListEntry;

public class PivotFacetHelper {

  /**
   * Encodes a value path as a string for the purposes of a refinement request
   *
   * @see PivotFacetValue#getValuePath
   * @see #decodeRefinementValuePath
   */
  public static String encodeRefinementValuePath(List<String> values) {
    // HACK: prefix flag every value to account for empty string vs null
    // NOTE: even if we didn't have to worry about null's smartSplit is stupid about
    // pruning empty strings from list
    // "^" prefix = null
    // "~" prefix = not null, may be empty string

    assert null != values;

    // special case: empty list => empty string
    if (values.isEmpty()) {
      return "";
    }
    
    StringBuilder out = new StringBuilder();
    for (String val : values) {
      if (null == val) {
        out.append('^');
      } else {
        out.append('~');
        StrUtils.appendEscapedTextToBuilder(out, val, ',');
      }
      out.append(',');
    }
    out.deleteCharAt(out.length()-1);  // prune the last separator
    return out.toString();
    // return StrUtils.join(values, ',');
  }

  /**
   * Decodes a value path string specified for refinement.
   *
   * @see #encodeRefinementValuePath
   */
  public static List<String> decodeRefinementValuePath(String valuePath) {
    List<String> rawvals = StrUtils.splitSmart(valuePath, ",", true);
    // special case: empty list => empty string
    if (rawvals.isEmpty()) return rawvals;

    List<String> out = new ArrayList<>(rawvals.size());
    for (String raw : rawvals) {
      assert 0 < raw.length();
      if ('^' == raw.charAt(0)) {
        assert 1 == raw.length();
        out.add(null);
      } else {
        assert '~' == raw.charAt(0);
        out.add(raw.substring(1));
      }
    }

    return out;
  }

  /** @see PivotListEntry#VALUE */
  @SuppressWarnings({"rawtypes"})
  public static Comparable getValue(NamedList<Object> pivotList) {
    return (Comparable) PivotListEntry.VALUE.extract(pivotList);
  }

  /** @see PivotListEntry#FIELD */
  public static String getField(NamedList<Object> pivotList) {
    return (String) PivotListEntry.FIELD.extract(pivotList);
  }
  
  /** @see PivotListEntry#COUNT */
  public static Integer getCount(NamedList<Object> pivotList) {
    return (Integer) PivotListEntry.COUNT.extract(pivotList);
  }

  /** @see PivotListEntry#PIVOT */
  @SuppressWarnings({"unchecked"})
  public static List<NamedList<Object>> getPivots(NamedList<Object> pivotList) {
    return (List<NamedList<Object>>) PivotListEntry.PIVOT.extract(pivotList);
  }
  
  /** @see PivotListEntry#STATS */
  @SuppressWarnings({"unchecked"})
  public static NamedList<NamedList<NamedList<?>>> getStats(NamedList<Object> pivotList) {
    return (NamedList<NamedList<NamedList<?>>>) PivotListEntry.STATS.extract(pivotList);
  }

  /** @see PivotListEntry#QUERIES */
  @SuppressWarnings({"unchecked"})
  public static NamedList<Number> getQueryCounts(NamedList<Object> pivotList) {
    return (NamedList<Number>) PivotListEntry.QUERIES.extract(pivotList);
  }
  
  /** @see PivotListEntry#RANGES */
  @SuppressWarnings({"unchecked"})
  public static SimpleOrderedMap<SimpleOrderedMap<Object>> getRanges(NamedList<Object> pivotList) {
    return (SimpleOrderedMap<SimpleOrderedMap<Object>>) PivotListEntry.RANGES.extract(pivotList);
  }
  
  /**
   * Given a mapping of keys to {@link StatsValues} representing the currently 
   * known "merged" stats (which may be null if none exist yet), and a 
   * {@link NamedList} containing the "stats" response block returned by an individual 
   * shard, this method accumulates the stats for each {@link StatsField} found in
   * the shard response with the existing mergeStats
   *
   * @return the original <code>merged</code> Map after modifying, or a new Map if the <code>merged</code> param was originally null.
   * @see StatsInfo#getStatsField
   * @see StatsValuesFactory#createStatsValues
   * @see StatsValues#accumulate(NamedList)
   */
  public static Map<String,StatsValues> mergeStats
    (Map<String,StatsValues> merged, 
     NamedList<NamedList<NamedList<?>>> remoteWrapper, 
     StatsInfo statsInfo) {

    if (null == merged) merged = new LinkedHashMap<>();

    NamedList<NamedList<?>> remoteStats = StatsComponent.unwrapStats(remoteWrapper);

    for (Entry<String,NamedList<?>> entry : remoteStats) {
      StatsValues receivingStatsValues = merged.get(entry.getKey());
      if (receivingStatsValues == null) {
        StatsField receivingStatsField = statsInfo.getStatsField(entry.getKey());
        if (null == receivingStatsField) {
          throw new SolrException(ErrorCode.SERVER_ERROR , "No stats.field found corresponding to pivot stats received from shard: "+entry.getKey());
        }
        receivingStatsValues = StatsValuesFactory.createStatsValues(receivingStatsField);
        merged.put(entry.getKey(), receivingStatsValues);
      }
      receivingStatsValues.accumulate(entry.getValue());
    }
    return merged;
  }

  /**
   * Merges query counts returned by a shard into global query counts.
   * Entries found only in shard's query counts will be added to global counts.
   * Entries found in both shard and global query counts will be summed.
   *
   * @param globalQueryCounts The global query counts (across all shards) in which to merge the shard query counts
   * @param shardQueryCounts  Named list from a shard response to be merged into the global counts.
   * @return NamedList containing merged values
   */
  static NamedList<Number> mergeQueryCounts(
      NamedList<Number> globalQueryCounts, NamedList<Number> shardQueryCounts) {
    if (globalQueryCounts == null) {
      return shardQueryCounts;
    }
    for (Entry<String, Number> entry : shardQueryCounts) {
      int idx = globalQueryCounts.indexOf(entry.getKey(), 0);
      if (idx == -1) {
        globalQueryCounts.add(entry.getKey(), entry.getValue());
      } else {
        globalQueryCounts.setVal(idx, FacetComponent.num(globalQueryCounts.getVal(idx).longValue() + entry.getValue().longValue()));
      }
    }
    return globalQueryCounts;
  }
}

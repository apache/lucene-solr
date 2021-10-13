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

package org.apache.solr.search.facet;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.*;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetBuilder;
import org.apache.solr.search.ExtendedQuery;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.WrappedQuery;
import org.apache.solr.util.DateMathParser;

import static org.apache.solr.search.facet.FacetContext.SKIP_FACET;

class FacetRangeProcessor extends FacetProcessor<FacetRange> {
  // TODO: the code paths for initial faceting, vs refinement, are very different...
  // TODO: ...it might make sense to have seperate classes w/a common base?
  // TODO: let FacetRange.createFacetProcessor decide which one to instantiate?

  final SchemaField sf;
  final Calc calc;
  final EnumSet<FacetParams.FacetRangeInclude> include;
  final long effectiveMincount;
  @SuppressWarnings({"rawtypes"})
  final Comparable start;
  @SuppressWarnings({"rawtypes"})
  final Comparable end;
  final String gap;
  final Object ranges;
  final boolean dv;

  /** Build by {@link #createRangeList} if and only if needed for basic faceting */
  List<Range> rangeList;
  /** Build by {@link #createRangeList} if and only if needed for basic faceting */
  List<Range> otherList;

  /**
   * Serves two purposes depending on the type of request.
   * <ul>
   * <li>If this is a phase#1 shard request, then {@link #createRangeList} will set this value (non null)
   *     if and only if it is needed for refinement (ie: <code>hardend:false</code> &amp; <code>other</code>
   *     that requires an end value low/high value calculation).  And it wil be included in the response</li>
   * <li>If this is a phase#2 refinement request, this variable will be used
   *     {@link #getOrComputeActualEndForRefinement} to track the value sent with the refinement request
   *     -- or to cache a recomputed value if the request omitted it -- for use in refining the
   *     <code>other</code> buckets that need them</li>
   * </ul>
   */
  @SuppressWarnings({"rawtypes"})
  Comparable actual_end = null; // null until/unless we need it

  FacetRangeProcessor(FacetContext fcontext, FacetRange freq) {
    super(fcontext, freq);
    include = freq.include;
    sf = fcontext.searcher.getSchema().getField(freq.field);
    calc = getCalcForField(sf);
    if (freq.ranges != null && (freq.start != null || freq.end != null || freq.gap != null)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Cannot set gap/start/end and ranges params together");
    }
    if (freq.ranges != null) {
      ranges = freq.ranges;
      start = null;
      end = null;
      gap = null;
    } else {
      start = calc.getValue(freq.start.toString());
      end = calc.getValue(freq.end.toString());
      gap = freq.gap.toString();
      ranges = null;
    }
    dv = freq.dv;
    if (dv) {
      if (sf.getType().getNumberType() == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Doc value range facet only supports number types.");
      }
      if (sf.multiValued()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Doc value range facet only supports single value types.");
      }
    }

    // Under the normal mincount=0, each shard will need to return 0 counts since we don't calculate buckets at the top level.
    // If mincount>0 then we could *potentially* set our sub mincount to 1...
    // ...but that would require sorting the buckets (by their val) at the top level
    //
    // Rather then do that, which could be complicated by non trivial field types, we'll force the sub-shard effectiveMincount
    // to be 0, ensuring that we can trivially merge all the buckets from every shard
    // (we have to filter the merged buckets by the original mincount either way)
    effectiveMincount = fcontext.isShard() ? 0 : freq.mincount;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void process() throws IOException {
    super.process();

    if (fcontext.facetInfo != null) { // refinement?
      response = refineFacets();
    } else {
      // phase#1: build list of all buckets and return full facets...
      createRangeList();
      response = getRangeCountsIndexed();
    }
  }

  /**
   * A basic interval tree implementation backed by a balanced BST.
   * Intended to be used solely for facet range accumulation.
   * TODO: generalize
   */
  private static class FacetRangeIntervalTree {
    private final Node root;

    private FacetRangeIntervalTree(Node root) {
      this.root = root;
    }
    
    private static FacetRangeIntervalTree create(List<Range> rangeList, List<Range> otherList) {
      // first combine ranges and preserve index/slot
      List<Pair<Integer, Range>> intervals = new ArrayList<>();
      for (int i = 0; i < rangeList.size(); i++) {
        intervals.add(new Pair<>(i, rangeList.get(i)));
      }
      for (int i = 0; i < otherList.size(); i++) {
        intervals.add(new Pair<>(rangeList.size() + i, otherList.get(i)));
      }
      // then sort for conversion to bst
      intervals.sort((p1, p2) -> {
        Range r1 = p1.second();
        Range r2 = p2.second();
        if (r1.low == null) {
          return r2.low == null ? 0 : -1;
        }
        if (r2.low == null) {
          return 1;
        }
        int res = r1.low.compareTo(r2.low);
        if (res == 0) {
          // tie break goes to if either include lower as that is technically a lower low value
          return r1.includeLower == r2.includeLower ? 0 : (r1.includeLower ? -1 : 1);
        }
        return res;
      });
      return new FacetRangeIntervalTree(convert(intervals, 0, intervals.size()-1));
    }
    
    private static Node convert(List<Pair<Integer, Range>> intervals, int lo, int hi) {
      if (lo > hi) {
        return null;
      }
      int mid = (lo + hi) / 2;
      Node left = convert(intervals, lo, mid-1);
      Node right = convert(intervals, mid+1, hi);
      Pair<Integer, Range> p = intervals.get(mid);
      return Node.create(left, right, p.second(), p.first());
    }
    
    private static int compare(long value, boolean includeValue, boolean high, long oValue, boolean oIncludeValue, boolean oHigh) {
      if (value > oValue) {
        return 1;
      } else if (value < oValue) {
        return -1;
      } else {
        if (includeValue && oIncludeValue) {
          // both inclusive, same value restriction
          return 0;
        } else if (includeValue) {
          // this inclusive, other exclusive
          // if other is a high point and exclusive, it must be less (and thus we are higher)
          // alternatively if it is a low point and exclusive, it must be more (and thus we are lower)
          return oHigh ? 1 : -1;
        } else if (oIncludeValue) {
          // this exclusive, other inclusive
          // reverse of above
          return high ? -1 : 1;
        } else {
          // both exclusive
          if (high == oHigh) {
            return 0;
          } else {
            return high ? -1 : 1;
          }
        }
      }
    }
    
    private void accum(long value, int doc, int localDoc, Accumulator accumulator) throws IOException {
      accumRec(root, value, doc, localDoc, accumulator);
    }
    
    private void accumRec(Node node, long value, int doc, int localDoc, Accumulator accumulator) throws IOException {
      if (node == null) {
        return; // past the leaf node
      }
      // check if we even need to check this subtree
      // if the value is greater than any subtree max, no need to traverse further in the subtree 
      if (compare(value, true, true, node.subtreeMaxValue, node.subtreeIncludeMaxValue, true) > 0) {
        return;
      }
      // at this point, always must go left as there might be ranges that extend past our value
      accumRec(node.left, value, doc, localDoc, accumulator);
      // then check / accumulate current
      if (node.range.includesDocValue(value)) {
        accumulator.accumulate(node.slot, doc, localDoc);
      }
      // optionally, go right if the subtree might have lower values equal to or less than us at this point
      // exit if the current node's low is greater as there would be no right subtree that would match given the sorting
      if (compare(value, true, true, node.range.dvLow, node.range.includeLower, false) < 0) {
        return;
      }
      accumRec(node.right, value, doc, localDoc, accumulator);
    }

    private static class Node {
      private final Node left;
      private final Node right;
      private final Range range;
      private final int slot;
      private final long subtreeMaxValue;
      private final boolean subtreeIncludeMaxValue;

      private Node(Node left, Node right, Range range, int slot, long subtreeMaxValue, boolean subtreeIncludeMaxValue) {
        this.left = left;
        this.right = right;
        this.range = range;
        this.slot = slot;
        this.subtreeMaxValue = subtreeMaxValue;
        this.subtreeIncludeMaxValue = subtreeIncludeMaxValue;
      }

      private static Node create(Node left, Node right, Range range, int slot) {
        long max = range.dvHigh;
        boolean includeMax = range.includeUpper;
        if (left != null) {
          if (compare(left.subtreeMaxValue, left.subtreeIncludeMaxValue, true, max, includeMax, true) > 0) {
            max = left.subtreeMaxValue;
            includeMax = left.subtreeIncludeMaxValue;
          }
        }
        if (right != null) {
          if (compare(right.subtreeMaxValue, right.subtreeIncludeMaxValue, true, max, includeMax, true) > 0) {
            max = right.subtreeMaxValue;
            includeMax = right.subtreeIncludeMaxValue;
          }
        }
        return new Node(left, right, range, slot, max, includeMax);
      }
    }
    
    @FunctionalInterface
    private interface Accumulator {
      // Called for each slot matched by a provided doc (the global and local doc are both provided for convenience).
      void accumulate(int slot, int doc, int localDoc) throws IOException;
    }
  }
  
  @SuppressWarnings({"rawtypes"})
  private static class Range {
    Object label;

    Comparable low;
    long dvLow;
    Comparable high;
    long dvHigh;
    boolean includeLower;
    boolean includeUpper;

    public Range(Object label, Comparable low, long dvLow, Comparable high, long dvHigh, boolean includeLower, boolean includeUpper) {
      this.label = label;
      this.low = low;
      this.dvLow = dvLow;
      this.high = high;
      this.dvHigh = dvHigh;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;
    }

    boolean includesDocValue(long test) {
      int lowc = dvLow == Long.MIN_VALUE && !includeLower ? -1 : Long.compare(dvLow, test);
      int highc = dvHigh == Long.MAX_VALUE && !includeUpper ? 1 : Long.compare(dvHigh, test);
      return (lowc < 0 || (lowc == 0 && includeLower)) && (highc > 0 || (highc == 0 && includeUpper));
    }
  }

  /**
   * Returns a {@link Calc} instance to use for <em>term</em> faceting over a numeric field.
   * This method is unused for <code>range</code> faceting, and exists solely as a helper method for other classes
   *
   * @param sf A field to facet on, must be of a type such that {@link FieldType#getNumberType} is non null
   * @return a <code>Calc</code> instance with {@link Calc#bitsToValue} and {@link Calc#bitsToSortableBits} methods suitable for the specified field.
   * @see FacetFieldProcessorByHashDV
   */
  public static Calc getNumericCalc(SchemaField sf) {
    Calc calc;
    final FieldType ft = sf.getType();

    if (ft instanceof TrieField || ft.isPointField()) {
      switch (ft.getNumberType()) {
        case FLOAT:
          calc = new FloatCalc(sf);
          break;
        case DOUBLE:
          calc = new DoubleCalc(sf);
          break;
        case INTEGER:
          calc = new IntCalc(sf);
          break;
        case LONG:
          calc = new LongCalc(sf);
          break;
        case DATE:
          calc = new DateCalc(sf, null);
          break;
        default:
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Expected numeric field type :" + sf);
      }
    } else {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Expected numeric field type :" + sf);
    }
    return calc;
  }

  /**
   * Helper method used in processor constructor
   * @return a <code>Calc</code> instance with {@link Calc#bitsToValue} and {@link Calc#bitsToSortableBits} methods suitable for the specified field.
   */
  private static Calc getCalcForField(SchemaField sf) {
    final FieldType ft = sf.getType();
    if (ft instanceof TrieField || ft.isPointField()) {
      switch (ft.getNumberType()) {
        case FLOAT:
          return new FloatCalc(sf);
        case DOUBLE:
          return new DoubleCalc(sf);
        case INTEGER:
          return new IntCalc(sf);
        case LONG:
          return new LongCalc(sf);
        case DATE:
          return new DateCalc(sf, null);
        default:
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Unable to range facet on numeric field of unexpected type:" + sf.getName());
      }
    } else if (ft instanceof CurrencyFieldType) {
      return new CurrencyCalc(sf);
    } else if (ft instanceof DateRangeField) {
      return new DateCalc(sf, null);
    }

    // if we made it this far, we have no idea what it is...
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Unable to range facet on field:" + sf.getName());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void createRangeList() throws IOException {

    rangeList = new ArrayList<>();
    otherList = new ArrayList<>(3);

    Comparable low = start;
    Comparable loop_end = this.end;

    if (ranges != null) {
      rangeList.addAll(parseRanges(ranges));
      return;
    }

    while (low.compareTo(end) < 0) {
      Comparable high = calc.addGap(low, gap);
      if (end.compareTo(high) < 0) {
        if (freq.hardend) {
          high = loop_end;
        } else {
          loop_end = high;
        }
      }
      if (high.compareTo(low) < 0) {
        throw new SolrException
            (SolrException.ErrorCode.BAD_REQUEST,
                "range facet infinite loop (is gap negative? did the math overflow?)");
      }
      if (high.compareTo(low) == 0) {
        throw new SolrException
            (SolrException.ErrorCode.BAD_REQUEST,
                "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: " + low + " + " + gap + " = " + high);
      }

      boolean incLower = (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
          (include.contains(FacetParams.FacetRangeInclude.EDGE) && 0 == low.compareTo(start)));
      boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
          (include.contains(FacetParams.FacetRangeInclude.EDGE) && 0 == high.compareTo(end)));

      Range range = new Range(calc.buildRangeLabel(low), low, calc.toSortableDocValue(low), high, calc.toSortableDocValue(high), incLower, incUpper);
      rangeList.add( range );

      low = high;
    }

    // no matter what other values are listed, we don't do
    // anything if "none" is specified.
    if (! freq.others.contains(FacetParams.FacetRangeOther.NONE) ) {
      final boolean all = freq.others.contains(FacetParams.FacetRangeOther.ALL);

      if (all || freq.others.contains(FacetParams.FacetRangeOther.BEFORE)) {
        otherList.add( buildBeforeRange() );
      }
      if (all || freq.others.contains(FacetParams.FacetRangeOther.AFTER)) {
        actual_end = loop_end;
        otherList.add( buildAfterRange() );
      }
      if (all || freq.others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
        actual_end = loop_end;
        otherList.add( buildBetweenRange() );
      }
    }
    // if we're not a shard request, or this is a hardend:true situation, then actual_end isn't needed
    if (freq.hardend || (! fcontext.isShard())) {
      actual_end = null;
    }
  }

  /**
   * Parses the given list of maps and returns list of Ranges
   *
   * @param input - list of map containing the ranges
   * @return list of {@link Range}
   */
  private List<Range> parseRanges(Object input) {
    if (!(input instanceof List)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expected List for ranges but got " + input.getClass().getSimpleName() + " = " + input
      );
    }
    @SuppressWarnings({"rawtypes"})
    List intervals = (List) input;
    List<Range> ranges = new ArrayList<>();
    for (Object obj : intervals) {
      if (!(obj instanceof Map)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Expected Map for range but got " + obj.getClass().getSimpleName() + " = " + obj);
      }
      @SuppressWarnings({"unchecked"})
      Range range;
      @SuppressWarnings({"unchecked"})
      Map<String, Object> interval = (Map<String, Object>) obj;
      if (interval.containsKey("range")) {
        range = getRangeByOldFormat(interval);
      } else {
        range = getRangeByNewFormat(interval);
      }
      ranges.add(range);
    }
    return ranges;
  }

  private boolean getBoolean(Map<String,Object> args, String paramName, boolean defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    // TODO: should we be more flexible and accept things like "true" (strings)?
    // Perhaps wait until the use case comes up.
    if (!(o instanceof Boolean)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expected boolean type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (Boolean)o;
  }

  private String getString(Map<String,Object> args, String paramName, boolean required) {
    Object o = args.get(paramName);
    if (o == null) {
      if (required) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Missing required parameter '" + paramName + "' for " + args);
      }
      return null;
    }
    if (!(o instanceof String)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expected string type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (String)o;
  }

  /**
   * Parses the range given in format {from:val1, to:val2, inclusive_to:true}
   * and returns the {@link Range}
   *
   * @param rangeMap Map containing the range info
   * @return {@link Range}
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Range getRangeByNewFormat(Map<String, Object> rangeMap) {
    Object fromObj = rangeMap.get("from");
    Object toObj = rangeMap.get("to");

    String fromStr = fromObj == null? "*" : fromObj.toString();
    String toStr = toObj == null? "*": toObj.toString();
    boolean includeUpper = getBoolean(rangeMap, "inclusive_to", false);
    boolean includeLower = getBoolean(rangeMap, "inclusive_from", true);

    Object key = rangeMap.get("key");
    // if (key == null) {
    //  key = (includeLower? "[": "(") + fromStr + "," + toStr + (includeUpper? "]": ")");
    // }
    // using the default key as custom key won't work with refine
    // refine would need both low and high values
    key = (includeLower? "[": "(") + fromStr + "," + toStr + (includeUpper? "]": ")");

    Comparable from = getComparableFromString(fromStr);
    Comparable to = getComparableFromString(toStr);

    if (from != null && to != null && from.compareTo(to) > 0) {
      // allowing from and to be same
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'from' is higher than 'to' in range for key: " + key);
    }

    long dvFrom = from != null ? calc.toSortableDocValue(from) : Long.MIN_VALUE;
    long dvTo = to != null ? calc.toSortableDocValue(to) : Long.MAX_VALUE;
    return new Range(key, from, dvFrom, to, dvTo, includeLower && from != null, includeUpper && to != null);
  }

  /**
   * Parses the range string from the map and Returns {@link Range}
   *
   * @param range map containing the interval
   * @return {@link Range}
   */
  private Range getRangeByOldFormat(Map<String, Object> range) {
    String key = getString(range, "key", false);
    String rangeStr = getString(range, "range", true);
    try {
      return parseRangeFromString(key, rangeStr);
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  /**
   * Parses the given string and returns Range.
   * This is adopted from {@link org.apache.solr.request.IntervalFacets}
   *
   * @param key The name of range which would be used as {@link Range}'s label
   * @param rangeStr The string containing the Range
   * @return {@link Range}
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private Range parseRangeFromString(String key, String rangeStr) throws SyntaxError {
    rangeStr = rangeStr.trim();
    if (rangeStr.isEmpty()) {
      throw new SyntaxError("empty facet range");
    }

    boolean includeLower = true, includeUpper = true;
    Comparable start = null, end = null;
    if (rangeStr.charAt(0) == '(') {
      includeLower = false;
    } else if (rangeStr.charAt(0) != '[') {
      throw new SyntaxError( "Invalid start character " + rangeStr.charAt(0) + " in facet range " + rangeStr);
    }

    final int lastNdx = rangeStr.length() - 1;
    if (rangeStr.charAt(lastNdx) == ')') {
      includeUpper = false;
    } else if (rangeStr.charAt(lastNdx) != ']') {
      throw new SyntaxError("Invalid end character " + rangeStr.charAt(lastNdx) + " in facet range " + rangeStr);
    }

    StringBuilder startStr = new StringBuilder(lastNdx);
    int i = unescape(rangeStr, 1, lastNdx, startStr);
    if (i == lastNdx) {
      if (rangeStr.charAt(lastNdx - 1) == ',') {
        throw new SyntaxError("Empty range limit");
      }
      throw new SyntaxError("Missing unescaped comma separating range ends in " + rangeStr);
    }
    start = getComparableFromString(startStr.toString());

    StringBuilder endStr = new StringBuilder(lastNdx);
    i = unescape(rangeStr, i, lastNdx, endStr);
    if (i != lastNdx) {
      throw new SyntaxError("Extra unescaped comma at index " + i + " in range " + rangeStr);
    }
    end = getComparableFromString(endStr.toString());

    if (start != null && end != null && start.compareTo(end) > 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'start' is higher than 'end' in range for key: " + rangeStr);
    }

    // not using custom key as it won't work with refine
    // refine would need both low and high values
    long dvStart = start != null ? calc.toSortableDocValue(start) : Long.MIN_VALUE;
    long dvEnd = end != null ? calc.toSortableDocValue(end) : Long.MAX_VALUE;
    return new Range(rangeStr, start, dvStart, end, dvEnd, includeLower && start != null, includeUpper && end != null);
  }

  /* Fill in sb with a string from i to the first unescaped comma, or n.
      Return the index past the unescaped comma, or n if no unescaped comma exists */
  private int unescape(String s, int i, int n, StringBuilder sb) throws SyntaxError {
    for (; i < n; ++i) {
      char c = s.charAt(i);
      if (c == '\\') {
        ++i;
        if (i < n) {
          c = s.charAt(i);
        } else {
          throw new SyntaxError("Unfinished escape at index " + i + " in facet range " + s);
        }
      } else if (c == ',') {
        return i + 1;
      }
      sb.append(c);
    }
    return n;
  }

  @SuppressWarnings({"rawtypes"})
  private Comparable getComparableFromString(String value) {
    value = value.trim();
    if ("*".equals(value)) {
      return null;
    }
    return calc.getValue(value);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private  SimpleOrderedMap getRangeCountsIndexed() throws IOException {

    final boolean hasSubFacets = !freq.getSubFacets().isEmpty();

    int slotCount = rangeList.size() + otherList.size();
    if (hasSubFacets) {
      intersections = new DocSet[slotCount];
      filters = new Query[slotCount];
    } else {
      intersections = null;
      filters = null;
    }


    createAccs(fcontext.base.size(), slotCount);

    if (dv) {
      // process docs in one pass, dropping into range buckets
      dvRangeStats(rangeList, otherList, hasSubFacets);
    } else {
      // get stats one at a time using queries
      for (int idx = 0; idx < rangeList.size(); idx++) {
        rangeStats(rangeList.get(idx), idx, hasSubFacets);
      }
      for (int idx = 0; idx < otherList.size(); idx++) {
        rangeStats(otherList.get(idx), rangeList.size() + idx, hasSubFacets);
      }
    }


    final SimpleOrderedMap res = new SimpleOrderedMap<>();
    List<SimpleOrderedMap> buckets = new ArrayList<>();
    res.add("buckets", buckets);

    for (int idx = 0; idx<rangeList.size(); idx++) {
      if (effectiveMincount > 0 && countAcc.getCount(idx) < effectiveMincount) continue;
      Range range = rangeList.get(idx);
      SimpleOrderedMap bucket = new SimpleOrderedMap();
      buckets.add(bucket);
      bucket.add("val", range.label);
      addStats(bucket, idx);
      if (hasSubFacets) doSubs(bucket, idx);
    }

    for (int idx = 0; idx<otherList.size(); idx++) {
      // we don't skip these buckets based on mincount
      Range range = otherList.get(idx);
      SimpleOrderedMap bucket = new SimpleOrderedMap();
      res.add(range.label.toString(), bucket);
      addStats(bucket, rangeList.size() + idx);
      if (hasSubFacets) doSubs(bucket, rangeList.size() + idx);
    }

    if (null != actual_end) {
      res.add(FacetRange.ACTUAL_END_JSON_KEY, calc.formatValue(actual_end));
    }

    return res;
  }

  private Query[] filters;
  private DocSet[] intersections;
  
  private void rangeStats(Range range, int slot, boolean hasSubFacets) throws IOException {
    Query rangeQ = getRangeQuery(range);
    // TODO: specialize count only
    DocSet intersection = fcontext.searcher.getDocSet(rangeQ, fcontext.base);
    if (hasSubFacets) {
      filters[slot] = rangeQ;
      intersections[slot] = intersection;  // save for later  // TODO: only save if number of slots is small enough?
    }
    int num = collect(intersection, slot, slotNum -> { return new SlotAcc.SlotContext(rangeQ); });
    countAcc.incrementCount(slot, num); // TODO: roll this into collect()
  }

  private void dvRangeStats(List<Range> rangeList, List<Range> otherList, boolean hasSubFacets) throws IOException {
    final FieldType ft = sf.getType();
    final String fieldName = sf.getName();
    final NumberType numericType = ft.getNumberType();
    if (numericType == null) {
      throw new IllegalStateException();
    }

    final IndexReader indexReader = fcontext.searcher.getIndexReader();
    final int maxDoc = indexReader.maxDoc();
    final Iterator<LeafReaderContext> ctxIt = indexReader.leaves().iterator();
    LeafReaderContext ctx = null;
    NumericDocValues longs = null;
    final int numSlots = rangeList.size() + otherList.size();
    DocSetBuilder[] setBuilders = hasSubFacets ? new DocSetBuilder[numSlots] : null;
    SlotAcc.SlotContext[] slotContexts = new SlotAcc.SlotContext[numSlots];
    // need to set the slot contexts up front
    for (int slot = 0; slot < numSlots; slot++) {
      Query rangeQ = getRangeQuery(slot < rangeList.size() ? rangeList.get(slot) : otherList.get(slot - rangeList.size()));
      slotContexts[slot] = new SlotAcc.SlotContext(rangeQ);
    }
    IntFunction<SlotAcc.SlotContext> getSlotContext = (int slot) -> slotContexts[slot];

    FacetRangeIntervalTree rangeIntervals = FacetRangeIntervalTree.create(rangeList, otherList);
    FacetRangeIntervalTree.Accumulator accumulator = (int slot, int doc, int localDoc) -> {
      countAcc.incrementCount(slot, 1);
      collect(localDoc, slot, getSlotContext);
      if (setBuilders != null) {
        DocSetBuilder setBuilder = setBuilders[slot];
        if (setBuilder == null) {
          // DocSetBuilder cost estimate calculation pulled from other usages.
          setBuilder = new DocSetBuilder(maxDoc, Math.min(64, (maxDoc >>> 10) + 4));
          setBuilders[slot] = setBuilder;
        }
        setBuilder.add(doc);
      }
    };
    
    for (DocIterator docsIt = fcontext.base.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
        do {
          ctx = ctxIt.next();
        } while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
        assert doc >= ctx.docBase;
        setNextReader(ctx);
        switch (numericType) {
          case LONG:
          case DATE:
          case INTEGER:
            longs = DocValues.getNumeric(ctx.reader(), fieldName);
            break;
          case FLOAT:
            longs = new FilterNumericDocValues(DocValues.getNumeric(ctx.reader(), fieldName)) {
              @Override
              public long longValue() throws IOException {
                return NumericUtils.sortableFloatBits((int) super.longValue());
              }
            };
            break;
          case DOUBLE:
            longs = new FilterNumericDocValues(DocValues.getNumeric(ctx.reader(), fieldName)) {
              @Override
              public long longValue() throws IOException {
                return NumericUtils.sortableDoubleBits(super.longValue());
              }
            };
            break;
          default:
            throw new AssertionError();
        }
      }
      final int localDoc = doc - ctx.docBase;
      int valuesDocID = longs.docID();
      if (valuesDocID < localDoc) {
        valuesDocID = longs.advance(localDoc);
      }
      if (valuesDocID == localDoc) {
        long value = longs.longValue();
        rangeIntervals.accum(value, doc, localDoc, accumulator);
      }
    }
    
    for (int slot = 0; slot < numSlots; slot++) {
      if (setBuilders != null) {
        // finally set the filters and intersection doc sets (only necessary w/ subfacets, setBuilders is always non null in that case)
        filters[slot] = slotContexts[slot].getSlotQuery();
        DocSetBuilder setBuilder = setBuilders[slot];
        if (setBuilder != null) {
          intersections[slot] = setBuilder.build(null);
        } else {
          intersections[slot] = DocSet.EMPTY;
        }
      }
      // and collect any zero counts where required (so that zero subfacets will still show up if a corresponding accumulator decides)
      if (countAcc.getCount(slot) == 0) {
        collect(DocSet.EMPTY, slot, getSlotContext);
      }
    }
  }

  private Query getRangeQuery(Range range) {
    final Query rangeQ;
    {
      final Query rangeQuery = sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low), range.high==null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
      if (fcontext.cache) {
        rangeQ = rangeQuery;
      } else if (rangeQuery instanceof ExtendedQuery) {
        ((ExtendedQuery) rangeQuery).setCache(false);
        rangeQ = rangeQuery;
      } else {
        final WrappedQuery wrappedQuery = new WrappedQuery(rangeQuery);
        wrappedQuery.setCache(false);
        rangeQ = wrappedQuery;
      }
    }
    return rangeQ;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void doSubs(SimpleOrderedMap bucket, int slot) throws IOException {
    // handle sub-facets for this bucket
    DocSet subBase = intersections[slot];
    try {
      processSubs(bucket, filters[slot], subBase, false, null);
    } finally {
      // subContext.base.decref();  // OFF-HEAP
      // subContext.base = null;  // do not modify context after creation... there may be deferred execution (i.e. streaming)
    }
  }

  // Essentially copied from SimpleFacets...
  // would be nice to unify this stuff w/ analytics component...
  /**
   * Perhaps someday instead of having a giant "instanceof" case
   * statement to pick an impl, we can add a "RangeFacetable" marker
   * interface to FieldTypes and they can return instances of these
   * directly from some method -- but until then, keep this locked down
   * and private.
   */
  static abstract class Calc {
    protected final SchemaField field;
    public Calc(final SchemaField field) {
      this.field = field;
    }

    /**
     * Used by {@link FacetFieldProcessorByHashDV} for field faceting on numeric types -- not used for <code>range</code> faceting
     */
    @SuppressWarnings({"rawtypes"})
    public Comparable bitsToValue(long bits) {
      return bits;
    }

    /**
     * Used by {@link FacetFieldProcessorByHashDV} for field faceting on numeric types -- not used for <code>range</code> faceting
     */
    public long bitsToSortableBits(long bits) {
      return bits;
    }

    /**
     * Given the low value for a bucket, generates the appropriate "label" object to use.
     * By default return the low object unmodified.
     */
    public Object buildRangeLabel(@SuppressWarnings("rawtypes") Comparable low) {
      return low;
    }

    /**
     * Formats a value into a label used in a response
     * Default Impl just uses toString()
     */
    public String formatValue(@SuppressWarnings("rawtypes") final Comparable val) {
      return val.toString();
    }

    /**
     * Parses a String param into a value throwing
     * an exception if not possible
     */
    @SuppressWarnings({"rawtypes"})
    public final Comparable getValue(final String rawval) {
      try {
        return parseStr(rawval);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse value "+rawval+" for field: " +
                field.getName(), e);
      }
    }

    /**
     * Parses a String param into a value.
     * Can throw a low level format exception as needed.
     */
    @SuppressWarnings({"rawtypes"})
    protected abstract Comparable parseStr(final String rawval)
        throws java.text.ParseException;

    /**
     * Parses a String param into a value that represents the gap and
     * can be included in the response, throwing
     * a useful exception if not possible.
     *
     * Note: uses Object as the return type instead of T for things like
     * Date where gap is just a DateMathParser string
     */
    public final Object getGap(final String gap) {
      try {
        return parseGap(gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse gap "+gap+" for field: " +
                field.getName(), e);
      }
    }

    /**
     * Parses a String param into a value that represents the gap and
     * can be included in the response.
     * Can throw a low level format exception as needed.
     *
     * Default Impl calls parseVal
     */
    protected Object parseGap(final String rawval) throws java.text.ParseException {
      return parseStr(rawval);
    }

    /**
     * Adds the String gap param to a low Range endpoint value to determine
     * the corresponding high Range endpoint value, throwing
     * a useful exception if not possible.
     */
    @SuppressWarnings({"rawtypes"})
    public final Comparable addGap(Comparable value, String gap) {
      try {
        return parseAndAddGap(value, gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't add gap "+gap+" to value " + value +
                " for field: " + field.getName(), e);
      }
    }
    /**
     * Adds the String gap param to a low Range endpoint value to determine
     * the corresponding high Range endpoint value.
     * Can throw a low level format exception as needed.
     */
    @SuppressWarnings({"rawtypes"})
    protected abstract Comparable parseAndAddGap(Comparable value, String gap)
        throws java.text.ParseException;

    /**
     * Converts the corresponding value produced by a {@link Calc} instance into the corresponding sortable doc values
     * value.
     */
    protected abstract long toSortableDocValue(Comparable calcValue);
  }

  private static class FloatCalc extends Calc {

    @SuppressWarnings("rawtypes")
    @Override
    public Comparable bitsToValue(long bits) {
      if (field.getType().isPointField() && field.multiValued()) {
        return NumericUtils.sortableIntToFloat((int)bits);
      } else {
        return Float.intBitsToFloat( (int)bits );
      }
    }

    @Override
    public long bitsToSortableBits(long bits) {
      return NumericUtils.sortableDoubleBits(bits);
    }

    public FloatCalc(final SchemaField f) { super(f); }
    @Override
    protected Float parseStr(String rawval) {
      return Float.valueOf(rawval);
    }
    @Override
    public Float parseAndAddGap(@SuppressWarnings("rawtypes") Comparable value, String gap) {
      return ((Number) value).floatValue() + Float.parseFloat(gap);
    }

    @Override
    protected long toSortableDocValue(Comparable calcValue) {
      return (long) NumericUtils.floatToSortableInt((float) calcValue);
    }
  }

  private static class DoubleCalc extends Calc {
    @Override
    @SuppressWarnings({"rawtypes"})
    public Comparable bitsToValue(long bits) {
      if (field.getType().isPointField() && field.multiValued()) {
        return NumericUtils.sortableLongToDouble(bits);
      } else {
        return Double.longBitsToDouble(bits);
      }
    }

    @Override
    public long bitsToSortableBits(long bits) {
      return NumericUtils.sortableDoubleBits(bits);
    }

    public DoubleCalc(final SchemaField f) { super(f); }
    @Override
    protected Double parseStr(String rawval) {
      return Double.valueOf(rawval);
    }
    @Override
    public Double parseAndAddGap(@SuppressWarnings("rawtypes") Comparable value, String gap) {
      return ((Number) value).doubleValue() + Double.parseDouble(gap);
    }

    @Override
    protected long toSortableDocValue(Comparable calcValue) {
      return NumericUtils.doubleToSortableLong((double) calcValue);
    }
  }

  private static class IntCalc extends Calc {

    public IntCalc(final SchemaField f) { super(f); }
    @Override
    @SuppressWarnings({"rawtypes"})
    public Comparable bitsToValue(long bits) {
      return (int)bits;
    }
    @Override
    protected Integer parseStr(String rawval) {
      return Integer.valueOf(rawval);
    }
    @Override
    public Integer parseAndAddGap(@SuppressWarnings("rawtypes") Comparable value, String gap) {
      return ((Number) value).intValue() + Integer.parseInt(gap);
    }

    @Override
    protected long toSortableDocValue(Comparable calcValue) {
      return (long) calcValue;
    }
  }

  private static class LongCalc extends Calc {

    public LongCalc(final SchemaField f) { super(f); }
    @Override
    protected Long parseStr(String rawval) {
      return Long.valueOf(rawval);
    }
    @Override
    public Long parseAndAddGap(@SuppressWarnings("rawtypes") Comparable value, String gap) {
      return ((Number) value).longValue() + Long.parseLong(gap);
    }

    @Override
    protected long toSortableDocValue(Comparable calcValue) {
      return (long) calcValue;
    }
  }

  private static class DateCalc extends Calc {
    private final Date now;
    public DateCalc(final SchemaField f,
                    final Date now) {
      super(f);
      this.now = now;
      if (!(field.getType() instanceof TrieDateField || field.getType().isPointField() ||
          field.getType() instanceof DateRangeField)) {
        throw new IllegalArgumentException("SchemaField must use field type extending TrieDateField, DateRangeField or PointField");
      }
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public Comparable bitsToValue(long bits) {
      return new Date(bits);
    }

    @Override
    public String formatValue(@SuppressWarnings("rawtypes") Comparable val) {
      return ((Date)val).toInstant().toString();
    }
    @Override
    protected Date parseStr(String rawval) {
      return DateMathParser.parseMath(now, rawval);
    }
    @Override
    protected Object parseGap(final String rawval) {
      return rawval;
    }
    @Override
    public Date parseAndAddGap(@SuppressWarnings("rawtypes") Comparable value, String gap) throws java.text.ParseException {
      final DateMathParser dmp = new DateMathParser();
      dmp.setNow((Date)value);
      return dmp.parseMath(gap);
    }

    @Override
    protected long toSortableDocValue(Comparable calcValue) {
      return ((Date) calcValue).getTime();
    }
  }

  private static class CurrencyCalc extends Calc {
    private String defaultCurrencyCode;
    private ExchangeRateProvider exchangeRateProvider;
    public CurrencyCalc(final SchemaField field) {
      super(field);
      if(!(this.field.getType() instanceof CurrencyFieldType)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Cannot perform range faceting over non CurrencyField fields");
      }
      defaultCurrencyCode =
          ((CurrencyFieldType)this.field.getType()).getDefaultCurrency();
      exchangeRateProvider =
          ((CurrencyFieldType)this.field.getType()).getProvider();
    }

    /**
     * Throws a Server Error that this type of operation is not supported for this field
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings({"rawtypes"})
    public Comparable bitsToValue(long bits) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Currency Field " + field.getName() + " can not be used in this way");
    }

    /**
     * Throws a Server Error that this type of operation is not supported for this field
     * {@inheritDoc}
     */
    @Override
    public long bitsToSortableBits(long bits) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Currency Field " + field.getName() + " can not be used in this way");
    }

    /**
     * Returns the short string representation of the CurrencyValue
     * @see CurrencyValue#strValue
     */
    @Override
    public Object buildRangeLabel(@SuppressWarnings("rawtypes") Comparable low) {
      return ((CurrencyValue)low).strValue();
    }

    @Override
    public String formatValue(@SuppressWarnings("rawtypes") Comparable val) {
      return ((CurrencyValue)val).strValue();
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    protected Comparable parseStr(final String rawval) throws java.text.ParseException {
      return CurrencyValue.parse(rawval, defaultCurrencyCode);
    }

    @Override
    protected Object parseGap(final String rawval) throws java.text.ParseException {
      return parseStr(rawval);
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    protected Comparable parseAndAddGap(Comparable value, String gap) throws java.text.ParseException{
      if (value == null) {
        throw new NullPointerException("Cannot perform range faceting on null CurrencyValue");
      }
      CurrencyValue val = (CurrencyValue) value;
      CurrencyValue gapCurrencyValue =
          CurrencyValue.parse(gap, defaultCurrencyCode);
      long gapAmount =
          CurrencyValue.convertAmount(this.exchangeRateProvider,
              gapCurrencyValue.getCurrencyCode(),
              gapCurrencyValue.getAmount(),
              val.getCurrencyCode());
      return new CurrencyValue(val.getAmount() + gapAmount,
          val.getCurrencyCode());

    }

    @Override
    protected long toSortableDocValue(Comparable calcValue) {
      return 0; // TODO
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected SimpleOrderedMap<Object> refineFacets() throws IOException {
    // this refineFacets method is patterned after FacetFieldProcessor.refineFacets such that
    // the same "_s" skip bucket syntax is used and FacetRangeMerger can subclass FacetRequestSortedMerger
    // for dealing with them & the refinement requests.
    //
    // But range faceting does *NOT* use the "leaves" and "partial" syntax
    //
    // If/When range facet becomes more like field facet in it's ability to sort and limit the "range buckets"
    // FacetRangeProcessor and FacetFieldProcessor should probably be refactored to share more code.

    boolean skipThisFacet = (fcontext.flags & SKIP_FACET) != 0;

    List<List> skip = FacetFieldProcessor.asList(fcontext.facetInfo.get("_s"));    // We have seen this bucket, so skip stats on it, and skip sub-facets except for the specified sub-facets that should calculate specified buckets.

    // sanity check our merger's super class didn't send us something we can't handle ...
    assert 0 == FacetFieldProcessor.asList(fcontext.facetInfo.get("_l")).size();
    assert 0 == FacetFieldProcessor.asList(fcontext.facetInfo.get("_p")).size();

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();
    List<SimpleOrderedMap> bucketList = new ArrayList<>( skip.size() );
    res.add("buckets", bucketList);

    // TODO: an alternate implementations can fill all accs at once
    createAccs(-1, 1);

    for (List bucketAndFacetInfo : skip) {
      assert bucketAndFacetInfo.size() == 2;
      Object bucketVal = bucketAndFacetInfo.get(0);
      Map<String,Object> facetInfo = (Map<String, Object>) bucketAndFacetInfo.get(1);

      bucketList.add( refineBucket(bucketVal, true, facetInfo ) );
    }

    { // refine the special "other" buckets

      // NOTE: we're re-using this variable for each special we look for...
      Map<String,Object> specialFacetInfo;

      specialFacetInfo = (Map<String, Object>) fcontext.facetInfo.get(FacetParams.FacetRangeOther.BEFORE.toString());
      if (null != specialFacetInfo) {
        res.add(FacetParams.FacetRangeOther.BEFORE.toString(),
            refineRange(buildBeforeRange(), skipThisFacet, specialFacetInfo));
      }

      specialFacetInfo = (Map<String, Object>) fcontext.facetInfo.get(FacetParams.FacetRangeOther.AFTER.toString());
      if (null != specialFacetInfo) {
        res.add(FacetParams.FacetRangeOther.AFTER.toString(),
            refineRange(buildAfterRange(), skipThisFacet, specialFacetInfo));
      }

      specialFacetInfo = (Map<String, Object>) fcontext.facetInfo.get(FacetParams.FacetRangeOther.BETWEEN.toString());
      if (null != specialFacetInfo) {
        res.add(FacetParams.FacetRangeOther.BETWEEN.toString(),
            refineRange(buildBetweenRange(), skipThisFacet, specialFacetInfo));
      }
    }

    return res;
  }

  /**
   * Returns the "Actual End" value sent from the merge as part of the refinement request (if any)
   * or re-computes it as needed using the Calc and caches the result for re-use
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private Comparable getOrComputeActualEndForRefinement() {
    if (null != actual_end) {
      return actual_end;
    }

    if (freq.hardend) {
      actual_end = this.end;
    } else if (fcontext.facetInfo.containsKey(FacetRange.ACTUAL_END_JSON_KEY)) {
      actual_end = calc.getValue(fcontext.facetInfo.get(FacetRange.ACTUAL_END_JSON_KEY).toString());
    } else {
      // a quick and dirty loop over the ranges (we don't need) to compute the actual_end...
      Comparable low = start;
      while (low.compareTo(end) < 0) {
        Comparable high = calc.addGap(low, gap);
        if (end.compareTo(high) < 0) {
          actual_end = high;
          break;
        }
        if (high.compareTo(low) <= 0) {
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Garbage input for facet refinement w/o " + FacetRange.ACTUAL_END_JSON_KEY);
        }
        low = high;
      }
    }

    assert null != actual_end;
    return actual_end;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private SimpleOrderedMap<Object> refineBucket(Object bucketVal, boolean skip, Map<String,Object> facetInfo) throws IOException {

    String val = bucketVal.toString();
    if (ranges != null) {
      try {
        Range range = parseRangeFromString(val, val);
        final SimpleOrderedMap<Object> bucket = refineRange(range, skip, facetInfo);
        bucket.add("val", range.label);
        return bucket;
      } catch (SyntaxError e) {
        // execution won't reach here as ranges are already validated
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

    Comparable low = calc.getValue(val);
    Comparable high = calc.addGap(low, gap);
    Comparable max_end = end;
    if (end.compareTo(high) < 0) {
      if (freq.hardend) {
        high = max_end;
      } else {
        max_end = high;
      }
    }
    if (high.compareTo(low) < 0) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "range facet infinite loop (is gap negative? did the math overflow?)");
    }
    if (high.compareTo(low) == 0) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: " + low + " + " + gap + " = " + high );
    }

    boolean incLower = (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
        (include.contains(FacetParams.FacetRangeInclude.EDGE) && 0 == low.compareTo(start)));
    boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
        (include.contains(FacetParams.FacetRangeInclude.EDGE) && 0 == high.compareTo(max_end)));
    
    Range range = new Range(calc.buildRangeLabel(low), low, calc.toSortableDocValue(low), high, calc.toSortableDocValue(high), incLower, incUpper);

    // now refine this range

    final SimpleOrderedMap<Object> bucket = refineRange(range, skip, facetInfo);
    bucket.add("val", range.label);

    return bucket;
  }

  /** Helper method for refining a Range
   * @see #fillBucket
   */
  private SimpleOrderedMap<Object> refineRange(Range range, boolean skip, Map<String,Object> facetInfo) throws IOException {
    final SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
    final Query domainQ = sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low), range.high==null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
    fillBucket(bucket, domainQ, null, skip, facetInfo);
    return bucket;
  }

  /** Helper method for building a "before" Range */
  private Range buildBeforeRange() {
    // include upper bound if "outer" or if first gap doesn't already include it
    final boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
        (!(include.contains(FacetParams.FacetRangeInclude.LOWER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE))));
    return new Range(FacetParams.FacetRangeOther.BEFORE.toString(), null, Long.MIN_VALUE, start, calc.toSortableDocValue(start), false, incUpper);
  }

  /** Helper method for building a "after" Range */
  private Range buildAfterRange() {
    @SuppressWarnings({"rawtypes"})
    final Comparable the_end = getOrComputeActualEndForRefinement();
    assert null != the_end;
    final boolean incLower = (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
        (!(include.contains(FacetParams.FacetRangeInclude.UPPER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE))));
    return new Range(FacetParams.FacetRangeOther.AFTER.toString(), the_end, calc.toSortableDocValue(the_end), null, Long.MAX_VALUE, incLower, false);
  }

  /** Helper method for building a "between" Range */
  private Range buildBetweenRange() {
    @SuppressWarnings({"rawtypes"})
    final Comparable the_end = getOrComputeActualEndForRefinement();
    assert null != the_end;
    final boolean incLower = (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
        include.contains(FacetParams.FacetRangeInclude.EDGE));
    final boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
        include.contains(FacetParams.FacetRangeInclude.EDGE));
    return new Range(FacetParams.FacetRangeOther.BETWEEN.toString(), start, calc.toSortableDocValue(start), the_end, calc.toSortableDocValue(the_end), incLower, incUpper);
  }
}

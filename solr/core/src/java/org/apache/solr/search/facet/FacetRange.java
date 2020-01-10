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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.CurrencyFieldType;
import org.apache.solr.schema.CurrencyValue;
import org.apache.solr.schema.ExchangeRateProvider;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.facet.SlotAcc.SlotContext;
import org.apache.solr.util.DateMathParser;

import static org.apache.solr.search.facet.FacetContext.SKIP_FACET;

public class FacetRange extends FacetRequestSorted {
  static final String ACTUAL_END_JSON_KEY = "_actual_end";
  
  String field;
  Object start;
  Object end;
  Object gap;
  Object ranges;
  boolean hardend = false;
  EnumSet<FacetRangeInclude> include;
  EnumSet<FacetRangeOther> others;

  {
    // defaults
    mincount = 0;
    limit = -1;
  }

  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    return new FacetRangeProcessor(fcontext, this);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetRangeMerger(this);
  }
  
  @Override
  public Map<String, Object> getFacetDescription() {
    Map<String, Object> descr = new HashMap<>();
    descr.put("field", field);
    if (ranges != null) {
      descr.put("ranges", ranges);
    } else {
      descr.put("start", start);
      descr.put("end", end);
      descr.put("gap", gap);
    }
    return descr;
  }
  
}


class FacetRangeProcessor extends FacetProcessor<FacetRange> {
  // TODO: the code paths for initial faceting, vs refinement, are very different...
  // TODO: ...it might make sense to have seperate classes w/a common base?
  // TODO: let FacetRange.createFacetProcessor decide which one to instantiate?
  
  final SchemaField sf;
  final Calc calc;
  final EnumSet<FacetRangeInclude> include;
  final long effectiveMincount;
  final Comparable start;
  final Comparable end;
  final String gap;
  final Object ranges;

  /** Build by {@link #createRangeList} if and only if needed for basic faceting */
  List<Range> rangeList;
  /** Build by {@link #createRangeList} if and only if needed for basic faceting */
  List<Range> otherList;

  /**
   * Serves two purposes depending on the type of request.
   * <ul>
   * <li>If this is a phase#1 shard request, then {@link #createRangeList} will set this value (non null)
   *     if and only if it is needed for refinement (ie: <code>hardend:false</code> &amp; <code>other</code>
   *     that requres an end value low/high value calculation).  And it wil be included in the response</li>
   * <li>If this is a phase#2 refinement request, this variable will be used 
   *     {@link #getOrComputeActualEndForRefinement} to track the value sent with the refinement request 
   *     -- or to cache a recomputed value if the request omitted it -- for use in refining the 
   *     <code>other</code> buckets that need them</li>
   * </ul>
   */
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

  private static class Range {
    Object label;
    Comparable low;
    Comparable high;
    boolean includeLower;
    boolean includeUpper;

    public Range(Object label, Comparable low, Comparable high, boolean includeLower, boolean includeUpper) {
      this.label = label;
      this.low = low;
      this.high = high;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;
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
    }

    // if we made it this far, we have no idea what it is...
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Unable to range facet on field:" + sf.getName());
  }

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

      boolean incLower = (include.contains(FacetRangeInclude.LOWER) ||
          (include.contains(FacetRangeInclude.EDGE) && 0 == low.compareTo(start)));
      boolean incUpper = (include.contains(FacetRangeInclude.UPPER) ||
          (include.contains(FacetRangeInclude.EDGE) && 0 == high.compareTo(end)));

      Range range = new Range(calc.buildRangeLabel(low), low, high, incLower, incUpper);
      rangeList.add( range );

      low = high;
    }

    // no matter what other values are listed, we don't do
    // anything if "none" is specified.
    if (! freq.others.contains(FacetRangeOther.NONE) ) {
      final boolean all = freq.others.contains(FacetRangeOther.ALL);

      if (all || freq.others.contains(FacetRangeOther.BEFORE)) {
        otherList.add( buildBeforeRange() );
      }
      if (all || freq.others.contains(FacetRangeOther.AFTER)) {
        actual_end = loop_end;
        otherList.add( buildAfterRange() );
      }
      if (all || freq.others.contains(FacetRangeOther.BETWEEN)) {
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
    List intervals = (List) input;
    List<Range> ranges = new ArrayList<>();
    for (Object obj : intervals) {
      if (!(obj instanceof Map)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Expected Map for range but got " + obj.getClass().getSimpleName() + " = " + obj);
      }
      Range range;
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

    return new Range(key, from, to, includeLower, includeUpper);
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
    return new Range(rangeStr, start, end, includeLower, includeUpper);
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

  private Comparable getComparableFromString(String value) {
    value = value.trim();
    if ("*".equals(value)) {
      return null;
    }
    return calc.getValue(value);
  }

  private  SimpleOrderedMap getRangeCountsIndexed() throws IOException {

    int slotCount = rangeList.size() + otherList.size();
    intersections = new DocSet[slotCount];
    filters = new Query[slotCount];


    createAccs(fcontext.base.size(), slotCount);

    for (int idx = 0; idx<rangeList.size(); idx++) {
      rangeStats(rangeList.get(idx), idx);
    }

    for (int idx = 0; idx<otherList.size(); idx++) {
      rangeStats(otherList.get(idx), rangeList.size() + idx);
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
      doSubs(bucket, idx);
    }

    for (int idx = 0; idx<otherList.size(); idx++) {
      // we don't skip these buckets based on mincount
      Range range = otherList.get(idx);
      SimpleOrderedMap bucket = new SimpleOrderedMap();
      res.add(range.label.toString(), bucket);
      addStats(bucket, rangeList.size() + idx);
      doSubs(bucket, rangeList.size() + idx);
    }

    if (null != actual_end) {
      res.add(FacetRange.ACTUAL_END_JSON_KEY, calc.formatValue(actual_end));
    }

    return res;
  }

  private Query[] filters;
  private DocSet[] intersections;
  private void rangeStats(Range range, int slot) throws IOException {
    Query rangeQ = sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low), range.high==null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
    // TODO: specialize count only
    DocSet intersection = fcontext.searcher.getDocSet(rangeQ, fcontext.base);
    filters[slot] = rangeQ;
    intersections[slot] = intersection;  // save for later  // TODO: only save if number of slots is small enough?
    int num = collect(intersection, slot, slotNum -> { return new SlotContext(rangeQ); });
    countAcc.incrementCount(slot, num); // TODO: roll this into collect()
  }

  private void doSubs(SimpleOrderedMap bucket, int slot) throws IOException {
    // handle sub-facets for this bucket
    if (freq.getSubFacets().size() > 0) {
      DocSet subBase = intersections[slot];
      try {
        processSubs(bucket, filters[slot], subBase, false, null);
      } finally {
        // subContext.base.decref();  // OFF-HEAP
        // subContext.base = null;  // do not modify context after creation... there may be deferred execution (i.e. streaming)
      }
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
    public Object buildRangeLabel(Comparable low) {
      return low;
    }
    
    /**
     * Formats a value into a label used in a response
     * Default Impl just uses toString()
     */
    public String formatValue(final Comparable val) {
      return val.toString();
    }

    /**
     * Parses a String param into a value throwing
     * an exception if not possible
     */
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
    protected abstract Comparable parseAndAddGap(Comparable value, String gap)
        throws java.text.ParseException;

  }

  private static class FloatCalc extends Calc {

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
    public Float parseAndAddGap(Comparable value, String gap) {
      return ((Number) value).floatValue() + Float.parseFloat(gap);
    }
  }
  private static class DoubleCalc extends Calc {
    @Override
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
    public Double parseAndAddGap(Comparable value, String gap) {
      return ((Number) value).doubleValue() + Double.parseDouble(gap);
    }
  }
  private static class IntCalc extends Calc {

    public IntCalc(final SchemaField f) { super(f); }
    @Override
    public Comparable bitsToValue(long bits) {
      return (int)bits;
    }
    @Override
    protected Integer parseStr(String rawval) {
      return Integer.valueOf(rawval);
    }
    @Override
    public Integer parseAndAddGap(Comparable value, String gap) {
      return ((Number) value).intValue() + Integer.parseInt(gap);
    }
  }
  private static class LongCalc extends Calc {

    public LongCalc(final SchemaField f) { super(f); }
    @Override
    protected Long parseStr(String rawval) {
      return Long.valueOf(rawval);
    }
    @Override
    public Long parseAndAddGap(Comparable value, String gap) {
      return ((Number) value).longValue() + Long.parseLong(gap);
    }
  }
  private static class DateCalc extends Calc {
    private final Date now;
    public DateCalc(final SchemaField f,
                    final Date now) {
      super(f);
      this.now = now;
      if (! (field.getType() instanceof TrieDateField) && !(field.getType().isPointField()) ) {
        throw new IllegalArgumentException("SchemaField must use field type extending TrieDateField, DateRangeField or PointField");
      }
    }

    @Override
    public Comparable bitsToValue(long bits) {
      return new Date(bits);
    }

    @Override
    public String formatValue(Comparable val) {
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
    public Date parseAndAddGap(Comparable value, String gap) throws java.text.ParseException {
      final DateMathParser dmp = new DateMathParser();
      dmp.setNow((Date)value);
      return dmp.parseMath(gap);
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
    public Object buildRangeLabel(Comparable low) {
      return ((CurrencyValue)low).strValue();
    }
    
    @Override
    public String formatValue(Comparable val) {
      return ((CurrencyValue)val).strValue();
    }

    @Override
    protected Comparable parseStr(final String rawval) throws java.text.ParseException {
      return CurrencyValue.parse(rawval, defaultCurrencyCode);
    }

    @Override
    protected Object parseGap(final String rawval) throws java.text.ParseException {
      return parseStr(rawval);
    }

    @Override
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

  }

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

      specialFacetInfo = (Map<String, Object>) fcontext.facetInfo.get(FacetRangeOther.BEFORE.toString());
      if (null != specialFacetInfo) {
        res.add(FacetRangeOther.BEFORE.toString(),
                refineRange(buildBeforeRange(), skipThisFacet, specialFacetInfo));
      }
      
      specialFacetInfo = (Map<String, Object>) fcontext.facetInfo.get(FacetRangeOther.AFTER.toString());
      if (null != specialFacetInfo) {
        res.add(FacetRangeOther.AFTER.toString(),
                refineRange(buildAfterRange(), skipThisFacet, specialFacetInfo));
      }
      
      specialFacetInfo = (Map<String, Object>) fcontext.facetInfo.get(FacetRangeOther.BETWEEN.toString());
      if (null != specialFacetInfo) {
        res.add(FacetRangeOther.BETWEEN.toString(),
                refineRange(buildBetweenRange(), skipThisFacet, specialFacetInfo));
      }
    }
      
    return res;
  }

  /** 
   * Returns the "Actual End" value sent from the merge as part of the refinement request (if any) 
   * or re-computes it as needed using the Calc and caches the result for re-use
   */
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

    boolean incLower = (include.contains(FacetRangeInclude.LOWER) ||
                        (include.contains(FacetRangeInclude.EDGE) && 0 == low.compareTo(start)));
    boolean incUpper = (include.contains(FacetRangeInclude.UPPER) ||
                        (include.contains(FacetRangeInclude.EDGE) && 0 == high.compareTo(max_end)));

    Range range = new Range(calc.buildRangeLabel(low), low, high, incLower, incUpper);

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
    final boolean incUpper = (include.contains(FacetRangeInclude.OUTER) ||
                              (!(include.contains(FacetRangeInclude.LOWER) ||
                                 include.contains(FacetRangeInclude.EDGE))));
    return new Range(FacetRangeOther.BEFORE.toString(), null, start, false, incUpper);
  }

  /** Helper method for building a "after" Range */
  private Range buildAfterRange() {
    final Comparable the_end = getOrComputeActualEndForRefinement();
    assert null != the_end;
    final boolean incLower = (include.contains(FacetRangeInclude.OUTER) ||
                              (!(include.contains(FacetRangeInclude.UPPER) ||
                                 include.contains(FacetRangeInclude.EDGE))));
    return new Range(FacetRangeOther.AFTER.toString(), the_end, null, incLower, false);
  }

  /** Helper method for building a "between" Range */
  private Range buildBetweenRange() {
    final Comparable the_end = getOrComputeActualEndForRefinement();
    assert null != the_end;
    final boolean incLower = (include.contains(FacetRangeInclude.LOWER) ||
                              include.contains(FacetRangeInclude.EDGE));
    final boolean incUpper = (include.contains(FacetRangeInclude.UPPER) ||
                              include.contains(FacetRangeInclude.EDGE));
    return new Range(FacetRangeOther.BETWEEN.toString(), start, the_end, incLower, incUpper);
  }
}

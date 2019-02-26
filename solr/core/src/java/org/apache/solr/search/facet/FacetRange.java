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

import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.*;
import org.apache.solr.search.DocSet;
import org.apache.solr.util.DateMathParser;

import java.io.IOException;
import java.util.*;

public class FacetRange extends FacetRequestSorted {
  String field;
  Object start;
  Object end;
  Object gap;
  Object interval;
  boolean hardend = false;
  EnumSet<FacetParams.FacetRangeInclude> include;
  EnumSet<FacetParams.FacetRangeOther> others;

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
    Map<String, Object> descr = new HashMap<String, Object>();
    descr.put("field", field);
    descr.put("start", start);
    descr.put("end", end);
    descr.put("gap", gap);
    descr.put("interval", interval);
    return descr;
  }
  
}


class FacetRangeProcessor extends FacetProcessor<FacetRange> {
  SchemaField sf;
  Calc calc;
  List<Range> rangeList;
  List<Range> otherList;
  long effectiveMincount;

  FacetRangeProcessor(FacetContext fcontext, FacetRange freq) {
    super(fcontext, freq);
  }

  @Override
  public void process() throws IOException {
    super.process();

    // Under the normal mincount=0, each shard will need to return 0 counts since we don't calculate buckets at the top level.
    // But if mincount>0 then our sub mincount can be set to 1.

    effectiveMincount = fcontext.isShard() ? (freq.mincount > 0 ? 1 : 0) : freq.mincount;
    sf = fcontext.searcher.getSchema().getField(freq.field);
    response = getRangeCounts();
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

  public static Calc getNumericCalc(SchemaField sf) {
    Calc calc;
    final FieldType ft = sf.getType();

    if (ft instanceof TrieField) {
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
    } else if (ft instanceof PointField) {
      // TODO, this is the same in Trie and Point now
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
    } 
    else {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Expected numeric field type :" + sf);
    }
    return calc;
  }

  private SimpleOrderedMap<Object> getRangeCounts() throws IOException {
    final FieldType ft = sf.getType();

    if (ft instanceof TrieField) {
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
                  "Unable to range facet on tried field of unexpected type:" + freq.field);
      }
    } else {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Unable to range facet on field:" + sf);
    }

    if (freq.gap != null && freq.interval == null) createRangeList();
    if (freq.gap == null && freq.interval != null) createIntervalRangeList();

    return getRangeCountsIndexed();
  }

  private void createIntervalRangeList() throws IOException {
    rangeList = new ArrayList<>();
    otherList = new ArrayList<>(3);

    Comparable start = calc.getValue(freq.start.toString());
    Comparable end = calc.getValue(freq.end.toString());
    EnumSet<FacetParams.FacetRangeInclude> include = freq.include;
    //    rangeList.add();
    List<Range> ranges = parseInterval(freq.interval);
    for (Range range : ranges) {
      rangeList.add(range);
    }
    createOtherList(start, end, include);
  }

  private List<Range> parseInterval(Object input) {
    //interval :[{key:"set1",value:"[0,10]"}]
    //@todo apoorv handle exception, sort orders
    List<Map<String, String>> intervals = (List<Map<String, String>>) input;
    List<Range> ranges = new ArrayList<>();
    for (Map<String, String> interval : intervals) {
      String key = interval.get("key");
      if (interval == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "empty facet interval");
      }      String intervalStr = interval.get("value").trim();
      if (intervalStr.length() == 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "empty facet interval");
      }
      Boolean startOpen = false, endOpen = false;
      Comparable start = null, end = null;
      if (intervalStr.charAt(0) == '(') {
        startOpen = true;
      } else if (intervalStr.charAt(0) == '[') {
        startOpen = false;
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid start character " + intervalStr.charAt(0) + " in facet interval " + intervalStr);
      }

      final int lastNdx = intervalStr.length() - 1;
      if (intervalStr.charAt(lastNdx) == ')') {
        endOpen = true;
      } else if (intervalStr.charAt(lastNdx) == ']') {
        endOpen = false;
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid end character " + intervalStr.charAt(0) + " in facet interval " + intervalStr);
      }

      StringBuilder startStr = new StringBuilder(lastNdx);
      int i = unescape(intervalStr, 1, lastNdx, startStr);
      if (i == lastNdx) {
        if (intervalStr.charAt(lastNdx - 1) == ',') {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Empty interval limit");
        }
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing unescaped comma separating interval ends in " + intervalStr);
      }
      try {
        start = calc.getValue(startStr.toString());
      } catch (SolrException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "Invalid start interval for key '%s': %s", key, e.getMessage()), e);
      }
      StringBuilder endStr = new StringBuilder(lastNdx);
      i = unescape(intervalStr, i, lastNdx, endStr);
      if (i != lastNdx) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Extra unescaped comma at index " + i + " in interval " + intervalStr);
      }
      try {
        end = calc.getValue(endStr.toString());
      } catch (SolrException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "Invalid end interval for key '%s': %s", key, e.getMessage()), e);
      }
      if (start != null && end != null && start.compareTo(end) > 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Start is higher than end in interval for key: " + key);
      }
      if (interval.get("key") == null) key = startStr.toString();
      Range range = new Range(key, start, end, startOpen, endOpen);
      ranges.add(range);
    }
    return ranges;
  }

  /* Fill in sb with a string from i to the first unescaped comma, or n.
      Return the index past the unescaped comma, or n if no unescaped comma exists */
  private int unescape(String s, int i, int n, StringBuilder sb) {
    for (; i < n; ++i) {
      char c = s.charAt(i);
      if (c == '\\') {
        ++i;
        if (i < n) {
          c = s.charAt(i);
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unfinished escape at index " + i + " in facet interval " + s);
        }
      } else if (c == ',') {
        return i + 1;
      }
      sb.append(c);
    }
    return n;
  }

  private void createRangeList() throws IOException {

    rangeList = new ArrayList<>();
    otherList = new ArrayList<>(3);

    Comparable start = calc.getValue(freq.start.toString());
    Comparable end = calc.getValue(freq.end.toString());
    EnumSet<FacetParams.FacetRangeInclude> include = freq.include;

    String gap = freq.gap.toString();

    Comparable low = start;

    while (low.compareTo(end) < 0) {
      Comparable high = calc.addGap(low, gap);
      if (end.compareTo(high) < 0) {
        if (freq.hardend) {
          high = end;
        } else {
          end = high;
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

      boolean incLower =
          (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
              (include.contains(FacetParams.FacetRangeInclude.EDGE) &&
                  0 == low.compareTo(start)));
      boolean incUpper =
          (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
              (include.contains(FacetParams.FacetRangeInclude.EDGE) &&
                  0 == high.compareTo(end)));

      Range range = new Range(low, low, high, incLower, incUpper);
      rangeList.add( range );

      low = high;
    }
    createOtherList(start, end, include);
  }

  private void createOtherList(Comparable start, Comparable end, EnumSet<FacetParams.FacetRangeInclude> include) {
    // no matter what other values are listed, we don't do
    // anything if "none" is specified.
    if (! freq.others.contains(FacetParams.FacetRangeOther.NONE) ) {

      boolean all = freq.others.contains(FacetParams.FacetRangeOther.ALL);

      if (all || freq.others.contains(FacetParams.FacetRangeOther.BEFORE)) {
        // include upper bound if "outer" or if first gap doesn't already include it
        boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
            (!(include.contains(FacetParams.FacetRangeInclude.LOWER) ||
                include.contains(FacetParams.FacetRangeInclude.EDGE))));
        otherList.add( new Range(FacetParams.FacetRangeOther.BEFORE.toString(), null, start, false, incUpper) );
      }
      if (all || freq.others.contains(FacetParams.FacetRangeOther.AFTER)) {
        // include lower bound if "outer" or if last gap doesn't already include it
        boolean incLower = (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
            (!(include.contains(FacetParams.FacetRangeInclude.UPPER) ||
                include.contains(FacetParams.FacetRangeInclude.EDGE))));
        otherList.add( new Range(FacetParams.FacetRangeOther.AFTER.toString(), end, null, incLower, false));
      }
      if (all || freq.others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
        boolean incLower = (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE));
        boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE));

        otherList.add( new Range(FacetParams.FacetRangeOther.BETWEEN.toString(), start, end, incLower, incUpper) );
      }
    }
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
      // we dont' skip these buckets based on mincount
      Range range = otherList.get(idx);
      SimpleOrderedMap bucket = new SimpleOrderedMap();
      res.add(range.label.toString(), bucket);
      addStats(bucket, rangeList.size() + idx);
      doSubs(bucket, rangeList.size() + idx);
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
    int num = collect(intersection, slot);
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

  private  SimpleOrderedMap<Object> rangeStats(Range range, boolean special ) throws IOException {
    SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

    // typically the start value of the range, but null for before/after/between
    if (!special) {
      bucket.add("val", range.label);
    }

    Query rangeQ = sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low), range.high==null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
    fillBucket(bucket, rangeQ, null, false, null);

    return bucket;
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

    public Comparable bitsToValue(long bits) {
      return bits;
    }

    public long bitsToSortableBits(long bits) {
      return bits;
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
     * the corrisponding high Range endpoint value, throwing
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
     * the corrisponding high Range endpoint value.
     * Can throw a low level format exception as needed.
     */
    protected abstract Comparable parseAndAddGap(Comparable value, String gap)
        throws java.text.ParseException;

  }

  private static class FloatCalc extends Calc {

    @Override
    public Comparable bitsToValue(long bits) {
      return Float.intBitsToFloat( (int)bits );
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
      return new Float(((Number)value).floatValue() + Float.parseFloat(gap));
    }
  }
  private static class DoubleCalc extends Calc {
    @Override
    public Comparable bitsToValue(long bits) {
      return Double.longBitsToDouble(bits);
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
      return new Double(((Number)value).doubleValue() + Double.parseDouble(gap));
    }
  }
  private static class IntCalc extends Calc {

    public IntCalc(final SchemaField f) { super(f); }
    @Override
    protected Integer parseStr(String rawval) {
      return Integer.valueOf(rawval);
    }
    @Override
    public Integer parseAndAddGap(Comparable value, String gap) {
      return new Integer(((Number)value).intValue() + Integer.parseInt(gap));
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
      return new Long(((Number)value).longValue() + Long.parseLong(gap));
    }
  }
  private static class DateCalc extends Calc {
    private final Date now;
    public DateCalc(final SchemaField f,
                    final Date now) {
      super(f);
      this.now = now;
      if (! (field.getType() instanceof TrieDateField) ) {
        throw new IllegalArgumentException("SchemaField must use field type extending TrieDateField or DateRangeField");
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

}

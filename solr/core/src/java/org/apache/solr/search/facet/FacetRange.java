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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.util.DateMathParser;

public class FacetRange extends FacetRequestSorted {
  String field;
  Object start;
  Object end;
  Object gap;
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
    SchemaField sf = fcontext.searcher.getSchema().getField(field);

    if (sf.hasDocValues() && !sf.multiValued()) {
      return new FacetRangeProcessorSingledValuedDV(fcontext, this);
    }

    return new FacetRangeProcessorDefault(fcontext, this);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetRangeMerger(this);
  }

  @Override
  public Map<String,Object> getFacetDescription() {
    Map<String,Object> descr = new HashMap<String,Object>();
    descr.put("field", field);
    descr.put("start", start);
    descr.put("end", end);
    descr.put("gap", gap);
    return descr;
  }

}

abstract class FacetRangeProcessor extends FacetProcessor<FacetRange> {
  SchemaField sf;
  Calc calc;
  List<Range> rangeList;
  Range[] otherList;
  long effectiveMincount;

  Comparable start;
  Comparable end;
  String gap;

  EnumSet<FacetParams.FacetRangeInclude> include;

  FacetRangeProcessor(FacetContext fcontext, FacetRange freq) {
    super(fcontext, freq);
  }

  @Override
  public void process() throws IOException {
    super.process();

    // Under the normal mincount=0, each shard will need to return 0 counts since we don't calculate buckets at the top
    // level.
    // But if mincount>0 then our sub mincount can be set to 1.

    effectiveMincount = fcontext.isShard() ? (freq.mincount > 0 ? 1 : 0) : freq.mincount;
    sf = fcontext.searcher.getSchema().getField(freq.field);
    response = getRangeCounts();
  }

  static class Range {
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
      final TrieField trie = (TrieField) ft;

      switch (trie.getType()) {
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
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Expected numeric field type :" + sf);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expected numeric field type :" + sf);
    }
    return calc;
  }

  private SimpleOrderedMap<Object> getRangeCounts() throws IOException {
    final FieldType ft = sf.getType();

    if (ft instanceof TrieField) {
      final TrieField trie = (TrieField) ft;

      switch (trie.getType()) {
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
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Unable to range facet on tried field of unexpected type:" + freq.field);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Unable to range facet on field:" + sf);
    }

    createRangeList();
    return getRangeCountsIndexed();
  }

  private void createRangeList() throws IOException {

    rangeList = new ArrayList<Range>();
    otherList = new Range[3];

    start = calc.getValue(freq.start.toString());
    end = calc.getValue(freq.end.toString());
    EnumSet<FacetParams.FacetRangeInclude> include = freq.include;

    gap = freq.gap.toString();

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
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "range facet infinite loop (is gap negative? did the math overflow?)");
      }
      if (high.compareTo(low) == 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: "
                + low + " + " + gap + " = " + high);
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
      rangeList.add(range);

      low = high;
    }

    // no matter what other values are listed, we don't do
    // anything if "none" is specified.
    if (!freq.others.contains(FacetParams.FacetRangeOther.NONE)) {

      boolean all = freq.others.contains(FacetParams.FacetRangeOther.ALL);

      if (all || freq.others.contains(FacetParams.FacetRangeOther.BEFORE)) {
        // include upper bound if "outer" or if first gap doesn't already include it
        boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
            (!(include.contains(FacetParams.FacetRangeInclude.LOWER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE))));
        otherList[0] = new Range(FacetParams.FacetRangeOther.BEFORE.toString(), null, start, false, incUpper);
      }
      if (all || freq.others.contains(FacetParams.FacetRangeOther.AFTER)) {
        // include lower bound if "outer" or if last gap doesn't already include it
        boolean incLower = (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
            (!(include.contains(FacetParams.FacetRangeInclude.UPPER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE))));
        otherList[1] = new Range(FacetParams.FacetRangeOther.AFTER.toString(), end, null, incLower, false);
      }
      if (all || freq.others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
        boolean incLower = (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE));
        boolean incUpper = (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
            include.contains(FacetParams.FacetRangeInclude.EDGE));

        otherList[2] = new Range(FacetParams.FacetRangeOther.BETWEEN.toString(), start, end, incLower, incUpper);
      }
    }

  }

  abstract SimpleOrderedMap getRangeCountsIndexed() throws IOException;

  protected SimpleOrderedMap getRangeCountsIndexedResults() throws IOException {
    final SimpleOrderedMap res = new SimpleOrderedMap<>();
    List<SimpleOrderedMap> buckets = new ArrayList<>();
    res.add("buckets", buckets);

    for (int idx = 0; idx < rangeList.size(); idx++) {
      if (effectiveMincount > 0 && countAcc.getCount(idx) < effectiveMincount) continue;
      Range range = rangeList.get(idx);
      SimpleOrderedMap bucket = new SimpleOrderedMap();
      buckets.add(bucket);
      bucket.add("val", range.label);
      addStats(bucket, idx);
      doSubs(bucket, generateFilterQuery(range), getRangeDocSet(idx));
    }

    for (int idx = 0; idx < otherList.length; idx++) {
      if (otherList[idx] == null) continue;
      // we dont' skip these buckets based on mincount
      Range range = otherList[idx];
      SimpleOrderedMap bucket = new SimpleOrderedMap();
      res.add(otherList[idx].label.toString(), bucket);
      addStats(bucket, rangeList.size() + idx);
      doSubs(bucket, generateFilterQuery(range), getRangeDocSet(rangeList.size() + idx));
    }

    return res;
  }

  protected void doSubs(SimpleOrderedMap bucket, Query filter, DocSet subBase) throws IOException {
    // handle sub-facets for this bucket
    if (freq.getSubFacets().size() > 0) {
      try {
        processSubs(bucket, filter, subBase);
      } finally {
        // subContext.base.decref(); // OFF-HEAP
        // subContext.base = null; // do not modify context after creation... there may be deferred execution (i.e.
        // streaming)
      }
    }
  }

  private SimpleOrderedMap<Object> rangeStats(Range range, boolean special) throws IOException {
    SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

    // typically the start value of the range, but null for before/after/between
    if (!special) {
      bucket.add("val", range.label);
    }

    Query rangeQ = sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low),
        range.high == null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
    fillBucket(bucket, rangeQ, null);

    return bucket;
  }

  protected Query generateFilterQuery(Range range) {
    return sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low),
        range.high == null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
  }

  abstract DocSet getRangeDocSet(int slot);

  // Essentially copied from SimpleFacets...
  // would be nice to unify this stuff w/ analytics component...
  /**
   * Perhaps someday instead of having a giant "instanceof" case statement to pick an impl, we can add a
   * "RangeFacetable" marker interface to FieldTypes and they can return instances of these directly from some method --
   * but until then, keep this locked down and private.
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
     * Formats a value into a label used in a response Default Impl just uses toString()
     */
    public String formatValue(final Comparable val) {
      return val.toString();
    }

    /**
     * Parses a String param into a value throwing an exception if not possible
     */
    public final Comparable getValue(final String rawval) {
      try {
        return parseStr(rawval);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse value " + rawval + " for field: " +
                field.getName(), e);
      }
    }

    /**
     * Parses a String param into a value. Can throw a low level format exception as needed.
     */
    protected abstract Comparable parseStr(final String rawval)
        throws java.text.ParseException;

    /**
     * Parses a String param into a value that represents the gap and can be included in the response, throwing a useful
     * exception if not possible.
     *
     * Note: uses Object as the return type instead of T for things like Date where gap is just a DateMathParser string
     */
    public final Object getGap(final String gap) {
      try {
        return parseGap(gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse gap " + gap + " for field: " +
                field.getName(), e);
      }
    }

    /**
     * Parses a String param into a value that represents the gap and can be included in the response. Can throw a low
     * level format exception as needed.
     *
     * Default Impl calls parseVal
     */
    protected Object parseGap(final String rawval) throws java.text.ParseException {
      return parseStr(rawval);
    }

    /**
     * Adds the String gap param to a low Range endpoint value to determine the corrisponding high Range endpoint value,
     * throwing a useful exception if not possible.
     */
    public final Comparable addGap(Comparable value, String gap) {
      try {
        return parseAndAddGap(value, gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't add gap " + gap + " to value " + value +
                " for field: " + field.getName(), e);
      }
    }

    /**
     * Adds the String gap param to a low Range endpoint value to determine the corrisponding high Range endpoint value.
     * Can throw a low level format exception as needed.
     */
    protected abstract Comparable parseAndAddGap(Comparable value, String gap)
        throws java.text.ParseException;

    protected abstract float getGapIndex(Comparable start, String gap, Comparable value);
  }

  private static class FloatCalc extends Calc {

    @Override
    public Comparable bitsToValue(long bits) {
      return Float.intBitsToFloat((int) bits);
    }

    @Override
    public long bitsToSortableBits(long bits) {
      return NumericUtils.sortableDoubleBits(bits);
    }

    public FloatCalc(final SchemaField f) {
      super(f);
    }

    @Override
    protected Float parseStr(String rawval) {
      return Float.valueOf(rawval);
    }

    @Override
    public Float parseAndAddGap(Comparable value, String gap) {
      return new Float(((Number) value).floatValue() + Float.valueOf(gap).floatValue());
    }

    @Override
    protected float getGapIndex(Comparable s, String g, Comparable v) {
      Float start = ((Number) s).floatValue();
      Float gap = Float.valueOf(g).floatValue();
      Float value = ((Number) v).floatValue();

      return (value - start) / gap;
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

    public DoubleCalc(final SchemaField f) {
      super(f);
    }

    @Override
    protected Double parseStr(String rawval) {
      return Double.valueOf(rawval);
    }

    @Override
    public Double parseAndAddGap(Comparable value, String gap) {
      return new Double(((Number) value).doubleValue() + Double.valueOf(gap).doubleValue());
    }

    @Override
    protected float getGapIndex(Comparable s, String g, Comparable v) {
      Double start = ((Number) s).doubleValue();
      Double gap = Double.valueOf(g).doubleValue();
      Double value = ((Number) v).doubleValue();

      return (float) ((value - start) / gap);
    }
  }

  private static class IntCalc extends Calc {

    public IntCalc(final SchemaField f) {
      super(f);
    }

    @Override
    protected Integer parseStr(String rawval) {
      return Integer.valueOf(rawval);
    }

    @Override
    public Integer parseAndAddGap(Comparable value, String gap) {
      return new Integer(((Number) value).intValue() + Integer.valueOf(gap).intValue());
    }

    @Override
    protected float getGapIndex(Comparable s, String g, Comparable v) {
      Integer start = ((Number) s).intValue();
      Integer gap = Integer.valueOf(g).intValue();
      Integer value = ((Number) v).intValue();

      return (value - start) / (float) gap;
    }
  }

  private static class LongCalc extends Calc {

    public LongCalc(final SchemaField f) {
      super(f);
    }

    @Override
    protected Long parseStr(String rawval) {
      return Long.valueOf(rawval);
    }

    @Override
    public Long parseAndAddGap(Comparable value, String gap) {
      return new Long(((Number) value).longValue() + Long.valueOf(gap).longValue());
    }

    @Override
    protected float getGapIndex(Comparable s, String g, Comparable v) {
      Long start = ((Number) s).longValue();
      Long gap = Long.valueOf(g).longValue();
      Long value = ((Number) v).longValue();

      return (value - start) / (float) gap;
    }
  }

  private static class DateCalc extends Calc {
    private final Date now;

    public DateCalc(final SchemaField f,
        final Date now) {
      super(f);
      this.now = now;
      if (!(field.getType() instanceof TrieDateField)) {
        throw new IllegalArgumentException("SchemaField must use field type extending TrieDateField or DateRangeField");
      }
    }

    @Override
    public Comparable bitsToValue(long bits) {
      return new Date(bits);
    }

    @Override
    public String formatValue(Comparable val) {
      return ((Date) val).toInstant().toString();
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
      dmp.setNow((Date) value);
      return dmp.parseMath(gap);
    }

    @Override
    protected float getGapIndex(Comparable s, String g, Comparable v) {
      try {
        Long start = ((Date) s).getTime();
        Long gap = parseAndAddGap(new Date(0), g).getTime();
        Long value = ((Date) v).getTime();

        return (value - start) / (float) gap;
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse gap " + g + " for field: " +
                field.getName(), e);
      }
    }
  }

}

class FacetRangeProcessorDefault extends FacetRangeProcessor {

  FacetRangeProcessorDefault(FacetContext fcontext, FacetRange freq) {
    super(fcontext, freq);
  }

  SimpleOrderedMap getRangeCountsIndexed() throws IOException {

    int slotCount = rangeList.size() + otherList.length;
    intersections = new DocSet[slotCount];
    filters = new Query[slotCount];

    createAccs(fcontext.base.size(), slotCount);

    for (int idx = 0; idx < rangeList.size(); idx++) {
      rangeStats(rangeList.get(idx), idx);
    }

    for (int idx = 0; idx < otherList.length; idx++) {
      if (otherList[idx] == null) continue;
      rangeStats(otherList[idx], rangeList.size() + idx);
    }

    return getRangeCountsIndexedResults();
  }

  private Query[] filters;
  private DocSet[] intersections;

  private void rangeStats(Range range, int slot) throws IOException {
    Query rangeQ = generateFilterQuery(range);
    // TODO: specialize count only
    DocSet intersection = fcontext.searcher.getDocSet(rangeQ, fcontext.base);
    filters[slot] = rangeQ;
    intersections[slot] = intersection; // save for later // TODO: only save if number of slots is small enough?
    int num = collect(intersection, slot);
    countAcc.incrementCount(slot, num); // TODO: roll this into collect()
  }

  private SimpleOrderedMap<Object> rangeStats(Range range, boolean special) throws IOException {
    SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

    // typically the start value of the range, but null for before/after/between
    if (!special) {
      bucket.add("val", range.label);
    }

    Query rangeQ = sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low),
        range.high == null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
    fillBucket(bucket, rangeQ, null);

    return bucket;
  }

  @Override
  DocSet getRangeDocSet(int slot) {
    return intersections[slot];
  }
}

class FacetRangeProcessorSingledValuedDV extends FacetRangeProcessor {
  NumericDocValues numericDocValues;
  DocSetAcc docSetAcc;

  FacetRangeProcessorSingledValuedDV(FacetContext fcontext, FacetRange freq) {
    super(fcontext, freq);
  }

  SimpleOrderedMap getRangeCountsIndexed() throws IOException {

    int slotCount = rangeList.size() + otherList.length;

    createAccs(fcontext.base.size(), slotCount);
    collectSlots(fcontext.base);

    return getRangeCountsIndexedResults();
  }

  protected void createAccs(int docCount, int slotCount) throws IOException {
    super.createAccs(docCount, slotCount);

    docSetAcc = new DocSetAcc(fcontext, slotCount);
  }

  @Override
  protected void setNextReader(LeafReaderContext ctx) throws IOException {
    super.setNextReader(ctx);

    numericDocValues = ctx.reader().getNumericDocValues(sf.getName());
    if (numericDocValues == null) {
      numericDocValues = DocValues.emptyNumeric();
    }
    
    docSetAcc.setNextReader(ctx);
  }

  @Override
  protected void collect(int segDoc) throws IOException {
    if (numericDocValues.advanceExact(segDoc)) {
      Long bits = numericDocValues.longValue();

      Comparable value = calc.bitsToValue(bits);
      int slot = (int) Math.floor(calc.getGapIndex(start, gap, value));

      Range low = slot > 0 && slot <= rangeList.size() ? rangeList.get(slot - 1) : null;
      Range mid = slot >= 0 && slot < rangeList.size() ? rangeList.get(slot) : null;
      Range high = slot >= -1 && slot < rangeList.size() - 1 ? rangeList.get(slot + 1) : null;

      if (low != null && isInsideRange(low, value)) collect(segDoc, slot - 1);
      if (mid != null && isInsideRange(mid, value)) collect(segDoc, slot);
      if (high != null && isInsideRange(high, value)) collect(segDoc, slot + 1);

      if (otherList[0] != null && isInsideRange(otherList[0], value)) collect(segDoc, rangeList.size());
      if (otherList[1] != null && isInsideRange(otherList[1], value)) collect(segDoc, rangeList.size() + 1);
      if (otherList[2] != null && isInsideRange(otherList[2], value)) collect(segDoc, rangeList.size() + 2);
    }
  }

  @Override
  protected void collect(int segDoc, int slot) throws IOException {
    super.collect(segDoc, slot);

    countAcc.incrementCount(slot, 1);
    docSetAcc.collect(segDoc, slot);
  }

  private boolean isInsideRange(Range range, Comparable value) {
    return (range.includeLower && range.low.compareTo(value) == 0)
        ||
        ((range.low == null || range.low.compareTo(value) < 0) && (range.high == null || range.high.compareTo(value) > 0))
        ||
        (range.includeUpper && range.high.compareTo(value) == 0);
  }

  @Override
  DocSet getRangeDocSet(int slot) {
    return docSetAcc.getDocSet(slot);
  }

}

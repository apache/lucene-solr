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
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.CurrencyFieldType;
import org.apache.solr.schema.CurrencyValue;
import org.apache.solr.schema.ExchangeRateProvider;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.PointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.facet.SlotAcc.SlotContext;
import org.apache.solr.util.DateMathParser;

import static org.apache.solr.search.facet.FacetContext.SKIP_FACET;

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
    // If mincount>0 then we could *potentially* set our sub mincount to 1...
    // ...but that would require sorting the buckets (by their val) at the top level
    //
    // Tather then do that, which could be complicated by non trivial field types, we'll force the sub-shard effectiveMincount
    // to be 0, ensuring that we can trivially merge all the buckets from every shard
    // (we have to filter the merged buckets by the original mincount either way)
    effectiveMincount = fcontext.isShard() ? 0 : freq.mincount;
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

  /**
   * Returns a {@link Calc} instance to use for <em>term</em> faceting over a numeric field.
   * This metod is unused for <code>range</code> faceting, and exists solely as a helper method for other classes
   * 
   * @param sf A field to facet on, must be of a type such that {@link FieldType#getNumberType} is non null
   * @return a <code>Calc</code> instance with {@link Calc#bitsToValue} and {@link Calc#bitsToSortableBits} methods suitable for the specified field.
   * @see FacetFieldProcessorByHashDV
   */
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
                  "Unable to range facet on tried field of unexpected type:" + freq.field);
      }
    } else if (ft instanceof CurrencyFieldType) {
      calc = new CurrencyCalc(sf);
    } else {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Unable to range facet on field:" + sf);
    }

    if (fcontext.facetInfo != null) {
      return refineFacets();
    }

    createRangeList();
    return getRangeCountsIndexed();
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

      Range range = new Range(calc.buildRangeLabel(low), low, high, incLower, incUpper);
      rangeList.add( range );

      low = high;
    }

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
      // we don't skip these buckets based on mincount
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
     * Given the low value for a bucket, generates the appropraite "label" object to use. 
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
      return new Float(((Number)value).floatValue() + Float.parseFloat(gap));
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
      return new Double(((Number)value).doubleValue() + Double.parseDouble(gap));
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

  // this refineFacets method is patterned after FacetFieldProcessor.refineFacets and should
  // probably be merged when range facet becomes more like field facet in it's ability to sort and limit
  protected SimpleOrderedMap<Object> refineFacets() throws IOException {
    boolean skipThisFacet = (fcontext.flags & SKIP_FACET) != 0;

    List leaves = FacetFieldProcessor.asList(fcontext.facetInfo.get("_l"));        // We have not seen this bucket: do full faceting for this bucket, including all sub-facets
    List<List> skip = FacetFieldProcessor.asList(fcontext.facetInfo.get("_s"));    // We have seen this bucket, so skip stats on it, and skip sub-facets except for the specified sub-facets that should calculate specified buckets.
    List<List> partial = FacetFieldProcessor.asList(fcontext.facetInfo.get("_p")); // We have not seen this bucket, do full faceting for this bucket, and most sub-facets... but some sub-facets are partial and should only visit specified buckets.

    // currently, only _s should be present for range facets.  In the future, range facets will
    // be more like field facets and will have the same refinement cases.  When that happens, we should try to unify the refinement code more
    assert leaves.size() == 0;
    assert partial.size() == 0;

    // For leaf refinements, we do full faceting for each leaf bucket.  Any sub-facets of these buckets will be fully evaluated.  Because of this, we should never
    // encounter leaf refinements that have sub-facets that return partial results.

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();
    List<SimpleOrderedMap> bucketList = new ArrayList<>( leaves.size() + skip.size() + partial.size() );
    res.add("buckets", bucketList);

    // TODO: an alternate implementations can fill all accs at once
    createAccs(-1, 1);

    for (Object bucketVal : leaves) {
      bucketList.add( refineBucket(bucketVal, false, null) );
    }

    for (List bucketAndFacetInfo : skip) {
      assert bucketAndFacetInfo.size() == 2;
      Object bucketVal = bucketAndFacetInfo.get(0);
      Map<String,Object> facetInfo = (Map<String, Object>) bucketAndFacetInfo.get(1);

      bucketList.add( refineBucket(bucketVal, true, facetInfo ) );
    }

    // The only difference between skip and missing is the value of "skip" passed to refineBucket
    for (List bucketAndFacetInfo : partial) {
      assert bucketAndFacetInfo.size() == 2;
      Object bucketVal = bucketAndFacetInfo.get(0);
      Map<String,Object> facetInfo = (Map<String, Object>) bucketAndFacetInfo.get(1);

      bucketList.add( refineBucket(bucketVal, false, facetInfo ) );
    }

    /*** special buckets
    if (freq.missing) {
      Map<String,Object> bucketFacetInfo = (Map<String,Object>)fcontext.facetInfo.get("missing");

      if (bucketFacetInfo != null || !skipThisFacet) {
        SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
        fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null, skipThisFacet, bucketFacetInfo);
        res.add("missing", missingBucket);
      }
    }
     **********/


    // If there are just a couple of leaves, and if the domain is large, then
    // going by term is likely the most efficient?
    // If the domain is small, or if the number of leaves is large, then doing
    // the normal collection method may be best.

    return res;
  }

  private SimpleOrderedMap<Object> refineBucket(Object bucketVal, boolean skip, Map<String,Object> facetInfo) throws IOException {
    // TODO: refactor this repeated code from above
    Comparable start = calc.getValue(bucketVal.toString());
    Comparable end = calc.getValue(freq.end.toString());
    EnumSet<FacetParams.FacetRangeInclude> include = freq.include;

    String gap = freq.gap.toString();

    Comparable low = calc.getValue(bucketVal.toString());
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

    Range range = new Range(calc.buildRangeLabel(low), low, high, incLower, incUpper);


    // now refine this range

    SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
    bucket.add("val", range.label);
    
    Query domainQ = sf.getType().getRangeQuery(null, sf, range.low == null ? null : calc.formatValue(range.low), range.high==null ? null : calc.formatValue(range.high), range.includeLower, range.includeUpper);
    fillBucket(bucket, domainQ, null, skip, facetInfo);

    return bucket;
  }
}

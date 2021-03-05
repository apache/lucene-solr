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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.CurrencyFieldType;
import org.apache.solr.schema.CurrencyValue;
import org.apache.solr.schema.DatePointField;
import org.apache.solr.schema.DateRangeField;
import org.apache.solr.schema.ExchangeRateProvider;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.util.DateMathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a single facet.range request along with all its parameters. This class
 * calculates all the ranges (gaps) required to be counted.
 */
public class RangeFacetRequest extends FacetComponent.FacetBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SchemaField schemaField;
  protected final String start;
  protected final String end;
  protected final String gap;
  protected final boolean hardEnd;
  protected final EnumSet<FacetParams.FacetRangeInclude> include;
  protected final EnumSet<FacetParams.FacetRangeOther> others;
  protected final FacetParams.FacetRangeMethod method;
  protected final int minCount;
  protected final boolean groupFacet;
  protected final List<FacetRange> facetRanges;

  /**
   * The computed start value of this range
   */
  protected final Object startObj;
  /**
   * The computed end value of this range taking into account facet.range.hardend
   */
  protected final Object endObj;
  /**
   * The computed gap between each range
   */
  protected final Object gapObj;

  public RangeFacetRequest(ResponseBuilder rb, String f) {
    super(rb, FacetParams.FACET_RANGE, f);

    IndexSchema schema = rb.req.getSchema();
    this.schemaField = schema.getField(facetOn);

    SolrParams params = SolrParams.wrapDefaults(localParams, rb.req.getParams());
    SolrParams required = new RequiredSolrParams(params);

    String methodStr = params.get(FacetParams.FACET_RANGE_METHOD);
    FacetParams.FacetRangeMethod method = (methodStr == null ? FacetParams.FacetRangeMethod.getDefault() : FacetParams.FacetRangeMethod.get(methodStr));

    if ((schemaField.getType() instanceof DateRangeField) && method.equals(FacetParams.FacetRangeMethod.DV)) {
      // the user has explicitly selected the FacetRangeMethod.DV method
      log.warn("Range facet method '{}' is not supported together with field type '{}'. Will use method '{}' instead"
          , FacetParams.FacetRangeMethod.DV, DateRangeField.class, FacetParams.FacetRangeMethod.FILTER);
      method = FacetParams.FacetRangeMethod.FILTER;
    }
    if (method.equals(FacetParams.FacetRangeMethod.DV) && !schemaField.hasDocValues() && (schemaField.getType().isPointField())) {
      log.warn("Range facet method '{}' is not supported on PointFields without docValues. Will use method '{}' instead"
          , FacetParams.FacetRangeMethod.DV
          , FacetParams.FacetRangeMethod.FILTER);
      method = FacetParams.FacetRangeMethod.FILTER;
    }

    this.start = required.getFieldParam(facetOn, FacetParams.FACET_RANGE_START);
    this.end = required.getFieldParam(facetOn, FacetParams.FACET_RANGE_END);


    this.gap = required.getFieldParam(facetOn, FacetParams.FACET_RANGE_GAP);
    this.minCount = params.getFieldInt(facetOn, FacetParams.FACET_MINCOUNT, 0);

    this.include = FacetParams.FacetRangeInclude.parseParam
        (params.getFieldParams(facetOn, FacetParams.FACET_RANGE_INCLUDE));

    this.hardEnd = params.getFieldBool(facetOn, FacetParams.FACET_RANGE_HARD_END, false);

    this.others = EnumSet.noneOf(FacetParams.FacetRangeOther.class);
    final String[] othersP = params.getFieldParams(facetOn, FacetParams.FACET_RANGE_OTHER);
    if (othersP != null && othersP.length > 0) {
      for (final String o : othersP) {
        others.add(FacetParams.FacetRangeOther.get(o));
      }
    }

    this.groupFacet = params.getBool(GroupParams.GROUP_FACET, false);
    if (groupFacet && method.equals(FacetParams.FacetRangeMethod.DV)) {
      // the user has explicitly selected the FacetRangeMethod.DV method
      log.warn("Range facet method '{}' is not supported together with '{}'. Will use method '{}' instead"
          , FacetParams.FacetRangeMethod.DV, GroupParams.GROUP_FACET, FacetParams.FacetRangeMethod.FILTER);
      method = FacetParams.FacetRangeMethod.FILTER;
    }

    this.method = method;

    RangeEndpointCalculator<? extends Comparable<?>> calculator = createCalculator();
    this.facetRanges = calculator.computeRanges();
    this.gapObj = calculator.getGap();
    this.startObj = calculator.getStart();
    this.endObj = calculator.getComputedEnd();
  }

  /**
   * Creates the right instance of {@link org.apache.solr.handler.component.RangeFacetRequest.RangeEndpointCalculator}
   * depending on the field type of the schema field
   */
  private RangeEndpointCalculator<? extends Comparable<?>> createCalculator() {
    RangeEndpointCalculator<?> calc;
    FieldType ft = schemaField.getType();

    if (ft instanceof TrieField) {
      switch (ft.getNumberType()) {
        case FLOAT:
          calc = new FloatRangeEndpointCalculator(this);
          break;
        case DOUBLE:
          calc = new DoubleRangeEndpointCalculator(this);
          break;
        case INTEGER:
          calc = new IntegerRangeEndpointCalculator(this);
          break;
        case LONG:
          calc = new LongRangeEndpointCalculator(this);
          break;
        case DATE:
          calc = new DateRangeEndpointCalculator(this, null);
          break;
        default:
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Unable to range facet on Trie field of unexpected type:" + this.facetOn);
      }
    } else if (ft instanceof DateRangeField) {
      calc = new DateRangeEndpointCalculator(this, null);
    } else if (ft.isPointField()) {
      switch (ft.getNumberType()) {
        case FLOAT:
          calc = new FloatRangeEndpointCalculator(this);
          break;
        case DOUBLE:
          calc = new DoubleRangeEndpointCalculator(this);
          break;
        case INTEGER:
          calc = new IntegerRangeEndpointCalculator(this);
          break;
        case LONG:
          calc = new LongRangeEndpointCalculator(this);
          break;
        case DATE:
          calc = new DateRangeEndpointCalculator(this, null);
          break;
        default:
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "Unable to range facet on Point field of unexpected type:" + this.facetOn);
      }
    } else if (ft instanceof CurrencyFieldType) {
      calc = new CurrencyRangeEndpointCalculator(this);
    } else {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Unable to range facet on field:" + schemaField);
    }

    return calc;
  }

  /**
   * @return the start of this range as specified by {@link FacetParams#FACET_RANGE_START} parameter
   */
  public String getStart() {
    return start;
  }

  /**
   * The end of this facet.range as specified by {@link FacetParams#FACET_RANGE_END} parameter
   * <p>
   * Note that the actual computed end value can be different depending on the
   * {@link FacetParams#FACET_RANGE_HARD_END} parameter. See {@link #endObj}
   */
  public String getEnd() {
    return end;
  }

  /**
   * @return an {@link EnumSet} containing all the values specified via
   * {@link FacetParams#FACET_RANGE_INCLUDE} parameter. Defaults to
   * {@link org.apache.solr.common.params.FacetParams.FacetRangeInclude#LOWER} if no parameter
   * is supplied. Includes all values from {@link org.apache.solr.common.params.FacetParams.FacetRangeInclude} enum
   * if {@link FacetParams#FACET_RANGE_INCLUDE} includes
   * {@link org.apache.solr.common.params.FacetParams.FacetRangeInclude#ALL}
   */
  public EnumSet<FacetParams.FacetRangeInclude> getInclude() {
    return include;
  }

  /**
   * @return the gap as specified by {@link FacetParams#FACET_RANGE_GAP} parameter
   */
  public String getGap() {
    return gap;
  }

  /**
   * @return the computed gap object
   */
  public Object getGapObj() {
    return gapObj;
  }

  /**
   * @return the boolean value of {@link FacetParams#FACET_RANGE_HARD_END} parameter
   */
  public boolean isHardEnd() {
    return hardEnd;
  }

  /**
   * @return an {@link EnumSet} of {@link org.apache.solr.common.params.FacetParams.FacetRangeOther} values
   * specified by {@link FacetParams#FACET_RANGE_OTHER} parameter
   */
  public EnumSet<FacetParams.FacetRangeOther> getOthers() {
    return others;
  }

  /**
   * @return the {@link org.apache.solr.common.params.FacetParams.FacetRangeMethod} to be used for computing
   * ranges determined either by the value of {@link FacetParams#FACET_RANGE_METHOD} parameter
   * or other internal constraints.
   */
  public FacetParams.FacetRangeMethod getMethod() {
    return method;
  }

  /**
   * @return the minimum allowed count for facet ranges as specified by {@link FacetParams#FACET_MINCOUNT}
   */
  public int getMinCount() {
    return minCount;
  }

  /**
   * @return the {@link SchemaField} instance representing the field on which ranges have to be calculated
   */
  public SchemaField getSchemaField() {
    return schemaField;
  }

  /**
   * @return the boolean value specified by {@link GroupParams#GROUP_FACET} parameter
   */
  public boolean isGroupFacet() {
    return groupFacet;
  }

  /**
   * @return a {@link List} of {@link org.apache.solr.handler.component.RangeFacetRequest.FacetRange} objects
   * representing the ranges (gaps) for which range counts are to be calculated.
   */
  public List<FacetRange> getFacetRanges() {
    return facetRanges;
  }

  /**
   * @return The computed start value of this range
   */
  public Object getStartObj() {
    return startObj;
  }

  /**
   * The end of this facet.range as calculated using the value of facet.range.end
   * parameter and facet.range.hardend. This can be different from the
   * value specified in facet.range.end if facet.range.hardend=true
   */
  public Object getEndObj() {
    return endObj;
  }

  /**
   * Represents a range facet response combined from all shards.
   * Provides helper methods to merge facet_ranges response from a shard.
   * See {@link #mergeFacetRangesFromShardResponse(LinkedHashMap, SimpleOrderedMap)}
   * and {@link #mergeContributionFromShard(SimpleOrderedMap)}
   */
  static class DistribRangeFacet {
    public SimpleOrderedMap<Object> rangeFacet;

    public DistribRangeFacet(SimpleOrderedMap<Object> rangeFacet) {
      this.rangeFacet = rangeFacet;
    }

    /**
     * Helper method to merge range facet values from a shard's response to already accumulated
     * values for each range.
     *
     * @param rangeCounts a {@link LinkedHashMap} containing the accumulated values for each range
     *                    keyed by the 'key' of the facet.range. Must not be null.
     * @param shardRanges the facet_ranges response from a shard. Must not be null.
     */
    public static void mergeFacetRangesFromShardResponse(LinkedHashMap<String, DistribRangeFacet> rangeCounts,
                                                         SimpleOrderedMap<SimpleOrderedMap<Object>> shardRanges) {
      assert shardRanges != null;
      assert rangeCounts != null;
      for (Map.Entry<String, SimpleOrderedMap<Object>> entry : shardRanges) {
        String rangeKey = entry.getKey();

        RangeFacetRequest.DistribRangeFacet existing = rangeCounts.get(rangeKey);
        if (existing == null) {
          rangeCounts.put(rangeKey, new RangeFacetRequest.DistribRangeFacet(entry.getValue()));
        } else {
          existing.mergeContributionFromShard(entry.getValue());
        }
      }
    }

    /**
     * Accumulates an individual facet_ranges count from a shard into global counts.
     * <p>
     * The implementation below uses the first encountered shard's
     * facet_ranges as the basis for subsequent shards' data to be merged.
     *
     * @param rangeFromShard the facet_ranges response from a shard
     */
    public void mergeContributionFromShard(SimpleOrderedMap<Object> rangeFromShard) {
      if (rangeFacet == null) {
        rangeFacet = rangeFromShard;
        return;
      }

      @SuppressWarnings("unchecked")
      NamedList<Integer> shardFieldValues
          = (NamedList<Integer>) rangeFromShard.get("counts");

      @SuppressWarnings("unchecked")
      NamedList<Integer> existFieldValues
          = (NamedList<Integer>) rangeFacet.get("counts");

      for (Map.Entry<String, Integer> existPair : existFieldValues) {
        final String key = existPair.getKey();
        // can be null if inconsistencies in shards responses
        Integer newValue = shardFieldValues.get(key);
        if (null != newValue) {
          Integer oldValue = existPair.getValue();
          existPair.setValue(oldValue + newValue);
        }
      }

      // merge facet.other=before/between/after/all if they exist
      for (FacetParams.FacetRangeOther otherKey : FacetParams.FacetRangeOther.values()) {
        if (otherKey == FacetParams.FacetRangeOther.NONE) continue;

        String name = otherKey.toString();
        Integer shardValue = (Integer) rangeFromShard.get(name);
        if (shardValue != null && shardValue > 0) {
          Integer existingValue = (Integer) rangeFacet.get(name);
          // shouldn't be null
          int idx = rangeFacet.indexOf(name, 0);
          rangeFacet.setVal(idx, existingValue + shardValue);
        }
      }
    }

    /**
     * Removes all counts under the given minCount from the accumulated facet_ranges.
     * <p>
     * Note: this method should only be called after all shard responses have been
     * accumulated using {@link #mergeContributionFromShard(SimpleOrderedMap)}
     *
     * @param minCount the minimum allowed count for any range
     */
    public void removeRangeFacetsUnderLimits(int minCount) {
      boolean replace = false;

      @SuppressWarnings("unchecked")
      NamedList<Number> vals = (NamedList<Number>) rangeFacet.get("counts");
      NamedList<Number> newList = new NamedList<>();
      for (Map.Entry<String, Number> pair : vals) {
        if (pair.getValue().longValue() >= minCount) {
          newList.add(pair.getKey(), pair.getValue());
        } else {
          replace = true;
        }
      }
      if (replace) {
        vals.clear();
        vals.addAll(newList);
      }
    }
  }

  /**
   * Perhaps someday instead of having a giant "instanceof" case
   * statement to pick an impl, we can add a "RangeFacetable" marker
   * interface to FieldTypes and they can return instances of these
   * directly from some method -- but until then, keep this locked down
   * and private.
   */
  private static abstract class RangeEndpointCalculator<T extends Comparable<T>> {
    protected final RangeFacetRequest rfr;
    protected final SchemaField field;

    /**
     * The end of the facet.range as determined by this calculator.
     * This can be different from the facet.range.end depending on the
     * facet.range.hardend parameter
     */
    protected T computedEnd;

    protected T start;

    protected Object gap;

    protected boolean computed = false;

    public RangeEndpointCalculator(RangeFacetRequest rfr) {
      this.rfr = rfr;
      this.field = rfr.getSchemaField();
    }

    /** The Computed End point of all ranges, as an Object of type suitable for direct inclusion in the response data */
    public Object getComputedEnd() {
      assert computed;
      return computedEnd;
    }

    /** The Start point of all ranges, as an Object of type suitable for direct inclusion in the response data */
    public Object getStart() {
      assert computed;
      return start;
    }

    /**
     * @return the parsed value of {@link FacetParams#FACET_RANGE_GAP} parameter. This type
     * of the returned object is the boxed type of the schema field type's primitive counterpart
     * except in the case of Dates in which case the returned type is just a string (because in
     * case of dates the gap can either be a date or a DateMath string).
     */
    public Object getGap() {
      assert computed;
      return gap;
    }

    /**
     * Formats a Range endpoint for use as a range label name in the response.
     * Default Impl just uses toString()
     */
    public String formatValue(final T val) {
      return val.toString();
    }

    /**
     * Parses a String param into an Range endpoint value throwing
     * a useful exception if not possible
     */
    public final T getValue(final String rawval) {
      try {
        return parseVal(rawval);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse value " + rawval + " for field: " +
                field.getName(), e);
      }
    }

    /**
     * Parses a String param into an Range endpoint.
     * Can throw a low level format exception as needed.
     */
    protected abstract T parseVal(final String rawval)
        throws java.text.ParseException;

    /**
     * Parses a String param into a value that represents the gap and
     * can be included in the response, throwing
     * a useful exception if not possible.
     * <p>
     * Note: uses Object as the return type instead of T for things like
     * Date where gap is just a DateMathParser string
     */
    protected final Object getGap(final String gap) {
      try {
        return parseGap(gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't parse gap " + gap + " for field: " +
                field.getName(), e);
      }
    }

    /**
     * Parses a String param into a value that represents the gap and
     * can be included in the response.
     * Can throw a low level format exception as needed.
     * <p>
     * Default Impl calls parseVal
     */
    protected Object parseGap(final String rawval)
        throws java.text.ParseException {
      return parseVal(rawval);
    }

    /**
     * Adds the String gap param to a low Range endpoint value to determine
     * the corresponding high Range endpoint value, throwing
     * a useful exception if not possible.
     */
    public final T addGap(T value, String gap) {
      try {
        return parseAndAddGap(value, gap);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Can't add gap " + gap + " to value " + value +
                " for field: " + field.getName(), e);
      }
    }

    /**
     * Adds the String gap param to a low Range endpoint value to determine
     * the corrisponding high Range endpoint value.
     * Can throw a low level format exception as needed.
     */
    protected abstract T parseAndAddGap(T value, String gap)
        throws java.text.ParseException;

    public List<FacetRange> computeRanges() {
      List<FacetRange> ranges = new ArrayList<>();

      this.gap = getGap(rfr.getGap());
      this.start = getValue(rfr.getStart());
      // not final, hardend may change this
      T end = getValue(rfr.getEnd());
      if (end.compareTo(start) < 0) {
        throw new SolrException
            (SolrException.ErrorCode.BAD_REQUEST,
                "range facet 'end' comes before 'start': " + end + " < " + start);
      }

      final EnumSet<FacetParams.FacetRangeInclude> include = rfr.getInclude();

      T low = start;

      while (low.compareTo(end) < 0) {
        T high = addGap(low, rfr.getGap());
        if (end.compareTo(high) < 0) {
          if (rfr.isHardEnd()) {
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
                  "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: " + low + " + " + rfr.getGap() + " = " + high);
        }

        final boolean includeLower =
            (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
                (include.contains(FacetParams.FacetRangeInclude.EDGE) &&
                    0 == low.compareTo(start)));
        final boolean includeUpper =
            (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
                (include.contains(FacetParams.FacetRangeInclude.EDGE) &&
                    0 == high.compareTo(end)));

        final String lowS = formatValue(low);
        final String highS = formatValue(high);

        ranges.add(new FacetRange(lowS, lowS, highS, includeLower, includeUpper));

        low = high;
      }

      // we must update the end value in RangeFacetRequest because the end is returned
      // as a separate element in the range facet response
      this.computedEnd = end;
      this.computed = true;

      // no matter what other values are listed, we don't do
      // anything if "none" is specified.
      if (!rfr.getOthers().contains(FacetParams.FacetRangeOther.NONE)) {

        boolean all = rfr.getOthers().contains(FacetParams.FacetRangeOther.ALL);
        final String startS = formatValue(start);
        final String endS = formatValue(end);

        if (all || rfr.getOthers().contains(FacetParams.FacetRangeOther.BEFORE)) {
          // include upper bound if "outer" or if first gap doesn't already include it
          ranges.add(new FacetRange(FacetParams.FacetRangeOther.BEFORE,
              null, startS, false, include.contains(FacetParams.FacetRangeInclude.OUTER) || include.contains(FacetParams.FacetRangeInclude.ALL) ||
              !(include.contains(FacetParams.FacetRangeInclude.LOWER) || include.contains(FacetParams.FacetRangeInclude.EDGE))));
        }
        if (all || rfr.getOthers().contains(FacetParams.FacetRangeOther.AFTER)) {
          // include lower bound if "outer" or if last gap doesn't already include it
          ranges.add(new FacetRange(FacetParams.FacetRangeOther.AFTER,
              endS, null, include.contains(FacetParams.FacetRangeInclude.OUTER) || include.contains(FacetParams.FacetRangeInclude.ALL) ||
              !(include.contains(FacetParams.FacetRangeInclude.UPPER) || include.contains(FacetParams.FacetRangeInclude.EDGE)), false));
        }
        if (all || rfr.getOthers().contains(FacetParams.FacetRangeOther.BETWEEN)) {
          ranges.add(new FacetRange(FacetParams.FacetRangeOther.BETWEEN, startS, endS,
              include.contains(FacetParams.FacetRangeInclude.LOWER) || include.contains(FacetParams.FacetRangeInclude.EDGE) || include.contains(FacetParams.FacetRangeInclude.ALL),
              include.contains(FacetParams.FacetRangeInclude.UPPER) || include.contains(FacetParams.FacetRangeInclude.EDGE) || include.contains(FacetParams.FacetRangeInclude.ALL)));
        }
      }

      return ranges;
    }

  }

  private static class FloatRangeEndpointCalculator
      extends RangeEndpointCalculator<Float> {

    public FloatRangeEndpointCalculator(final RangeFacetRequest rangeFacetRequest) {
      super(rangeFacetRequest);
    }

    @Override
    protected Float parseVal(String rawval) {
      return Float.valueOf(rawval);
    }

    @Override
    public Float parseAndAddGap(Float value, String gap) {
      return value.floatValue() + Float.parseFloat(gap);
    }
  }

  private static class DoubleRangeEndpointCalculator
      extends RangeEndpointCalculator<Double> {

    public DoubleRangeEndpointCalculator(final RangeFacetRequest rangeFacetRequest) {
      super(rangeFacetRequest);
    }

    @Override
    protected Double parseVal(String rawval) {
      return Double.valueOf(rawval);
    }

    @Override
    public Double parseAndAddGap(Double value, String gap) {
      return value.doubleValue() + Double.parseDouble(gap);
    }
  }

  private static class IntegerRangeEndpointCalculator
      extends RangeEndpointCalculator<Integer> {

    public IntegerRangeEndpointCalculator(final RangeFacetRequest rangeFacetRequest) {
      super(rangeFacetRequest);
    }

    @Override
    protected Integer parseVal(String rawval) {
      return Integer.valueOf(rawval);
    }

    @Override
    public Integer parseAndAddGap(Integer value, String gap) {
      return value.intValue() + Integer.parseInt(gap);
    }
  }

  private static class LongRangeEndpointCalculator
      extends RangeEndpointCalculator<Long> {

    public LongRangeEndpointCalculator(final RangeFacetRequest rangeFacetRequest) {
      super(rangeFacetRequest);
    }

    @Override
    protected Long parseVal(String rawval) {
      return Long.valueOf(rawval);
    }

    @Override
    public Long parseAndAddGap(Long value, String gap) {
      return value.longValue() + Long.parseLong(gap);
    }
  }

  private static class DateRangeEndpointCalculator
      extends RangeEndpointCalculator<Date> {
    private static final String TYPE_ERR_MSG = "SchemaField must use field type extending TrieDateField or DateRangeField";
    private final Date now;

    public DateRangeEndpointCalculator(final RangeFacetRequest rangeFacetRequest,
                                       final Date now) {
      super(rangeFacetRequest);
      this.now = now;
      if (!(field.getType() instanceof TrieDateField)
          && !(field.getType() instanceof DateRangeField)
          && !(field.getType() instanceof DatePointField)) {
        throw new IllegalArgumentException(TYPE_ERR_MSG);
      }
    }

    @Override
    public String formatValue(Date val) {
      return val.toInstant().toString();
    }

    @Override
    protected Date parseVal(String rawval) {
      return DateMathParser.parseMath(now, rawval);
    }

    @Override
    protected Object parseGap(final String rawval) {
      return rawval;
    }

    @Override
    public Date parseAndAddGap(Date value, String gap) throws java.text.ParseException {
      final DateMathParser dmp = new DateMathParser();
      dmp.setNow(value);
      return dmp.parseMath(gap);
    }
  }

  private static class CurrencyRangeEndpointCalculator
    extends RangeEndpointCalculator<CurrencyValue> {
    private String defaultCurrencyCode;
    private ExchangeRateProvider exchangeRateProvider;
    public CurrencyRangeEndpointCalculator(final RangeFacetRequest rangeFacetRequest) {
      super(rangeFacetRequest);
      if(!(this.field.getType() instanceof CurrencyFieldType)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Cannot perform range faceting over non CurrencyField fields");
      }
      defaultCurrencyCode =
        ((CurrencyFieldType)this.field.getType()).getDefaultCurrency();
      exchangeRateProvider =
        ((CurrencyFieldType)this.field.getType()).getProvider();
    }

    @Override
    protected Object parseGap(String rawval) throws java.text.ParseException {
      return parseVal(rawval).strValue();
    }

    @Override
    public String formatValue(CurrencyValue val) {
      return val.strValue();
    }

    /** formats the value as a String since {@link CurrencyValue} is not suitable for response writers */
    @Override
    public Object getComputedEnd() {
      assert computed;
      return formatValue(computedEnd);
    }
    
    /** formats the value as a String since {@link CurrencyValue} is not suitable for response writers */
    @Override
    public Object getStart() {
      assert computed;
      return formatValue(start);
    }
    
    @Override
    protected CurrencyValue parseVal(String rawval) {
      return CurrencyValue.parse(rawval, defaultCurrencyCode);
    }

    @Override
    public CurrencyValue parseAndAddGap(CurrencyValue value, String gap) {
      if(value == null) {
        throw new NullPointerException("Cannot perform range faceting on null CurrencyValue");
      }
      CurrencyValue gapCurrencyValue =
        CurrencyValue.parse(gap, defaultCurrencyCode);
      long gapAmount =
        CurrencyValue.convertAmount(this.exchangeRateProvider,
                                    gapCurrencyValue.getCurrencyCode(),
                                    gapCurrencyValue.getAmount(),
                                    value.getCurrencyCode());
      return new CurrencyValue(value.getAmount() + gapAmount,
                               value.getCurrencyCode());
    }
  }
  
  /**
   * Represents a single facet range (or gap) for which the count is to be calculated
   */
  public static class FacetRange {
    public final FacetParams.FacetRangeOther other;
    public final String name;
    public final String lower;
    public final String upper;
    public final boolean includeLower;
    public final boolean includeUpper;

    private FacetRange(FacetParams.FacetRangeOther other, String name, String lower, String upper, boolean includeLower, boolean includeUpper) {
      this.other = other;
      this.name = name;
      this.lower = lower;
      this.upper = upper;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;
    }

    /**
     * Construct a facet range for a {@link org.apache.solr.common.params.FacetParams.FacetRangeOther} instance
     */
    public FacetRange(FacetParams.FacetRangeOther other, String lower, String upper, boolean includeLower, boolean includeUpper) {
      this(other, other.toString(), lower, upper, includeLower, includeUpper);
    }

    /**
     * Construct a facet range for the give name
     */
    public FacetRange(String name, String lower, String upper, boolean includeLower, boolean includeUpper) {
      this(null, name, lower, upper, includeLower, includeUpper);
    }
  }
}


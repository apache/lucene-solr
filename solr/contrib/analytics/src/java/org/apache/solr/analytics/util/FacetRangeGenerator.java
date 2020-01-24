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
package org.apache.solr.analytics.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.analytics.facet.RangeFacet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumericFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.DateMathParser;


/**
 * Calculates a set of {@link FacetRange}s for a given {@link RangeFacet}.
 */
public abstract class FacetRangeGenerator<T extends Comparable<T>> {
  protected final SchemaField field;
  protected final RangeFacet rangeFacet;

  public FacetRangeGenerator(final RangeFacet rangeFacet) {
    this.field = rangeFacet.getField();
    this.rangeFacet = rangeFacet;
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
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't parse value "+rawval+" for field: " + field.getName(), e);
    }
  }

  /**
   * Parses a String param into an Range endpoint.
   * Can throw a low level format exception as needed.
   */
  protected abstract T parseVal(final String rawval) throws java.text.ParseException;

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
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't parse gap "+gap+" for field: " + field.getName(), e);
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
    return parseVal(rawval);
  }

  /**
   * Adds the String gap param to a low Range endpoint value to determine
   * the corrisponding high Range endpoint value, throwing
   * a useful exception if not possible.
   */
  public final T addGap(T value, String gap) {
    try {
      return parseAndAddGap(value, gap);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't add gap "+gap+" to value " + value + " for field: " + field.getName(), e);
    }
  }

  /**
   * Adds the String gap param to a low Range endpoint value to determine
   * the corrisponding high Range endpoint value.
   * Can throw a low level format exception as needed.
   */
  protected abstract T parseAndAddGap(T value, String gap) throws java.text.ParseException;

  public static class FacetRange {
    public final String name;
    public final String lower;
    public final String upper;
    public final boolean includeLower;
    public final boolean includeUpper;
    private final String facetValue;

    public FacetRange(String name, String lower, String upper, boolean includeLower, boolean includeUpper) {
      this.name = name;
      this.lower = lower;
      this.upper = upper;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;

      String value = "(*";
      if (lower != null) {
        value = (includeLower ? "[" : "(") + lower;
      }
      value += " TO ";
      if (upper == null) {
        value += "*)";
      } else {
        value += upper + (includeUpper? "]" : ")");
      }
      facetValue = value;
    }

    @Override
    public String toString() {
        return facetValue;
    }
  }

  public List<FacetRange> getRanges(){

    final T start = getValue(rangeFacet.getStart());
    T end = getValue(rangeFacet.getEnd()); // not final, hardend may change this

    if( end.compareTo(start) < 0 ){
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "range facet 'end' comes before 'start': "+end+" < "+start);
    }

    // explicitly return the gap.  compute this early so we are more
    // likely to catch parse errors before attempting math
    final List<String> gaps = rangeFacet.getGaps();
    String gap = gaps.get(0);

    final EnumSet<FacetRangeInclude> include = rangeFacet.getInclude();

    T low = start;

    List<FacetRange> ranges = new ArrayList<>();

    int gapCounter = 0;

    while (low.compareTo(end) < 0) {
      if (gapCounter<gaps.size()) {
        gap = gaps.get(gapCounter++);
      }
      T high = addGap(low,gap);
      if (end.compareTo(high) < 0) {
        if (rangeFacet.isHardEnd()){
          high = end;
        } else {
          end = high;
        }
      }

      if (high.compareTo(low) < 0) {
        throw new SolrException (SolrException.ErrorCode.BAD_REQUEST, "range facet infinite loop (is gap negative? did the math overflow?)");
      }

      if (high.compareTo(low) == 0) {
        throw new SolrException (SolrException.ErrorCode.BAD_REQUEST, "range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: " + low + " + " + gap + " = " + high );
      }

      final boolean includeLower = (include.contains(FacetRangeInclude.ALL) ||
                                    include.contains(FacetRangeInclude.LOWER) ||
                                   (include.contains(FacetRangeInclude.EDGE) &&
                                   0 == low.compareTo(start)));
      final boolean includeUpper = (include.contains(FacetRangeInclude.ALL) ||
                                    include.contains(FacetRangeInclude.UPPER) ||
                                   (include.contains(FacetRangeInclude.EDGE) &&
                                   0 == high.compareTo(end)));

      final String lowS = formatValue(low);
      final String highS = formatValue(high);

      ranges.add( new FacetRange(lowS,lowS,highS,includeLower,includeUpper) );
      low = high;
    }

    final Set<FacetRangeOther> others = rangeFacet.getOthers();
    if (null != others && 0 < others.size() ) {

      // no matter what other values are listed, we don't do
      // anything if "none" is specified.
      if( !others.contains(FacetRangeOther.NONE) ) {

        boolean all = others.contains(FacetRangeOther.ALL);

        if (all || others.contains(FacetRangeOther.BEFORE)) {
          // include upper bound if "outer" or if first gap doesn't already include it
          ranges.add( new FacetRange(FacetRangeOther.BEFORE.toString(),
                                        null, formatValue(start), false, include.contains(FacetRangeInclude.OUTER) || include.contains(FacetRangeInclude.ALL) ||
                                                            !(include.contains(FacetRangeInclude.LOWER) || include.contains(FacetRangeInclude.EDGE)) ) );

        }
        if (all || others.contains(FacetRangeOther.AFTER)) {
          // include lower bound if "outer" or if last gap doesn't already include it
          ranges.add( new FacetRange(FacetRangeOther.AFTER.toString(),
                                        formatValue(end), null, include.contains(FacetRangeInclude.OUTER) || include.contains(FacetRangeInclude.ALL) ||
                                                   !(include.contains(FacetRangeInclude.UPPER) || include.contains(FacetRangeInclude.EDGE)), false) );
        }
        if (all || others.contains(FacetRangeOther.BETWEEN)) {
          ranges.add( new FacetRange(FacetRangeOther.BETWEEN.toString(), formatValue(start), formatValue(end),
                                        include.contains(FacetRangeInclude.LOWER) || include.contains(FacetRangeInclude.EDGE) || include.contains(FacetRangeInclude.ALL),
                                        include.contains(FacetRangeInclude.UPPER) || include.contains(FacetRangeInclude.EDGE) || include.contains(FacetRangeInclude.ALL)) );
        }
      }

    }

    return ranges;
  }

  public static FacetRangeGenerator<? extends Comparable<?>> create(RangeFacet rangeFacet){
    final SchemaField sf = rangeFacet.getField();
    final FieldType ft = sf.getType();
    final FacetRangeGenerator<?> calc;
    if (ft instanceof NumericFieldType) {
      switch (ft.getNumberType()) {
        case FLOAT:
          calc = new FloatFacetRangeGenerator(rangeFacet);
          break;
        case DOUBLE:
          calc = new DoubleFacetRangeGenerator(rangeFacet);
          break;
        case INTEGER:
          calc = new IntegerFacetRangeGenerator(rangeFacet);
          break;
        case LONG:
          calc = new LongFacetRangeGenerator(rangeFacet);
          break;
        case DATE:
          calc = new DateFacetRangeGenerator(rangeFacet, null);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to range facet on numeric field of unexpected type: " + sf.getName());
      }
    } else {
      throw new SolrException (SolrException.ErrorCode.BAD_REQUEST, "Unable to range facet on non-numeric field: " + sf);
    }
    return calc;
  }
}
class IntegerFacetRangeGenerator extends FacetRangeGenerator<Integer> {
  public IntegerFacetRangeGenerator(final RangeFacet rangeFacet) { super(rangeFacet); }

  @Override
  protected Integer parseVal(String rawval) {
    return Integer.valueOf(rawval);
  }
  @Override
  public Integer parseAndAddGap(Integer value, String gap) {
    return value.intValue() + Integer.valueOf(gap).intValue();
  }
}
class LongFacetRangeGenerator extends FacetRangeGenerator<Long> {
  public LongFacetRangeGenerator(final RangeFacet rangeFacet) { super(rangeFacet); }

  @Override
  protected Long parseVal(String rawval) {
    return Long.valueOf(rawval);
  }
  @Override
  public Long parseAndAddGap(Long value, String gap) {
    return value.longValue() + Long.valueOf(gap).longValue();
  }
}

class FloatFacetRangeGenerator extends FacetRangeGenerator<Float> {
  public FloatFacetRangeGenerator(final RangeFacet rangeFacet) { super(rangeFacet); }

  @Override
  protected Float parseVal(String rawval) {
    return Float.valueOf(rawval);
  }
  @Override
  public Float parseAndAddGap(Float value, String gap) {
    return value.floatValue() + Float.valueOf(gap).floatValue();
  }
}

class DoubleFacetRangeGenerator extends FacetRangeGenerator<Double> {
  public DoubleFacetRangeGenerator(final RangeFacet rangeFacet) { super(rangeFacet); }

  @Override
  protected Double parseVal(String rawval) {
    return Double.valueOf(rawval);
  }
  @Override
  public Double parseAndAddGap(Double value, String gap) {
    return value.doubleValue() + Double.valueOf(gap).doubleValue();
  }
}
class DateFacetRangeGenerator extends FacetRangeGenerator<Date> {
  private final Date now;
  public DateFacetRangeGenerator(final RangeFacet rangeFacet, final Date now) {
    super(rangeFacet);
    this.now = now;
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

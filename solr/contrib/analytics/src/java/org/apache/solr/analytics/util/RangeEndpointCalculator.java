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

import org.apache.solr.analytics.request.RangeFacetRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.util.DateMathParser;


public abstract class RangeEndpointCalculator<T extends Comparable<T>> {
  protected final SchemaField field;
  protected final RangeFacetRequest request;
  
  public RangeEndpointCalculator(final RangeFacetRequest request) {
    this.field = request.getField();
    this.request = request;
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
    
    public FacetRange(String name, String lower, String upper, boolean includeLower, boolean includeUpper) {
      this.name = name;
      this.lower = lower;
      this.upper = upper;
      this.includeLower = includeLower;
      this.includeUpper = includeUpper;
    }
  }
  
  public List<FacetRange> getRanges(){

    final T start = getValue(request.getStart());
    T end = getValue(request.getEnd()); // not final, hardend may change this
    
    if( end.compareTo(start) < 0 ){
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "range facet 'end' comes before 'start': "+end+" < "+start);
    }
    
    // explicitly return the gap.  compute this early so we are more 
    // likely to catch parse errors before attempting math
    final String[] gaps = request.getGaps();
    String gap = gaps[0];
    
    final EnumSet<FacetRangeInclude> include = request.getInclude();
        
    T low = start;
    
    List<FacetRange> ranges = new ArrayList<>();
    
    int gapCounter = 0;
    
    while (low.compareTo(end) < 0) {
      if (gapCounter<gaps.length) {
        gap = gaps[gapCounter++];
      }
      T high = addGap(low,gap);
      if (end.compareTo(high) < 0) {
        if (request.isHardEnd()){
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
    
    final Set<FacetRangeOther> others = request.getOthers();
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
  
  public static RangeEndpointCalculator<? extends Comparable<?>> create(RangeFacetRequest request){
    final SchemaField sf = request.getField();
    final FieldType ft = sf.getType();
    final RangeEndpointCalculator<?> calc;
    if (ft instanceof TrieField) {
      switch (ft.getNumberType()) {
        case FLOAT:
          calc = new FloatRangeEndpointCalculator(request);
          break;
        case DOUBLE:
          calc = new DoubleRangeEndpointCalculator(request);
          break;
        case INTEGER:
          calc = new IntegerRangeEndpointCalculator(request);
          break;
        case LONG:
          calc = new LongRangeEndpointCalculator(request);
          break;
        case DATE:
          calc = new DateRangeEndpointCalculator(request, null);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to range facet on tried field of unexpected type:" + sf.getName());
      }
    } else {
      throw new SolrException (SolrException.ErrorCode.BAD_REQUEST, "Unable to range facet on field:" + sf);
    } 
    return calc;
  }
  
  public static class FloatRangeEndpointCalculator extends RangeEndpointCalculator<Float> {
  
    public FloatRangeEndpointCalculator(final RangeFacetRequest request) { super(request); }
    
    @Override
    protected Float parseVal(String rawval) {
      return Float.valueOf(rawval);
    }
    
    @Override
    public Float parseAndAddGap(Float value, String gap) {
      return new Float(value.floatValue() + Float.parseFloat(gap));
    }
    
  }
  
  public static class DoubleRangeEndpointCalculator extends RangeEndpointCalculator<Double> {
  
    public DoubleRangeEndpointCalculator(final RangeFacetRequest request) { super(request); }
    
    @Override
    protected Double parseVal(String rawval) {
      return Double.valueOf(rawval);
    }
    
    @Override
    public Double parseAndAddGap(Double value, String gap) {
      return new Double(value.doubleValue() + Double.parseDouble(gap));
    }
    
  }
  
  public static class IntegerRangeEndpointCalculator extends RangeEndpointCalculator<Integer> {
  
    public IntegerRangeEndpointCalculator(final RangeFacetRequest request) { super(request); }
    
    @Override
    protected Integer parseVal(String rawval) {
      return Integer.valueOf(rawval);
    }
    
    @Override
    public Integer parseAndAddGap(Integer value, String gap) {
      return new Integer(value.intValue() + Integer.parseInt(gap));
    }
    
  }
  
  public static class LongRangeEndpointCalculator extends RangeEndpointCalculator<Long> {
  
    public LongRangeEndpointCalculator(final RangeFacetRequest request) { super(request); }
    
    @Override
    protected Long parseVal(String rawval) {
      return Long.valueOf(rawval);
    }
    
    @Override
    public Long parseAndAddGap(Long value, String gap) {
      return new Long(value.longValue() + Long.parseLong(gap));
    }
    
  }
  
  public static class DateRangeEndpointCalculator extends RangeEndpointCalculator<Date> {
    private final Date now;
    public DateRangeEndpointCalculator(final RangeFacetRequest request, final Date now) { 
      super(request); 
      this.now = now;
      if (! (field.getType() instanceof TrieDateField) ) {
        throw new IllegalArgumentException("SchemaField must use field type extending TrieDateField");
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
}

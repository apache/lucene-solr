/**
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

package org.apache.solr.common.params;

import org.apache.solr.common.SolrException;

import java.util.EnumSet;

/**
 * Facet parameters
 */
public interface FacetParams {

  /**
   * Should facet counts be calculated?
   */
  public static final String FACET = "facet";

  /** What method should be used to do the faceting */
  public static final String FACET_METHOD = FACET + ".method";

  /** Value for FACET_METHOD param to indicate that Solr should enumerate over terms
   * in a field to calculate the facet counts.
   */
  public static final String FACET_METHOD_enum = "enum";

  /** Value for FACET_METHOD param to indicate that Solr should enumerate over documents
   * and count up terms by consulting an uninverted representation of the field values
   * (such as the FieldCache used for sorting).
   */
  public static final String FACET_METHOD_fc = "fc";
  
  /**
   * Any lucene formated queries the user would like to use for
   * Facet Constraint Counts (multi-value)
   */
  public static final String FACET_QUERY = FACET + ".query";
  /**
   * Any field whose terms the user wants to enumerate over for
   * Facet Constraint Counts (multi-value)
   */
  public static final String FACET_FIELD = FACET + ".field";

  /**
   * The offset into the list of facets.
   * Can be overridden on a per field basis.
   */
  public static final String FACET_OFFSET = FACET + ".offset";

  /**
   * Numeric option indicating the maximum number of facet field counts
   * be included in the response for each field - in descending order of count.
   * Can be overridden on a per field basis.
   */
  public static final String FACET_LIMIT = FACET + ".limit";

  /**
   * Numeric option indicating the minimum number of hits before a facet should
   * be included in the response.  Can be overridden on a per field basis.
   */
  public static final String FACET_MINCOUNT = FACET + ".mincount";

  /**
   * Boolean option indicating whether facet field counts of "0" should 
   * be included in the response.  Can be overridden on a per field basis.
   */
  public static final String FACET_ZEROS = FACET + ".zeros";

  /**
   * Boolean option indicating whether the response should include a 
   * facet field count for all records which have no value for the 
   * facet field. Can be overridden on a per field basis.
   */
  public static final String FACET_MISSING = FACET + ".missing";

  /**
   * String option: "count" causes facets to be sorted
   * by the count, "index" results in index order.
   */
  public static final String FACET_SORT = FACET + ".sort";

  public static final String FACET_SORT_COUNT = "count";
  public static final String FACET_SORT_COUNT_LEGACY = "true";
  public static final String FACET_SORT_INDEX = "index";
  public static final String FACET_SORT_INDEX_LEGACY = "false";

  /**
   * Only return constraints of a facet field with the given prefix.
   */
  public static final String FACET_PREFIX = FACET + ".prefix";

 /**
   * When faceting by enumerating the terms in a field,
   * only use the filterCache for terms with a df >= to this parameter.
   */
  public static final String FACET_ENUM_CACHE_MINDF = FACET + ".enum.cache.minDf";
  /**
   * Any field whose terms the user wants to enumerate over for
   * Facet Contraint Counts (multi-value)
   */
  public static final String FACET_DATE = FACET + ".date";
  /**
   * Date string indicating the starting point for a date facet range.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_DATE_START = FACET_DATE + ".start";
  /**
   * Date string indicating the endinging point for a date facet range.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_DATE_END = FACET_DATE + ".end";
  /**
   * Date Math string indicating the interval of sub-ranges for a date
   * facet range.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_DATE_GAP = FACET_DATE + ".gap";
  /**
   * Boolean indicating how counts should be computed if the range
   * between 'start' and 'end' is not evenly divisible by 'gap'.  If
   * this value is true, then all counts of ranges involving the 'end'
   * point will use the exact endpoint specified -- this includes the
   * 'between' and 'after' counts as well as the last range computed
   * using the 'gap'.  If the value is false, then 'gap' is used to
   * compute the effective endpoint closest to the 'end' param which
   * results in the range between 'start' and 'end' being evenly
   * divisible by 'gap'.
   * The default is false.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_DATE_HARD_END = FACET_DATE + ".hardend";
  /**
   * String indicating what "other" ranges should be computed for a
   * date facet range (multi-value).
   * Can be overriden on a per field basis.
   * @see FacetRangeOther
   */
  public static final String FACET_DATE_OTHER = FACET_DATE + ".other";

  /**
   * <p>
   * Multivalued string indicating what rules should be applied to determine 
   * when the the ranges generated for date faceting should be inclusive or 
   * exclusive of their end points.
   * </p>
   * <p>
   * The default value if none are specified is: [lower,upper,edge] <i>(NOTE: This is different then FACET_RANGE_INCLUDE)</i>
   * </p>
   * <p>
   * Can be overriden on a per field basis.
   * </p>
   * @see FacetRangeInclude
   * @see #FACET_RANGE_INCLUDE
   */
  public static final String FACET_DATE_INCLUDE = FACET_DATE + ".include";

  /**
   * Any numerical field whose terms the user wants to enumerate over
   * Facet Contraint Counts for selected ranges.
   */
  public static final String FACET_RANGE = FACET + ".range";
  /**
   * Number indicating the starting point for a numerical range facet.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_RANGE_START = FACET_RANGE + ".start";
  /**
   * Number indicating the ending point for a numerical range facet.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_RANGE_END = FACET_RANGE + ".end";
  /**
   * Number indicating the interval of sub-ranges for a numerical
   * facet range.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_RANGE_GAP = FACET_RANGE + ".gap";
  /**
   * Boolean indicating how counts should be computed if the range
   * between 'start' and 'end' is not evenly divisible by 'gap'.  If
   * this value is true, then all counts of ranges involving the 'end'
   * point will use the exact endpoint specified -- this includes the
   * 'between' and 'after' counts as well as the last range computed
   * using the 'gap'.  If the value is false, then 'gap' is used to
   * compute the effective endpoint closest to the 'end' param which
   * results in the range between 'start' and 'end' being evenly
   * divisible by 'gap'.
   * The default is false.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_RANGE_HARD_END = FACET_RANGE + ".hardend";
  /**
   * String indicating what "other" ranges should be computed for a
   * numerical range facet (multi-value).
   * Can be overriden on a per field basis.
   */
  public static final String FACET_RANGE_OTHER = FACET_RANGE + ".other";

  /**
   * <p>
   * Multivalued string indicating what rules should be applied to determine 
   * when the the ranges generated for numeric faceting should be inclusive or 
   * exclusive of their end points.
   * </p>
   * <p>
   * The default value if none are specified is: lower
   * </p>
   * <p>
   * Can be overriden on a per field basis.
   * </p>
   * @see FacetRangeInclude
   */
  public static final String FACET_RANGE_INCLUDE = FACET_RANGE + ".include";


  /**
   * An enumeration of the legal values for {@link #FACET_RANGE_OTHER} and {@link #FACET_DATE_OTHER} ...
   * <ul>
   * <li>before = the count of matches before the start</li>
   * <li>after = the count of matches after the end</li>
   * <li>between = the count of all matches between start and end</li>
   * <li>all = all of the above (default value)</li>
   * <li>none = no additional info requested</li>
   * </ul>
   * @see #FACET_RANGE_OTHER
   * @see #FACET_DATE_OTHER
   */
  public enum FacetRangeOther {
    BEFORE, AFTER, BETWEEN, ALL, NONE;
    @Override
    public String toString() { return super.toString().toLowerCase(); }
    public static FacetRangeOther get(String label) {
      try {
        return valueOf(label.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           label+" is not a valid type of 'other' range facet information",e);
      }
    }
  }
  
  /**
   * @deprecated Use {@link FacetRangeOther}
   */
  @Deprecated
  public enum FacetDateOther {
    BEFORE, AFTER, BETWEEN, ALL, NONE;
    @Override
    public String toString() { return super.toString().toLowerCase(); }
    public static FacetDateOther get(String label) {
      try {
        return valueOf(label.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           label+" is not a valid type of 'other' range facet information",e);
      }
    }
  }
  
  /**
   * An enumeration of the legal values for {@link #FACET_DATE_INCLUDE} and {@link #FACET_RANGE_INCLUDE}
   *
   * <ul>
   * <li>lower = all gap based ranges include their lower bound</li>
   * <li>upper = all gap based ranges include their upper bound</li>
   * <li>edge = the first and last gap ranges include their edge bounds (ie: lower 
   *     for the first one, upper for the last one) even if the corrisponding 
   *     upper/lower option is not specified
   * </li>
   * <li>outer = the BEFORE and AFTER ranges 
   *     should be inclusive of their bounds, even if the first or last ranges 
   *     already include thouse boundaries.
   * </li>
   * <li>all = shorthand for lower, upper, edge, and outer</li>
   * </ul>
   * @see #FACET_DATE_INCLUDE
   * @see #FACET_RANGE_INCLUDE
   */
  public enum FacetRangeInclude {
    ALL, LOWER, UPPER, EDGE, OUTER;
    @Override
    public String toString() { return super.toString().toLowerCase(); }
    public static FacetRangeInclude get(String label) {
      try {
        return valueOf(label.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           label+" is not a valid type of for range 'include' information",e);
      }
    }
    /**
     * Convinience method for parsing the param value according to the 
     * correct semantics and applying the default of "LOWER"
     */
    public static EnumSet<FacetRangeInclude> parseParam(final String[] param) {
      // short circut for default behavior
      if (null == param || 0 == param.length ) 
        return EnumSet.of(LOWER);

      // build up set containing whatever is specified
      final EnumSet<FacetRangeInclude> include 
        = EnumSet.noneOf(FacetRangeInclude.class);
      for (final String o : param) {
        include.add(FacetRangeInclude.get(o));
      }

      // if set contains all, then we're back to short circuting
      if (include.contains(FacetRangeInclude.ALL)) 
        return EnumSet.allOf(FacetRangeInclude.class);

      // use whatever we've got.
      return include;
    }
  }

}


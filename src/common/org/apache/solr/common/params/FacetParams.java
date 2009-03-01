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
   * @see FacetDateOther
   */
  public static final String FACET_DATE_OTHER = FACET_DATE + ".other";

    /**
   * An enumeration of the legal values for FACET_DATE_OTHER...
   * <ul>
   * <li>before = the count of matches before the start date</li>
   * <li>after = the count of matches after the end date</li>
   * <li>between = the count of all matches between start and end</li>
   * <li>all = all of the above (default value)</li>
   * <li>none = no additional info requested</li>
   * </ul>
   * @see #FACET_DATE_OTHER
   */
  public enum FacetDateOther {
    BEFORE, AFTER, BETWEEN, ALL, NONE;
    public String toString() { return super.toString().toLowerCase(); }
    public static FacetDateOther get(String label) {
      try {
        return valueOf(label.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           label+" is not a valid type of 'other' date facet information",e);
      }
    }
  }
  

}


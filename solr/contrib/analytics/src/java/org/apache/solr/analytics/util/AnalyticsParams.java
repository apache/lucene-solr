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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.search.function.ConcatStringFunction;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public interface AnalyticsParams {
  // Full length Analytics Params
  public static final String ANALYTICS = "olap";
  
  public static final String REQUEST = "o|olap";

  public static final String EXPRESSION = "s|stat|statistic";
  public static final String HIDDEN_EXPRESSION = "hs|hiddenstat|hiddenstatistic";

  public static final String FIELD_FACET = "ff|fieldfacet";
  public static final String LIMIT = "l|limit";
  public static final String OFFSET = "off|offset";
  public static final String HIDDEN = "h|hidden";
  public static final String SHOW_MISSING = "sm|showmissing";
  public static final String SORT_STATISTIC ="ss|sortstat|sortstatistic";
  public static final String SORT_DIRECTION ="sd|sortdirection";
  
  public static final String RANGE_FACET = "rf|rangefacet";
  public static final String START = "st|start";
  public static final String END = "e|end";
  public static final String GAP = "g|gap";
  public static final String HARDEND = "he|hardend";
  public static final String INCLUDE_BOUNDARY = "ib|includebound";
  public static final String OTHER_RANGE = "or|otherrange";
  
  public static final String QUERY_FACET = "qf|queryfacet";
  public static final String DEPENDENCY = "d|dependecy";
  public static final String QUERY = "q|query";
  
  //Defaults
  public static final boolean DEFAULT_ABBREVIATE_PREFIX = true;
  public static final String DEFAULT_SORT_DIRECTION = "ascending";
  public static final int DEFAULT_LIMIT = -1;
  public static final boolean DEFAULT_HIDDEN = false;
  public static final boolean DEFAULT_HARDEND = false;
  public static final boolean DEFAULT_SHOW_MISSING = false;
  public static final FacetRangeInclude DEFAULT_INCLUDE = FacetRangeInclude.LOWER;
  public static final FacetRangeOther DEFAULT_OTHER = FacetRangeOther.NONE;
  
  // Statistic Function Names (Cannot share names with ValueSource & Expression Functions)
  public static final String STAT_COUNT = "count";
  public static final String STAT_MISSING = "missing";
  public static final String STAT_SUM = "sum";
  public static final String STAT_SUM_OF_SQUARES = "sumofsquares";
  public static final String STAT_STANDARD_DEVIATION = "stddev";
  public static final String STAT_MEAN = "mean";
  public static final String STAT_UNIQUE = "unique";
  public static final String STAT_MEDIAN = "median";
  public static final String STAT_PERCENTILE = "percentile";
  public static final String STAT_MIN = "min";
  public static final String STAT_MAX = "max";
  
  public static final List<String> ALL_STAT_LIST = Collections.unmodifiableList(Lists.newArrayList(STAT_COUNT, STAT_MISSING, STAT_SUM, STAT_SUM_OF_SQUARES, STAT_STANDARD_DEVIATION, STAT_MEAN, STAT_UNIQUE, STAT_MEDIAN, STAT_PERCENTILE,STAT_MIN,STAT_MAX));
  public static final Set<String> ALL_STAT_SET = Collections.unmodifiableSet(Sets.newLinkedHashSet(ALL_STAT_LIST));

  // ValueSource & Expression Function Names (Cannot share names with Statistic Functions)
  // No specific type
  final static String FILTER = "filter";
  final static String RESULT = "result";
  final static String QUERY_RESULT = "qresult";
  
  // Numbers
  final static String CONSTANT_NUMBER = "const_num";
  final static String NEGATE = "neg";
  final static String ABSOLUTE_VALUE = "abs";
  final static String LOG = "log";
  final static String ADD = "add";
  final static String MULTIPLY = "mult";
  final static String DIVIDE = "div";
  final static String POWER = "pow";
  public static final Set<String> NUMERIC_OPERATION_SET = Collections.unmodifiableSet(Sets.newLinkedHashSet(Lists.newArrayList(CONSTANT_NUMBER,NEGATE,ABSOLUTE_VALUE,LOG,ADD,MULTIPLY,DIVIDE,POWER)));
  
  // Dates
  final static String CONSTANT_DATE = "const_date";
  final static String DATE_MATH = "date_math";
  public static final Set<String> DATE_OPERATION_SET = Collections.unmodifiableSet(Sets.newLinkedHashSet(Lists.newArrayList(CONSTANT_DATE,DATE_MATH)));
  
  //Strings
  final static String CONSTANT_STRING = "const_str";
  final static String REVERSE = "rev";
  final static String CONCATENATE = ConcatStringFunction.NAME;
  public static final Set<String> STRING_OPERATION_SET = Collections.unmodifiableSet(Sets.newLinkedHashSet(Lists.newArrayList(CONSTANT_STRING,REVERSE,CONCATENATE)));
  
  // Field Source Wrappers
}

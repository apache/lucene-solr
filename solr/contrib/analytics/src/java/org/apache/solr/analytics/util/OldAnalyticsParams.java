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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsExpressionSortRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsRangeFacetRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsSortRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsValueFacetRequest;
import org.apache.solr.analytics.function.mapping.FillMissingFunction;

/**
 * Specifies the format of the old olap-style analytics requests.
 */
public interface OldAnalyticsParams {
  // Old request language
  public static final String OLD_ANALYTICS = "olap";

  public static final String OLD_PREFIX = "o|olap";

  public static final String OLD_EXPRESSION = "s|stat|statistic";

  public static class OldRequest {
    public String name;
    public Map<String,String> expressions = new HashMap<>();
    public Map<String,OldFieldFacet> fieldFacets = new HashMap<>();
    public Map<String,OldRangeFacet> rangeFacets = new HashMap<>();
    public Map<String,OldQueryFacet> queryFacets = new HashMap<>();
  }

  public static final String FIELD_FACET = "(?:ff|fieldfacet)";
  public static final String VALUE_FACET = "(?:vf|valuefacet)";
  public static final String LIMIT = "(?:l|limit)";
  public static final String OFFSET = "(?:off|offset)";
  public static final String SHOW_MISSING = "(?:sm|showmissing)";
  public static final String SORT_EXPRESSION ="(?:se|sortexpr|sortexpression)";
  public static final String OLAP_SORT_EXPRESSION ="(?:ss|sortstat|sortstatistic)";
  public static final String SORT_DIRECTION ="(?:sd|sortdirection)";

  public static class OldFieldFacet {
    public String field;
    public String showMissing;
    public String limit;
    public String offset;
    public String sortExpr;
    public String sortDir;
  }

  public static class FieldFacetParamParser {
    public static String regexParamList = LIMIT + "|" + OFFSET + "|" + SHOW_MISSING + "|" + OLAP_SORT_EXPRESSION + "|" + SORT_DIRECTION;

    private static Predicate<String> isLimit = Pattern.compile("^" + LIMIT + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isOffset = Pattern.compile("^" + OFFSET + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isShowMissing = Pattern.compile("^" + SHOW_MISSING + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isSortExpr = Pattern.compile("^" + OLAP_SORT_EXPRESSION + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isSortDir = Pattern.compile("^" + SORT_DIRECTION + "$", Pattern.CASE_INSENSITIVE).asPredicate();

    public static void applyParam(AnalyticsValueFacetRequest facet, String param, String value) {
      if (isLimit.test(param)) {
        getSort(facet).limit = Integer.parseInt(value);
      } else if (isOffset.test(param)) {
        getSort(facet).offset = Integer.parseInt(value);
      } else if (isShowMissing.test(param)) {
        facet.expression = FillMissingFunction.name + "(" + facet.expression + ",\"(MISSING)\")";
      } else if (isSortExpr.test(param)) {
        AnalyticsSortRequest sort = getSort(facet);
        AnalyticsExpressionSortRequest criterion;
        if (sort.criteria.size() == 0) {
          criterion = new AnalyticsExpressionSortRequest();
          sort.criteria.add(criterion);
        } else {
          criterion = (AnalyticsExpressionSortRequest) sort.criteria.get(0);
        }
        criterion.expression = value;
      } else if (isSortDir.test(param)) {
        AnalyticsSortRequest sort = getSort(facet);
        AnalyticsExpressionSortRequest criterion;
        if (sort.criteria.size() == 0) {
          criterion = new AnalyticsExpressionSortRequest();
          sort.criteria.add(criterion);
        } else {
          criterion = (AnalyticsExpressionSortRequest) sort.criteria.get(0);
        }
        criterion.direction = value;
      }
    }

    public static AnalyticsSortRequest getSort(AnalyticsValueFacetRequest facet) {
      if (facet.sort == null) {
        facet.sort = new AnalyticsSortRequest();
        facet.sort.criteria = new ArrayList<>();
      }
      return facet.sort;
    }
  }

  public static final String RANGE_FACET = "(?:rf|rangefacet)";
  public static final String START = "(?:st|start)";
  public static final String END = "(?:e|end)";
  public static final String GAP = "(?:g|gap)";
  public static final String HARDEND = "(?:he|hardend)";
  public static final String INCLUDE_BOUNDARY = "(?:ib|includebound)";
  public static final String OTHER_RANGE = "(?:or|otherrange)";

  public static class OldRangeFacet {
    public String field;
    public String start;
    public String end;
    public String gaps;
    public String hardend;
    public String[] include;
    public String[] others;
  }

  public static class RangeFacetParamParser {
    public static String regexParamList = START + "|" + END + "|" + GAP + "|" + HARDEND + "|" + INCLUDE_BOUNDARY + "|" + OTHER_RANGE;

    private static Predicate<String> isStart = Pattern.compile("^" + START + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isEnd = Pattern.compile("^" + END + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isGap = Pattern.compile("^" + GAP + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isHardEnd = Pattern.compile("^" + HARDEND + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isTrue = Pattern.compile("^t|true$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isFalse = Pattern.compile("^f|false$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isInclude = Pattern.compile("^" + INCLUDE_BOUNDARY + "$", Pattern.CASE_INSENSITIVE).asPredicate();
    private static Predicate<String> isOther = Pattern.compile("^" + OTHER_RANGE + "$", Pattern.CASE_INSENSITIVE).asPredicate();

    public static void applyParam(AnalyticsRangeFacetRequest facet, String param, String[] values) {
      if (isStart.test(param)) {
        facet.start = values[0];
      } else if (isEnd.test(param)) {
        facet.end = values[0];
      } else if (isGap.test(param)) {
        facet.gaps = Arrays.asList(values[0].split(","));
      } else if (isHardEnd.test(param)) {
        if (isTrue.test(values[0])) {
          facet.hardend = true;
        } else if (isFalse.test(values[0])) {
          facet.hardend = false;
        }
      } else if (isInclude.test(param)) {
        facet.include = Arrays.asList(values);
      } else if (isOther.test(param)) {
        facet.others = Arrays.asList(values);
      }
    }
  }

  public static class OldQueryFacet {
    public String name;
    public String[] queries;
  }

  public static final String QUERY_FACET = "(?:qf|queryfacet)";
  public static final String QUERY = "(?:q|query)";

  //Defaults
  public static final boolean DEFAULT_ABBREVIATE_PREFIX = true;
}

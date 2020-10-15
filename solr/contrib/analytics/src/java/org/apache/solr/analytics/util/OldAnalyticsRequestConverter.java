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

import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsGroupingRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsQueryFacetRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsRangeFacetRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsRequest;
import org.apache.solr.analytics.AnalyticsRequestParser.AnalyticsValueFacetRequest;
import org.apache.solr.common.params.SolrParams;

/**
 * Converts Analytics Requests in the old olap-style format to the new format.
 */
public class OldAnalyticsRequestConverter implements OldAnalyticsParams {
  // Old language Parsing
  private static final Pattern oldExprPattern =
      Pattern.compile("^(?:"+OLD_PREFIX+")\\.([^\\.]+)\\.(?:"+OLD_EXPRESSION+")\\.([^\\.]+)$", Pattern.CASE_INSENSITIVE);
  private static final Pattern oldFieldFacetPattern =
      Pattern.compile("^(?:"+OLD_PREFIX+")\\.([^\\.]+)\\.(?:"+FIELD_FACET+")$", Pattern.CASE_INSENSITIVE);
  private static final Pattern oldFieldFacetParamPattern =
      Pattern.compile("^(?:"+OLD_PREFIX+")\\.([^\\.]+)\\.(?:"+FIELD_FACET+")\\.(.*(?=\\.))\\.("+FieldFacetParamParser.regexParamList+")$", Pattern.CASE_INSENSITIVE);
  private static final Pattern oldRangeFacetParamPattern =
      Pattern.compile("^(?:"+OLD_PREFIX+")\\.([^\\.]+)\\.(?:"+RANGE_FACET+")\\.(.*(?=\\.))\\.("+RangeFacetParamParser.regexParamList+")$", Pattern.CASE_INSENSITIVE);
  private static final Pattern oldQueryFacetParamPattern =
      Pattern.compile("^(?:"+OLD_PREFIX+")\\.([^\\.]+)\\.(?:"+QUERY_FACET+")\\.([^\\.]+)\\.("+QUERY+")$", Pattern.CASE_INSENSITIVE);

  /**
   * Convert the old olap-style Analytics Request in the given params to
   * an analytics request string using the current format.
   *
   * @param params to find the analytics request in
   * @return an analytics request string
   */
  public static AnalyticsRequest convert(SolrParams params) {
    AnalyticsRequest request = new AnalyticsRequest();
    request.expressions = new HashMap<>();
    request.groupings = new HashMap<>();
    Iterator<String> paramsIterator = params.getParameterNamesIterator();
    while (paramsIterator.hasNext()) {
      String param = paramsIterator.next();
      CharSequence paramSequence = param.subSequence(0, param.length());
      parseParam(request, param, paramSequence, params);
    }
    return request;
  }

  private static void parseParam(AnalyticsRequest request, String param, CharSequence paramSequence, SolrParams params) {
    // Check if grouped expression
    Matcher m = oldExprPattern.matcher(paramSequence);
    if (m.matches()) {
      addExpression(request,m.group(1),m.group(2),params.get(param));
      return;
    }

    // Check if field facet parameter
    m = oldFieldFacetPattern.matcher(paramSequence);
    if (m.matches()) {
      addFieldFacets(request,m.group(1),params.getParams(param));
      return;
    }

    // Check if field facet parameter
    m = oldFieldFacetParamPattern.matcher(paramSequence);
    if (m.matches()) {
      setFieldFacetParam(request,m.group(1),m.group(2),m.group(3),params.getParams(param));
      return;
    }

    // Check if field facet parameter
    m = oldFieldFacetParamPattern.matcher(paramSequence);
    if (m.matches()) {
      setFieldFacetParam(request,m.group(1),m.group(2),m.group(3),params.getParams(param));
      return;
    }

    // Check if range facet parameter
    m = oldRangeFacetParamPattern.matcher(paramSequence);
    if (m.matches()) {
      setRangeFacetParam(request,m.group(1),m.group(2),m.group(3),params.getParams(param));
      return;
    }

    // Check if query
    m = oldQueryFacetParamPattern.matcher(paramSequence);
    if (m.matches()) {
      setQueryFacetParam(request,m.group(1),m.group(2),m.group(3),params.getParams(param));
      return;
    }
  }

  private static AnalyticsGroupingRequest getGrouping(AnalyticsRequest request, String name) {
    AnalyticsGroupingRequest grouping = request.groupings.get(name);
    if (grouping == null) {
      grouping = new AnalyticsGroupingRequest();
      grouping.expressions = new HashMap<>();
      grouping.facets = new HashMap<>();
      request.groupings.put(name, grouping);
    }
    return grouping;
  }

  private static void addFieldFacets(AnalyticsRequest request, String groupingName, String[] params) {
    AnalyticsGroupingRequest grouping = getGrouping(request, groupingName);

    for (String param : params) {
      if (!grouping.facets.containsKey(param)) {
        AnalyticsValueFacetRequest fieldFacet = new AnalyticsValueFacetRequest();
        fieldFacet.expression = param;
        grouping.facets.put(param, fieldFacet);
      }
    }
  }

  private static void setFieldFacetParam(AnalyticsRequest request, String groupingName, String field, String paramType, String[] params) {
    AnalyticsGroupingRequest grouping = getGrouping(request, groupingName);

    AnalyticsValueFacetRequest fieldFacet = (AnalyticsValueFacetRequest) grouping.facets.get(field);

    if (fieldFacet == null) {
      fieldFacet = new AnalyticsValueFacetRequest();
      fieldFacet.expression = field;
      grouping.facets.put(field, fieldFacet);
    }
    FieldFacetParamParser.applyParam(fieldFacet, paramType, params[0]);
  }

  private static void setRangeFacetParam(AnalyticsRequest request, String groupingName, String field, String paramType, String[] params) {
    AnalyticsGroupingRequest grouping = getGrouping(request, groupingName);

    AnalyticsRangeFacetRequest rangeFacet = (AnalyticsRangeFacetRequest) grouping.facets.get(field);
    if (rangeFacet == null) {
      rangeFacet = new AnalyticsRangeFacetRequest();
      rangeFacet.field = field;
      grouping.facets.put(field, rangeFacet);
    }
    RangeFacetParamParser.applyParam(rangeFacet, paramType, params);
  }

  private static void setQueryFacetParam(AnalyticsRequest request, String groupingName, String facetName, String paramType, String[] params) {
    AnalyticsGroupingRequest grouping = getGrouping(request, groupingName);

    AnalyticsQueryFacetRequest queryFacet = new AnalyticsQueryFacetRequest();
    queryFacet.queries = new HashMap<>();
    if (paramType.equals("query")||paramType.equals("q")) {
      for (String param : params) {
        queryFacet.queries.put(param, param);
      }
    }
    grouping.facets.put(facetName, queryFacet);
  }

  private static void addExpression(AnalyticsRequest request, String groupingName, String expressionName, String expression) {
    request.expressions.put(groupingName + expressionName, expression);

    getGrouping(request, groupingName).expressions.put(expressionName, expression);
  }
}

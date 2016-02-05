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
package org.apache.solr.analytics.request;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Contains the specifications of an Analytics Request, specifically a name,
 * a list of Expressions, a list of field facets, a list of range facets, a list of query facets
 * and the list of expressions and their results calculated in previous AnalyticsRequests.
 */
public class AnalyticsRequest {
  
  private String name;
  private List<ExpressionRequest> expressions;
  private Set<String> hiddenExpressions;
  private List<FieldFacetRequest> fieldFacets;
  private List<RangeFacetRequest> rangeFacets;
  private List<QueryFacetRequest> queryFacets;
  
  public AnalyticsRequest(String name) {
    this.name = name;
    expressions = new ArrayList<>();
    hiddenExpressions = new HashSet<>();
    fieldFacets = new ArrayList<>();
    rangeFacets = new ArrayList<>();
    queryFacets = new ArrayList<>();
  }
  
  public String getName() {
    return name;
  }
  
  public void setExpressions(List<ExpressionRequest> expressions) {
    this.expressions = expressions;
  }

  public void addExpression(ExpressionRequest expressionRequest) {
    expressions.add(expressionRequest);
  }
  
  public List<ExpressionRequest> getExpressions() {
    return expressions;
  }

  public void addHiddenExpression(ExpressionRequest expressionRequest) {
    expressions.add(expressionRequest);
    hiddenExpressions.add(expressionRequest.getName());
  }
  
  public Set<String> getHiddenExpressions() {
    return hiddenExpressions;
  }
  
  public void setFieldFacets(List<FieldFacetRequest> fieldFacets) {
    this.fieldFacets = fieldFacets;
  }
  
  public List<FieldFacetRequest> getFieldFacets() {
    return fieldFacets;
  }
  
  public void setRangeFacets(List<RangeFacetRequest> rangeFacets) {
    this.rangeFacets = rangeFacets;
  }
  
  public List<RangeFacetRequest> getRangeFacets() {
    return rangeFacets;
  }
  
  public void setQueryFacets(List<QueryFacetRequest> queryFacets) {
    this.queryFacets = queryFacets;
  }
  
  public List<QueryFacetRequest> getQueryFacets() {
    return queryFacets;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("<AnalyticsRequest name=" + name + ">");
    for (ExpressionRequest exp : expressions) {
      builder.append(exp.toString());
    }
    for (FieldFacetRequest facet : fieldFacets) {
      builder.append(facet.toString());
    }
    for (RangeFacetRequest facet : rangeFacets) {
      builder.append(facet.toString());
    }
    for (QueryFacetRequest facet : queryFacets) {
      builder.append(facet.toString());
    }
    builder.append("</AnalyticsRequest>");
    return builder.toString();
  }
}

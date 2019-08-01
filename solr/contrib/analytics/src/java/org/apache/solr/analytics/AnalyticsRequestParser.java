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
package org.apache.solr.analytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.solr.analytics.facet.PivotFacet;
import org.apache.solr.analytics.facet.PivotNode;
import org.apache.solr.analytics.facet.QueryFacet;
import org.apache.solr.analytics.facet.RangeFacet;
import org.apache.solr.analytics.facet.ValueFacet;
import org.apache.solr.analytics.facet.SortableFacet.FacetSortSpecification;
import org.apache.solr.analytics.facet.compare.DelegatingComparator;
import org.apache.solr.analytics.facet.compare.FacetValueComparator;
import org.apache.solr.analytics.facet.compare.FacetResultsComparator;
import org.apache.solr.analytics.value.AnalyticsValue;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.ComparableValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class to manage the parsing of new-style analytics requests.
 */
public class AnalyticsRequestParser {

  private static ObjectMapper mapper = new ObjectMapper();

  public static void init() {
    mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    mapper.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, true);
  }

  public static final String analyticsParamName = "analytics";

  private static Predicate<String> sortAscending   = acceptNames("ascending", "asc", "a");
  private static Predicate<String> sortDescending  = acceptNames("descending", "desc", "d");

  private static Predicate<String> acceptNames(String... names) {
    return Pattern.compile("^(?:" + Arrays.stream(names).reduce((a,b) -> a + "|" + b).orElse("") + ")$", Pattern.CASE_INSENSITIVE).asPredicate();
  }

  // Defaults
  public static final String DEFAULT_SORT_DIRECTION = "ascending";
  public static final int DEFAULT_OFFSET = 0;
  public static final int DEFAULT_LIMIT = -1;
  public static final boolean DEFAULT_HARDEND = false;

  @JsonInclude(Include.NON_EMPTY)
  public static class AnalyticsRequest {
    public Map<String, String> functions;
    public Map<String, String> expressions;

    public Map<String, AnalyticsGroupingRequest> groupings;
  }

  public static class AnalyticsGroupingRequest {
    public Map<String, String> expressions;

    public Map<String, AnalyticsFacetRequest> facets;
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "type"
  )
  @JsonSubTypes({
    @Type(value = AnalyticsValueFacetRequest.class, name = "value"),
    @Type(value = AnalyticsPivotFacetRequest.class, name = "pivot"),
    @Type(value = AnalyticsRangeFacetRequest.class, name = "range"),
    @Type(value = AnalyticsQueryFacetRequest.class, name = "query") }
  )
  @JsonInclude(Include.NON_EMPTY)
  public static interface AnalyticsFacetRequest { }

  @JsonTypeName("value")
  public static class AnalyticsValueFacetRequest implements AnalyticsFacetRequest {
    public String expression;
    public AnalyticsSortRequest sort;
  }

  @JsonTypeName("pivot")
  public static class AnalyticsPivotFacetRequest implements AnalyticsFacetRequest {
    public List<AnalyticsPivotRequest> pivots;
  }

  public static class AnalyticsPivotRequest {
    public String name;
    public String expression;
    public AnalyticsSortRequest sort;
  }

  @JsonInclude(Include.NON_EMPTY)
  public static class AnalyticsSortRequest {
    public List<AnalyticsSortCriteriaRequest> criteria;
    public int limit = DEFAULT_LIMIT;
    public int offset = DEFAULT_OFFSET;
  }

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "type"
  )
  @JsonSubTypes({
    @Type(value = AnalyticsExpressionSortRequest.class, name = "expression"),
    @Type(value = AnalyticsFacetValueSortRequest.class, name = "facetvalue") }
  )
  @JsonInclude(Include.NON_EMPTY)
  public static abstract class AnalyticsSortCriteriaRequest {
    public String direction;
  }

  @JsonTypeName("expression")
  public static class AnalyticsExpressionSortRequest extends AnalyticsSortCriteriaRequest {
    public String expression;
  }

  @JsonTypeName("facetvalue")
  public static class AnalyticsFacetValueSortRequest extends AnalyticsSortCriteriaRequest { }

  @JsonTypeName("range")
  public static class AnalyticsRangeFacetRequest implements AnalyticsFacetRequest {
    public String field;
    public String start;
    public String end;
    public List<String> gaps;
    public boolean hardend = DEFAULT_HARDEND;
    public List<String> include;
    public List<String> others;
  }

  @JsonTypeName("query")
  public static class AnalyticsQueryFacetRequest implements AnalyticsFacetRequest {
    public Map<String, String> queries;
  }

  /* ***************
   * Request & Groupings
   * ***************/

  public static AnalyticsRequestManager parse(AnalyticsRequest request, ExpressionFactory expressionFactory, boolean isDistribRequest) throws SolrException {
    AnalyticsRequestManager manager = constructRequest(request, expressionFactory, isDistribRequest);
    if (isDistribRequest) {
      try {
        manager.analyticsRequest = mapper.writeValueAsString(request);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
    return manager;
  }

  public static AnalyticsRequestManager parse(String rawRequest, ExpressionFactory expressionFactory, boolean isDistribRequest) throws SolrException {
    JsonParser parser;
    try {
      parser = new JsonFactory().createParser(rawRequest)
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    AnalyticsRequest request;
    try {
      request = mapper.readValue(parser, AnalyticsRequest.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    AnalyticsRequestManager manager = constructRequest(request, expressionFactory, isDistribRequest);
    if (isDistribRequest) {
      manager.analyticsRequest = rawRequest;
    }
    return manager;
  }

  private static AnalyticsRequestManager constructRequest(AnalyticsRequest request, ExpressionFactory expressionFactory, boolean isDistribRequest) throws SolrException {
    expressionFactory.startRequest();

    // Functions
    if (request.functions != null) {
      request.functions.forEach( (funcSig, retSig) -> expressionFactory.addUserDefinedVariableFunction(funcSig, retSig));
    }

    // Expressions
    Map<String,AnalyticsExpression> topLevelExpressions;
    if (request.expressions != null) {
      topLevelExpressions = constructExpressions(request.expressions, expressionFactory);
    } else {
      topLevelExpressions = new HashMap<>();
    }
    AnalyticsRequestManager manager = new AnalyticsRequestManager(expressionFactory.createReductionManager(isDistribRequest), topLevelExpressions.values());

    // Groupings
    if (request.groupings != null) {
      request.groupings.forEach( (name, grouping) -> {
        manager.addGrouping(constructGrouping(name, grouping, expressionFactory, isDistribRequest));
      });
    }
    return manager;
  }

  private static AnalyticsGroupingManager constructGrouping(String name, AnalyticsGroupingRequest grouping, ExpressionFactory expressionFactory, boolean isDistribRequest) throws SolrException {
    expressionFactory.startGrouping();

    // Expressions
    if (grouping.expressions == null || grouping.expressions.size() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Groupings must contain at least one expression, '" + name + "' has none.");
    }

    Map<String,AnalyticsExpression> expressions = constructExpressions(grouping.expressions, expressionFactory);
    AnalyticsGroupingManager manager = new AnalyticsGroupingManager(name,
                                                                    expressionFactory.createGroupingReductionManager(isDistribRequest),
                                                                    expressions.values());

    if (grouping.facets == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Groupings must contain at least one facet, '" + name + "' has none.");
    }
    // Parse the facets
    grouping.facets.forEach( (facetName, facet) -> {
      if (facet instanceof AnalyticsValueFacetRequest) {
        manager.addFacet(constructValueFacet(facetName, (AnalyticsValueFacetRequest) facet, expressionFactory, expressions));
      } else if (facet instanceof AnalyticsPivotFacetRequest) {
        manager.addFacet(constructPivotFacet(facetName, (AnalyticsPivotFacetRequest) facet, expressionFactory, expressions));
      } else if (facet instanceof AnalyticsRangeFacetRequest) {
        manager.addFacet(constructRangeFacet(facetName, (AnalyticsRangeFacetRequest) facet, expressionFactory.getSchema()));
      } else if (facet instanceof AnalyticsQueryFacetRequest) {
        manager.addFacet(constructQueryFacet(facetName, (AnalyticsQueryFacetRequest) facet));
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The facet type, '" + facet.getClass().toString() + "' in "
            + "grouping '" + name + "' is not a valid type of facet");
      }
    });

    return manager;
  }

  /* ***************
   * Expression & Functions
   * ***************/

  private static Map<String, AnalyticsExpression> constructExpressions(Map<String, String> rawExpressions, ExpressionFactory expressionFactory) throws SolrException {
    Map<String, AnalyticsExpression> expressions = new HashMap<>();
    rawExpressions.forEach( (name, expression) -> {
      AnalyticsValueStream exprVal = expressionFactory.createExpression(expression);
      if (exprVal instanceof AnalyticsValue) {
        if (exprVal.getExpressionType().isReduced()) {
          expressions.put(name, (new AnalyticsExpression(name, (AnalyticsValue)exprVal)));
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST,"Top-level expressions must be reduced, the '" + name + "' expression is not.");
        }
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Top-level expressions must be single-valued, the '" + name + "' expression is not.");
      }
    });
    return expressions;
  }

  /* ***************
   * FACETS
   * ***************/

  /*
   * Value Facets
   */

  private static ValueFacet constructValueFacet(String name, AnalyticsValueFacetRequest facetRequest, ExpressionFactory expressionFactory, Map<String, AnalyticsExpression> expressions) throws SolrException {
    if (facetRequest.expression == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Value Facets must contain a mapping expression to facet over, '" + name + "' has none.");
    }

    // The second parameter must be a mapping expression
    AnalyticsValueStream expr = expressionFactory.createExpression(facetRequest.expression);
    if (!expr.getExpressionType().isUnreduced()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Value Facet expressions must be mapping expressions, "
          + "the following expression in value facet '" + name + "' contains a reduction: " + facetRequest.expression);
    }
    if (!(expr instanceof StringValueStream)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Value Facet expressions must be castable to string expressions, "
          + "the following expression in value facet '" + name + "' is not: " + facetRequest.expression);
    }

    ValueFacet facet = new ValueFacet(name, (StringValueStream)expr);

    // Check if the value facet is sorted
    if (facetRequest.sort != null) {
      facet.setSort(constructSort(facetRequest.sort, expressions));
    }
    return facet;
  }

  /*
   * Pivot Facets
   */

  private static PivotFacet constructPivotFacet(String name, AnalyticsPivotFacetRequest facetRequest, ExpressionFactory expressionFactory, Map<String, AnalyticsExpression> expressions) throws SolrException {
    PivotNode<?> topPivot = null;

    // Pivots
    if (facetRequest.pivots == null || facetRequest.pivots.size() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Pivot Facets must contain at least one pivot to facet over, '" + name + "' has none.");
    }

    ListIterator<AnalyticsPivotRequest> iter = facetRequest.pivots.listIterator(facetRequest.pivots.size());
    while (iter.hasPrevious()) {
      topPivot = constructPivot(iter.previous(), topPivot, expressionFactory, expressions);
    }

    return new PivotFacet(name, topPivot);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PivotNode<?> constructPivot(AnalyticsPivotRequest pivotRequest,
                                      PivotNode<?> childPivot,
                                      ExpressionFactory expressionFactory,
                                      Map<String, AnalyticsExpression> expressions) throws SolrException {
    if (pivotRequest.name == null || pivotRequest.name.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Pivots must have a name.");
    }
    if (pivotRequest.expression == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Pivots must have an expression to facet over, '" + pivotRequest.name + "' does not.");
    }

    // The second parameter must be a mapping expression
    AnalyticsValueStream expr = expressionFactory.createExpression(pivotRequest.expression);
    if (!expr.getExpressionType().isUnreduced()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Pivot expressions must be mapping expressions, "
          + "the following expression in pivot '" + pivotRequest.name + "' contains a reduction: " + pivotRequest.expression);
    }
    if (!(expr instanceof StringValueStream)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Pivot expressions must be castable to string expressions, "
          + "the following expression in pivot '" + pivotRequest.name + "' is not: '" + pivotRequest.expression);
    }

    PivotNode<?> pivot;
    if (childPivot == null) {
      pivot = new PivotNode.PivotLeaf(pivotRequest.name, (StringValueStream)expr);
    } else {
      pivot = new PivotNode.PivotBranch(pivotRequest.name, (StringValueStream)expr, childPivot);
    }

    // Check if the pivot is sorted
    if (pivotRequest.sort != null) {
      pivot.setSort(constructSort(pivotRequest.sort, expressions));
    }
    return pivot;
  }

  /*
   * Range Facets
   */

  private static RangeFacet constructRangeFacet(String name, AnalyticsRangeFacetRequest facetRequest, IndexSchema schema) throws SolrException {
    if (facetRequest.field == null || facetRequest.field.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Range Facets must specify a field to facet over, '" +name + "' does not.");
    }
    SchemaField field = schema.getFieldOrNull(facetRequest.field);
    if (field == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Range Facets must have a valid field as the second parameter. The '" + name + "' facet "
          + "tries to facet over the non-existent field: " + facetRequest.field);
    }

    if (facetRequest.start == null || facetRequest.start.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Range Facets must specify a start value, '" +name + "' does not.");
    }
    if (facetRequest.end == null || facetRequest.end.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Range Facets must specify a end value, '" +name + "' does not.");
    }
    if (facetRequest.gaps == null || facetRequest.gaps.size() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Range Facets must specify a gap or list of gaps to determine facet buckets, '" +name + "' does not.");
    }
    RangeFacet facet = new RangeFacet(name, field, facetRequest.start, facetRequest.end, facetRequest.gaps);

    facet.setHardEnd(facetRequest.hardend);

    if (facetRequest.include != null && facetRequest.include.size() > 0) {
      facet.setInclude(constructInclude(facetRequest.include));
    }
    if (facetRequest.others != null && facetRequest.others.size() > 0) {
      facet.setOthers(constructOthers(facetRequest.others, name));
    }
    return facet;
  }

  private static EnumSet<FacetRangeInclude> constructInclude(List<String> includes) throws SolrException {
    return FacetRangeInclude.parseParam(includes.toArray(new String[includes.size()]));
  }

  private static EnumSet<FacetRangeOther> constructOthers(List<String> othersRequest, String facetName) throws SolrException {
    EnumSet<FacetRangeOther> others = EnumSet.noneOf(FacetRangeOther.class);
    for (String rawOther : othersRequest) {
      if (!others.add(FacetRangeOther.get(rawOther))) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Duplicate include value '" + rawOther + "' found in range facet '" + facetName + "'");
      }
    }
    if (others.contains(FacetRangeOther.NONE)) {
      if (others.size() > 1) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Include value 'NONE' is used with other includes in a range facet '" + facetName + "'. "
            + "If 'NONE' is used, it must be the only include.");
      }
      return EnumSet.noneOf(FacetRangeOther.class);
    }
    if (others.contains(FacetRangeOther.ALL)) {
      if (others.size() > 1) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Include value 'ALL' is used with other includes in a range facet '" + facetName + "'. "
            + "If 'ALL' is used, it must be the only include.");
      }
      return EnumSet.of(FacetRangeOther.BEFORE, FacetRangeOther.BETWEEN, FacetRangeOther.AFTER);
    }
    return others;
  }

  /*
   * Query Facets
   */

  private static QueryFacet constructQueryFacet(String name, AnalyticsQueryFacetRequest facetRequest) throws SolrException {
    if (facetRequest.queries == null || facetRequest.queries.size() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Query Facets must be contain at least 1 query to facet over, '" + name + "' does not.");
    }

    // The first param must be the facet name
    return new QueryFacet(name, facetRequest.queries);
  }

  /*
   * Facet Sorting
   */

  private static FacetSortSpecification constructSort(AnalyticsSortRequest sortRequest, Map<String, AnalyticsExpression> expressions) throws SolrException {
    if (sortRequest.criteria == null || sortRequest.criteria.size() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Sorts must be given at least 1 criteria.");
    }

    return new FacetSortSpecification(constructSortCriteria(sortRequest.criteria, expressions), sortRequest.limit, sortRequest.offset);
  }

  private static FacetResultsComparator constructSortCriteria(List<AnalyticsSortCriteriaRequest> criteria, Map<String, AnalyticsExpression> expressions) {
    ArrayList<FacetResultsComparator> comparators = new ArrayList<>();
    for (AnalyticsSortCriteriaRequest criterion : criteria) {
      FacetResultsComparator comparator;
      if (criterion instanceof AnalyticsExpressionSortRequest) {
        comparator = constructExpressionSortCriteria((AnalyticsExpressionSortRequest) criterion, expressions);
      } else if (criterion instanceof AnalyticsFacetValueSortRequest) {
        comparator = constructFacetValueSortCriteria((AnalyticsFacetValueSortRequest) criterion);
      } else {
        // Shouldn't happen
        throw new SolrException(ErrorCode.BAD_REQUEST,"Sort Criteria must either be expressions or facetValues, '" + criterion.getClass().getName() + "' given.");
      }
      if (criterion.direction != null && criterion.direction.length() > 0) {
        if (sortAscending.test(criterion.direction)) {
          comparator.setDirection(true);
        } else if (sortDescending.test(criterion.direction)) {
          comparator.setDirection(false);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST,"Sort direction '" + criterion.direction + " is not a recognized direction.");
        }
      }
      comparators.add(comparator);
    }
    return DelegatingComparator.joinComparators(comparators);
  }

  private static FacetResultsComparator constructExpressionSortCriteria(AnalyticsExpressionSortRequest criterion, Map<String, AnalyticsExpression> expressions) {
    if (criterion.expression == null || criterion.expression.length() == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Expression Sorts must contain an expression parameter, none given.");
    }

    AnalyticsExpression expression = expressions.get(criterion.expression);
    if (expression == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Sort Expression not defined within the grouping: " + criterion.expression);
    }
    if (!(expression.getExpression() instanceof ComparableValue)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Expression Sorts must be comparable, the following is not: " + criterion.expression);
    }
    return ((ComparableValue)expression.getExpression()).getObjectComparator(expression.getName());
  }

  private static FacetResultsComparator constructFacetValueSortCriteria(AnalyticsFacetValueSortRequest criterion) {
    return new FacetValueComparator();
  }
}

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
package org.apache.solr.handler.sql;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.sql.functions.ArrayContainsAll;
import org.apache.solr.handler.sql.functions.ArrayContainsAny;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter} relational expression in Solr.
 */
class SolrFilter extends Filter implements SolrRel {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern CALCITE_TIMESTAMP_REGEX = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
  private final RexBuilder builder;

  SolrFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == SolrRel.CONVENTION;
    assert getConvention() == child.getConvention();
    builder = child.getCluster().getRexBuilder();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public SolrFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new SolrFilter(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if (getInput() instanceof SolrAggregate) {
      HavingTranslator translator = new HavingTranslator(SolrRules.solrFieldNames(getRowType()), implementor.reverseAggMappings, builder);
      String havingPredicate = translator.translateMatch(condition);
      implementor.setHavingPredicate(havingPredicate);
    } else {
      Translator translator = new Translator(SolrRules.solrFieldNames(getRowType()), builder);
      String query = translator.translateMatch(condition);
      implementor.addQuery(query);
      implementor.setNegativeQuery(query.startsWith("-"));
    }
  }

  private static class Translator {

    protected final List<String> fieldNames;
    private final RexBuilder builder;

    Translator(List<String> fieldNames, RexBuilder builder) {
      this.fieldNames = fieldNames;
      this.builder = builder;
    }

    protected String translateMatch(RexNode condition) {
      if (log.isDebugEnabled()) {
        log.debug("translateMatch condition={} {}", condition.getKind(), condition.getClass().getName());
      }

      final SqlKind kind = condition.getKind();

      if (condition.isA(SqlKind.SEARCH)) {
        return translateSearch(condition);
      } else if (kind.belongsTo(SqlKind.COMPARISON) || kind == SqlKind.NOT) {
        return translateComparison(condition);
      } else if (condition.isA(SqlKind.AND)) {
        return translateAndOrBetween(condition);
      } else if (condition.isA(SqlKind.OR)) {
        return "(" + translateOr(condition) + ")";
      } else if (kind == SqlKind.LIKE) {
        return translateLike(condition);
      } else if (kind == SqlKind.IS_NOT_NULL || kind == SqlKind.IS_NULL) {
        return translateIsNullOrIsNotNull(condition);
      } else if (kind == SqlKind.OTHER_FUNCTION) {
        return translateCustomFunction(condition);
      } else {
        return null;
      }
    }

    protected String translateCustomFunction(RexNode condition) {
      RexCall call = (RexCall) condition;
      if (call.op instanceof ArrayContainsAll) {
        return translateArrayContainsUDF(call, "AND");
      } else if (call.op instanceof ArrayContainsAny) {
        return translateArrayContainsUDF(call, "OR");
      } else  {
        throw new RuntimeException("Custom function '" + call.op + "' not supported");
      }
    }

    private String translateArrayContainsUDF(RexCall call, String booleanOperator) {
      List<RexNode> operands = call.getOperands();
      RexInputRef fieldOperand = (RexInputRef) operands.get(0);
      String fieldName = fieldNames.get(fieldOperand.getIndex());
      RexNode valuesNode = operands.get(1);
      if (valuesNode instanceof RexLiteral) {
        return fieldName + ":\"" + ((RexLiteral) valuesNode).getValueAs(String.class) + "\"";
      } else if (valuesNode instanceof  RexCall) {
        RexCall valuesRexCall = (RexCall) operands.get(1);
        String valuesString =
                valuesRexCall.getOperands()
                        .stream()
                        .map(op -> ((RexLiteral) op).getValueAs(String.class))
                        .filter(Objects::nonNull)
                        .map(value -> "\"" + value.trim()  + "\"")
                        .collect(Collectors.joining(" " + booleanOperator + " "));
        return fieldName + ":(" + valuesString + ")";
      } {
        return null;
      }
    }

    protected String translateAndOrBetween(RexNode condition) {
      // see if this is a translated range query of greater than or equals and less than or equal on same field
      // if so, then collapse into a single range criteria, e.g. field:[gte TO lte] instead of two ranges AND'd together
      RexCall call = (RexCall) condition;
      List<RexNode> operands = call.getOperands();
      String query = null;
      if (operands.size() == 2) {
        RexNode lhs = operands.get(0);
        RexNode rhs = operands.get(1);
        if (lhs.getKind() == SqlKind.GREATER_THAN_OR_EQUAL && rhs.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
          query = translateBetween(lhs, rhs);
        } else if (lhs.getKind() == SqlKind.LESS_THAN_OR_EQUAL && rhs.getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
          // just swap the nodes
          query = translateBetween(rhs, lhs);
        }
      }
      query = (query != null ? query : translateAnd(condition));
      if (log.isDebugEnabled()) {
        log.debug("translated query match={}", query);
      }
      return "(" + query + ")";
    }

    protected String translateBetween(RexNode gteNode, RexNode lteNode) {
      Pair<String, RexLiteral> gte = getFieldValuePair(gteNode);
      Pair<String, RexLiteral> lte = getFieldValuePair(lteNode);
      String fieldName = gte.getKey();
      String query = null;
      if (fieldName.equals(lte.getKey()) && compareRexLiteral(gte.right, lte.right) < 0) {
        query = fieldName + ":[" + toSolrLiteral(gte.getValue()) + " TO " + toSolrLiteral(lte.getValue()) + "]";
      }

      return query;
    }

    @SuppressWarnings("unchecked")
    private int compareRexLiteral(final RexLiteral gte, final RexLiteral lte) {
      return gte.getValue().compareTo(lte.getValue());
    }

    protected String translateIsNullOrIsNotNull(RexNode node) {
      if (!(node instanceof RexCall)) {
        throw new AssertionError("expected RexCall for predicate but found: " + node);
      }
      RexCall call = (RexCall) node;
      List<RexNode> operands = call.getOperands();
      if (operands.size() != 1) {
        throw new AssertionError("expected 1 operand for " + node);
      }

      final RexNode left = operands.get(0);
      if (left instanceof RexInputRef) {
        String name = fieldNames.get(((RexInputRef) left).getIndex());
        SqlKind kind = node.getKind();
        return kind == SqlKind.IS_NOT_NULL ? "+" + name + ":*" : "(*:* -" + name + ":*)";
      }

      throw new AssertionError("expected field ref but found " + left);
    }

    protected String translateOr(RexNode condition) {
      List<String> ors = new ArrayList<>();
      for (RexNode node : RelOptUtil.disjunctions(condition)) {
        String orQuery = translateMatch(node);
        if (orQuery.startsWith("-")) {
          orQuery = "(*:* "+orQuery+")";
        }
        ors.add(orQuery);
      }
      return String.join(" OR ", ors);
    }

    protected String translateAnd(RexNode node0) {
      List<String> andStrings = new ArrayList<>();
      List<String> notStrings = new ArrayList<>();

      List<RexNode> ands = new ArrayList<>();
      List<RexNode> nots = new ArrayList<>();
      RelOptUtil.decomposeConjunction(node0, ands, nots);


      for (RexNode node : ands) {
        String andQuery = translateMatch(node);
        if (andQuery.startsWith("-")) {
          andQuery = "(*:* "+andQuery+")";
        }
        andStrings.add(andQuery);
      }

      String andString = String.join(" AND ", andStrings);

      if (!nots.isEmpty()) {
        for (RexNode node : nots) {
          notStrings.add(translateMatch(node));
        }
        String notString = String.join(" NOT ", notStrings);
        return "(" + andString + ") NOT (" + notString + ")";
      } else {
        return andString;
      }
    }

    protected String translateLike(RexNode like) {
      Pair<String, RexLiteral> pair = getFieldValuePair(like);
      String terms = pair.getValue().toString().trim();
      terms = terms.replace("'", "").replace('%', '*').replace('_', '?');
      boolean wrappedQuotes = false;
      if (!terms.startsWith("(") && !terms.startsWith("[") && !terms.startsWith("{")) {
        // restore the * and ? after escaping
        terms = "\"" + ClientUtils.escapeQueryChars(terms).replace("\\*", "*").replace("\\?", "?") + "\"";
        wrappedQuotes = true;
      }

      String query = pair.getKey() + ":" + terms;
      return wrappedQuotes ? "{!complexphrase}" + query : query;
    }

    protected String translateComparison(RexNode node) {
      final SqlKind kind = node.getKind();
      if (kind == SqlKind.NOT) {
        RexNode negated = ((RexCall) node).getOperands().get(0);
        return "-" + (negated.getKind() == SqlKind.LIKE ? translateLike(negated) : translateMatch(negated));
      }

      Pair<String, RexLiteral> binaryTranslated = getFieldValuePair(node);
      final String key = binaryTranslated.getKey();
      RexLiteral value = binaryTranslated.getValue();
      switch (kind) {
        case EQUALS:
          return toEqualsClause(key, value, node);
        case NOT_EQUALS:
          return "-" + toEqualsClause(key, value, node);
        case LESS_THAN:
          return "(" + key + ": [ * TO " + toSolrLiteral(value) + " })";
        case LESS_THAN_OR_EQUAL:
          return "(" + key + ": [ * TO " + toSolrLiteral(value) + " ])";
        case GREATER_THAN:
          return "(" + key + ": { " + toSolrLiteral(value) + " TO * ])";
        case GREATER_THAN_OR_EQUAL:
          return "(" + key + ": [ " + toSolrLiteral(value) + " TO * ])";
        case LIKE:
          return translateLike(node);
        case IS_NOT_NULL:
        case IS_NULL:
          return translateIsNullOrIsNotNull(node);
        default:
          throw new AssertionError("cannot translate " + node);
      }
    }

    private String toEqualsClause(String key, RexLiteral value, RexNode node) {
      SqlTypeName fieldTypeName = ((RexCall) node).getOperands().get(0).getType().getSqlTypeName();
      String terms = toSolrLiteralForEquals(value, fieldTypeName).trim();

      boolean wrappedQuotes = false;
      if (!terms.startsWith("(") && !terms.startsWith("[") && !terms.startsWith("{")) {
        terms = "\"" + ClientUtils.escapeQueryChars(terms) + "\"";
        wrappedQuotes = true;
      }

      String clause = key + ":" + terms;
      if (terms.contains("*") && wrappedQuotes) {
        clause = "{!complexphrase}" + clause;
      }

      return clause;
    }

    // translate to a literal string value for Solr queries, such as translating a
    // Calcite timestamp value into an ISO-8601 formatted timestamp that Solr likes
    private String toSolrLiteral(RexLiteral literal) {
      Object value2 = literal.getValue2();
      SqlTypeName typeName = literal.getTypeName();
      final String solrLiteral;
      if (value2 instanceof Long && (typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
        // return as an ISO-8601 timestamp
        solrLiteral = Instant.ofEpochMilli((Long) value2).toString();
      } else {
        solrLiteral = value2.toString();
      }
      return solrLiteral;
    }

    // special case handling for expressions like: WHERE timestamp = '2021-06-04 04:00:00'
    // Calcite passes the right hand side as a string instead of as a Long
    private String toSolrLiteralForEquals(RexLiteral literal, SqlTypeName fieldTypeName) {
      Object value2 = literal.getValue2();
      final String solrLiteral;
      // oddly, for = criteria with a timestamp field, Calcite passes us a String instead of a Long as it does with other operators like >
      if (value2 instanceof String && fieldTypeName == SqlTypeName.TIMESTAMP && CALCITE_TIMESTAMP_REGEX.matcher((String) value2).matches()) {
        String timestamp = ((String) value2).replace(' ', 'T').replace("'", "");
        if (Character.isDigit(timestamp.charAt(timestamp.length() - 1))) {
          timestamp += "Z";
        }
        solrLiteral = timestamp;
      } else {
        solrLiteral = toSolrLiteral(literal);
      }
      return solrLiteral;
    }

    protected Pair<String, RexLiteral> getFieldValuePair(RexNode node) {
      if (!(node instanceof RexCall)) {
        throw new AssertionError("expected RexCall for predicate but found: " + node);
      }

      RexCall call = (RexCall) node;
      Pair<String, RexLiteral> binaryTranslated = call.getOperands().size() == 2 ? translateBinary(call) : null;
      if (binaryTranslated == null) {
        throw new AssertionError("unsupported predicate expression: " + node);
      }

      return binaryTranslated;
    }

    /**
     * Translates a call to a binary operator, reversing arguments if necessary.
     */
    protected Pair<String, RexLiteral> translateBinary(RexCall call) {
      List<RexNode> operands = call.getOperands();
      if (operands.size() != 2) {
        throw new AssertionError("Invalid number of arguments - " + operands.size());
      }
      final RexNode left = operands.get(0);
      final RexNode right = operands.get(1);
      final Pair<String, RexLiteral> a = translateBinary2(left, right);
      if (a != null) {
        return a;
      }

      // we can swap these if doing an equals / not equals
      if (call.op.kind == SqlKind.EQUALS || call.op.kind == SqlKind.NOT_EQUALS) {
        final Pair<String, RexLiteral> b = translateBinary2(right, left);
        if (b != null) {
          return b;
        }
      }

      if (left.getKind() == SqlKind.CAST && right.getKind() == SqlKind.CAST) {
        return translateBinary2(((RexCall) left).operands.get(0), ((RexCall) right).operands.get(0));
      }

      // for WHERE clause like: pdatex >= '2021-07-13T15:12:10.037Z'
      if (left.getKind() == SqlKind.INPUT_REF && right.getKind() == SqlKind.CAST) {
        final RexCall cast = ((RexCall) right);
        if (cast.operands.size() == 1 && cast.operands.get(0).getKind() == SqlKind.LITERAL) {
          return translateBinary2(left, cast.operands.get(0));
        }
      }

      throw new AssertionError("cannot translate call " + call);
    }

    /**
     * Translates a call to a binary operator. Returns whether successful.
     */
    protected Pair<String, RexLiteral> translateBinary2(RexNode left, RexNode right) {
      if (log.isDebugEnabled()) {
        log.debug("translateBinary2 left={} right={}", left, right);
      }
      if (right.getKind() != SqlKind.LITERAL) {
        if (log.isDebugEnabled()) {
          log.debug("right != SqlKind.LITERAL, return null");
        }
        return null;
      }

      final RexLiteral rightLiteral = (RexLiteral) right;
      switch (left.getKind()) {
        case INPUT_REF:
          final RexInputRef left1 = (RexInputRef) left;
          String name = fieldNames.get(left1.getIndex());
          return new Pair<>(name, rightLiteral);
        case CAST:
          return translateBinary2(((RexCall) left).operands.get(0), right);
//        case OTHER_FUNCTION:
//          String itemName = SolrRules.isItem((RexCall) left);
//          if (itemName != null) {
//            return translateOp2(op, itemName, rightLiteral);
//          }
        default:
          return null;
      }
    }

    /**
     * A search node can be an IN or NOT IN clause or a BETWEEN
     */
    protected String translateSearch(RexNode condition) {
      final String fieldName = getSolrFieldName(condition);

      RexCall expanded = (RexCall) RexUtil.expandSearch(builder, null, condition);
      final RexNode peekAt0 = !expanded.operands.isEmpty() ? expanded.operands.get(0) : null;
      if (expanded.op.kind == SqlKind.AND) {
        // See if NOT IN was translated into a big AND not
        if (peekAt0 instanceof RexCall) {
          RexCall op0 = (RexCall) peekAt0;
          if (op0.op.kind == SqlKind.NOT_EQUALS) {
            return "*:* -" + fieldName + ":" + toOrSetOnSameField(expanded);
          }
        }
      } else if (expanded.op.kind == SqlKind.OR) {
        if (peekAt0 instanceof RexCall) {
          RexCall op0 = (RexCall) peekAt0;
          if (op0.op.kind == SqlKind.EQUALS) {
            return fieldName + ":" + toOrSetOnSameField(expanded);
          }
        }
      }

      if (expanded.getKind() != SqlKind.SEARCH) {
        // passing a search back to translateMatch would lead to infinite recursion ...
        return translateMatch(expanded);
      }

      // don't know how to handle this search!
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported search filter: " + condition);
    }

    protected String toOrSetOnSameField(RexCall search) {
      String orClause = search.operands.stream().map(n -> {
        RexCall next = (RexCall) n;
        RexLiteral lit = (RexLiteral) next.getOperands().get(1);
        return "\"" + toSolrLiteral(lit) + "\"";
      }).collect(Collectors.joining(" OR "));
      return "(" + orClause + ")";
    }

    protected String getSolrFieldName(RexNode node) {
      RexCall call = (RexCall) node;
      final RexNode left = call.getOperands().get(0);
      if (left instanceof RexInputRef) {
        return fieldNames.get(((RexInputRef) left).getIndex());
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Expected Solr field name for " + call.getKind() + " but found " + left);
    }
  }

  private static class HavingTranslator extends Translator {

    private final Map<String, String> reverseAggMappings;

    HavingTranslator(List<String> fieldNames, Map<String, String> reverseAggMappings, RexBuilder builder) {
      super(fieldNames, builder);
      this.reverseAggMappings = reverseAggMappings;
    }

    @Override
    protected String translateMatch(RexNode condition) {
      if (condition.getKind().belongsTo(SqlKind.COMPARISON)) {
        return translateComparison(condition);
      } else if (condition.isA(SqlKind.AND)) {
        return translateAnd(condition);
      } else if (condition.isA(SqlKind.OR)) {
        return translateOr(condition);
      } else {
        return null;
      }
    }

    @Override
    protected String translateOr(RexNode condition) {
      List<String> ors = new ArrayList<>();
      for (RexNode node : RelOptUtil.disjunctions(condition)) {
        ors.add(translateMatch(node));
      }
      StringBuilder builder = new StringBuilder();

      builder.append("or(");
      for (int i = 0; i < ors.size(); i++) {
        if (i > 0) {
          builder.append(",");
        }

        builder.append(ors.get(i));
      }
      builder.append(")");
      return builder.toString();
    }

    @Override
    protected String translateAnd(RexNode node0) {
      List<String> andStrings = new ArrayList<>();
      List<String> notStrings = new ArrayList<>();

      List<RexNode> ands = new ArrayList<>();
      List<RexNode> nots = new ArrayList<>();

      RelOptUtil.decomposeConjunction(node0, ands, nots);

      for (RexNode node : ands) {
        andStrings.add(translateMatch(node));
      }

      StringBuilder builder = new StringBuilder();

      builder.append("and(");
      for (int i = 0; i < andStrings.size(); i++) {
        if (i > 0) {
          builder.append(",");
        }

        builder.append(andStrings.get(i));
      }
      builder.append(")");


      if (!nots.isEmpty()) {
        for (RexNode node : nots) {
          notStrings.add(translateMatch(node));
        }

        StringBuilder notBuilder = new StringBuilder();
        for (int i = 0; i < notStrings.size(); i++) {
          if (i > 0) {
            notBuilder.append(",");
          }
          notBuilder.append("not(");
          notBuilder.append(notStrings.get(i));
          notBuilder.append(")");
        }

        return "and(" + builder.toString() + "," + notBuilder.toString() + ")";
      } else {
        return builder.toString();
      }
    }

    /**
     * Translates a call to a binary operator, reversing arguments if necessary.
     */
    @Override
    protected Pair<String, RexLiteral> translateBinary(RexCall call) {
      List<RexNode> operands = call.getOperands();
      if (operands.size() != 2) {
        throw new AssertionError("Invalid number of arguments - " + operands.size());
      }
      final RexNode left = operands.get(0);
      final RexNode right = operands.get(1);
      final Pair<String, RexLiteral> a = translateBinary2(left, right);

      if (a != null) {
        if (reverseAggMappings.containsKey(a.getKey())) {
          return new Pair<>(reverseAggMappings.get(a.getKey()), a.getValue());
        }
        return a;
      }

      if (call.op.kind == SqlKind.EQUALS || call.op.kind == SqlKind.NOT_EQUALS) {
        final Pair<String, RexLiteral> b = translateBinary2(right, left);
        if (b != null) {
          return b;
        }
      }

      throw new AssertionError("cannot translate call " + call);
    }

    @Override
    protected String translateComparison(RexNode node) {
      Pair<String, RexLiteral> binaryTranslated = getFieldValuePair(node);
      switch (node.getKind()) {
        case EQUALS:
          String terms = binaryTranslated.getValue().getValue2().toString().trim();
          return "eq(" + binaryTranslated.getKey() + "," + terms + ")";
        case NOT_EQUALS:
          return "not(eq(" + binaryTranslated.getKey() + "," + binaryTranslated.getValue() + "))";
        case LESS_THAN:
          return "lt(" + binaryTranslated.getKey() + "," + binaryTranslated.getValue() + ")";
        case LESS_THAN_OR_EQUAL:
          return "lteq(" + binaryTranslated.getKey() + "," + binaryTranslated.getValue() + ")";
        case GREATER_THAN:
          return "gt(" + binaryTranslated.getKey() + "," + binaryTranslated.getValue() + ")";
        case GREATER_THAN_OR_EQUAL:
          return "gteq(" + binaryTranslated.getKey() + "," + binaryTranslated.getValue() + ")";
        default:
          throw new AssertionError("cannot translate " + node);
      }
    }
  }
}

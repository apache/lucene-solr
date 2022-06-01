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
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
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
import org.apache.solr.common.StringUtils;
import org.apache.solr.handler.sql.functions.ArrayContainsAll;
import org.apache.solr.handler.sql.functions.ArrayContainsAny;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter} relational expression in Solr.
 */
class SolrFilter extends Filter implements SolrRel {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern CALCITE_TIMESTAMP_REGEX =
      Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?$");
  private static final Pattern CALCITE_DATE_ONLY_REGEX = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

  private static final class AndClause {
    boolean isBetween;
    String query;

    AndClause(String query, boolean isBetween) {
      this.query = query;
      this.isBetween = isBetween;
    }

    String toQuery() {
      return "(" + query + ")";
    }
  }

  private final RexBuilder builder;

  SolrFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
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
      HavingTranslator translator =
          new HavingTranslator(getRowType(), implementor.reverseAggMappings, builder);
      String havingPredicate = translator.translateMatch(condition);
      implementor.setHavingPredicate(havingPredicate);
    } else {
      Translator translator = new Translator(getRowType(), builder);
      String query = translator.translateMatch(condition);
      implementor.addQuery(query);
      implementor.setNegativeQuery(query.startsWith("-"));
    }
  }

  private static class Translator {

    protected final RelDataType rowType;
    protected final List<String> fieldNames;
    private final RexBuilder builder;

    Translator(RelDataType rowType, RexBuilder builder) {
      this.rowType = rowType;
      this.fieldNames = SolrRules.solrFieldNames(rowType);
      this.builder = builder;
    }

    protected RelDataType getFieldType(String field) {
      RelDataTypeField f = rowType.getField(field, true, false);
      return f != null ? f.getType() : null;
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
        return translateAndOrBetween(condition, false).toQuery();
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
      } else {
        throw new RuntimeException("Custom function '" + call.op + "' not supported");
      }
    }

    private String translateArrayContainsUDF(RexCall call, String booleanOperator) {
      List<RexNode> operands = call.getOperands();
      RexInputRef fieldOperand = (RexInputRef) operands.get(0);
      String fieldName = fieldNames.get(fieldOperand.getIndex());
      RexNode valuesNode = operands.get(1);
      if (valuesNode instanceof RexLiteral) {
        String literal = toSolrLiteral(fieldName, (RexLiteral) valuesNode);
        if (!StringUtils.isEmpty(literal)) {
          return fieldName + ":\"" + ClientUtils.escapeQueryChars(literal.trim()) + "\"";
        } else {
          return null;
        }
      } else if (valuesNode instanceof RexCall) {
        RexCall valuesRexCall = (RexCall) operands.get(1);
        String valuesString =
            valuesRexCall.getOperands().stream()
                .map(op -> toSolrLiteral(fieldName, (RexLiteral) op))
                .filter(value -> !StringUtils.isEmpty(value))
                .map(value -> "\"" + ClientUtils.escapeQueryChars(value.trim()) + "\"")
                .collect(Collectors.joining(" " + booleanOperator + " "));
        return fieldName + ":(" + valuesString + ")";
      }
      {
        return null;
      }
    }

    protected AndClause translateAndOrBetween(RexNode condition, boolean isNegated) {
      // see if this is a translated range query of greater than or equals and less than or equal on
      // same field if so, then collapse into a single range criteria, e.g. field:[gte TO lte]
      // instead of two ranges AND'd together
      RexCall call = (RexCall) condition;
      List<RexNode> operands = call.getOperands();
      String query = null;
      boolean isBetween = false;
      if (operands.size() == 2) {
        RexNode lhs = operands.get(0);
        RexNode rhs = operands.get(1);
        if (lhs.getKind() == SqlKind.GREATER_THAN_OR_EQUAL
            && rhs.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
          query = translateBetween(lhs, rhs, isNegated);
          isBetween = true;
        } else if (lhs.getKind() == SqlKind.LESS_THAN_OR_EQUAL
            && rhs.getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
          // just swap the nodes
          query = translateBetween(rhs, lhs, isNegated);
          isBetween = true;
        }
      }

      if (query == null) {
        query = translateAnd(condition);
      }

      if (log.isDebugEnabled()) {
        log.debug("translated query match={}", query);
      }

      return new AndClause(query, isBetween);
    }

    protected String translateBetween(RexNode gteNode, RexNode lteNode, boolean isNegated) {
      Pair<String, RexLiteral> gte = getFieldValuePair(gteNode);
      Pair<String, RexLiteral> lte = getFieldValuePair(lteNode);
      String fieldName = gte.getKey();
      String query = null;
      if (fieldName.equals(lte.getKey()) && compareRexLiteral(gte.right, lte.right) < 0) {
        if (isNegated) {
          // we want the values outside the bounds of the range, so use an OR with non-inclusive
          // bounds
          query =
              fieldName
                  + ":[* TO "
                  + toSolrLiteral(fieldName, gte.getValue())
                  + "} OR "
                  + fieldName
                  + ":{"
                  + toSolrLiteral(fieldName, lte.getValue())
                  + " TO *]";
        } else {
          query =
              fieldName
                  + ":["
                  + toSolrLiteral(fieldName, gte.getValue())
                  + " TO "
                  + toSolrLiteral(fieldName, lte.getValue())
                  + "]";
        }
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
          orQuery = "(*:* " + orQuery + ")";
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
          andQuery = "(*:* " + andQuery + ")";
        }
        andStrings.add(andQuery);
      }

      if (!nots.isEmpty()) {
        for (RexNode node : nots) {
          if (node.isA(SqlKind.AND)) {
            AndClause andClause = translateAndOrBetween(node, true);
            // if the NOT BETWEEN was converted to an OR'd range with exclusive bounds,
            // just AND it as the negation has already been applied
            if (andClause.isBetween) {
              andStrings.add(andClause.toQuery());
            } else {
              notStrings.add(andClause.toQuery());
            }
          } else {
            notStrings.add(translateMatch(node));
          }
        }

        String query = "";
        if (!andStrings.isEmpty()) {
          String andString = String.join(" AND ", andStrings);
          query += "(" + andString + ")";
        }
        if (!notStrings.isEmpty()) {
          if (!query.isEmpty()) {
            query += " AND ";
          }
          for (int i = 0; i < notStrings.size(); i++) {
            if (i > 0) {
              query += " AND ";
            }
            query += " (*:* -" + notStrings.get(i) + ")";
          }
        }
        return query.trim();
      } else {
        return String.join(" AND ", andStrings);
      }
    }

    protected String translateLike(RexNode like) {
      Pair<Pair<String, RexLiteral>, Character> pairWithEscapeCharacter = getFieldValuePairWithEscapeCharacter(like);
      Pair<String, RexLiteral> pair = pairWithEscapeCharacter.getKey();
      Character escapeChar = pairWithEscapeCharacter.getValue();

      String terms = pair.getValue().toString().trim();
      terms = translateLikeTermToSolrSyntax(terms, escapeChar);

      if (!terms.startsWith("(") && !terms.startsWith("[") && !terms.startsWith("{")) {
        terms = escapeWithWildcard(terms);

        // if terms contains multiple words and one or more wildcard chars, then we need to employ the complexphrase parser
        // but that expects the terms wrapped in double-quotes, not parens
        boolean hasMultipleTerms = terms.split("\\s+").length > 1;
        if (hasMultipleTerms && (terms.contains("*") || terms.contains("?"))) {
          String quotedTerms = "\"" + terms.substring(1, terms.length() - 1) + "\"";
          String query = ClientUtils.encodeLocalParamVal(pair.getKey() + ":" + quotedTerms);
          return String.format(Locale.ROOT, "{!complexphrase v=%s}", query);
        }
      } // else treat as an embedded Solr query and pass-through

      return pair.getKey() + ":" + terms;
    }

    private String translateLikeTermToSolrSyntax(String term, Character escapeChar) {
      boolean isEscaped = false;
      StringBuilder sb = new StringBuilder();
      // Special character % and _ are escaped with escape character and single quote is escaped
      // with another single quote
      // If single quote is escaped with escape character, calcite parser fails
      for (int i = 0; i < term.length(); i++) {
        char c = term.charAt(i);
        if (!isEscaped && escapeChar != null && escapeChar == c) {
          isEscaped = true;
        } else if (c == '%' && !isEscaped) {
          sb.append('*');
        } else if (c == '_' && !isEscaped) {
          sb.append("?");
        } else if (c == '\'') {
          if (i > 0 && term.charAt(i - 1) == '\'') {
            sb.append(c);
          }
        } else {
          sb.append(c);
          if (isEscaped) isEscaped = false;
        }
      }
      return sb.toString();
    }

      protected String translateComparison(RexNode node) {
      final SqlKind kind = node.getKind();
      if (kind == SqlKind.NOT) {
        RexNode negated = ((RexCall) node).getOperands().get(0);
        if (negated.isA(SqlKind.AND)) {
          AndClause andClause = translateAndOrBetween(negated, true);
          // if the resulting andClause is a "between" then don't negate it as it's already
          // been converted to an OR'd exclusive range
          return andClause.isBetween ? andClause.toQuery() : "-" + andClause.toQuery();
        } else {
          return "-"
              + (negated.getKind() == SqlKind.LIKE
              ? translateLike(negated)
              : translateMatch(negated));
        }
      }

      Pair<String, RexLiteral> binaryTranslated = getFieldValuePair(node);
      final String key = binaryTranslated.getKey();
      RexLiteral value = binaryTranslated.getValue();
      switch (kind) {
        case EQUALS:
          return toEqualsClause(key, value);
        case NOT_EQUALS:
          return "-" + toEqualsClause(key, value);
        case LESS_THAN:
          return "(" + key + ": [ * TO " + toSolrLiteral(key, value) + " })";
        case LESS_THAN_OR_EQUAL:
          return "(" + key + ": [ * TO " + toSolrLiteral(key, value) + " ])";
        case GREATER_THAN:
          return "(" + key + ": { " + toSolrLiteral(key, value) + " TO * ])";
        case GREATER_THAN_OR_EQUAL:
          return "(" + key + ": [ " + toSolrLiteral(key, value) + " TO * ])";
        case LIKE:
          return translateLike(node);
        case IS_NOT_NULL:
        case IS_NULL:
          return translateIsNullOrIsNotNull(node);
        default:
          throw new AssertionError("cannot translate " + node);
      }
    }

    private String toEqualsClause(String key, RexLiteral value) {
      if ("".equals(key)) {
        // special handling for 1 = 0 kind of clause
        return "-*:*";
      }

      String terms = toSolrLiteral(key, value).trim();

      if (!terms.startsWith("(") && !terms.startsWith("[") && !terms.startsWith("{")) {
        if (terms.contains("*") || terms.contains("?")) {
          terms = escapeWithWildcard(terms);
        } else {
          terms = "\"" + ClientUtils.escapeQueryChars(terms) + "\"";
        }
      }

      return key + ":" + terms;
    }

    // Wrap filter criteria containing wildcard with parens and unescape the wildcards after
    // escaping protected query chars
    private String escapeWithWildcard(String terms) {
      String escaped =
              ClientUtils.escapeQueryChars(terms)
                      .replace("\\*", "*")
                      .replace("\\?", "?")
                      .replace("\\ ", " ");
      // if multiple terms, then wrap with parens
      if (escaped.split("\\s+").length > 1) {
        escaped = "(" + escaped + ")";
      }
      return escaped;
    }

    // translate to a literal string value for Solr queries, such as translating a
    // Calcite timestamp value into an ISO-8601 formatted timestamp that Solr likes
    private String toSolrLiteral(String solrField, RexLiteral literal) {
      Object value2 = literal != null ? literal.getValue2() : null;
      if (value2 == null) {
        return "";
      }

      SqlTypeName typeName = literal.getTypeName();
      String solrLiteral = null;
      if (value2 instanceof Long
          && (typeName == SqlTypeName.TIMESTAMP
          || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
        // return as an ISO-8601 timestamp
        solrLiteral = Instant.ofEpochMilli((Long) value2).toString();
      } else if (typeName == SqlTypeName.TIMESTAMP
          && value2 instanceof String
          && CALCITE_TIMESTAMP_REGEX.matcher((String) value2).matches()) {
        solrLiteral = toSolrTimestamp((String) value2);
      } else if (typeName == SqlTypeName.CHAR
          && value2 instanceof String
          && (CALCITE_TIMESTAMP_REGEX.matcher((String) value2).matches()
          || CALCITE_DATE_ONLY_REGEX.matcher((String) value2).matches())) {
        // looks like a Calcite timestamp, what type of field in Solr?
        RelDataType fieldType = getFieldType(solrField);
        if (fieldType != null && fieldType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
          solrLiteral = toSolrTimestamp((String) value2);
        }
      } else if (typeName == SqlTypeName.DECIMAL) {
        BigDecimal bigDecimal = literal.getValueAs(BigDecimal.class);
        if (bigDecimal != null) {
          solrLiteral = bigDecimal.toString();
        } else {
          solrLiteral = "";
        }
      }
      return solrLiteral != null ? solrLiteral : value2.toString();
    }

    private String toSolrTimestamp(final String ts) {
      String timestamp = ts;
      if (ts.indexOf(' ') != -1) {
        timestamp = ts.replace(' ', 'T').replace("'", "");
      } else if (ts.length() == 10) {
        timestamp = ts + "T00:00:00Z";
      }
      if (Character.isDigit(timestamp.charAt(timestamp.length() - 1))) {
        timestamp += "Z";
      }
      return timestamp;
    }

    protected Pair<Pair<String, RexLiteral>, Character> getFieldValuePairWithEscapeCharacter(RexNode node) {
      if (!(node instanceof RexCall)) {
        throw new AssertionError("expected RexCall for predicate but found: " + node);
      }
      RexCall call = (RexCall) node;
      if (call.getOperands().size() == 3) {
        RexNode escapeNode = call.getOperands().get(2);
        Character escapeChar = null;
        if (escapeNode.getKind() == SqlKind.LITERAL) {
          RexLiteral literal = (RexLiteral) escapeNode;
          if (literal.getTypeName() == SqlTypeName.CHAR) {
            escapeChar = literal.getValueAs(Character.class);
          }
        }
        return Pair.of(translateBinary2(call.getOperands().get(0), call.getOperands().get(1)), escapeChar);
      } else {
        return Pair.of(getFieldValuePair(node), null);
      }
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

      // special case for queries like WHERE 1=0 (which should match no docs)
      // this is now required since we're forcing Calcite's simplify to false
      if (left.getKind() == SqlKind.LITERAL && right.getKind() == SqlKind.LITERAL) {
        String leftLit = toSolrLiteral("", (RexLiteral) left);
        String rightLit = toSolrLiteral("", (RexLiteral) right);
        if (!leftLit.equals(rightLit)) {
          // they are equal lits ~ match no docs
          return new Pair<>("", (RexLiteral) right);
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
            return "*:* -" + fieldName + ":" + toOrSetOnSameField(fieldName, expanded);
          }
        }
      } else if (expanded.op.kind == SqlKind.OR) {
        if (peekAt0 instanceof RexCall) {
          RexCall op0 = (RexCall) peekAt0;
          if (op0.op.kind == SqlKind.EQUALS) {
            return fieldName + ":" + toOrSetOnSameField(fieldName, expanded);
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

    protected String toOrSetOnSameField(String solrField, RexCall search) {
      String orClause =
          search.operands.stream()
              .map(
                  n -> {
                    RexCall next = (RexCall) n;
                    RexLiteral lit = (RexLiteral) next.getOperands().get(1);
                    return "\"" + toSolrLiteral(solrField, lit) + "\"";
                  })
              .collect(Collectors.joining(" OR "));
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

    HavingTranslator(RelDataType rowType, Map<String, String> reverseAggMappings, RexBuilder builder) {
      super(rowType, builder);
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

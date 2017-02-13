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

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter} relational expression in Solr.
 */
class SolrFilter extends Filter implements SolrRel {
  SolrFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == SolrRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public SolrFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new SolrFilter(getCluster(), traitSet, input, condition);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if(getInput() instanceof SolrAggregate) {
      HavingTranslator translator = new HavingTranslator(SolrRules.solrFieldNames(getRowType()), implementor.reverseAggMappings);
      String havingPredicate = translator.translateMatch(condition);
      implementor.setHavingPredicate(havingPredicate);
    } else {
      Translator translator = new Translator(SolrRules.solrFieldNames(getRowType()));
      String query = translator.translateMatch(condition);
      implementor.addQuery(query);
      implementor.setNegativeQuery(translator.negativeQuery);
    }
  }

  private static class Translator {

    private final List<String> fieldNames;
    public boolean negativeQuery = true;

    Translator(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    private String translateMatch(RexNode condition) {
      if (condition.getKind().belongsTo(SqlKind.COMPARISON)) {
        return translateComparison(condition);
      } else if (condition.isA(SqlKind.AND)) {
        return "(" + translateAnd(condition) + ")";
      } else if (condition.isA(SqlKind.OR)) {
        return "(" + translateOr(condition) + ")";
      } else {
        return null;
      }
    }

    private String translateOr(RexNode condition) {
      List<String> ors = new ArrayList<>();
      for (RexNode node : RelOptUtil.disjunctions(condition)) {
        ors.add(translateMatch(node));
      }
      return String.join(" OR ", ors);
    }

    private String translateAnd(RexNode node0) {
      List<String> andStrings = new ArrayList();
      List<String> notStrings = new ArrayList();

      List<RexNode> ands = new ArrayList();
      List<RexNode> nots = new ArrayList();
      RelOptUtil.decomposeConjunction(node0, ands, nots);


      for (RexNode node : ands) {
        andStrings.add(translateMatch(node));
      }

      String andString = String.join(" AND ", andStrings);

      if (nots.size() > 0) {
        for (RexNode node : nots) {
          notStrings.add(translateMatch(node));
        }
        String notString = String.join(" NOT ", notStrings);
        return "(" + andString + ") NOT (" + notString + ")";
      } else {
        return andString;
      }
    }

    private String translateComparison(RexNode node) {
      Pair<String, RexLiteral> binaryTranslated = null;
      if (((RexCall) node).getOperands().size() == 2) {
        binaryTranslated = translateBinary((RexCall) node);
      }

      switch (node.getKind()) {
        case NOT:
          return "-" + translateComparison(((RexCall) node).getOperands().get(0));
        case EQUALS:
          String terms = binaryTranslated.getValue().toString().trim();
          terms = terms.replace("'","");
          if (!terms.startsWith("(") && !terms.startsWith("[") && !terms.startsWith("{")) {
            terms = "\"" + terms + "\"";
          }

          String clause = binaryTranslated.getKey() + ":" + terms;
          this.negativeQuery = false;
          return clause;
        case NOT_EQUALS:
          return "-(" + binaryTranslated.getKey() + ":" + binaryTranslated.getValue() + ")";
        case LESS_THAN:
          this.negativeQuery = false;
          return "(" + binaryTranslated.getKey() + ": [ * TO " + binaryTranslated.getValue() + " })";
        case LESS_THAN_OR_EQUAL:
          this.negativeQuery = false;
          return "(" + binaryTranslated.getKey() + ": [ * TO " + binaryTranslated.getValue() + " ])";
        case GREATER_THAN:
          this.negativeQuery = false;
          return "(" + binaryTranslated.getKey() + ": { " + binaryTranslated.getValue() + " TO * ])";
        case GREATER_THAN_OR_EQUAL:
          this.negativeQuery = false;
          return "(" + binaryTranslated.getKey() + ": [ " + binaryTranslated.getValue() + " TO * ])";
        default:
          throw new AssertionError("cannot translate " + node);
      }
    }

    /**
     * Translates a call to a binary operator, reversing arguments if necessary.
     */
    private Pair<String, RexLiteral> translateBinary(RexCall call) {
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
      final Pair<String, RexLiteral> b = translateBinary2(right, left);
      if (b != null) {
        return b;
      }
      throw new AssertionError("cannot translate call " + call);
    }

    /**
     * Translates a call to a binary operator. Returns whether successful.
     */
    private Pair<String, RexLiteral> translateBinary2(RexNode left, RexNode right) {
      switch (right.getKind()) {
        case LITERAL:
          break;
        default:
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
  }

  private static class HavingTranslator {

    private final List<String> fieldNames;
    private Map<String,String> reverseAggMappings;

    HavingTranslator(List<String> fieldNames, Map<String, String> reverseAggMappings) {
      this.fieldNames = fieldNames;
      this.reverseAggMappings = reverseAggMappings;
    }

    private String translateMatch(RexNode condition) {
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

    private String translateOr(RexNode condition) {
      List<String> ors = new ArrayList<>();
      for (RexNode node : RelOptUtil.disjunctions(condition)) {
        ors.add(translateMatch(node));
      }
      StringBuilder builder = new StringBuilder();

      builder.append("or(");
      int i = 0;
      for (i = 0; i < ors.size(); i++) {
        if (i > 0) {
          builder.append(",");
        }

        builder.append(ors.get(i));
      }
      builder.append(")");
      return builder.toString();
    }

    private String translateAnd(RexNode node0) {
      List<String> andStrings = new ArrayList();
      List<String> notStrings = new ArrayList();

      List<RexNode> ands = new ArrayList();
      List<RexNode> nots = new ArrayList();

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


      if (nots.size() > 0) {
        for (RexNode node : nots) {
          notStrings.add(translateMatch(node));
        }

        StringBuilder notBuilder = new StringBuilder();
        for(int i=0; i< notStrings.size(); i++) {
          if(i > 0) {
            notBuilder.append(",");
          }
          notBuilder.append("not(");
          notBuilder.append(notStrings.get(i));
          notBuilder.append(")");
        }

        return "and(" + builder.toString() + ","+ notBuilder.toString()+")";
      } else {
        return builder.toString();
      }
    }

    private String translateComparison(RexNode node) {
      Pair<String, RexLiteral> binaryTranslated = null;
      if (((RexCall) node).getOperands().size() == 2) {
        binaryTranslated = translateBinary((RexCall) node);
      }

      switch (node.getKind()) {
        case EQUALS:
          String terms = binaryTranslated.getValue().toString().trim();
          String clause = "eq(" + binaryTranslated.getKey() + "," + terms + ")";
          return clause;
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

    /**
     * Translates a call to a binary operator, reversing arguments if necessary.
     */
    private Pair<String, RexLiteral> translateBinary(RexCall call) {
      List<RexNode> operands = call.getOperands();
      if (operands.size() != 2) {
        throw new AssertionError("Invalid number of arguments - " + operands.size());
      }
      final RexNode left = operands.get(0);
      final RexNode right = operands.get(1);
      final Pair<String, RexLiteral> a = translateBinary2(left, right);

      if (a != null) {
        if(reverseAggMappings.containsKey(a.getKey())) {
          return new Pair<String, RexLiteral>(reverseAggMappings.get(a.getKey()),a.getValue());
        }
        return a;
      }
      final Pair<String, RexLiteral> b = translateBinary2(right, left);
      if (b != null) {
        return b;
      }
      throw new AssertionError("cannot translate call " + call);
    }

    /**
     * Translates a call to a binary operator. Returns whether successful.
     */
    private Pair<String, RexLiteral> translateBinary2(RexNode left, RexNode right) {
      switch (right.getKind()) {
        case LITERAL:
          break;
        default:
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
  }
}

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

import java.util.AbstractList;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Rules and relational operators for
 * {@link SolrRel#CONVENTION}
 * calling convention.
 */
public class SolrRules {
  private SolrRules() {}

  static final RelOptRule[] RULES = {
    SolrFilterRule.INSTANCE,
    SolrProjectRule.INSTANCE,
//    SolrSortRule.INSTANCE
  };

  static List<String> solrFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override
          public String get(int index) {
            return rowType.getFieldList().get(index).getName();
          }

          @Override
          public int size() {
            return rowType.getFieldCount();
          }
        });
  }

  /** Translator from {@link RexNode} to strings in Solr's expression language. */
  static class RexToSolrTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    RexToSolrTranslator(JavaTypeFactory typeFactory, List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override
    public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /** Base class for planner rules that convert a relational expression to Solr calling convention. */
  abstract static class SolrConverterRule extends ConverterRule {
    final Convention out;

    public SolrConverterRule(Class<? extends RelNode> clazz, String description) {
      this(clazz, Predicates.<RelNode>alwaysTrue(), description);
    }

    public <R extends RelNode> SolrConverterRule(Class<R> clazz, Predicate<? super R> predicate, String description) {
      super(clazz, predicate, Convention.NONE, SolrRel.CONVENTION, description);
      this.out = SolrRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link LogicalFilter} to a {@link SolrFilter}.
   */
  private static class SolrFilterRule extends SolrConverterRule {
    private static final SolrFilterRule INSTANCE = new SolrFilterRule();

    private SolrFilterRule() {
      super(LogicalFilter.class, "SolrFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new SolrFilter(
          rel.getCluster(),
          traitSet,
          convert(filter.getInput(), out),
          filter.getCondition());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject} to a {@link SolrProject}.
   */
  private static class SolrProjectRule extends SolrConverterRule {
    private static final SolrProjectRule INSTANCE = new SolrProjectRule();

    private SolrProjectRule() {
      super(LogicalProject.class, "SolrProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new SolrProject(
          rel.getCluster(),
          traitSet,
          convert(project.getInput(), out),
          project.getProjects(),
          project.getRowType());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a {@link SolrSort}.
   */
  private static class SolrSortRule extends SolrConverterRule {
    public static final SolrSortRule INSTANCE = new SolrSortRule();

    private SolrSortRule() {
      super(Sort.class, "SolrSortRule");
    }

    public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
      return new SolrSort(
          rel.getCluster(),
          traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation(),
          sort.fetch);
    }
  }
}

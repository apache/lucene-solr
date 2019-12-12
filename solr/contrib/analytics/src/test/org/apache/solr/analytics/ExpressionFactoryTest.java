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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.value.constant.ConstantValue;
import org.apache.solr.schema.IndexSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExpressionFactoryTest extends SolrTestCaseJ4 {

  private static IndexSchema indexSchema;

  @BeforeClass
  public static void createSchemaAndFields() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
    assertU(adoc("id", "1",
        "int_i", "1",
        "int_im", "1",
        "long_l", "1",
        "long_lm", "1",
        "float_f", "1",
        "float_fm", "1",
        "double_d", "1",
        "double_dm", "1",
        "date_dt", "1800-12-31T23:59:59Z",
        "date_dtm", "1800-12-31T23:59:59Z",
        "string_s", "1",
        "string_sm", "1",
        "boolean_b", "true",
        "boolean_bm", "false"
    ));
    assertU(commit());

    indexSchema = h.getCore().getLatestSchema();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    indexSchema = null;
  }

  private ExpressionFactory getExpressionFactory() {
    ExpressionFactory fact = new ExpressionFactory(indexSchema);
    fact.startRequest();
    return fact;
  }

  @Test
  public void userDefinedVariableFunctionTest() {
    ExpressionFactory fact = getExpressionFactory();

    // Single parameter function
    fact.startRequest();
    fact.addUserDefinedVariableFunction("single_func(a)", "sum(add(a,double_d,float_f))");
    assertEquals("div(sum(add(int_i,double_d,float_f)),count(string_s))", fact.createExpression("div(single_func(int_i),count(string_s))").getExpressionStr());

    // Multi parameter function
    fact.startRequest();
    fact.addUserDefinedVariableFunction("multi_func(a,b,c)", "median(if(boolean_b,add(a,b),c))");
    assertEquals("div(median(if(boolean_b,add(int_i,double_d),float_f)),count(string_s))", fact.createExpression("div(multi_func(int_i,double_d,float_f),count(string_s))").getExpressionStr());

    // Function within function
    fact.startRequest();
    fact.addUserDefinedVariableFunction("inner_func(a,b)", "div(add(a,b),b)");
    fact.addUserDefinedVariableFunction("outer_func(a,b,c)", "pow(inner_func(a,b),c)");
    assertEquals("div(median(pow(div(add(int_i,double_d),double_d),float_f)),count(string_s))", fact.createExpression("div(median(outer_func(int_i,double_d,float_f)),count(string_s))").getExpressionStr());

    // Variable parameter function
    fact.startRequest();
    fact.addUserDefinedVariableFunction("var_func(a,b..)", "div(add(b),a)");
    assertEquals("unique(div(add(double_d,float_f),int_i))", fact.createExpression("unique(var_func(int_i,double_d,float_f))").getExpressionStr());
    assertEquals("unique(div(add(double_d,float_f,long_l),int_i))", fact.createExpression("unique(var_func(int_i,double_d,float_f,long_l))").getExpressionStr());

    // Variable parameter function with for-each
    fact.startRequest();
    fact.addUserDefinedVariableFunction("var_func_fe(a,b..)", "div(add(b:abs(_)),a)");
    assertEquals("unique(div(add(abs(double_d),abs(float_f)),int_i))", fact.createExpression("unique(var_func_fe(int_i,double_d,float_f))").getExpressionStr());
    assertEquals("unique(div(add(abs(double_d),abs(float_f),abs(long_l)),int_i))", fact.createExpression("unique(var_func_fe(int_i,double_d,float_f,long_l))").getExpressionStr());
  }

  @Test
  public void reuseFunctionsTest() {
    ExpressionFactory fact = getExpressionFactory();

    // Two ungrouped exactly the same expression
    fact.startRequest();
    assertTrue("The objects of the two mapping expressions are not the same.",
        fact.createExpression("pow(int_i,double_d)") == fact.createExpression("pow(int_i,double_d)"));
    assertTrue("The objects of the two reduced expressions are not the same.",
        fact.createExpression("unique(add(int_i,double_d))") == fact.createExpression("unique(add(int_i,double_d))"));

    // Two ungrouped different expressions
    fact.startRequest();
    assertFalse("The objects of the two mapping expressions are not the same.",
        fact.createExpression("pow(int_i,double_d)") == fact.createExpression("pow(int_i,float_f)"));
    assertFalse("The objects of the two reduced expressions are not the same.",
        fact.createExpression("unique(add(int_i,double_d))") == fact.createExpression("unique(add(int_i,float_f))"));

    // Grouped and ungrouped mapping expression
    fact.startRequest();
    Object ungrouped = fact.createExpression("pow(int_i,double_d)");
    fact.startGrouping();
    Object grouped = fact.createExpression("pow(int_i,double_d)");
    assertTrue("The objects of the two mapping expressions are not the same.", ungrouped == grouped);

    // Grouped and ungrouped diferent mapping expressions
    fact.startRequest();
    ungrouped = fact.createExpression("pow(int_i,double_d)");
    fact.startGrouping();
    grouped = fact.createExpression("pow(int_i,float_f)");
    assertFalse("The objects of the two mapping expressions are not the same.", ungrouped == grouped);

    // Grouped and ungrouped reduced expression.
    fact.startRequest();
    ungrouped = fact.createExpression("unique(add(int_i,double_d))");
    fact.startGrouping();
    grouped = fact.createExpression("unique(add(int_i,double_d))");
    assertTrue("The objects of the two mapping expressions are not the same.", ungrouped == grouped);

    // Grouped and ungrouped different reduced expressions.
    fact.startRequest();
    ungrouped = fact.createExpression("unique(add(int_i,double_d))");
    fact.startGrouping();
    grouped = fact.createExpression("unique(add(int_i,float_f))");
    assertFalse("The objects of the two mapping expressions are the same.", ungrouped == grouped);
  }

  @Test
  public void constantFunctionConversionTest() {
    ExpressionFactory fact = getExpressionFactory();
    fact.startRequest();

    assertTrue(fact.createExpression("add(1,2)") instanceof ConstantValue);
    assertTrue(fact.createExpression("add(1,2,2,3,4)") instanceof ConstantValue);
    assertTrue(fact.createExpression("add(1)") instanceof ConstantValue);
    assertTrue(fact.createExpression("concat(add(1,2), ' is a number')") instanceof ConstantValue);

    assertFalse(fact.createExpression("sum(add(1,2))") instanceof ConstantValue);
    assertFalse(fact.createExpression("sum(int_i)") instanceof ConstantValue);
    assertFalse(fact.createExpression("sub(1,long_l)") instanceof ConstantValue);
  }

  public void testReductionManager(ReductionCollectionManager manager, boolean hasExpressions, String... fields) {
    Set<String> usedFields = new HashSet<>(Arrays.asList(fields));
    manager.getUsedFields().forEach( field -> {
      assertTrue("Field '" + field.getName() + "' is either not unique or should not exist in the reductionManager.", usedFields.remove(field.getName()));
    });
    assertEquals(hasExpressions, manager.needsCollection());
  }

  @Test
  public void reductionManagerCreationTest() {
    ExpressionFactory fact = getExpressionFactory();

    // No expressions
    fact.startRequest();
    testReductionManager(fact.createReductionManager(false), false);

    // No fields
    fact.startRequest();
    fact.createExpression("sum(add(1,2))");
    testReductionManager(fact.createReductionManager(false), true);

    // Multiple expressions
    fact.startRequest();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("sum(add(int_i,double_d))");
    testReductionManager(fact.createReductionManager(false), true, "int_i", "float_f", "double_d");
  }

  @Test
  public void groupingReductionManagerCreationTest() {
    ExpressionFactory fact = getExpressionFactory();

    // No grouped expressions
    fact.startRequest();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("sum(add(int_i,double_d))");
    fact.startGrouping();
    testReductionManager(fact.createGroupingReductionManager(false), false);

    // No grouped fields
    fact.startRequest();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("sum(add(int_i,double_d))");
    fact.startGrouping();
    fact.createExpression("sum(add(1,2))");
    testReductionManager(fact.createGroupingReductionManager(false), true);

    // Single grouping, no ungrouped
    fact.startRequest();
    fact.startGrouping();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("sum(add(int_i,double_d))");
    testReductionManager(fact.createGroupingReductionManager(false), true, "int_i", "float_f", "double_d");

    // Single grouping, with ungrouped
    fact.startRequest();
    fact.createExpression("count(string_s)");
    fact.startGrouping();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("sum(add(int_i,double_d))");
    testReductionManager(fact.createGroupingReductionManager(false), true, "int_i", "float_f", "double_d");

    // Multiple groupings, no ungrouped
    fact.startRequest();
    fact.startGrouping();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("sum(add(int_i,double_d))");
    testReductionManager(fact.createGroupingReductionManager(false), true, "int_i", "float_f", "double_d");
    fact.startGrouping();
    testReductionManager(fact.createGroupingReductionManager(false), false);
    fact.startGrouping();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("ordinal(1,concat(string_s,'-extra'))");
    testReductionManager(fact.createGroupingReductionManager(false), true, "int_i", "float_f", "string_s");

    // Multiple groupings, with grouped
    fact.startRequest();
    fact.createExpression("count(string_s)");
    fact.createExpression("median(date_math(date_dt,'+1DAY'))");
    fact.startGrouping();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("sum(add(int_i,double_d))");
    testReductionManager(fact.createGroupingReductionManager(false), true, "int_i", "float_f", "double_d");
    fact.startGrouping();
    testReductionManager(fact.createGroupingReductionManager(false), false);
    fact.startGrouping();
    fact.createExpression("unique(add(int_i,float_f))");
    fact.createExpression("ordinal(1,concat(string_s,'-extra'))");
    testReductionManager(fact.createGroupingReductionManager(false), true, "int_i", "float_f", "string_s");
  }
}

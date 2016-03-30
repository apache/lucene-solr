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
package org.apache.solr.analytics.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.analytics.statistics.StatsCollector;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.util.DateMathParser;

public class ExpressionFactory {

  /**
   * Creates a single expression that contains delegate expressions and/or 
   * a StatsCollector.
   * StatsCollectors are given as input and not created within the method so that
   * expressions can share the same StatsCollectors, minimizing computation.
   * 
   * @param expression String representation of the desired expression
   * @param statsCollectors List of StatsCollectors to build the expression with. 
   * @return the expression
   */
  @SuppressWarnings("deprecation")
  public static Expression create(String expression, StatsCollector[] statsCollectors) {
    int paren = expression.indexOf('(');
    if (paren<=0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "The expression ["+expression+"] has no arguments and is not supported.");
    }
    String topOperation = expression.substring(0,paren).trim();
    String operands;
    try {
      operands = expression.substring(paren+1, expression.lastIndexOf(')')).trim();
    } catch (Exception e) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Missing closing parenthesis in ["+expression+"]",e);
    }
    
    // Builds a statistic, constant or recursively builds an expression tree
    
    // Statistic 
    if (AnalyticsParams.ALL_STAT_SET.contains(topOperation)) {
      if (topOperation.equals(AnalyticsParams.STAT_PERCENTILE)) {
        operands = expression.substring(expression.indexOf(',')+1, expression.lastIndexOf(')')).trim();
        topOperation = topOperation+"_"+expression.substring(expression.indexOf('(')+1, expression.indexOf(',')).trim();
      }
      StatsCollector collector = null;
      // Finds the desired counter and builds an expression around it and the desired statistic.
      for (StatsCollector c : statsCollectors) {
        if (c.valueSourceString().equals(operands)) { 
          collector = c;
          break;
        }
      }
      if (collector == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "ValueSource ["+operands+"] in Expression ["+expression+"] not found.");
      }
      return new BaseExpression(collector, topOperation);
    }
    // Constant
    if (topOperation.equals(AnalyticsParams.CONSTANT_NUMBER)) {
      try {
        return new ConstantNumberExpression(Double.parseDouble(operands));
      } catch (NumberFormatException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "The constant "+operands+" cannot be converted into a number.",e);
      }
    } else if (topOperation.equals(AnalyticsParams.CONSTANT_DATE)) {
      return new ConstantDateExpression(DateMathParser.parseMath(null, operands));
    } else if (topOperation.equals(AnalyticsParams.CONSTANT_STRING)) {
      operands = expression.substring(paren+1, expression.lastIndexOf(')'));
      return new ConstantStringExpression(operands);
    }
    
    // Complex Delegating Expressions
    String[] arguments = getArguments(operands);
    Expression[] expArgs = new Expression[arguments.length];
    for (int count = 0; count < arguments.length; count++) {
      // Recursively builds delegate expressions
      expArgs[count] = create(arguments[count], statsCollectors);
    }
    
    // Single Delegate Expressions
    if (expArgs.length==1) {
      // Numeric Expression
      if (topOperation.equals(AnalyticsParams.NEGATE)) {
        return new NegateExpression(expArgs[0]);
      }
      if (topOperation.equals(AnalyticsParams.ABSOLUTE_VALUE)) {
        return new AbsoluteValueExpression(expArgs[0]);
      }
      // String Expression
      else if (topOperation.equals(AnalyticsParams.REVERSE)) {
        return new ReverseExpression(expArgs[0]);
      }
      throw new SolrException(ErrorCode.BAD_REQUEST, topOperation+" does not have the correct number of arguments.");
    }  else {
      // Multi Delegate Expressions
      // Numeric Expression
      if (topOperation.equals(AnalyticsParams.ADD)) {
        return new AddExpression(expArgs);
      } else if (topOperation.equals(AnalyticsParams.MULTIPLY)) {
        return new MultiplyExpression(expArgs);
      }
      // Date Expression
      else if (topOperation.equals(AnalyticsParams.DATE_MATH)) {
        return new DateMathExpression(expArgs);
      } 
      // String Expression
      else if (topOperation.equals(AnalyticsParams.CONCATENATE)) {
        return new ConcatenateExpression(expArgs);
      } 
      // Dual Delegate Expressions
      else if (expArgs.length==2 && (topOperation.equals(AnalyticsParams.DIVIDE) || topOperation.equals(AnalyticsParams.POWER) 
          || topOperation.equals(AnalyticsParams.LOG))) {
        // Numeric Expression
        if (topOperation.equals(AnalyticsParams.DIVIDE)) {
          return new DivideExpression(expArgs[0], expArgs[1]);
        } else if (topOperation.equals(AnalyticsParams.POWER)) {
          return new PowerExpression(expArgs[0], expArgs[1]);
        } else if (topOperation.equals(AnalyticsParams.LOG)) {
          return new LogExpression(expArgs[0], expArgs[1]);
        }
        return null;
      }
      throw new SolrException(ErrorCode.BAD_REQUEST, topOperation+" does not have the correct number of arguments or is unsupported.");
    }
    
  }
  
  /**
   * Splits up an Expression's arguments.
   * 
   * @param expression Current expression string
   * @return List The list of arguments
   */
  public static String[] getArguments(String expression) {
    String[] strings = new String[1];
    int stack = 0;
    int start = 0;
    List<String> arguments = new ArrayList<>();
    char[] chars = expression.toCharArray();
    for (int count = 0; count < expression.length(); count++) {
      char c = chars[count];
      if (c==',' && stack == 0) {
        arguments.add(expression.substring(start, count).replace("\\(","(").replace("\\)",")").replace("\\,",",").trim());
        start = count+1;
      } else if (c == '(') {
        stack ++;
      } else if (c == ')') {
        stack --;
      } else if (c == '\\') {
        ; // Do nothing.
      }
    }
    if (stack==0) {
      arguments.add(expression.substring(start).trim());
    }
    return arguments.toArray(strings);
  }
}

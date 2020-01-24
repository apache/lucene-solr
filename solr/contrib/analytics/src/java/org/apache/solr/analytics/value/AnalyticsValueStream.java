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
package org.apache.solr.analytics.value;

import java.util.Arrays;
import java.util.Locale;
import java.util.function.Consumer;

import org.apache.solr.analytics.ExpressionFactory;
import org.apache.solr.analytics.value.constant.ConstantValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A multi-valued analytics value, the super-type of all Analytics value types.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #streamObjects},
 * resulting in different values on each call.
 */
public interface AnalyticsValueStream {
  /**
   * Get the name of function or value.
   *
   * @return the name of function/value
   */
  String getName();
  /**
   * Get the expression string of the analytics value stream. Must be unique to the expression.
   * If passed to {@link ExpressionFactory#createExpression(String)}, the exact same expression should be created.
   *
   * @return the name of function/value
   */
  String getExpressionStr();
  /**
   * Stream the object representations of all current values, if any exist.
   *
   * @param cons The consumer to accept the values
   */
  void streamObjects(Consumer<Object> cons);

  /**
   * Converts this value to a {@link ConstantValue} if it's expression type is {@link ExpressionType#CONST}.
   *
   * If the value is reduced then no conversion will occur and the value itself will be returned.
   *
   * @return a constant representation of this value
   */
  AnalyticsValueStream convertToConstant();

  public static abstract class AbstractAnalyticsValueStream implements AnalyticsValueStream {
    @Override
    public AnalyticsValueStream convertToConstant() {
      return this;
    }
  }

  /**
   * The types of expressions.
   */
  static enum ExpressionType {
    CONST             (true, true),
    FIELD             (true,  false),
    UNREDUCED_MAPPING (true,  false),
    REDUCTION         (false, true),
    REDUCED_MAPPING   (false, true);

    private final boolean unreduced;
    private final boolean reduced;

    ExpressionType(boolean unreduced, boolean reduced) {
      this.unreduced = unreduced;
      this.reduced = reduced;
    }

    public boolean isUnreduced() {
      return unreduced;
    }
    public boolean isReduced() {
      return reduced;
    }
  }

  /**
   * Get the type of the expression that this class represents.
   *
   * @return the expression type
   */
  ExpressionType getExpressionType();

  /**
   * Helper to create an expression string for a function.
   *
   * @param funcName the name of the function
   * @param params the parameters of the function
   * @return a valid expression string for the function.
   */
  static String createExpressionString(String funcName,
                                       AnalyticsValueStream... params) {
    return String.format(Locale.ROOT, "%s(%s)",
                         funcName,
                         Arrays.stream(params).
                                map(param -> param.getExpressionStr()).
                                reduce((a, b) -> a + "," + b).orElseGet(() -> ""));
  }

  /**
   * Determine whether the expression is a unreduced mapping expression, a reduced mapping expression, or a constant.
   *
   * @param exprString the string representing the expression, used when creating exceptions
   * @param params the parameters
   * @return the expression type
   * @throws SolrException if the params are incompatable types,
   * for example if reduced and unreduced params are both included
   */
  static ExpressionType determineMappingPhase(String exprString, AnalyticsValueStream... params) throws SolrException {
    boolean unreduced = true;
    boolean reduced = true;
    for (AnalyticsValueStream param : params) {
      unreduced &= param.getExpressionType().isUnreduced();
      reduced &= param.getExpressionType().isReduced();
    }
    if (unreduced && reduced) {
      return ExpressionType.CONST;
    }
    else if (unreduced) {
      return ExpressionType.UNREDUCED_MAPPING;
    }
    else if (reduced) {
      return ExpressionType.REDUCED_MAPPING;
    }
    else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The following expression contains incorrect parameters. " +
                            "(ReductionFunctions cannot be in the paramters of other ReductionFunctions): " + exprString);
    }
  }
}

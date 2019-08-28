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
package org.apache.solr.analytics.function.mapping;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoBoolInBoolOutLambda;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * Contains all logical mapping functions.
 * <p>
 * Logical mapping functions can be used in the following ways:
 * <ul>
 * <li>If a single {@link BooleanValueStream} is passed in, a {@link BooleanValue} representing the logical operation
 * on all of the values for each document is returned.
 * <li>If a {@link BooleanValueStream} and a {@link BooleanValue} are passed in, a {@link BooleanValue} representing the logical operation on
 * the {@link BooleanValue} and each of the values of the {@link BooleanValueStream} for a document is returned.
 * (Or the other way, since the Value and ValueStream can be used in either order)
 * <li>If multiple {@link BooleanValue}s are passed in, a {@link BooleanValue} representing the logical operation on all values is returned.
 * </ul>
 */
public class LogicFunction {

  private static BooleanValueStream createBitwiseFunction(String name, TwoBoolInBoolOutLambda comp, AnalyticsValueStream... params) {
    if (params.length == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires parameters.");
    }
    else if (params.length == 1) {
      if (params[0] instanceof BooleanValueStream) {
        return LambdaFunction.createBooleanLambdaFunction(name, comp, (BooleanValueStream)params[0]);
      }
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires boolean parameters. Incorrect param: "+params[0].getExpressionStr());
    }
    else if (params.length == 2) {
      AnalyticsValueStream param1 = params[0];
      AnalyticsValueStream param2 = params[1];
      if (param1 instanceof BooleanValueStream && param2 instanceof BooleanValueStream) {
        return LambdaFunction.createBooleanLambdaFunction(name, comp, (BooleanValueStream)param1, (BooleanValueStream)param2);
      }
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires boolean parameters.");
    }
    BooleanValue[] castedParams = new BooleanValue[params.length];
    for (int i = 0; i < params.length; i++) {
      if (params[i] instanceof BooleanValue) {
        castedParams[i] = (BooleanValue) params[i];
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that all parameters be single-valued if more than 2 are given.");
      }
    }
    return LambdaFunction.createBooleanLambdaFunction(name, comp, castedParams);
  };

  /**
   * A mapping function for the logical operation AND.
   */
  public static class AndFunction {
    public static final String name = "and";
    public static final CreatorFunction creatorFunction = (params -> {
      return LogicFunction.createBitwiseFunction(name, (a,b) -> a && b, params);
    });
  }

  /**
   * A mapping function for the logical operation OR.
   */
  public static class OrFunction {
    public static final String name = "or";
    public static final CreatorFunction creatorFunction = (params -> {
      return LogicFunction.createBitwiseFunction(name, (a,b) -> a || b, params);
    });
  }
}
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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import java.util.Arrays;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.function.mapping.LambdaFunction.TwoStringInStringOutLambda;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.constant.ConstantStringValue;
import org.apache.solr.analytics.value.constant.ConstantValue;

/**
 * A concatenation mapping function, combining the string values of the given parameters. (At least 1 parameter is required)
 * <p>
 * Uses:
 * <ul>
 * <li>If a single {@link StringValueStream} is passed in, a {@link StringValue} representing the concatenation of the values for each document is returned.
 * No ordering is guaranteed while concatenating.
 * <li>If a {@link StringValue} and a {@link StringValueStream} are passed in, a {@link StringValueStream} representing the concatenation of
 * the Value and each of the values of the ValueStream for a document is returned.
 * (Or the other way, since the Value and ValueStream can be used in either order)
 * <li>If any number (more than 0) of {@link StringValue}s are passed in, a {@link StringValue} representing the concatenation of all values is returned.
 * If any values don't exist, the overall concatenation value will still exist with an empty string used for any missing values. If none of the parameter
 * values exist, then the overall concatenation value will not exist.
 * </ul>
 */
public class ConcatFunction {
  public static final String name = "concat";
  public static final CreatorFunction creatorFunction = (params -> {
    return createConcatFunction(name, name, (a,b) -> a + b, params);
  });

  /**
   * A concatenation mapping function, combining the string values of the given parameters with a given separating string.
   * <br>
   * The usage is exactly the same as the {@link ConcatFunction}, however a {@link ConstantStringValue} separator is added as the first argument
   * of every usage. So the acceptable inputs are as follows:
   * <ul>
   * <li>{@value #name} ( {@link ConstantStringValue} , {@link StringValueStream} )
   * <li>{@value #name} ( {@link ConstantStringValue} , {@link StringValueStream} , {@link StringValue} )
   * <li>{@value #name} ( {@link ConstantStringValue} , {@link StringValue} , {@link StringValueStream} )
   * <li>{@value #name} ( {@link ConstantStringValue} , {@link StringValue} ... )
   * </ul>
   * The {@link ConstantStringValue} separator is used to separate every two (or more) string during concatenation. If only one string value exists,
   * then the separator will not be used.
   */
  public static class SeparatedConcatFunction {
    public static final String name = "concat_sep";
    public static final CreatorFunction creatorFunction = (params -> {
      if (params.length < 2) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 2 parameters.");
      } else if (!(params[0] instanceof StringValue && params[0] instanceof ConstantValue)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that the first parameter to be a constant string.");
      }
      final String sep = ((StringValue)params[0]).getString();
      String uniqueName = name + "(" + sep + ")";
      return createConcatFunction(name, uniqueName, (a,b) -> a + sep + b, Arrays.copyOfRange(params, 1, params.length));
    });
  }

  private static StringValueStream createConcatFunction(String functionName, String uniqueName, TwoStringInStringOutLambda lambda, AnalyticsValueStream[] params) {
    if (params.length == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "The "+functionName+" function requires parameters.");
    } else if (params.length == 1) {
      if (params[0] instanceof StringValueStream) {
        return LambdaFunction.createStringLambdaFunction(uniqueName, lambda, (StringValueStream)params[0]);
      }
      throw new SolrException(ErrorCode.BAD_REQUEST, "The "+functionName+" function requires that all parameters be string-castable.");
    } else if (params.length == 2) {
      // If it is not a pair of a single valued and multi valued string, then it will be taken care of below
      if (params[0] instanceof StringValueStream && params[1] instanceof StringValueStream
          && !(params[0] instanceof StringValue && params[1] instanceof StringValue)) {
        return LambdaFunction.createStringLambdaFunction(uniqueName, lambda, (StringValueStream)params[0], (StringValueStream)params[1]);
      }
    }
    StringValue[] castedParams = new StringValue[params.length];
    for (int i = 0; i < params.length; i++) {
      if (params[i] instanceof StringValue) {
        castedParams[i] = (StringValue) params[i];
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "The "+functionName+" function requires that all parameters be string-castable, and if more than 2 parameters"
            + " are provided then all must be single-valued.");
      }
    }
    return LambdaFunction.createStringLambdaFunction(uniqueName, lambda, castedParams, false);
  }
}
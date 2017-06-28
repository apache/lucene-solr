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
import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.constant.ConstantStringValue;

/**
 * A concatenation mapping function, combining the string values of the given parameters. (At least 1 parameter is required)
 * <p>
 * Multiple comparable {@link StringValue}s are passed in and a {@link StringValue} representing the concatenation of all values is returned. 
 */
public class ConcatFunction {
  public static final String name = "concat";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires parameters.");
    } 
    else if (params.length == 1 && params[0] instanceof StringValue) {
      return params[0];
    } 
    StringValue[] castedParams = new StringValue[params.length];
    for (int i = 0; i < params.length; i++) {
      if (params[i] instanceof StringValue) {
        castedParams[i] = (StringValue) params[i];
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that all parameters be single-valued and convertible to string values.");
      }
    }
    return LambdaFunction.createStringLambdaFunction(name, (a,b) -> a+b, castedParams, false);
  });
  
  /**
   * A concatenation mapping function, combining the string values of the given parameters with a given separating string.
   * <br>
   * Multiple comparable {@link StringValue}s are passed in and a {@link StringValue} representing the separated concatenation of all values is returned.
   * <p>
   * The first parameter must be a constant string (e.g. ",").
   * The remaining parameters are the {@link StringValue} expressions to concatenate. (At least 1 expression is required)
   */
  public static class ConcatSeparatedFunction {
    public static final String name = "concatsep";
    public static final CreatorFunction creatorFunction = (params -> {
      if (params.length < 2) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 2 parameters.");
      } else if (!(params[0] instanceof ConstantStringValue)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that the first parameter to be a constant string.");
      }
      final String sep = ((ConstantStringValue)params[0]).getString();
      StringValue[] castedParams = new StringValue[params.length - 1];
      for (int i = 0; i < castedParams.length; i++) {
        if (params[i + 1] instanceof StringValue) {
          castedParams[i] = (StringValue) params[i + 1];
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that all non-separator parameters be single-valued and convertible to string values.");
        }
      }
      return LambdaFunction.createStringLambdaFunction(name, (a,b) -> a + sep + b, castedParams, false);
    });
  }
}
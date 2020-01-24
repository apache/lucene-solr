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
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;

/**
 * An addition mapping function.
 * <p>
 * Uses:
 * <ul>
 * <li>If a single numeric ValueStream is passed in, a {@link DoubleValue} representing the sum of the values for each document is returned.
 * <li>If a numeric ValueStream and a numeric Value are passed in, a {@link DoubleValueStream} representing the sum of
 * the Value and each of the values of the ValueStream for a document is returned.
 * (Or the other way, since the Value and ValueStream can be used in either order)
 * <li>If multiple numeric Values are passed in, a {@link DoubleValue} representing the sum of all values is returned.
 * </ul>
 */
public class AddFunction {
  public static final String name = "add";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires parameters.");
    }
    else if (params.length == 1) {
      if (params[0] instanceof DoubleValueStream) {
        return LambdaFunction.createDoubleLambdaFunction(name, (a,b) -> a+b, (DoubleValueStream)params[0]);
      }
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires numeric parameters. Incorrect param: "+params[0].getExpressionStr());
    }
    else if (params.length == 2) {
      AnalyticsValueStream param1 = params[0];
      AnalyticsValueStream param2 = params[1];
      if (param1 instanceof DoubleValueStream && param2 instanceof DoubleValueStream) {
        return LambdaFunction.createDoubleLambdaFunction(name, (a,b) -> a+b, (DoubleValueStream)param1, (DoubleValueStream)param2);
      }
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires numeric parameters.");
    }
    DoubleValue[] castedParams = new DoubleValue[params.length];
    for (int i = 0; i < params.length; i++) {
      if (params[i] instanceof DoubleValue) {
        castedParams[i] = (DoubleValue) params[i];
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires that all parameters be single-valued if more than 2 are given.");
      }
    }
    return LambdaFunction.createDoubleLambdaFunction(name, (a,b) -> a+b, castedParams);
  });
}
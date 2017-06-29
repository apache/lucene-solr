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
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.constant.ConstantStringValue;

/**
 * A string join mapping function.
 * <p>
 * Takes a {@link StringValueStream} as the first parameter and a {@link ConstantStringValue} (e.g. ",") as the second parameter
 * and a {@link StringValue} is returned.
 * <br>
 * The second parameter is used as the separator while joining all values the first parameter has for a document.
 * No order is guaranteed while joining the string values
 */
public class JoinFunction {
  public static final String name = "join";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 parameters.");
    } 
    AnalyticsValueStream param1 = params[0];
    AnalyticsValueStream param2 = params[1];
    if (!(param2 instanceof ConstantStringValue)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires the second parameter to be a constant string");
    }
    String sep = ((StringValue)param2).getString();
    if (!(param1 instanceof StringValueStream)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires the first parameter to be castable to a string");
    }
    if (param1 instanceof StringValue) {
      return param1;
    }
    String uniqueName = name + "(" + sep + ")";
    return LambdaFunction.createStringLambdaFunction(uniqueName, (a,b) -> a + sep + b, (StringValueStream)params[0]);
  });
}
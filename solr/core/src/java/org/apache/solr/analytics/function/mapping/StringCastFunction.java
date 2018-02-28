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
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A mapping function that casts any input to a {@link StringValue} or {@link StringValueStream}.
 */
public class StringCastFunction {
  public static final String name = "string";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramater, " + params.length + " found.");
    }
    AnalyticsValueStream param = params[0];
    if (param instanceof StringValueStream) {
      return LambdaFunction.createStringLambdaFunction(name, a -> a, (StringValueStream)param);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a string-castable parameter.");
    }
  });
}
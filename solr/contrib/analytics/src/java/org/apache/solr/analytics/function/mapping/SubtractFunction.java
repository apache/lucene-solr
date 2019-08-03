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
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A subtraction mapping function.
 * <p>
 * Uses:
 * <ul>
 * <li>If two numeric Values are passed in, a {@link DoubleValue} representing the first subtracted by the second is returned.
 * <li>If a numeric ValueStream and a numeric Value are passed in, a {@link DoubleValueStream} representing the Value subtracted by
 * each of the values of the ValueStream for a document is returned.
 * (Or the other way, since the Value and ValueStream can be used in either order)
 * </ul>
 */
public class SubtractFunction {
  public static final String name = "sub";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramaters, " + params.length + " found.");
    }
    AnalyticsValueStream param1 = params[0];
    AnalyticsValueStream param2 = params[1];
    if (param1 instanceof DoubleValueStream && param2 instanceof DoubleValueStream) {
      return LambdaFunction.createDoubleLambdaFunction(name, (a,b) -> a-b, (DoubleValueStream)param1, (DoubleValueStream)param2);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires numeric parameters.");
    }
  });
}
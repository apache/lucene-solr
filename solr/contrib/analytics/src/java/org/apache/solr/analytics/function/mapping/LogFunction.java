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
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A logarithm mapping function.
 * <p>
 * Uses:
 * <ul>
 * <li>If one numeric Value or ValueStream is passed in, a {@link DoubleValue} or {@link DoubleValueStream}
 * representing the natural logarithm is returned.
 * <li>If two numeric Values are passed in, a {@link DoubleValue} representing the logarithm of the first with the second as the base is returned.
 * <li>If a numeric ValueStream and a numeric Value are passed in, a {@link DoubleValueStream} representing the logarithm of
 * the Value with each of the values of the ValueStream for a document as the base is returned.
 * (Or the other way, since the Value and ValueStream can be used in either order)
 * </ul>
 */
public class LogFunction {
  public static final String name = "log";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires at least 1.");
    } else if (params.length == 1) {
      return LambdaFunction.createDoubleLambdaFunction(name, (a) -> Math.log(a), (DoubleValueStream)params[0]);
    } else if (params.length == 2) {
      return LambdaFunction.createDoubleLambdaFunction(name, (a,b) -> Math.log(a)/Math.log(b), (DoubleValueStream)params[0], (DoubleValueStream)params[1]);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function accepts at most 2 paramaters, " + params.length + " found.");
    }
  });
}
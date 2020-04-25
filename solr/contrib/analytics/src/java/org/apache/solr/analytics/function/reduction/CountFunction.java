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
package org.apache.solr.analytics.function.reduction;

import java.util.function.UnaryOperator;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.function.ReductionFunction;
import org.apache.solr.analytics.function.reduction.data.CountCollector;
import org.apache.solr.analytics.function.reduction.data.CountCollector.ExpressionCountCollector;
import org.apache.solr.analytics.function.reduction.data.CountCollector.TotalCountCollector;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.LongValue.AbstractLongValue;
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A reduction function which either counts the number of values that the parameter expression contains,
 * or the number of documents returned if no parameter is given.
 */
public class CountFunction extends AbstractLongValue implements ReductionFunction {
  private CountCollector collector;
  public static final String name = "count";
  private final String exprStr;
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length == 0) {
      return new CountFunction();
    }
    if (params.length == 1) {
      return new CountFunction(params[0]);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function a max of 1 paramater, " + params.length + " given.");
  });

  public CountFunction() {
    this.collector = new TotalCountCollector();
    this.exprStr = AnalyticsValueStream.createExpressionString(name);
  }

  public CountFunction(AnalyticsValueStream param) {
    this.collector = new ExpressionCountCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public long getLong() {
    return collector.count();
  }
  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (CountCollector)sync.apply(collector);
  }

  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public String getName() {
    return name;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.REDUCTION;
  }
}
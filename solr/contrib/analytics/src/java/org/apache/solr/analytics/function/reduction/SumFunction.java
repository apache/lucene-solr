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

import java.io.Serializable;
import java.util.function.UnaryOperator;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.function.ReductionFunction;
import org.apache.solr.analytics.function.reduction.data.ReductionData;
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.analytics.function.reduction.data.SumCollector;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.DoubleValue.AbstractDoubleValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A reduction function which returns the sum of the values of the given expression.
 */
public class SumFunction extends AbstractDoubleValue implements ReductionFunction {
  private SumCollector collector;
  public static final String name = "sum";
  private final String exprStr;
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramater, " + params.length + " found.");
    }
    DoubleValueStream casted;
    try {
      casted = (DoubleValueStream) params[0];
    }
    catch (ClassCastException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires numeric parameter. " +
          "Incorrect parameter: "+params[0].getExpressionStr());
    }
    return new SumFunction(casted);
  });

  public SumFunction(DoubleValueStream param) {
    this.collector = new SumCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public double getDouble() {
    return collector.sum();
  }
  @Override
  public boolean exists() {
    return collector.exists();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SumCollector) sync.apply(collector);
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

  protected static class SumData extends ReductionData implements Serializable {
    private static final long serialVersionUID = 5920718235872898338L;
    double sum;
  }
}
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
import org.apache.solr.analytics.function.reduction.data.MinCollector.DoubleMinCollector;
import org.apache.solr.analytics.function.reduction.data.MinCollector.FloatMinCollector;
import org.apache.solr.analytics.function.reduction.data.MinCollector.IntMinCollector;
import org.apache.solr.analytics.function.reduction.data.MinCollector.LongMinCollector;
import org.apache.solr.analytics.function.reduction.data.MinCollector.StringMinCollector;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.DateValue.AbstractDateValue;
import org.apache.solr.analytics.value.DoubleValue.AbstractDoubleValue;
import org.apache.solr.analytics.value.FloatValue.AbstractFloatValue;
import org.apache.solr.analytics.value.IntValue.AbstractIntValue;
import org.apache.solr.analytics.value.LongValue.AbstractLongValue;
import org.apache.solr.analytics.value.StringValue.AbstractStringValue;
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A reduction function which returns the minumum value of the given expression.
 */
public class MinFunction {
  public static final String name = "min";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramater, " + params.length + " found.");
    }
    AnalyticsValueStream param = params[0];
    if (param instanceof DateValueStream) {
      return new DateMinFunction((DateValueStream)param);
    }
    if (param instanceof IntValueStream) {
      return new IntMinFunction((IntValueStream)param);
    }
    if (param instanceof LongValueStream) {
      return new LongMinFunction((LongValueStream)param);
    }
    if (param instanceof FloatValueStream) {
      return new FloatMinFunction((FloatValueStream)param);
    }
    if (param instanceof DoubleValueStream) {
      return new DoubleMinFunction((DoubleValueStream)param);
    }
    if (param instanceof StringValueStream) {
      return new StringMinFunction((StringValueStream)param);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a comparable parameter. " +
          "Incorrect parameter: "+params[0].getExpressionStr());
  });

  static class IntMinFunction extends AbstractIntValue implements ReductionFunction {
    private IntMinCollector collector;
    public static final String name = MinFunction.name;
    private final String exprStr;

    public IntMinFunction(IntValueStream param) {
      this.collector = new IntMinCollector(param);
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    @Override
    public int getInt() {
      return collector.exists() ? collector.min() : 0;
    }
    @Override
    public boolean exists() {
      return collector.exists();
    }

    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (IntMinCollector)sync.apply(collector);
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return ExpressionType.REDUCTION;
    }
  }

  static class LongMinFunction extends AbstractLongValue implements ReductionFunction {
    private LongMinCollector collector;
    public static final String name = MinFunction.name;
    private final String exprStr;

    public LongMinFunction(LongValueStream param) {
      this.collector = new LongMinCollector(param);
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    @Override
    public long getLong() {
      return collector.exists() ? collector.min() : 0;
    }
    @Override
    public boolean exists() {
      return collector.exists();
    }

    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (LongMinCollector)sync.apply(collector);
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return ExpressionType.REDUCTION;
    }
  }

  static class FloatMinFunction extends AbstractFloatValue implements ReductionFunction {
    private FloatMinCollector collector;
    public static final String name = MinFunction.name;
    private final String exprStr;

    public FloatMinFunction(FloatValueStream param) {
      this.collector = new FloatMinCollector(param);
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    @Override
    public float getFloat() {
      return collector.exists() ? collector.min() : 0;
    }
    @Override
    public boolean exists() {
      return collector.exists();
    }

    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (FloatMinCollector)sync.apply(collector);
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return ExpressionType.REDUCTION;
    }
  }

  static class DoubleMinFunction extends AbstractDoubleValue implements ReductionFunction {
    private DoubleMinCollector collector;
    public static final String name = MinFunction.name;
    private final String exprStr;

    public DoubleMinFunction(DoubleValueStream param) {
      this.collector = new DoubleMinCollector(param);
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    @Override
    public double getDouble() {
      return collector.exists() ? collector.min() : 0;
    }
    @Override
    public boolean exists() {
      return collector.exists();
    }

    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (DoubleMinCollector)sync.apply(collector);
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return ExpressionType.REDUCTION;
    }
  }

  static class DateMinFunction extends AbstractDateValue implements ReductionFunction {
    private LongMinCollector collector;
    public static final String name = MinFunction.name;
    private final String exprStr;

    public DateMinFunction(LongValueStream param) {
      this.collector = new LongMinCollector(param);
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    @Override
    public long getLong() {
      return collector.exists() ? collector.min() : 0;
    }
    @Override
    public boolean exists() {
      return collector.exists();
    }

    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (LongMinCollector)sync.apply(collector);
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return ExpressionType.REDUCTION;
    }
  }

  static class StringMinFunction extends AbstractStringValue implements ReductionFunction {
    private StringMinCollector collector;
    public static final String name = MinFunction.name;
    private final String exprStr;

    public StringMinFunction(StringValueStream param) {
      this.collector = new StringMinCollector(param);
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    @Override
    public String getString() {
      return collector.exists() ? collector.min() : null;
    }
    @Override
    public boolean exists() {
      return collector.exists();
    }

    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (StringMinCollector)sync.apply(collector);
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return exprStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return ExpressionType.REDUCTION;
    }
  }
}


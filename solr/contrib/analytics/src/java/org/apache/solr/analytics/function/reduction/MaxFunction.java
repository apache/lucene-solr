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
import org.apache.solr.analytics.function.reduction.data.MaxCollector.DoubleMaxCollector;
import org.apache.solr.analytics.function.reduction.data.MaxCollector.FloatMaxCollector;
import org.apache.solr.analytics.function.reduction.data.MaxCollector.IntMaxCollector;
import org.apache.solr.analytics.function.reduction.data.MaxCollector.LongMaxCollector;
import org.apache.solr.analytics.function.reduction.data.MaxCollector.StringMaxCollector;
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
 * A reduction function which returns the maximum value of the given expression.
 */
public class MaxFunction {
  public static final String name = "max";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramater, " + params.length + " found.");
    }
    AnalyticsValueStream param = params[0];
    if (param instanceof DateValueStream) {
      return new DateMaxFunction((DateValueStream)param);
    }
    if (param instanceof IntValueStream) {
      return new IntMaxFunction((IntValueStream)param);
    }
    if (param instanceof LongValueStream) {
      return new LongMaxFunction((LongValueStream)param);
    }
    if (param instanceof FloatValueStream) {
      return new FloatMaxFunction((FloatValueStream)param);
    }
    if (param instanceof DoubleValueStream) {
      return new DoubleMaxFunction((DoubleValueStream)param);
    }
    if (param instanceof StringValueStream) {
      return new StringMaxFunction((StringValueStream)param);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a comparable parameter. " +
          "Incorrect parameter: "+params[0].getExpressionStr());
  });
}
class IntMaxFunction extends AbstractIntValue implements ReductionFunction {
  private IntMaxCollector collector;
  public static final String name = MaxFunction.name;
  private final String exprStr;

  public IntMaxFunction(IntValueStream param) {
    this.collector = new IntMaxCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public int getInt() {
    return collector.exists() ? collector.max() : 0;
  }
  @Override
  public boolean exists() {
    return collector.exists();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (IntMaxCollector)sync.apply(collector);
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
class LongMaxFunction extends AbstractLongValue implements ReductionFunction {
  private LongMaxCollector collector;
  public static final String name = MaxFunction.name;
  private final String exprStr;

  public LongMaxFunction(LongValueStream param) {
    this.collector = new LongMaxCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public long getLong() {
    return collector.exists() ? collector.max() : 0;
  }
  @Override
  public boolean exists() {
    return collector.exists();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (LongMaxCollector)sync.apply(collector);
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
class FloatMaxFunction extends AbstractFloatValue implements ReductionFunction {
  private FloatMaxCollector collector;
  public static final String name = MaxFunction.name;
  private final String exprStr;

  public FloatMaxFunction(FloatValueStream param) {
    this.collector = new FloatMaxCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public float getFloat() {
    return collector.exists() ? collector.max() : 0;
  }
  @Override
  public boolean exists() {
    return collector.exists();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (FloatMaxCollector)sync.apply(collector);
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
class DoubleMaxFunction extends AbstractDoubleValue implements ReductionFunction {
  private DoubleMaxCollector collector;
  public static final String name = MaxFunction.name;
  private final String exprStr;

  public DoubleMaxFunction(DoubleValueStream param) {
    this.collector = new DoubleMaxCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public double getDouble() {
    return collector.exists() ? collector.max() : 0;
  }
  @Override
  public boolean exists() {
    return collector.exists();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (DoubleMaxCollector)sync.apply(collector);
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
class DateMaxFunction extends AbstractDateValue implements ReductionFunction {
  private LongMaxCollector collector;
  public static final String name = MaxFunction.name;
  private final String exprStr;

  public DateMaxFunction(LongValueStream param) {
    this.collector = new LongMaxCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public long getLong() {
    return collector.exists() ? collector.max() : 0;
  }
  @Override
  public boolean exists() {
    return collector.exists();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (LongMaxCollector)sync.apply(collector);
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
class StringMaxFunction extends AbstractStringValue implements ReductionFunction {
  private StringMaxCollector collector;
  public static final String name = MaxFunction.name;
  private final String exprStr;

  public StringMaxFunction(StringValueStream param) {
    this.collector = new StringMaxCollector(param);
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  @Override
  public String getString() {
    return collector.exists() ? collector.max() : null;
  }
  @Override
  public boolean exists() {
    return collector.exists();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (StringMaxCollector)sync.apply(collector);
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
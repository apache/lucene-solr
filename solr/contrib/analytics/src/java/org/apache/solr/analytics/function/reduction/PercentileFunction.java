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

import java.util.Locale;
import java.util.function.UnaryOperator;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.function.ReductionFunction;
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedDoubleListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedFloatListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedIntListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedLongListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedStringListCollector;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValue;
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
import org.apache.solr.analytics.value.constant.ConstantValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A reduction function which returns the given percentile of the sorted values of the given expression.
 */
public class PercentileFunction {
  public static final String name = "percentile";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramater, " + params.length + " found.");
    }
    AnalyticsValueStream percValue = params[0];
    double perc = 0;
    if (percValue instanceof DoubleValue && percValue instanceof ConstantValue) {
      perc = ((DoubleValue)percValue).getDouble();
      if (perc < 0 || perc >= 100) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a percentile between [0, 100), " + perc + " found.");
      }
      perc /= 100;
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a constant double value (the percentile) as the first argument.");
    }
    AnalyticsValueStream param = params[1];
    if (param instanceof DateValueStream) {
      return new DatePercentileFunction((DateValueStream)param, perc);
    }else if (param instanceof IntValueStream) {
      return new IntPercentileFunction((IntValueStream)param, perc);
    } else if (param instanceof LongValueStream) {
      return new LongPercentileFunction((LongValueStream)param, perc);
    } else if (param instanceof FloatValueStream) {
      return new FloatPercentileFunction((FloatValueStream)param, perc);
    } else if (param instanceof DoubleValueStream) {
      return new DoublePercentileFunction((DoubleValueStream)param, perc);
    } else if (param instanceof StringValueStream) {
      return new StringPercentileFunction((StringValueStream)param, perc);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a comparable parameter.");
  });

  protected static String createPercentileExpressionString(AnalyticsValueStream param, double perc) {
    return String.format(Locale.ROOT, "%s(%s,%s)",
                         name,
                         perc,
                         param.getExpressionStr());
  }
}
class IntPercentileFunction extends AbstractIntValue implements ReductionFunction {
  private SortedIntListCollector collector;
  private double percentile;
  public static final String name = PercentileFunction.name;
  private final String exprStr;

  public IntPercentileFunction(IntValueStream param, double percentile) {
    this.collector = new SortedIntListCollector(param);
    this.percentile = percentile;
    this.exprStr = PercentileFunction.createPercentileExpressionString(param, percentile);
  }

  @Override
  public int getInt() {
    int size = collector.size();
    return size > 0 ? collector.get((int) Math.round(percentile * size - .5)) : 0;
  }
  @Override
  public boolean exists() {
    return collector.size() > 0;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedIntListCollector)sync.apply(collector);
    collector.calcPercentile(percentile);
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
class LongPercentileFunction extends AbstractLongValue implements ReductionFunction {
  private SortedLongListCollector collector;
  private double percentile;
  public static final String name = PercentileFunction.name;
  private final String exprStr;

  public LongPercentileFunction(LongValueStream param, double percentile) {
    this.collector = new SortedLongListCollector(param);
    this.percentile = percentile;
    this.exprStr = PercentileFunction.createPercentileExpressionString(param, percentile);
  }

  @Override
  public long getLong() {
    int size = collector.size();
    return size > 0 ? collector.get((int) Math.round(percentile * size - .5)) : 0;
  }
  @Override
  public boolean exists() {
    return collector.size() > 0;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedLongListCollector)sync.apply(collector);
    collector.calcPercentile(percentile);
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
class FloatPercentileFunction extends AbstractFloatValue implements ReductionFunction {
  private SortedFloatListCollector collector;
  private double percentile;
  public static final String name = PercentileFunction.name;
  private final String exprStr;

  public FloatPercentileFunction(FloatValueStream param, double percentile) {
    this.collector = new SortedFloatListCollector(param);
    this.percentile = percentile;
    this.exprStr = PercentileFunction.createPercentileExpressionString(param, percentile);
  }

  @Override
  public float getFloat() {
    int size = collector.size();
    return size > 0 ? collector.get((int) Math.round(percentile * size - .5)) : 0;
  }
  @Override
  public boolean exists() {
    return collector.size() > 0;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedFloatListCollector)sync.apply(collector);
    collector.calcPercentile(percentile);
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
class DoublePercentileFunction extends AbstractDoubleValue implements ReductionFunction {
  private SortedDoubleListCollector collector;
  private double percentile;
  public static final String name = PercentileFunction.name;
  private final String exprStr;

  public DoublePercentileFunction(DoubleValueStream param, double percentile) {
    this.collector = new SortedDoubleListCollector(param);
    this.percentile = percentile;
    this.exprStr = PercentileFunction.createPercentileExpressionString(param, percentile);
  }

  @Override
  public double getDouble() {
    int size = collector.size();
    return size > 0 ? collector.get((int) Math.round(percentile * size - .5)) : 0;
  }
  @Override
  public boolean exists() {
    return collector.size() > 0;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedDoubleListCollector)sync.apply(collector);
    collector.calcPercentile(percentile);
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
class DatePercentileFunction extends AbstractDateValue implements ReductionFunction {
  private SortedLongListCollector collector;
  private double percentile;
  public static final String name = PercentileFunction.name;
  private final String exprStr;

  public DatePercentileFunction(LongValueStream param, double percentile) {
    this.collector = new SortedLongListCollector(param);
    this.percentile = percentile;
    this.exprStr = PercentileFunction.createPercentileExpressionString(param, percentile);
  }

  @Override
  public long getLong() {
    int size = collector.size();
    return size > 0 ? collector.get((int) Math.round(percentile * size - .5)) : 0;
  }
  @Override
  public boolean exists() {
    return collector.size() > 0;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedLongListCollector)sync.apply(collector);
    collector.calcPercentile(percentile);
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
class StringPercentileFunction extends AbstractStringValue implements ReductionFunction {
  private SortedStringListCollector collector;
  private double percentile;
  public static final String name = PercentileFunction.name;
  private final String exprStr;

  public StringPercentileFunction(StringValueStream param, double percentile) {
    this.collector = new SortedStringListCollector(param);
    this.percentile = percentile;
    this.exprStr = PercentileFunction.createPercentileExpressionString(param, percentile);
  }

  @Override
  public String getString() {
    int size = collector.size();
    return size > 0 ? collector.get((int) Math.round(percentile * size - .5)) : null;
  }
  @Override
  public boolean exists() {
    return collector.size() > 0;
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedStringListCollector)sync.apply(collector);
    collector.calcPercentile(percentile);
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
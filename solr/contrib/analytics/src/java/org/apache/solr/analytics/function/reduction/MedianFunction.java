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
import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedDoubleListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedFloatListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedIntListCollector;
import org.apache.solr.analytics.function.reduction.data.SortedListCollector.SortedLongListCollector;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.DateValue.AbstractDateValue;
import org.apache.solr.analytics.value.DoubleValue.AbstractDoubleValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A reduction function which returns the median value of the given expression.
 */
public class MedianFunction {
  public static final String name = "median";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramater, " + params.length + " found.");
    }
    AnalyticsValueStream param = params[0];
    if (param instanceof DateValueStream) {
      return new DateMedianFunction((DateValueStream)param);
    } else if (param instanceof IntValueStream) {
      return new IntMedianFunction((IntValueStream)param);
    } else if (param instanceof LongValueStream) {
      return new LongMedianFunction((LongValueStream)param);
    } else if (param instanceof FloatValueStream) {
      return new FloatMedianFunction((FloatValueStream)param);
    } else if (param instanceof DoubleValueStream) {
      return new DoubleMedianFunction((DoubleValueStream)param);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a date or numeric parameter.");
  });

  abstract static class NumericMedianFunction<T extends Comparable<T>> extends AbstractDoubleValue implements ReductionFunction {
    protected SortedListCollector<T> collector;
    public static final String name = MedianFunction.name;
    private final String exprStr;

    public NumericMedianFunction(DoubleValueStream param, SortedListCollector<T> collector) {
      this.collector = collector;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    protected abstract double collectOrd(int ord);

    @Override
    public double getDouble() {
      int size = collector.size();
      if (size == 0) {
        return 0;
      }
      if (size % 2 == 0) {
        return (collectOrd(size/2) + collectOrd(size/2 - 1))/2;
      } else {
        return collectOrd(size/2);
      }
    }
    @Override
    public boolean exists() {
      return collector.size() > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (SortedListCollector<T>)sync.apply(collector);
      collector.calcMedian();
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

  static class IntMedianFunction extends NumericMedianFunction<Integer> {
    public IntMedianFunction(IntValueStream param) {
      super((DoubleValueStream) param, new SortedIntListCollector(param));
    }

    @Override
    protected double collectOrd(int ord) {
      return collector.get(ord);
    }
  }

  static class LongMedianFunction extends NumericMedianFunction<Long> {
    public LongMedianFunction(LongValueStream param) {
      super((DoubleValueStream) param, new SortedLongListCollector(param));
    }

    @Override
    protected double collectOrd(int ord) {
      return collector.get(ord);
    }
  }

  static class FloatMedianFunction extends NumericMedianFunction<Float> {
    public FloatMedianFunction(FloatValueStream param) {
      super((DoubleValueStream) param, new SortedFloatListCollector(param));
    }

    @Override
    protected double collectOrd(int ord) {
      return collector.get(ord);
    }
  }

  static class DoubleMedianFunction extends NumericMedianFunction<Double> {
    public DoubleMedianFunction(DoubleValueStream param) {
      super(param, new SortedDoubleListCollector(param));
    }

    @Override
    protected double collectOrd(int ord) {
      return collector.get(ord);
    }
  }

  static class DateMedianFunction extends AbstractDateValue implements ReductionFunction {
    private SortedLongListCollector collector;
    public static final String name = MedianFunction.name;
    private final String exprStr;

    public DateMedianFunction(DateValueStream param) {
      this.collector = new SortedLongListCollector(param);
      this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    }

    @Override
    public long getLong() {
      int size = collector.size();
      if (size == 0) {
        return 0;
      }
      if (size % 2 == 0) {
        return (collector.get(size/2) + collector.get(size/2 - 1))/2;
      } else {
        return collector.get(size/2);
      }
    }
    @Override
    public boolean exists() {
      return collector.size() > 0;
    }

    @Override
    public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
      collector = (SortedLongListCollector)sync.apply(collector);
      collector.calcMedian();
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


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
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValue;
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
 * A reduction function which returns the given ordinal of the sorted values of the given expression.
 */
public class OrdinalFunction {
  public static final String name = "ordinal";
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramater, " + params.length + " found.");
    }
    AnalyticsValueStream percValue = params[0];
    int ord = 0;
    if (params[0] instanceof IntValue && params[0] instanceof ConstantValue) {
      ord = ((IntValue)percValue).getInt();
      if (ord == 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires the ordinal to be >= 1 or <= -1, 0 is not accepted.");
      }
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a constant int value (the ordinal) as the first argument.");
    }
    AnalyticsValueStream param = params[1];
    if (param instanceof DateValueStream) {
      return new DateOrdinalFunction((DateValueStream)param, ord);
    }else if (param instanceof IntValueStream) {
      return new IntOrdinalFunction((IntValueStream)param, ord);
    } else if (param instanceof LongValueStream) {
      return new LongOrdinalFunction((LongValueStream)param, ord);
    } else if (param instanceof FloatValueStream) {
      return new FloatOrdinalFunction((FloatValueStream)param, ord);
    } else if (param instanceof DoubleValueStream) {
      return new DoubleOrdinalFunction((DoubleValueStream)param, ord);
    } else if (param instanceof StringValueStream) {
      return new StringOrdinalFunction((StringValueStream)param, ord);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a comparable parameter.");
  });

  protected static String createOrdinalExpressionString(AnalyticsValueStream param, double ord) {
    return String.format(Locale.ROOT, "%s(%s,%s)",
                         name,
                         ord,
                         param.getExpressionStr());
  }
}
class IntOrdinalFunction extends AbstractIntValue implements ReductionFunction {
  private SortedIntListCollector collector;
  private int ordinal;
  public static final String name = OrdinalFunction.name;
  private final String exprStr;

  public IntOrdinalFunction(IntValueStream param, int ordinal) {
    this.collector = new SortedIntListCollector(param);
    this.ordinal = ordinal;
    this.exprStr = OrdinalFunction.createOrdinalExpressionString(param, ordinal);
  }

  @Override
  public int getInt() {
    int size = collector.size();
    if (ordinal > 0) {
      return ordinal <= size ? collector.get(ordinal - 1) : 0;
    } else {
      return (ordinal * -1) <= size ? collector.get(size + ordinal) : 0;
    }
  }
  @Override
  public boolean exists() {
    return (ordinal > 0 ? ordinal : (ordinal * -1)) <= collector.size();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedIntListCollector)sync.apply(collector);
    collector.calcOrdinal(ordinal);
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
class LongOrdinalFunction extends AbstractLongValue implements ReductionFunction {
  private SortedLongListCollector collector;
  private int ordinal;
  public static final String name = OrdinalFunction.name;
  private final String exprStr;

  public LongOrdinalFunction(LongValueStream param, int ordinal) {
    this.collector = new SortedLongListCollector(param);
    this.ordinal = ordinal;
    this.exprStr = OrdinalFunction.createOrdinalExpressionString(param, ordinal);
  }

  @Override
  public long getLong() {
    int size = collector.size();
    if (ordinal > 0) {
      return ordinal <= size ? collector.get(ordinal - 1) : 0;
    } else {
      return (ordinal * -1) <= size ? collector.get(size + ordinal) : 0;
    }
  }
  @Override
  public boolean exists() {
    return (ordinal > 0 ? ordinal : (ordinal * -1)) <= collector.size();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedLongListCollector)sync.apply(collector);
    collector.calcOrdinal(ordinal);
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
class FloatOrdinalFunction extends AbstractFloatValue implements ReductionFunction {
  private SortedFloatListCollector collector;
  private int ordinal;
  public static final String name = OrdinalFunction.name;
  private final String exprStr;

  public FloatOrdinalFunction(FloatValueStream param, int ordinal) {
    this.collector = new SortedFloatListCollector(param);
    this.ordinal = ordinal;
    this.exprStr = OrdinalFunction.createOrdinalExpressionString(param, ordinal);
  }

  @Override
  public float getFloat() {
    int size = collector.size();
    if (ordinal > 0) {
      return ordinal <= size ? collector.get(ordinal - 1) : 0;
    } else {
      return (ordinal * -1) <= size ? collector.get(size + ordinal) : 0;
    }
  }
  @Override
  public boolean exists() {
    return (ordinal > 0 ? ordinal : (ordinal * -1)) <= collector.size();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedFloatListCollector)sync.apply(collector);
    collector.calcOrdinal(ordinal);
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
class DoubleOrdinalFunction extends AbstractDoubleValue implements ReductionFunction {
  private SortedDoubleListCollector collector;
  private int ordinal;
  public static final String name = OrdinalFunction.name;
  private final String exprStr;

  public DoubleOrdinalFunction(DoubleValueStream param, int ordinal) {
    this.collector = new SortedDoubleListCollector(param);
    this.ordinal = ordinal;
    this.exprStr = OrdinalFunction.createOrdinalExpressionString(param, ordinal);
  }

  @Override
  public double getDouble() {
    int size = collector.size();
    if (ordinal > 0) {
      return ordinal <= size ? collector.get(ordinal - 1) : 0;
    } else {
      return (ordinal * -1) <= size ? collector.get(size + ordinal) : 0;
    }
  }
  @Override
  public boolean exists() {
    return (ordinal > 0 ? ordinal : (ordinal * -1)) <= collector.size();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedDoubleListCollector)sync.apply(collector);
    collector.calcOrdinal(ordinal);
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
class DateOrdinalFunction extends AbstractDateValue implements ReductionFunction {
  private SortedLongListCollector collector;
  private int ordinal;
  public static final String name = OrdinalFunction.name;
  private final String exprStr;

  public DateOrdinalFunction(LongValueStream param, int ordinal) {
    this.collector = new SortedLongListCollector(param);
    this.ordinal = ordinal;
    this.exprStr = OrdinalFunction.createOrdinalExpressionString(param, ordinal);
  }

  @Override
  public long getLong() {
    int size = collector.size();
    if (ordinal > 0) {
      return ordinal <= size ? collector.get(ordinal - 1) : 0;
    } else {
      return (ordinal * -1) <= size ? collector.get(size + ordinal) : 0;
    }
  }
  @Override
  public boolean exists() {
    return (ordinal > 0 ? ordinal : (ordinal * -1)) <= collector.size();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedLongListCollector)sync.apply(collector);
    collector.calcOrdinal(ordinal);
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
class StringOrdinalFunction extends AbstractStringValue implements ReductionFunction {
  private SortedStringListCollector collector;
  private int ordinal;
  public static final String name = OrdinalFunction.name;
  private final String exprStr;

  public StringOrdinalFunction(StringValueStream param, int ordinal) {
    this.collector = new SortedStringListCollector(param);
    this.ordinal = ordinal;
    this.exprStr = OrdinalFunction.createOrdinalExpressionString(param, ordinal);
  }

  @Override
  public String getString() {
    int size = collector.size();
    if (ordinal > 0) {
      return ordinal <= size ? collector.get(ordinal - 1) : null;
    } else {
      return (ordinal * -1) <= size ? collector.get(size + ordinal) : null;
    }
  }
  @Override
  public boolean exists() {
    return (ordinal > 0 ? ordinal : (ordinal * -1)) <= collector.size();
  }

  @Override
  public void synchronizeDataCollectors(UnaryOperator<ReductionDataCollector<?>> sync) {
    collector = (SortedStringListCollector)sync.apply(collector);
    collector.calcOrdinal(ordinal);
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
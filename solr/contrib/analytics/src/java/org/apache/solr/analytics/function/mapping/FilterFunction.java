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

import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.value.AnalyticsValue;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValue;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValue;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValue;
import org.apache.solr.analytics.value.StringValueStream;
import org.apache.solr.analytics.value.AnalyticsValue.AbstractAnalyticsValue;
import org.apache.solr.analytics.value.AnalyticsValueStream.AbstractAnalyticsValueStream;
import org.apache.solr.analytics.value.BooleanValue.AbstractBooleanValue;
import org.apache.solr.analytics.value.BooleanValueStream.AbstractBooleanValueStream;
import org.apache.solr.analytics.value.DateValue.AbstractDateValue;
import org.apache.solr.analytics.value.DateValueStream.AbstractDateValueStream;
import org.apache.solr.analytics.value.DoubleValue.AbstractDoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream.AbstractDoubleValueStream;
import org.apache.solr.analytics.value.FloatValue.AbstractFloatValue;
import org.apache.solr.analytics.value.FloatValueStream.AbstractFloatValueStream;
import org.apache.solr.analytics.value.IntValue.AbstractIntValue;
import org.apache.solr.analytics.value.IntValueStream.AbstractIntValueStream;
import org.apache.solr.analytics.value.LongValue.AbstractLongValue;
import org.apache.solr.analytics.value.LongValueStream.AbstractLongValueStream;
import org.apache.solr.analytics.value.StringValue.AbstractStringValue;
import org.apache.solr.analytics.value.StringValueStream.AbstractStringValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A mapping function to filter a Value or ValueStream. For each document, the value exists if the second parameter
 * is true and it doesn't exist otherwise.
 * <p>
 * The first parameter can be any type of analytics expression. (Required)
 * <br>
 * The second parameter must be a {@link BooleanValue}. (Required)
 */
public class FilterFunction {
  public static final String name = "filter";

  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramaters, " + params.length + " found.");
    }
    if (!(params[1] instanceof BooleanValue)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires the second paramater to be single-valued and boolean.");
    }

    AnalyticsValueStream baseExpr = params[0];
    BooleanValue filterExpr = (BooleanValue)params[1];

    if (baseExpr instanceof DateValue) {
      return new DateFilterFunction((DateValue)baseExpr,filterExpr);
    }
    if (baseExpr instanceof DateValueStream) {
      return new DateStreamFilterFunction((DateValueStream)baseExpr,filterExpr);
    }
    if (baseExpr instanceof BooleanValue) {
      return new BooleanFilterFunction((BooleanValue)baseExpr,filterExpr);
    }
    if (baseExpr instanceof BooleanValueStream) {
      return new BooleanStreamFilterFunction((BooleanValueStream)baseExpr,filterExpr);
    }
    if (baseExpr instanceof IntValue) {
      return new IntFilterFunction((IntValue)baseExpr,filterExpr);
    }
    if (baseExpr instanceof IntValueStream) {
      return new IntStreamFilterFunction((IntValueStream)baseExpr,filterExpr);
    }
    if (baseExpr instanceof LongValue) {
      return new LongFilterFunction((LongValue)baseExpr,filterExpr);
    }
    if (baseExpr instanceof LongValueStream) {
      return new LongStreamFilterFunction((LongValueStream)baseExpr,filterExpr);
    }
    if (baseExpr instanceof FloatValue) {
      return new FloatFilterFunction((FloatValue)baseExpr,filterExpr);
    }
    if (baseExpr instanceof FloatValueStream) {
      return new FloatStreamFilterFunction((FloatValueStream)baseExpr,filterExpr);
    }
    if (baseExpr instanceof DoubleValue) {
      return new DoubleFilterFunction((DoubleValue)baseExpr,filterExpr);
    }
    if (baseExpr instanceof DoubleValueStream) {
      return new DoubleStreamFilterFunction((DoubleValueStream)baseExpr,filterExpr);
    }
    if (baseExpr instanceof StringValue) {
      return new StringFilterFunction((StringValue)baseExpr,filterExpr);
    }
    if (baseExpr instanceof StringValueStream) {
      return new StringStreamFilterFunction((StringValueStream)baseExpr,filterExpr);
    }
    if (baseExpr instanceof AnalyticsValue) {
      return new ValueFilterFunction((AnalyticsValue)baseExpr,filterExpr);
    }
    return new StreamFilterFunction(baseExpr,filterExpr);
  });

  static class StreamFilterFunction extends AbstractAnalyticsValueStream {
    private final AnalyticsValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StreamFilterFunction(AnalyticsValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamObjects(Consumer<Object> cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamObjects(cons);
      }
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
      return funcType;
    }
  }

  static class ValueFilterFunction extends AbstractAnalyticsValue {
    private final AnalyticsValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public ValueFilterFunction(AnalyticsValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public Object getObject() {
      Object value = baseExpr.getObject();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }

  static class BooleanStreamFilterFunction extends AbstractBooleanValueStream {
    private final BooleanValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public BooleanStreamFilterFunction(BooleanValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamBooleans(BooleanConsumer cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamBooleans(cons);
      }
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
      return funcType;
    }
  }

  static class BooleanFilterFunction extends AbstractBooleanValue {
    private final BooleanValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public BooleanFilterFunction(BooleanValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public boolean getBoolean() {
      boolean value = baseExpr.getBoolean();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }

  static class IntStreamFilterFunction extends AbstractIntValueStream {
    private final IntValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public IntStreamFilterFunction(IntValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamInts(IntConsumer cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamInts(cons);
      }
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
      return funcType;
    }
  }

  static class IntFilterFunction extends AbstractIntValue {
    private final IntValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public IntFilterFunction(IntValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public int getInt() {
      int value = baseExpr.getInt();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }

  static class LongStreamFilterFunction extends AbstractLongValueStream {
    private final LongValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public LongStreamFilterFunction(LongValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamLongs(cons);
      }
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
      return funcType;
    }
  }

  static class LongFilterFunction extends AbstractLongValue {
    private final LongValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public LongFilterFunction(LongValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public long getLong() {
      long value = baseExpr.getLong();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }

  static class FloatStreamFilterFunction extends AbstractFloatValueStream {
    private final FloatValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public FloatStreamFilterFunction(FloatValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamFloats(FloatConsumer cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamFloats(cons);
      }
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
      return funcType;
    }
  }

  static class FloatFilterFunction extends AbstractFloatValue {
    private final FloatValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public FloatFilterFunction(FloatValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public float getFloat() {
      float value = baseExpr.getFloat();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }

  static class DoubleStreamFilterFunction extends AbstractDoubleValueStream {
    private final DoubleValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DoubleStreamFilterFunction(DoubleValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamDoubles(DoubleConsumer cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamDoubles(cons);
      }
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
      return funcType;
    }
  }

  static class DoubleFilterFunction extends AbstractDoubleValue {
    private final DoubleValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DoubleFilterFunction(DoubleValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public double getDouble() {
      double value = baseExpr.getDouble();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }

  static class DateStreamFilterFunction extends AbstractDateValueStream {
    private final DateValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateStreamFilterFunction(DateValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamLongs(cons);
      }
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
      return funcType;
    }
  }

  static class DateFilterFunction extends AbstractDateValue {
    private final DateValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateFilterFunction(DateValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public long getLong() {
      long value = baseExpr.getLong();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }

  static class StringStreamFilterFunction extends AbstractStringValueStream {
    private final StringValueStream baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StringStreamFilterFunction(StringValueStream baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    @Override
    public void streamStrings(Consumer<String> cons) {
      if (filterExpr.getBoolean() && filterExpr.exists()) {
        baseExpr.streamStrings(cons);
      }
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
      return funcType;
    }
  }

  static class StringFilterFunction extends AbstractStringValue {
    private final StringValue baseExpr;
    private final BooleanValue filterExpr;
    public static final String name = FilterFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StringFilterFunction(StringValue baseExpr, BooleanValue filterExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.filterExpr = filterExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,filterExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,filterExpr);
    }

    boolean exists = false;

    @Override
    public String getString() {
      String value = baseExpr.getString();
      exists = baseExpr.exists() && filterExpr.getBoolean() && filterExpr.exists();
      return value;
    }
    @Override
    public boolean exists() {
      return exists;
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
      return funcType;
    }
  }
}


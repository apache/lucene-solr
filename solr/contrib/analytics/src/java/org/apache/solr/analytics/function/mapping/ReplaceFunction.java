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
 * A mapping function to replace an {@link AnalyticsValue} from an {@link AnalyticsValue} or an {@link AnalyticsValueStream}
 * with a different {@link AnalyticsValue}.
 * For each document, all values from the base parameter matching the comparison parameter will be replaced with the fill parameter.
 * <p>
 * The first parameter can be any type of analytics expression. If the parameter is multi-valued, then the return will be multi-valued. (Required)
 * <br>
 * The second parameter, which is the value to compare to the first parameter, must be an {@link AnalyticsValue}, aka single-valued. (Required)
 * <br>
 * The third parameter, which is the value to fill the first parameter with, must be an {@link AnalyticsValue}, aka single-valued. (Required)
 * <p>
 * The resulting Value or ValueStream will be typed with the closest super-type of the three parameters.
 * (e.g. {@value #name}(double,int,float) will return a double)
 */
public class ReplaceFunction {
  public static final String name = "replace";

  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 3) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 3 paramaters, " + params.length + " found.");
    }
    if (!(params[1] instanceof AnalyticsValue && params[2] instanceof AnalyticsValue)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires the comparator and fill parameters to be single-valued.");
    }

    AnalyticsValueStream baseExpr = params[0];
    AnalyticsValue compExpr = (AnalyticsValue)params[1];
    AnalyticsValue fillExpr = (AnalyticsValue)params[2];

    if (baseExpr instanceof DateValue && compExpr instanceof DateValue && fillExpr instanceof DateValue) {
      return new DateReplaceFunction((DateValue)baseExpr,(DateValue)compExpr,(DateValue)fillExpr);
    }
    if (baseExpr instanceof DateValueStream && compExpr instanceof DateValue && fillExpr instanceof DateValue) {
      return new DateStreamReplaceFunction((DateValueStream)baseExpr,(DateValue)compExpr,(DateValue)fillExpr);
    }
    if (baseExpr instanceof BooleanValue && compExpr instanceof BooleanValue && fillExpr instanceof BooleanValue) {
      return new BooleanReplaceFunction((BooleanValue)baseExpr,(BooleanValue)compExpr,(BooleanValue)fillExpr);
    }
    if (baseExpr instanceof BooleanValueStream && compExpr instanceof BooleanValue && fillExpr instanceof BooleanValue) {
      return new BooleanStreamReplaceFunction((BooleanValueStream)baseExpr,(BooleanValue)compExpr,(BooleanValue)fillExpr);
    }
    if (baseExpr instanceof IntValue && compExpr instanceof IntValue && fillExpr instanceof IntValue) {
      return new IntReplaceFunction((IntValue)baseExpr,(IntValue)compExpr,(IntValue)fillExpr);
    }
    if (baseExpr instanceof IntValueStream && compExpr instanceof IntValue && fillExpr instanceof IntValue) {
      return new IntStreamReplaceFunction((IntValueStream)baseExpr,(IntValue)compExpr,(IntValue)fillExpr);
    }
    if (baseExpr instanceof LongValue && compExpr instanceof LongValue && fillExpr instanceof LongValue) {
      return new LongReplaceFunction((LongValue)baseExpr,(LongValue)compExpr,(LongValue)fillExpr);
    }
    if (baseExpr instanceof LongValueStream && compExpr instanceof LongValue && fillExpr instanceof LongValue) {
      return new LongStreamReplaceFunction((LongValueStream)baseExpr,(LongValue)compExpr,(LongValue)fillExpr);
    }
    if (baseExpr instanceof FloatValue && compExpr instanceof FloatValue && fillExpr instanceof FloatValue) {
      return new FloatReplaceFunction((FloatValue)baseExpr,(FloatValue)compExpr,(FloatValue)fillExpr);
    }
    if (baseExpr instanceof FloatValueStream && compExpr instanceof FloatValue && fillExpr instanceof FloatValue) {
      return new FloatStreamReplaceFunction((FloatValueStream)baseExpr,(FloatValue)compExpr,(FloatValue)fillExpr);
    }
    if (baseExpr instanceof DoubleValue && compExpr instanceof DoubleValue && fillExpr instanceof DoubleValue) {
      return new DoubleReplaceFunction((DoubleValue)baseExpr,(DoubleValue)compExpr,(DoubleValue)fillExpr);
    }
    if (baseExpr instanceof DoubleValueStream && compExpr instanceof DoubleValue && fillExpr instanceof DoubleValue) {
      return new DoubleStreamReplaceFunction((DoubleValueStream)baseExpr,(DoubleValue)compExpr,(DoubleValue)fillExpr);
    }
    if (baseExpr instanceof StringValue && compExpr instanceof StringValue && fillExpr instanceof StringValue) {
      return new StringReplaceFunction((StringValue)baseExpr,(StringValue)compExpr,(StringValue)fillExpr);
    }
    if (baseExpr instanceof StringValueStream && compExpr instanceof StringValue && fillExpr instanceof StringValue) {
      return new StringStreamReplaceFunction((StringValueStream)baseExpr,(StringValue)compExpr,(StringValue)fillExpr);
    }
    if (baseExpr instanceof AnalyticsValue) {
      return new ValueReplaceFunction((AnalyticsValue)baseExpr,compExpr,fillExpr);
    }
    return new StreamReplaceFunction(baseExpr,compExpr,fillExpr);

  });

  static class StreamReplaceFunction extends AbstractAnalyticsValueStream {
    private final AnalyticsValueStream baseExpr;
    private final AnalyticsValue compExpr;
    private final AnalyticsValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StreamReplaceFunction(AnalyticsValueStream baseExpr, AnalyticsValue compExpr, AnalyticsValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamObjects(Consumer<Object> cons) {
      Object compValue = compExpr.getObject();
      if (compExpr.exists()) {
        final Object fillValue = fillExpr.getObject();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamObjects(value -> {
          if (value.equals(compValue)) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class ValueReplaceFunction extends AbstractAnalyticsValue {
    private final AnalyticsValue baseExpr;
    private final AnalyticsValue compExpr;
    private final AnalyticsValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public ValueReplaceFunction(AnalyticsValue baseExpr, AnalyticsValue compExpr, AnalyticsValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public Object getObject() {
      Object value = baseExpr.getObject();
      exists = baseExpr.exists();
      Object comp = compExpr.getObject();
      if (exists && compExpr.exists() && value.equals(comp)) {
        value = fillExpr.getObject();
        exists = fillExpr.exists();
      }
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

  static class BooleanStreamReplaceFunction extends AbstractBooleanValueStream {
    private final BooleanValueStream baseExpr;
    private final BooleanValue compExpr;
    private final BooleanValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public BooleanStreamReplaceFunction(BooleanValueStream baseExpr, BooleanValue compExpr, BooleanValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamBooleans(BooleanConsumer cons) {
      boolean compValue = compExpr.getBoolean();
      if (compExpr.exists()) {
        final boolean fillValue = fillExpr.getBoolean();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamBooleans(value -> {
          if (value == compValue) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class BooleanReplaceFunction extends AbstractBooleanValue {
    private final BooleanValue baseExpr;
    private final BooleanValue compExpr;
    private final BooleanValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public BooleanReplaceFunction(BooleanValue baseExpr, BooleanValue compExpr, BooleanValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public boolean getBoolean() {
      boolean value = baseExpr.getBoolean();
      exists = baseExpr.exists();
      boolean comp = compExpr.getBoolean();
      if (exists && compExpr.exists() && value == comp) {
        value = fillExpr.getBoolean();
        exists = fillExpr.exists();
      }
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

  static class IntStreamReplaceFunction extends AbstractIntValueStream {
    private final IntValueStream baseExpr;
    private final IntValue compExpr;
    private final IntValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public IntStreamReplaceFunction(IntValueStream baseExpr, IntValue compExpr, IntValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamInts(IntConsumer cons) {
      int compValue = compExpr.getInt();
      if (compExpr.exists()) {
        final int fillValue = fillExpr.getInt();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamInts(value -> {
          if (value == compValue) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class IntReplaceFunction extends AbstractIntValue {
    private final IntValue baseExpr;
    private final IntValue compExpr;
    private final IntValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public IntReplaceFunction(IntValue baseExpr, IntValue compExpr, IntValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public int getInt() {
      int value = baseExpr.getInt();
      exists = baseExpr.exists();
      int comp = compExpr.getInt();
      if (exists && compExpr.exists() && value == comp) {
        value = fillExpr.getInt();
        exists = fillExpr.exists();
      }
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

  static class LongStreamReplaceFunction extends AbstractLongValueStream {
    private final LongValueStream baseExpr;
    private final LongValue compExpr;
    private final LongValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public LongStreamReplaceFunction(LongValueStream baseExpr, LongValue compExpr, LongValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      long compValue = compExpr.getLong();
      if (compExpr.exists()) {
        final long fillValue = fillExpr.getLong();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamLongs(value -> {
          if (value == compValue) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class LongReplaceFunction extends AbstractLongValue {
    private final LongValue baseExpr;
    private final LongValue compExpr;
    private final LongValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public LongReplaceFunction(LongValue baseExpr, LongValue compExpr, LongValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public long getLong() {
      long value = baseExpr.getLong();
      exists = baseExpr.exists();
      long comp = compExpr.getLong();
      if (exists && compExpr.exists() && value == comp) {
        value = fillExpr.getLong();
        exists = fillExpr.exists();
      }
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

  static class FloatStreamReplaceFunction extends AbstractFloatValueStream {
    private final FloatValueStream baseExpr;
    private final FloatValue compExpr;
    private final FloatValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public FloatStreamReplaceFunction(FloatValueStream baseExpr, FloatValue compExpr, FloatValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamFloats(FloatConsumer cons) {
      float compValue = compExpr.getFloat();
      if (compExpr.exists()) {
        final float fillValue = fillExpr.getFloat();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamFloats(value -> {
          if (value == compValue) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class FloatReplaceFunction extends AbstractFloatValue {
    private final FloatValue baseExpr;
    private final FloatValue compExpr;
    private final FloatValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public FloatReplaceFunction(FloatValue baseExpr, FloatValue compExpr, FloatValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public float getFloat() {
      float value = baseExpr.getFloat();
      exists = baseExpr.exists();
      float comp = compExpr.getFloat();
      if (exists && compExpr.exists() && value == comp) {
        value = fillExpr.getFloat();
        exists = fillExpr.exists();
      }
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

  static class DoubleStreamReplaceFunction extends AbstractDoubleValueStream {
    private final DoubleValueStream baseExpr;
    private final DoubleValue compExpr;
    private final DoubleValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DoubleStreamReplaceFunction(DoubleValueStream baseExpr, DoubleValue compExpr, DoubleValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamDoubles(DoubleConsumer cons) {
      double compValue = compExpr.getDouble();
      if (compExpr.exists()) {
        final double fillValue = fillExpr.getDouble();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamDoubles(value -> {
          if (value == compValue) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class DoubleReplaceFunction extends AbstractDoubleValue {
    private final DoubleValue baseExpr;
    private final DoubleValue compExpr;
    private final DoubleValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DoubleReplaceFunction(DoubleValue baseExpr, DoubleValue compExpr, DoubleValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public double getDouble() {
      double value = baseExpr.getDouble();
      exists = baseExpr.exists();
      double comp = compExpr.getDouble();
      if (exists && compExpr.exists() && value == comp) {
        value = fillExpr.getDouble();
        exists = fillExpr.exists();
      }
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

  static class DateStreamReplaceFunction extends AbstractDateValueStream {
    private final DateValueStream baseExpr;
    private final DateValue compExpr;
    private final DateValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateStreamReplaceFunction(DateValueStream baseExpr, DateValue compExpr, DateValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      long compValue = compExpr.getLong();
      if (compExpr.exists()) {
        final long fillValue = fillExpr.getLong();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamLongs(value -> {
          if (value == compValue) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class DateReplaceFunction extends AbstractDateValue {
    private final DateValue baseExpr;
    private final DateValue compExpr;
    private final DateValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateReplaceFunction(DateValue baseExpr, DateValue compExpr, DateValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public long getLong() {
      long value = baseExpr.getLong();
      exists = baseExpr.exists();
      long comp = compExpr.getLong();
      if (exists && compExpr.exists() && value == comp) {
        value = fillExpr.getLong();
        exists = fillExpr.exists();
      }
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

  static class StringStreamReplaceFunction extends AbstractStringValueStream {
    private final StringValueStream baseExpr;
    private final StringValue compExpr;
    private final StringValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StringStreamReplaceFunction(StringValueStream baseExpr, StringValue compExpr, StringValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    @Override
    public void streamStrings(Consumer<String> cons) {
      String compValue = compExpr.getString();
      if (compExpr.exists()) {
        final String fillValue = fillExpr.getString();
        final boolean fillExists = fillExpr.exists();
        baseExpr.streamStrings(value -> {
          if (value.equals(compValue)) {
            if (fillExists) {
              cons.accept(fillValue);
            }
          } else {
            cons.accept(value);
          }
        });
      }
      else {
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

  static class StringReplaceFunction extends AbstractStringValue {
    private final StringValue baseExpr;
    private final StringValue compExpr;
    private final StringValue fillExpr;
    public static final String name = ReplaceFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StringReplaceFunction(StringValue baseExpr, StringValue compExpr, StringValue fillExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.compExpr = compExpr;
      this.fillExpr = fillExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
    }

    boolean exists = false;

    @Override
    public String getString() {
      String value = baseExpr.getString();
      exists = baseExpr.exists();
      String comp = compExpr.getString();
      if (exists && compExpr.exists() && value.equals(comp)) {
        value = fillExpr.getString();
        exists = fillExpr.exists();
      }
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


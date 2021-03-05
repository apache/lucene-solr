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
import org.apache.solr.analytics.value.AnalyticsValueStream.AbstractAnalyticsValueStream;
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
 * A mapping function to remove an {@link AnalyticsValue} from an {@link AnalyticsValue} or an {@link AnalyticsValueStream}.
 * For each document, the value exists if it doesn't equal the value of the second parameter.
 * <p>
 * The first parameter can be any type of analytics expression. If the parameter is multi-valued, then the return will be multi-valued. (Required)
 * <br>
 * The second parameter, which is the value to remove from the first parameter, must be an {@link AnalyticsValue}, aka single-valued. (Required)
 * <p>
 * The resulting Value or ValueStream will be typed with the closest super-type of the two parameters.
 * (e.g. {@value #name}(int,float) will return a float)
 */
public class RemoveFunction {
  public static final String name = "remove";

  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramaters, " + params.length + " found.");
    }
    if (!(params[1] instanceof AnalyticsValue)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires the remove (2nd) paramater to be single-valued.");
    }

    AnalyticsValueStream baseExpr = params[0];
    AnalyticsValue removeExpr = (AnalyticsValue)params[1];

    if (baseExpr instanceof DateValue && removeExpr instanceof DateValue) {
      return new DateRemoveFunction((DateValue)baseExpr,(DateValue)removeExpr);
    }
    if (baseExpr instanceof DateValueStream && removeExpr instanceof DateValue) {
      return new DateStreamRemoveFunction((DateValueStream)baseExpr,(DateValue)removeExpr);
    }
    if (baseExpr instanceof BooleanValue && removeExpr instanceof BooleanValue) {
      return new BooleanRemoveFunction((BooleanValue)baseExpr,(BooleanValue)removeExpr);
    }
    if (baseExpr instanceof BooleanValueStream && removeExpr instanceof BooleanValue) {
      return new BooleanStreamRemoveFunction((BooleanValueStream)baseExpr,(BooleanValue)removeExpr);
    }
    if (baseExpr instanceof IntValue && removeExpr instanceof IntValue) {
      return new IntRemoveFunction((IntValue)baseExpr,(IntValue)removeExpr);
    }
    if (baseExpr instanceof IntValueStream && removeExpr instanceof IntValue) {
      return new IntStreamRemoveFunction((IntValueStream)baseExpr,(IntValue)removeExpr);
    }
    if (baseExpr instanceof LongValue && removeExpr instanceof LongValue) {
      return new LongRemoveFunction((LongValue)baseExpr,(LongValue)removeExpr);
    }
    if (baseExpr instanceof LongValueStream && removeExpr instanceof LongValue) {
      return new LongStreamRemoveFunction((LongValueStream)baseExpr,(LongValue)removeExpr);
    }
    if (baseExpr instanceof FloatValue && removeExpr instanceof FloatValue) {
      return new FloatRemoveFunction((FloatValue)baseExpr,(FloatValue)removeExpr);
    }
    if (baseExpr instanceof FloatValueStream && removeExpr instanceof FloatValue) {
      return new FloatStreamRemoveFunction((FloatValueStream)baseExpr,(FloatValue)removeExpr);
    }
    if (baseExpr instanceof DoubleValue && removeExpr instanceof DoubleValue) {
      return new DoubleRemoveFunction((DoubleValue)baseExpr,(DoubleValue)removeExpr);
    }
    if (baseExpr instanceof DoubleValueStream && removeExpr instanceof DoubleValue) {
      return new DoubleStreamRemoveFunction((DoubleValueStream)baseExpr,(DoubleValue)removeExpr);
    }
    if (baseExpr instanceof StringValue && removeExpr instanceof StringValue) {
      return new StringRemoveFunction((StringValue)baseExpr,(StringValue)removeExpr);
    }
    if (baseExpr instanceof StringValueStream && removeExpr instanceof StringValue) {
      return new StringStreamRemoveFunction((StringValueStream)baseExpr,(StringValue)removeExpr);
    }
    if (baseExpr instanceof AnalyticsValue) {
      return new ValueRemoveFunction((AnalyticsValue)baseExpr,removeExpr);
    }
    return new StreamRemoveFunction(baseExpr,removeExpr);
  });

  static class StreamRemoveFunction extends AbstractAnalyticsValueStream {
    private final AnalyticsValueStream baseExpr;
    private final AnalyticsValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StreamRemoveFunction(AnalyticsValueStream baseExpr, AnalyticsValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamObjects(Consumer<Object> cons) {
      Object removeValue = removeExpr.getObject();
      if (removeExpr.exists()) {
        baseExpr.streamObjects(value -> {
          if (!removeValue.equals(value)) cons.accept(value);
        });
      } else {
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

  static class ValueRemoveFunction extends AbstractAnalyticsValue {
    private final AnalyticsValue baseExpr;
    private final AnalyticsValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public ValueRemoveFunction(AnalyticsValue baseExpr, AnalyticsValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public Object getObject() {
      Object value = baseExpr.getObject();
      exists = false;
      if (baseExpr.exists()) {
        exists = value.equals(removeExpr.getObject()) ? (removeExpr.exists() ? false : true) : true;
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

  static class BooleanStreamRemoveFunction extends AbstractBooleanValueStream {
    private final BooleanValueStream baseExpr;
    private final BooleanValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public BooleanStreamRemoveFunction(BooleanValueStream baseExpr, BooleanValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamBooleans(BooleanConsumer cons) {
      boolean removeValue = removeExpr.getBoolean();
      if (removeExpr.exists()) {
        baseExpr.streamBooleans(value -> {
          if (removeValue != value) cons.accept(value);
        });
      } else {
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

  static class BooleanRemoveFunction extends AbstractBooleanValue {
    private final BooleanValue baseExpr;
    private final BooleanValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public BooleanRemoveFunction(BooleanValue baseExpr, BooleanValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public boolean getBoolean() {
      boolean value = baseExpr.getBoolean();
      exists = false;
      if (baseExpr.exists()) {
        exists = value==removeExpr.getBoolean() ? (removeExpr.exists() ? false : true) : true;
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

  static class IntStreamRemoveFunction extends AbstractIntValueStream {
    private final IntValueStream baseExpr;
    private final IntValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public IntStreamRemoveFunction(IntValueStream baseExpr, IntValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamInts(IntConsumer cons) {
      int removeValue = removeExpr.getInt();
      if (removeExpr.exists()) {
        baseExpr.streamInts(value -> {
          if (removeValue != value) cons.accept(value);
        });
      } else {
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

  static class IntRemoveFunction extends AbstractIntValue {
    private final IntValue baseExpr;
    private final IntValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public IntRemoveFunction(IntValue baseExpr, IntValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public int getInt() {
      int value = baseExpr.getInt();
      exists = false;
      if (baseExpr.exists()) {
        exists = value==removeExpr.getInt() ? (removeExpr.exists() ? false : true) : true;
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

  static class LongStreamRemoveFunction extends AbstractLongValueStream {
    private final LongValueStream baseExpr;
    private final LongValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public LongStreamRemoveFunction(LongValueStream baseExpr, LongValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      long removeValue = removeExpr.getLong();
      if (removeExpr.exists()) {
        baseExpr.streamLongs(value -> {
          if (removeValue != value) cons.accept(value);
        });
      } else {
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

  static class LongRemoveFunction extends AbstractLongValue {
    private final LongValue baseExpr;
    private final LongValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public LongRemoveFunction(LongValue baseExpr, LongValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public long getLong() {
      long value = baseExpr.getLong();
      exists = false;
      if (baseExpr.exists()) {
        exists = value==removeExpr.getLong() ? (removeExpr.exists() ? false : true) : true;
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

  static class FloatStreamRemoveFunction extends AbstractFloatValueStream {
    private final FloatValueStream baseExpr;
    private final FloatValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public FloatStreamRemoveFunction(FloatValueStream baseExpr, FloatValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamFloats(FloatConsumer cons) {
      float removeValue = removeExpr.getFloat();
      if (removeExpr.exists()) {
        baseExpr.streamFloats(value -> {
          if (removeValue != value) cons.accept(value);
        });
      } else {
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

  static class FloatRemoveFunction extends AbstractFloatValue {
    private final FloatValue baseExpr;
    private final FloatValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public FloatRemoveFunction(FloatValue baseExpr, FloatValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public float getFloat() {
      float value = baseExpr.getFloat();
      exists = false;
      if (baseExpr.exists()) {
        exists = value==removeExpr.getFloat() ? (removeExpr.exists() ? false : true) : true;
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

  static class DoubleStreamRemoveFunction extends AbstractDoubleValueStream {
    private final DoubleValueStream baseExpr;
    private final DoubleValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DoubleStreamRemoveFunction(DoubleValueStream baseExpr, DoubleValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamDoubles(DoubleConsumer cons) {
      double removeValue = removeExpr.getDouble();
      if (removeExpr.exists()) {
        baseExpr.streamDoubles(value -> {
          if (removeValue != value) cons.accept(value);
        });
      } else {
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

  static class DoubleRemoveFunction extends AbstractDoubleValue {
    private final DoubleValue baseExpr;
    private final DoubleValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DoubleRemoveFunction(DoubleValue baseExpr, DoubleValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public double getDouble() {
      double value = baseExpr.getDouble();
      exists = false;
      if (baseExpr.exists()) {
        exists = value==removeExpr.getDouble() ? (removeExpr.exists() ? false : true) : true;
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

  static class DateStreamRemoveFunction extends AbstractDateValueStream {
    private final DateValueStream baseExpr;
    private final DateValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateStreamRemoveFunction(DateValueStream baseExpr, DateValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      long removeValue = removeExpr.getLong();
      if (removeExpr.exists()) {
        baseExpr.streamLongs(value -> {
          if (removeValue != value) cons.accept(value);
        });
      } else {
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

  static class DateRemoveFunction extends AbstractDateValue {
    private final DateValue baseExpr;
    private final DateValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public DateRemoveFunction(DateValue baseExpr, DateValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public long getLong() {
      long value = baseExpr.getLong();
      exists = false;
      if (baseExpr.exists()) {
        exists = value==removeExpr.getLong() ? (removeExpr.exists() ? false : true) : true;
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

  static class StringStreamRemoveFunction extends AbstractStringValueStream {
    private final StringValueStream baseExpr;
    private final StringValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StringStreamRemoveFunction(StringValueStream baseExpr, StringValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    @Override
    public void streamStrings(Consumer<String> cons) {
      String removeValue = removeExpr.getString();
      if (removeExpr.exists()) {
        baseExpr.streamStrings(value -> {
          if (!removeValue.equals(value)) cons.accept(value);
        });
      } else {
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

  static class StringRemoveFunction extends AbstractStringValue {
    private final StringValue baseExpr;
    private final StringValue removeExpr;
    public static final String name = RemoveFunction.name;
    private final String exprStr;
    private final ExpressionType funcType;

    public StringRemoveFunction(StringValue baseExpr, StringValue removeExpr) throws SolrException {
      this.baseExpr = baseExpr;
      this.removeExpr = removeExpr;
      this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,removeExpr);
      this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,removeExpr);
    }

    boolean exists = false;

    @Override
    public String getString() {
      String value = baseExpr.getString();
      exists = false;
      if (baseExpr.exists()) {
        exists = value.equals(removeExpr.getString()) ? (removeExpr.exists() ? false : true) : true;
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


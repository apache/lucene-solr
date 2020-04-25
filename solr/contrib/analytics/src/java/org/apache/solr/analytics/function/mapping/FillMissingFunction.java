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
 * A mapping function to fill all non-existing values with a given value.
 * <p>
 * Uses:
 * <ul>
 * <li>If two Values are passed in, a Value mimicking the first parameter with the second parameter used as filler will be returned.
 * <li>If two ValueStreams are passed in, a ValueStream mimicking the first parameter with the second parameter used as filler will be returned.
 * </ul>
 * <p>
 * The resulting Value or ValueStream will be typed with the closest super-type of the two parameters.
 * (e.g. {@value #name}(double,int) will return a double)
 */
public class FillMissingFunction {
  public static final String name = "fill_missing";

  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 2) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 2 paramaters, " + params.length + " found.");
    }

    AnalyticsValueStream baseExpr = params[0];
    AnalyticsValueStream fillExpr = params[1];
    if (baseExpr instanceof DateValue && fillExpr instanceof DateValue) {
      return new DateFillMissingFunction((DateValue)baseExpr,(DateValue)fillExpr);
    }
    if (baseExpr instanceof DateValueStream && fillExpr instanceof DateValueStream) {
      return new DateStreamFillMissingFunction((DateValueStream)baseExpr,(DateValueStream)fillExpr);
    }
    if (baseExpr instanceof BooleanValue && fillExpr instanceof BooleanValue) {
      return new BooleanFillMissingFunction((BooleanValue)baseExpr,(BooleanValue)fillExpr);
    }
    if (baseExpr instanceof BooleanValueStream && fillExpr instanceof BooleanValueStream) {
      return new BooleanStreamFillMissingFunction((BooleanValueStream)baseExpr,(BooleanValueStream)fillExpr);
    }
    if (baseExpr instanceof IntValue && fillExpr instanceof IntValue) {
      return new IntFillMissingFunction((IntValue)baseExpr,(IntValue)fillExpr);
    }
    if (baseExpr instanceof IntValueStream && fillExpr instanceof IntValueStream) {
      return new IntStreamFillMissingFunction((IntValueStream)baseExpr,(IntValueStream)fillExpr);
    }
    if (baseExpr instanceof LongValue && fillExpr instanceof LongValue) {
      return new LongFillMissingFunction((LongValue)baseExpr,(LongValue)fillExpr);
    }
    if (baseExpr instanceof LongValueStream && fillExpr instanceof LongValueStream) {
      return new LongStreamFillMissingFunction((LongValueStream)baseExpr,(LongValueStream)fillExpr);
    }
    if (baseExpr instanceof FloatValue && fillExpr instanceof FloatValue) {
      return new FloatFillMissingFunction((FloatValue)baseExpr,(FloatValue)fillExpr);
    }
    if (baseExpr instanceof FloatValueStream && fillExpr instanceof FloatValueStream) {
      return new FloatStreamFillMissingFunction((FloatValueStream)baseExpr,(FloatValueStream)fillExpr);
    }
    if (baseExpr instanceof DoubleValue && fillExpr instanceof DoubleValue) {
      return new DoubleFillMissingFunction((DoubleValue)baseExpr,(DoubleValue)fillExpr);
    }
    if (baseExpr instanceof DoubleValueStream && fillExpr instanceof DoubleValueStream) {
      return new DoubleStreamFillMissingFunction((DoubleValueStream)baseExpr,(DoubleValueStream)fillExpr);
    }
    if (baseExpr instanceof StringValue && fillExpr instanceof StringValue) {
      return new StringFillMissingFunction((StringValue)baseExpr,(StringValue)fillExpr);
    }
    if (baseExpr instanceof StringValueStream && fillExpr instanceof StringValueStream) {
      return new StringStreamFillMissingFunction((StringValueStream)baseExpr,(StringValueStream)fillExpr);
    }
    if (baseExpr instanceof AnalyticsValue && fillExpr instanceof AnalyticsValue) {
      return new ValueFillMissingFunction((AnalyticsValue)baseExpr,(AnalyticsValue)fillExpr);
    }
    return new StreamFillMissingFunction(baseExpr,fillExpr);
  });
}
class StreamFillMissingFunction extends AbstractAnalyticsValueStream implements Consumer<Object> {
  private final AnalyticsValueStream baseExpr;
  private final AnalyticsValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StreamFillMissingFunction(AnalyticsValueStream baseExpr, AnalyticsValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  Consumer<Object> cons;

  @Override
  public void streamObjects(Consumer<Object> cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamObjects(this);
    if (!exists) {
      fillExpr.streamObjects(cons);
    }
  }
  @Override
  public void accept(Object value) {
    exists = true;
    cons.accept(value);
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
class ValueFillMissingFunction extends AbstractAnalyticsValue {
  private final AnalyticsValue baseExpr;
  private final AnalyticsValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public ValueFillMissingFunction(AnalyticsValue baseExpr, AnalyticsValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public Object getObject() {
    Object value = baseExpr.getObject();
    exists = true;
    if (!baseExpr.exists()) {
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
class BooleanStreamFillMissingFunction extends AbstractBooleanValueStream implements BooleanConsumer {
  private final BooleanValueStream baseExpr;
  private final BooleanValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanStreamFillMissingFunction(BooleanValueStream baseExpr, BooleanValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  BooleanConsumer cons;

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamBooleans(this);
    if (!exists) {
      fillExpr.streamBooleans(cons);
    }
  }
  @Override
  public void accept(boolean value) {
    exists = true;
    cons.accept(value);
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
class BooleanFillMissingFunction extends AbstractBooleanValue {
  private final BooleanValue baseExpr;
  private final BooleanValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanFillMissingFunction(BooleanValue baseExpr, BooleanValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public boolean getBoolean() {
    boolean value = baseExpr.getBoolean();
    exists = true;
    if (!baseExpr.exists()) {
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
class IntStreamFillMissingFunction extends AbstractIntValueStream implements IntConsumer {
  private final IntValueStream baseExpr;
  private final IntValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntStreamFillMissingFunction(IntValueStream baseExpr, IntValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  IntConsumer cons;

  @Override
  public void streamInts(IntConsumer cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamInts(this);
    if (!exists) {
      fillExpr.streamInts(cons);
    }
  }
  @Override
  public void accept(int value) {
    exists = true;
    cons.accept(value);
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
class IntFillMissingFunction extends AbstractIntValue {
  private final IntValue baseExpr;
  private final IntValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntFillMissingFunction(IntValue baseExpr, IntValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public int getInt() {
    int value = baseExpr.getInt();
    exists = true;
    if (!baseExpr.exists()) {
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
class LongStreamFillMissingFunction extends AbstractLongValueStream implements LongConsumer {
  private final LongValueStream baseExpr;
  private final LongValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongStreamFillMissingFunction(LongValueStream baseExpr, LongValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  LongConsumer cons;

  @Override
  public void streamLongs(LongConsumer cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamLongs(this);
    if (!exists) {
      fillExpr.streamLongs(cons);
    }
  }
  @Override
  public void accept(long value) {
    exists = true;
    cons.accept(value);
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
class LongFillMissingFunction extends AbstractLongValue {
  private final LongValue baseExpr;
  private final LongValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongFillMissingFunction(LongValue baseExpr, LongValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public long getLong() {
    long value = baseExpr.getLong();
    exists = true;
    if (!baseExpr.exists()) {
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
class FloatStreamFillMissingFunction extends AbstractFloatValueStream implements FloatConsumer {
  private final FloatValueStream baseExpr;
  private final FloatValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatStreamFillMissingFunction(FloatValueStream baseExpr, FloatValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  FloatConsumer cons;

  @Override
  public void streamFloats(FloatConsumer cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamFloats(this);
    if (!exists) {
      fillExpr.streamFloats(cons);
    }
  }
  @Override
  public void accept(float value) {
    exists = true;
    cons.accept(value);
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
class FloatFillMissingFunction extends AbstractFloatValue {
  private final FloatValue baseExpr;
  private final FloatValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatFillMissingFunction(FloatValue baseExpr, FloatValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public float getFloat() {
    float value = baseExpr.getFloat();
    exists = true;
    if (!baseExpr.exists()) {
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
class DoubleStreamFillMissingFunction extends AbstractDoubleValueStream implements DoubleConsumer {
  private final DoubleValueStream baseExpr;
  private final DoubleValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleStreamFillMissingFunction(DoubleValueStream baseExpr, DoubleValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  DoubleConsumer cons;

  @Override
  public void streamDoubles(DoubleConsumer cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamDoubles(this);
    if (!exists) {
      fillExpr.streamDoubles(cons);
    }
  }
  @Override
  public void accept(double value) {
    exists = true;
    cons.accept(value);
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
class DoubleFillMissingFunction extends AbstractDoubleValue {
  private final DoubleValue baseExpr;
  private final DoubleValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleFillMissingFunction(DoubleValue baseExpr, DoubleValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public double getDouble() {
    double value = baseExpr.getDouble();
    exists = true;
    if (!baseExpr.exists()) {
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
class DateStreamFillMissingFunction extends AbstractDateValueStream implements LongConsumer {
  private final DateValueStream baseExpr;
  private final DateValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateStreamFillMissingFunction(DateValueStream baseExpr, DateValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  LongConsumer cons;

  @Override
  public void streamLongs(LongConsumer cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamLongs(this);
    if (!exists) {
      fillExpr.streamLongs(cons);
    }
  }
  @Override
  public void accept(long value) {
    exists = true;
    cons.accept(value);
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
class DateFillMissingFunction extends AbstractDateValue {
  private final DateValue baseExpr;
  private final DateValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateFillMissingFunction(DateValue baseExpr, DateValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public long getLong() {
    long value = baseExpr.getLong();
    exists = true;
    if (!baseExpr.exists()) {
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
class StringStreamFillMissingFunction extends AbstractStringValueStream implements Consumer<String> {
  private final StringValueStream baseExpr;
  private final StringValueStream fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringStreamFillMissingFunction(StringValueStream baseExpr, StringValueStream fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;
  Consumer<String> cons;

  @Override
  public void streamStrings(Consumer<String> cons) {
    exists = false;
    this.cons = cons;
    baseExpr.streamStrings(this);
    if (!exists) {
      fillExpr.streamStrings(cons);
    }
  }
  @Override
  public void accept(String value) {
    exists = true;
    cons.accept(value);
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
class StringFillMissingFunction extends AbstractStringValue {
  private final StringValue baseExpr;
  private final StringValue fillExpr;
  public static final String name = FillMissingFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringFillMissingFunction(StringValue baseExpr, StringValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,fillExpr);
  }

  boolean exists = false;

  @Override
  public String getString() {
    String value = baseExpr.getString();
    exists = true;
    if (!baseExpr.exists()) {
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
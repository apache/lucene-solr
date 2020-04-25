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
 * An if-else mapping function.
 * <p>
 * Three arguments are required. The first, the conditional parameter, must be a {@link BooleanValue} and
 * the later two, the if and else parameters, can be any type of {@link AnalyticsValueStream}.
 * For each document, if the conditional value is true then the if-value is used otherwise the else-value is used.
 * <p>
 * The resulting Value or ValueStream will be typed with the closest super-type of the two non-conditional parameters.
 * (e.g. {@value #name}(boolean,double,int) will return a double)
 * If two {@link AnalyticsValue}s are passed as the if-else parameters, an {@link AnalyticsValue} will be returned.
 * If either parameter isn't single-valued, a {@link AnalyticsValueStream} will be returned.
 */
public class IfFunction extends AbstractAnalyticsValueStream {
  private final BooleanValue ifExpr;
  private final AnalyticsValueStream thenExpr;
  private final AnalyticsValueStream elseExpr;
  public static final String name = "if";
  private final String exprStr;
  private final ExpressionType funcType;
  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 3) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 3 paramaters, " + params.length + " found.");
    }
    if (!(params[0] instanceof BooleanValue)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires single-valued numeric parameters. " +
                      "Incorrect parameter: "+params[0].getExpressionStr());
    }

    BooleanValue castedIf = (BooleanValue) params[0];
    AnalyticsValueStream thenExpr = params[1];
    AnalyticsValueStream elseExpr = params[2];

    if (thenExpr instanceof DateValue && elseExpr instanceof DateValue) {
      return new DateIfFunction(castedIf,(DateValue)thenExpr,(DateValue)elseExpr);
    }
    if (thenExpr instanceof DateValueStream && elseExpr instanceof DateValueStream) {
      return new DateStreamIfFunction(castedIf,(DateValueStream)thenExpr,(DateValueStream)elseExpr);
    }
    if (thenExpr instanceof BooleanValue && elseExpr instanceof BooleanValue) {
      return new BooleanIfFunction(castedIf,(BooleanValue)thenExpr,(BooleanValue)elseExpr);
    }
    if (thenExpr instanceof BooleanValueStream && elseExpr instanceof BooleanValueStream) {
      return new BooleanStreamIfFunction(castedIf,(BooleanValueStream)thenExpr,(BooleanValueStream)elseExpr);
    }
    if (thenExpr instanceof IntValue && elseExpr instanceof IntValue) {
      return new IntIfFunction(castedIf,(IntValue)thenExpr,(IntValue)elseExpr);
    }
    if (thenExpr instanceof IntValueStream && elseExpr instanceof IntValueStream) {
      return new IntStreamIfFunction(castedIf,(IntValueStream)thenExpr,(IntValueStream)elseExpr);
    }
    if (thenExpr instanceof LongValue && elseExpr instanceof LongValue) {
      return new LongIfFunction(castedIf,(LongValue)thenExpr,(LongValue)elseExpr);
    }
    if (thenExpr instanceof LongValueStream && elseExpr instanceof LongValueStream) {
      return new LongStreamIfFunction(castedIf,(LongValueStream)thenExpr,(LongValueStream)elseExpr);
    }
    if (thenExpr instanceof FloatValue && elseExpr instanceof FloatValue) {
      return new FloatIfFunction(castedIf,(FloatValue)thenExpr,(FloatValue)elseExpr);
    }
    if (thenExpr instanceof FloatValueStream && elseExpr instanceof FloatValueStream) {
      return new FloatStreamIfFunction(castedIf,(FloatValueStream)thenExpr,(FloatValueStream)elseExpr);
    }
    if (thenExpr instanceof DoubleValue && elseExpr instanceof DoubleValue) {
      return new DoubleIfFunction(castedIf,(DoubleValue)thenExpr,(DoubleValue)elseExpr);
    }
    if (thenExpr instanceof DoubleValueStream && elseExpr instanceof DoubleValueStream) {
      return new DoubleStreamIfFunction(castedIf,(DoubleValueStream)thenExpr,(DoubleValueStream)elseExpr);
    }
    if (thenExpr instanceof StringValue && elseExpr instanceof StringValue) {
      return new StringIfFunction(castedIf,(StringValue)thenExpr,(StringValue)elseExpr);
    }
    if (thenExpr instanceof StringValueStream && elseExpr instanceof StringValueStream) {
      return new StringStreamIfFunction(castedIf,(StringValueStream)thenExpr,(StringValueStream)elseExpr);
    }
    if (thenExpr instanceof AnalyticsValue && elseExpr instanceof AnalyticsValue) {
      return new ValueIfFunction(castedIf,(AnalyticsValue)thenExpr,(AnalyticsValue)elseExpr);
    }
    return new IfFunction(castedIf,thenExpr,elseExpr);
  });

  public IfFunction(BooleanValue ifExpr, AnalyticsValueStream thenExpr, AnalyticsValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamObjects(Consumer<Object> cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamObjects(cons);
      }
      else {
        elseExpr.streamObjects(cons);
      }
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
class ValueIfFunction extends AbstractAnalyticsValue {
  private final BooleanValue ifExpr;
  private final AnalyticsValue thenExpr;
  private final AnalyticsValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public ValueIfFunction(BooleanValue ifExpr, AnalyticsValue thenExpr, AnalyticsValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public Object getObject() {
    exists = false;
    Object value = null;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getObject();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getObject();
        exists = elseExpr.exists();
      }
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
class BooleanStreamIfFunction extends AbstractBooleanValueStream {
  private final BooleanValue ifExpr;
  private final BooleanValueStream thenExpr;
  private final BooleanValueStream elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanStreamIfFunction(BooleanValue ifExpr, BooleanValueStream thenExpr, BooleanValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamBooleans(cons);
      }
      else {
        elseExpr.streamBooleans(cons);
      }
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
class BooleanIfFunction extends AbstractBooleanValue {
  private final BooleanValue ifExpr;
  private final BooleanValue thenExpr;
  private final BooleanValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public BooleanIfFunction(BooleanValue ifExpr, BooleanValue thenExpr, BooleanValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public boolean getBoolean() {
    exists = false;
    boolean value = false;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getBoolean();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getBoolean();
        exists = elseExpr.exists();
      }
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
class IntStreamIfFunction extends AbstractIntValueStream {
  private final BooleanValue ifExpr;
  private final IntValueStream thenExpr;
  private final IntValueStream elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntStreamIfFunction(BooleanValue ifExpr, IntValueStream thenExpr, IntValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamInts(IntConsumer cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamInts(cons);
      }
      else {
        elseExpr.streamInts(cons);
      }
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
class IntIfFunction extends AbstractIntValue {
  private final BooleanValue ifExpr;
  private final IntValue thenExpr;
  private final IntValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public IntIfFunction(BooleanValue ifExpr, IntValue thenExpr, IntValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public int getInt() {
    exists = false;
    int value = 0;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getInt();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getInt();
        exists = elseExpr.exists();
      }
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
class LongStreamIfFunction extends AbstractLongValueStream {
  private final BooleanValue ifExpr;
  private final LongValueStream thenExpr;
  private final LongValueStream elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongStreamIfFunction(BooleanValue ifExpr, LongValueStream thenExpr, LongValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamLongs(cons);
      }
      else {
        elseExpr.streamLongs(cons);
      }
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
class LongIfFunction extends AbstractLongValue {
  private final BooleanValue ifExpr;
  private final LongValue thenExpr;
  private final LongValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public LongIfFunction(BooleanValue ifExpr, LongValue thenExpr, LongValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public long getLong() {
    exists = false;
    long value = 0;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getLong();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getLong();
        exists = elseExpr.exists();
      }
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
class FloatStreamIfFunction extends AbstractFloatValueStream {
  private final BooleanValue ifExpr;
  private final FloatValueStream thenExpr;
  private final FloatValueStream elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatStreamIfFunction(BooleanValue ifExpr, FloatValueStream thenExpr, FloatValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamFloats(FloatConsumer cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamFloats(cons);
      }
      else {
        elseExpr.streamFloats(cons);
      }
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
class FloatIfFunction extends AbstractFloatValue {
  private final BooleanValue ifExpr;
  private final FloatValue thenExpr;
  private final FloatValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public FloatIfFunction(BooleanValue ifExpr, FloatValue thenExpr, FloatValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public float getFloat() {
    exists = false;
    float value = 0;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getFloat();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getFloat();
        exists = elseExpr.exists();
      }
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
class DoubleStreamIfFunction extends AbstractDoubleValueStream {
  private final BooleanValue ifExpr;
  private final DoubleValueStream thenExpr;
  private final DoubleValueStream elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleStreamIfFunction(BooleanValue ifExpr, DoubleValueStream thenExpr, DoubleValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamDoubles(DoubleConsumer cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamDoubles(cons);
      }
      else {
        elseExpr.streamDoubles(cons);
      }
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
class DoubleIfFunction extends AbstractDoubleValue {
  private final BooleanValue ifExpr;
  private final DoubleValue thenExpr;
  private final DoubleValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DoubleIfFunction(BooleanValue ifExpr, DoubleValue thenExpr, DoubleValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public double getDouble() {
    exists = false;
    double value = 0;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getDouble();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getDouble();
        exists = elseExpr.exists();
      }
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
class DateStreamIfFunction extends AbstractDateValueStream {
  private final BooleanValue ifExpr;
  private final DateValueStream thenExpr;
  private final DateValueStream elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateStreamIfFunction(BooleanValue ifExpr, DateValueStream thenExpr, DateValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamLongs(cons);
      }
      else {
        elseExpr.streamLongs(cons);
      }
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
class DateIfFunction extends AbstractDateValue {
  private final BooleanValue ifExpr;
  private final DateValue thenExpr;
  private final DateValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public DateIfFunction(BooleanValue ifExpr, DateValue thenExpr, DateValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public long getLong() {
    exists = false;
    long value = 0;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getLong();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getLong();
        exists = elseExpr.exists();
      }
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
class StringStreamIfFunction extends AbstractStringValueStream {
  private final BooleanValue ifExpr;
  private final StringValueStream thenExpr;
  private final StringValueStream elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringStreamIfFunction(BooleanValue ifExpr, StringValueStream thenExpr, StringValueStream elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  @Override
  public void streamStrings(Consumer<String> cons) {
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        thenExpr.streamStrings(cons);
      }
      else {
        elseExpr.streamStrings(cons);
      }
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
class StringIfFunction extends AbstractStringValue {
  private final BooleanValue ifExpr;
  private final StringValue thenExpr;
  private final StringValue elseExpr;
  public static final String name = IfFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;

  public StringIfFunction(BooleanValue ifExpr, StringValue thenExpr, StringValue elseExpr) throws SolrException {
    this.ifExpr = ifExpr;
    this.thenExpr = thenExpr;
    this.elseExpr = elseExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,ifExpr,thenExpr,elseExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,ifExpr,thenExpr,elseExpr);
  }

  private boolean exists = false;

  @Override
  public String getString() {
    exists = false;
    String value = null;
    boolean ifValue = ifExpr.getBoolean();
    if (ifExpr.exists()) {
      if (ifValue) {
        value = thenExpr.getString();
        exists = thenExpr.exists();
      }
      else {
        value = elseExpr.getString();
        exists = elseExpr.exists();
      }
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
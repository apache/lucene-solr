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

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValue;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValue;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValue;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.IntValue.AbstractIntValue;
import org.apache.solr.analytics.value.IntValueStream.AbstractIntValueStream;
import org.apache.solr.analytics.value.LongValue.AbstractLongValue;
import org.apache.solr.analytics.value.LongValueStream.AbstractLongValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * An abstract decimal numeric converting mapping function. For example "round()" would convert a float to an int and a double to a long.
 * <p>
 * Takes a numeric Double or Float ValueStream or Value and returns a Long or Int ValueStream or Value, respectively.
 */
public class DecimalNumericConversionFunction {

  /**
   * Create a numeric conversion mapping function.
   *
   * @param name the name of the function
   * @param fconv the method to convert floats to ints
   * @param dconv the method to convert doubles to longs
   * @param params the parameters of the function
   * @return an instance of the conversion function using the given parameters.
   */
  public static LongValueStream createDecimalConversionFunction(String name, ConvertFloatFunction fconv, ConvertDoubleFunction dconv, AnalyticsValueStream... params) {
    if (params.length != 1) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 1 paramaters, " + params.length + " found.");
    }
    AnalyticsValueStream param = params[0];
    if (param instanceof LongValueStream) {
      return (LongValueStream)param;
    }
    if (param instanceof FloatValueStream) {
      if (param instanceof FloatValue) {
        return new ConvertFloatValueFunction(name, (FloatValue)param, fconv);
      }
      return new ConvertFloatStreamFunction(name, (FloatValueStream)param, fconv);
    } else if (param instanceof DoubleValueStream) {
      if (param instanceof DoubleValue) {
        return new ConvertDoubleValueFunction(name, (DoubleValue)param, dconv);
      }
      return new ConvertDoubleStreamFunction(name, (DoubleValueStream)param, dconv);
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires a numeric parameter.");
    }
  }

  /**
   * A numeric mapping function that returns the floor of the input.
   */
  public static class FloorFunction {
    public static final String name = "floor";
    public static final CreatorFunction creatorFunction = (params -> {
      return DecimalNumericConversionFunction.createDecimalConversionFunction(name, val -> (int)Math.floor(val), val -> (long)Math.floor(val), params);
    });
  }

  /**
   * A numeric mapping function that returns the ceiling of the input.
   */
  public static class CeilingFunction {
    public static final String name = "ceil";
    public static final CreatorFunction creatorFunction = (params -> {
      return DecimalNumericConversionFunction.createDecimalConversionFunction(name, val -> (int)Math.ceil(val), val -> (long)Math.ceil(val), params);
    });
  }

  /**
   * A numeric mapping function that returns the rounded input.
   */
  public static class RoundFunction {
    public static final String name = "round";
    public static final CreatorFunction creatorFunction = (params -> {
      return DecimalNumericConversionFunction.createDecimalConversionFunction(name, val -> Math.round(val), val -> Math.round(val), params);
    });
  }

  @FunctionalInterface
  public static interface ConvertFloatFunction {
    public int convert(float value);
  }

  @FunctionalInterface
  public static interface ConvertDoubleFunction {
    public long convert(double value);
  }

  /**
   * A function to convert a {@link FloatValue} to a {@link IntValue}.
   */
  static class ConvertFloatValueFunction extends AbstractIntValue {
    private final String name;
    private final FloatValue param;
    private final ConvertFloatFunction conv;
    private final String funcStr;
    private final ExpressionType funcType;

    public ConvertFloatValueFunction(String name, FloatValue param, ConvertFloatFunction conv) {
      this.name = name;
      this.param = param;
      this.conv = conv;
      this.funcStr = AnalyticsValueStream.createExpressionString(name,param);
      this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,param);
    }

    @Override
    public int getInt() {
      return conv.convert(param.getFloat());
    }
    @Override
    public boolean exists() {
      return param.exists();
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return funcStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return funcType;
    }
  }

  /**
   * A function to convert a {@link FloatValueStream} to a {@link IntValueStream}.
   */
  static class ConvertFloatStreamFunction extends AbstractIntValueStream {
    private final String name;
    private final FloatValueStream param;
    private final ConvertFloatFunction conv;
    private final String funcStr;
    private final ExpressionType funcType;

    public ConvertFloatStreamFunction(String name, FloatValueStream param, ConvertFloatFunction conv) {
      this.name = name;
      this.param = param;
      this.conv = conv;
      this.funcStr = AnalyticsValueStream.createExpressionString(name,param);
      this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,param);
    }

    @Override
    public void streamInts(IntConsumer cons) {
      param.streamFloats( value -> cons.accept(conv.convert(value)));
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return funcStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return funcType;
    }
  }

  /**
   * A function to convert a {@link DoubleValue} to a {@link LongValue}.
   */
  static class ConvertDoubleValueFunction extends AbstractLongValue {
    private final String name;
    private final DoubleValue param;
    private final ConvertDoubleFunction conv;
    private final String funcStr;
    private final ExpressionType funcType;

    public ConvertDoubleValueFunction(String name, DoubleValue param, ConvertDoubleFunction conv) {
      this.name = name;
      this.param = param;
      this.conv = conv;
      this.funcStr = AnalyticsValueStream.createExpressionString(name,param);
      this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,param);
    }

    @Override
    public long getLong() {
      return conv.convert(param.getDouble());
    }
    @Override
    public boolean exists() {
      return param.exists();
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return funcStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return funcType;
    }
  }

  /**
   * A function to convert a {@link DoubleValueStream} to a {@link LongValueStream}.
   */
  static class ConvertDoubleStreamFunction extends AbstractLongValueStream {
    private final String name;
    private final DoubleValueStream param;
    private final ConvertDoubleFunction conv;
    private final String funcStr;
    private final ExpressionType funcType;

    public ConvertDoubleStreamFunction(String name, DoubleValueStream param, ConvertDoubleFunction conv) {
      this.name = name;
      this.param = param;
      this.conv = conv;
      this.funcStr = AnalyticsValueStream.createExpressionString(name,param);
      this.funcType = AnalyticsValueStream.determineMappingPhase(funcStr,param);
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      param.streamDoubles( value -> cons.accept(conv.convert(value)));
    }

    @Override
    public String getName() {
      return name;
    }
    @Override
    public String getExpressionStr() {
      return funcStr;
    }
    @Override
    public ExpressionType getExpressionType() {
      return funcType;
    }
  }
}


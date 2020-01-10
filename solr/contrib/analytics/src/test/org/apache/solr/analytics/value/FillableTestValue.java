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
package org.apache.solr.analytics.value;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.util.function.FloatConsumer;
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

public class FillableTestValue {
  public static class TestAnalyticsValue extends AbstractAnalyticsValue {
    private final ExpressionType expressionType;

    private Object value;
    private boolean exists;

    public TestAnalyticsValue() {
      this(ExpressionType.CONST);
    }

    public TestAnalyticsValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestAnalyticsValue setValue(Object value) {
      this.value = value;
      return this;
    }

    public TestAnalyticsValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public Object getObject() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_analytics_value"; }

    @Override
    public String getExpressionStr() { return "test_analytics_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestAnalyticsValueStream extends AbstractAnalyticsValueStream {
    private final ExpressionType expressionType;

    private Object[] values;

    public TestAnalyticsValueStream() {
      this(ExpressionType.CONST);
    }

    public TestAnalyticsValueStream(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestAnalyticsValueStream setValues(Object... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamObjects(Consumer<Object> cons) {
      for (int i = 0; i < values.length; ++i) {
        cons.accept(values[i]);
      }
    }

    @Override
    public String getName() { return "test_analytics_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_analytics_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestIntValue extends AbstractIntValue {
    private final ExpressionType expressionType;

    private int value;
    private boolean exists;

    public TestIntValue() {
      this(ExpressionType.CONST);
    }

    public TestIntValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestIntValue setValue(int value) {
      this.value = value;
      return this;
    }

    public TestIntValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public int getInt() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_int_value"; }

    @Override
    public String getExpressionStr() { return "test_int_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestIntValueStream extends AbstractIntValueStream {
    private int[] values;

    public TestIntValueStream() {
      this.values = new int[0];
    }

    public TestIntValueStream setValues(int... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamInts(IntConsumer cons) {
      for (int i = 0; i < values.length; ++i) {
        cons.accept(values[i]);
      }
    }

    @Override
    public String getName() { return "test_int_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_int_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return ExpressionType.UNREDUCED_MAPPING; }
  }

  public static class TestLongValue extends AbstractLongValue {
    private final ExpressionType expressionType;

    private long value;
    private boolean exists;

    public TestLongValue() {
      this(ExpressionType.CONST);
    }

    public TestLongValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestLongValue setValue(long value) {
      this.value = value;
      return this;
    }

    public TestLongValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public long getLong() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_long_value"; }

    @Override
    public String getExpressionStr() { return "test_long_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestLongValueStream extends AbstractLongValueStream {
    private long[] values;

    public TestLongValueStream() {
      this.values = new long[0];
    }

    public TestLongValueStream setValues(long... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      for (int i = 0; i < values.length; ++i) {
        cons.accept(values[i]);
      }
    }

    @Override
    public String getName() { return "test_long_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_long_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return ExpressionType.UNREDUCED_MAPPING; }
  }

  public static class TestFloatValue extends AbstractFloatValue {
    private final ExpressionType expressionType;

    private float value;
    private boolean exists;

    public TestFloatValue() {
      this(ExpressionType.CONST);
    }

    public TestFloatValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestFloatValue setValue(float value) {
      this.value = value;
      return this;
    }

    public TestFloatValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public float getFloat() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_float_value"; }

    @Override
    public String getExpressionStr() { return "test_float_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestFloatValueStream extends AbstractFloatValueStream {
    private float[] values;

    public TestFloatValueStream() {
      this.values = new float[0];
    }

    public TestFloatValueStream setValues(float... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamFloats(FloatConsumer cons) {
      for (int i = 0; i < values.length; ++i) {
        cons.accept(values[i]);
      }
    }

    @Override
    public String getName() { return "test_float_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_float_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return ExpressionType.UNREDUCED_MAPPING; }
  }

  public static class TestDoubleValue extends AbstractDoubleValue {
    private final ExpressionType expressionType;

    private double value;
    private boolean exists;

    public TestDoubleValue() {
      this(ExpressionType.CONST);
    }

    public TestDoubleValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestDoubleValue setValue(double value) {
      this.value = value;
      return this;
    }

    public TestDoubleValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public double getDouble() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_double_value"; }

    @Override
    public String getExpressionStr() { return "test_double_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestDoubleValueStream extends AbstractDoubleValueStream {
    private double[] values;

    public TestDoubleValueStream() {
      this.values = new double[0];
    }

    public TestDoubleValueStream setValues(double... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamDoubles(DoubleConsumer cons) {
      for (int i = 0; i < values.length; ++i) {
        cons.accept(values[i]);
      }
    }

    @Override
    public String getName() { return "test_double_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_double_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return ExpressionType.UNREDUCED_MAPPING; }
  }

  public static class TestBooleanValue extends AbstractBooleanValue {
    private final ExpressionType expressionType;

    private boolean value;
    private boolean exists;

    public TestBooleanValue() {
      this(ExpressionType.CONST);
    }

    public TestBooleanValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestBooleanValue setValue(boolean value) {
      this.value = value;
      return this;
    }

    public TestBooleanValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public boolean getBoolean() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_boolean_value"; }

    @Override
    public String getExpressionStr() { return "test_boolean_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestBooleanValueStream extends AbstractBooleanValueStream {
    private boolean[] values;

    public TestBooleanValueStream() {
      this.values = new boolean[0];
    }

    public TestBooleanValueStream setValues(boolean... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamBooleans(BooleanConsumer cons) {
      for (int i = 0; i < values.length; ++i) {
        cons.accept(values[i]);
      }
    }

    @Override
    public String getName() { return "test_boolean_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_boolean_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return ExpressionType.UNREDUCED_MAPPING; }
  }

  public static class TestDateValue extends AbstractDateValue {
    private final ExpressionType expressionType;

    private long value;
    private boolean exists;

    public TestDateValue() {
      this(ExpressionType.CONST);
    }

    public TestDateValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestDateValue setValue(String value) {
      try {
        this.value = Instant.parse(value).toEpochMilli();
      } catch (DateTimeParseException e) {
        this.value = 0;
      }
      return this;
    }

    public TestDateValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public long getLong() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_date_value"; }

    @Override
    public String getExpressionStr() { return "test_date_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestDateValueStream extends AbstractDateValueStream {
    private String[] values;

    public TestDateValueStream() {
      this.values = new String[0];
    }

    public TestDateValueStream setValues(String... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamLongs(LongConsumer cons) {
      for (int i = 0; i < values.length; ++i) {
        try {
          cons.accept(Instant.parse(values[i]).toEpochMilli());
        } catch (DateTimeParseException e) { }
      }
    }

    @Override
    public String getName() { return "test_date_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_date_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return ExpressionType.UNREDUCED_MAPPING; }
  }

  public static class TestStringValue extends AbstractStringValue {
    private final ExpressionType expressionType;

    private String value;
    private boolean exists;

    public TestStringValue() {
      this(ExpressionType.CONST);
    }

    public TestStringValue(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public TestStringValue setValue(String value) {
      this.value = value;
      return this;
    }

    public TestStringValue setExists(boolean exists) {
      this.exists = exists;
      return this;
    }

    @Override
    public String getString() {
      return value;
    }

    @Override
    public boolean exists() {
      return exists;
    }

    @Override
    public String getName() { return "test_string_value"; }

    @Override
    public String getExpressionStr() { return "test_string_value"; }

    @Override
    public ExpressionType getExpressionType() { return expressionType; }
  }

  public static class TestStringValueStream extends AbstractStringValueStream {
    private String[] values;

    public TestStringValueStream() {
      this.values = new String[0];
    }

    public TestStringValueStream setValues(String... values) {
      this.values = values;
      return this;
    }

    @Override
    public void streamStrings(Consumer<String> cons) {
      for (int i = 0; i < values.length; ++i) {
        cons.accept(values[i]);
      }
    }

    @Override
    public String getName() { return "test_string_value_stream"; }

    @Override
    public String getExpressionStr() { return "test_string_value_stream"; }

    @Override
    public ExpressionType getExpressionType() { return ExpressionType.UNREDUCED_MAPPING; }
  }
}

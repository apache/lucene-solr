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

import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.value.constant.ConstantIntValue;

/**
 * A single-valued analytics value that can be represented as an int.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #getInt()} and {@link #exists()},
 * resulting in different values on each call.
 */
public interface IntValue extends IntValueStream, AnalyticsValue {
  /**
   * Get the int representation of the current value.
   * <p>
   * NOTE: The value returned is not valid unless calling {@link #exists()} afterwards returns {@code TRUE}.
   *
   * @return the current value
   */
  int getInt();

  /**
   * An interface that represents all of the types a {@link IntValue} should be able to cast to.
   */
  public static interface CastingIntValue extends IntValue, LongValue, FloatValue,DoubleValue, StringValue, ComparableValue { }

  /**
   * An abstract base for {@link CastingIntValue} that automatically casts to all types if {@link #getInt()} and {@link #exists()} are implemented.
   */
  public static abstract class AbstractIntValue implements CastingIntValue {
    @Override
    public long getLong() {
      return getInt();
    }
    @Override
    public float getFloat() {
      return getInt();
    }
    @Override
    public double getDouble() {
      return getInt();
    }
    @Override
    public String getString() {
      int val = getInt();
      return exists() ? Integer.toString(val) : null;
    }
    @Override
    public Object getObject() {
      int val = getInt();
      return exists() ? val : null;
    }
    @Override
    public void streamInts(IntConsumer cons) {
      int val = getInt();
      if (exists()) {
        cons.accept(val);
      }
    }
    @Override
    public void streamLongs(LongConsumer cons) {
      long val = getLong();
      if (exists()) {
        cons.accept(val);
      }
    }
    @Override
    public void streamFloats(FloatConsumer cons) {
      float val = getFloat();
      if (exists()) {
        cons.accept(val);
      }
    }
    @Override
    public void streamDoubles(DoubleConsumer cons) {
      double val = getDouble();
      if (exists()) {
        cons.accept(val);
      }
    }
    @Override
    public void streamStrings(Consumer<String> cons) {
      String val = getString();
      if (exists()) {
        cons.accept(val);
      }
    }
    @Override
    public void streamObjects(Consumer<Object> cons) {
      Object val = getObject();
      if (exists()) {
        cons.accept(val);
      }
    }
    @Override
    public AnalyticsValue convertToConstant() {
      if (getExpressionType().equals(ExpressionType.CONST)) {
        return new ConstantIntValue(getInt());
      }
      return this;
    }
    @Override
    public ExpressionComparator<Integer> getObjectComparator(String expression) {
      return new ExpressionComparator<>(expression);
    }
  }
}

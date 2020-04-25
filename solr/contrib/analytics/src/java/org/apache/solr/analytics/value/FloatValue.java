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

import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.value.constant.ConstantFloatValue;

/**
 * A single-valued analytics value that can be represented as a float.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #getFloat()} and {@link #exists()},
 * resulting in different values on each call.
 */
public interface FloatValue extends FloatValueStream, AnalyticsValue {
  /**
   * Get the float representation of the current value.
   * <p>
   * NOTE: The value returned is not valid unless calling {@link #exists()} afterwards returns {@code TRUE}.
   *
   * @return the current value
   */
  float getFloat();

  /**
   * An interface that represents all of the types a {@link FloatValue} should be able to cast to.
   */
  public static interface CastingFloatValue extends FloatValue, DoubleValue, StringValue, ComparableValue {}

  /**
   * An abstract base for {@link CastingFloatValue} that automatically casts to all types if {@link #getFloat()} and {@link #exists()} are implemented.
   */
  public static abstract class AbstractFloatValue implements CastingFloatValue {
    @Override
    public double getDouble() {
      return getFloat();
    }
    @Override
    public String getString() {
      float val = getFloat();
      return exists() ? Float.toString(val) : null;
    }
    @Override
    public Object getObject() {
      float val = getFloat();
      return exists() ? val : null;
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
        return new ConstantFloatValue(getFloat());
      }
      return this;
    }
    @Override
    public ExpressionComparator<Float> getObjectComparator(String expression) {
      return new ExpressionComparator<>(expression);
    }
  }
}

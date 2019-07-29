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

import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.value.constant.ConstantBooleanValue;

/**
 * A single-valued analytics value that can be represented as a boolean.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #getBoolean()} and {@link #exists()},
 * resulting in different values on each call.
 */
public interface BooleanValue extends BooleanValueStream, AnalyticsValue {
  /**
   * Get the boolean representation of the current value.
   * <p>
   * NOTE: The value returned is not valid unless calling {@link #exists()} afterwards returns {@code TRUE}.
   *
   * @return the current value
   */
  boolean getBoolean();

  /**
   * An interface that represents all of the types a {@link BooleanValue} should be able to cast to.
   */
  public static interface CastingBooleanValue extends BooleanValue, StringValue, ComparableValue {}

  /**
   * An abstract base for {@link CastingBooleanValue} that automatically casts to all types if {@link #getBoolean()} and {@link #exists()} are implemented.
   */
  public static abstract class AbstractBooleanValue implements CastingBooleanValue {
    @Override
    public String getString() {
      boolean val = getBoolean();
      return exists() ? Boolean.toString(val) : null;
    }
    @Override
    public Object getObject() {
      boolean val = getBoolean();
      return exists() ? val : null;
    }
    @Override
    public void streamBooleans(BooleanConsumer cons) {
      boolean val = getBoolean();
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
        return new ConstantBooleanValue(getBoolean());
      }
      return this;
    }
    @Override
    public ExpressionComparator<Boolean> getObjectComparator(String expression) {
      return new ExpressionComparator<>(expression);
    }
  }
}

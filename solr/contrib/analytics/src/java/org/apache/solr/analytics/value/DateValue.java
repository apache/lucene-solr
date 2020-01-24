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
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.value.constant.ConstantDateValue;

/**
 * A single-valued analytics value that can be represented as a date.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #getDate()} and {@link #exists()},
 * resulting in different values on each call.
 * <p>
 * NOTE: Most date expressions work with the {@code long} representation of the date, so that less
 * objects need to be created during execution.
 */
public interface DateValue extends DateValueStream, LongValue {
  /**
   * Get the date representation of the current value.
   * <p>
   * NOTE: The value returned is not valid unless calling {@link #exists()} afterwards returns {@code TRUE}.
   *
   * @return the current value
   */
  Date getDate();

  /**
   * An interface that represents all of the types a {@link DateValue} should be able to cast to.
   */
  public static interface CastingDateValue extends DateValue, LongValue, StringValue, ComparableValue {}

  /**
   * An abstract base for {@link CastingDateValue} that automatically casts to all types if {@link #getLong()} and {@link #exists()} are implemented.
   */
  public static abstract class AbstractDateValue implements CastingDateValue {
    @Override
    public Date getDate() {
      long val = getLong();
      return exists() ? new Date(val) : null;
    }
    @Override
    public String getString() {
      long val = getLong();
      return exists() ? Instant.ofEpochMilli(val).toString() : null;
    }
    @Override
    public Object getObject() {
      return getDate();
    }
    @Override
    public void streamDates(Consumer<Date> cons) {
      Date val = getDate();
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
        return new ConstantDateValue(getLong());
      }
      return this;
    }
    @Override
    public ExpressionComparator<Date> getObjectComparator(String expression) {
      return new ExpressionComparator<>(expression);
    }
  }
}

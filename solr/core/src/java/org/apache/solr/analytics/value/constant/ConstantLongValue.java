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
package org.apache.solr.analytics.value.constant;

import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.facet.compare.ConstantComparator;
import org.apache.solr.analytics.value.LongValue;
import org.apache.solr.analytics.value.LongValue.CastingLongValue;

/**
 * A constant {@link LongValue}. Every call to {@link #getLong()} and other methods will return the same constant value.
 */
public class ConstantLongValue extends ConstantValue implements CastingLongValue {
  private final long value;
  private final String valueStr;
  public static final String name = "const_long";
  private final String exprStr;

  public ConstantLongValue(long value) {
    this.value = value;
    this.valueStr = Long.toString(value);
    this.exprStr = ConstantValue.createExpressionString(this, valueStr);
  }


  @Override
  public long getLong() {
    return value;
  }
  @Override
  public double getDouble() {
    return value;
  }
  @Override
  public String getString() {
    return valueStr;
  }
  @Override
  public Object getObject() {
    return value;
  }
  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    cons.accept(value);
  }
  @Override
  public void streamDoubles(DoubleConsumer cons) {
    cons.accept(value);
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    cons.accept(valueStr);
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    cons.accept(value);
  }

  @Override
  public ConstantComparator getObjectComparator(String expression) {
    return new ConstantComparator();
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
    return ExpressionType.CONST;
  }
}
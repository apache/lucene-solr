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

import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.facet.compare.ConstantComparator;
import org.apache.solr.analytics.value.DateValue;
import org.apache.solr.analytics.value.DateValue.CastingDateValue;

/**
 * A constant {@link DateValue}. Every call to {@link #getDate()} and other methods will return the same constant value.
 */
public class ConstantDateValue extends ConstantValue implements CastingDateValue {
  private final long value;
  private final Date valueDate;
  private final String valueStr;
  public static final String name = "const_date";
  private final String exprStr;

  public ConstantDateValue(long value) {
    this.value = value;
    this.valueDate = new Date(value);
    this.valueStr = Instant.ofEpochMilli(value).toString();
    this.exprStr = ConstantValue.createExpressionString(this, valueStr);
  }


  @Override
  public long getLong() {
    return value;
  }
  @Override
  public Date getDate() {
    return valueDate;
  }
  @Override
  public String getString() {
    return valueStr;
  }
  @Override
  public Object getObject() {
    return valueDate;
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
  public void streamDates(Consumer<Date> cons) {
    cons.accept(valueDate);
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    cons.accept(valueStr);
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    cons.accept(valueDate);
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
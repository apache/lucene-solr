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
package org.apache.solr.analytics.function.field;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.value.DateValue.CastingDateValue;
import org.apache.solr.schema.DatePointField;
import org.apache.solr.schema.TrieDateField;

/**
 * An analytics wrapper for a single-valued {@link TrieDateField} or {@link DatePointField} with DocValues enabled.
 */
public class DateField extends AnalyticsField implements CastingDateValue {
  private NumericDocValues docValues;
  private long value;
  private boolean exists;

  public DateField(String fieldName) {
    super(fieldName);
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = DocValues.getNumeric(context.reader(), fieldName);
  }

  @Override
  public void collect(int doc) throws IOException {
    exists = docValues.advanceExact(doc);
    if (exists) {
      value = docValues.longValue();
    }
  }

  @Override
  public long getLong() {
    return value;
  }
  @Override
  public Date getDate() {
    return exists ? new Date(value) : null;
  }
  @Override
  public String getString() {
    return exists ? Instant.ofEpochMilli(value).toString() : null;
  }
  @Override
  public Object getObject() {
    return exists ? value : null;
  }
  @Override
  public boolean exists() {
    return exists;
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    if (exists) {
      cons.accept(value);
    }
  }
  @Override
  public void streamDates(Consumer<Date> cons) {
    if (exists) {
      cons.accept(new Date(value));
    }
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    if (exists) {
      cons.accept(Instant.ofEpochMilli(value).toString());
    }
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    if (exists) {
      cons.accept(new Date(value));
    }
  }

  @Override
  public ExpressionComparator<Date> getObjectComparator(String expression) {
    return new ExpressionComparator<>(expression);
  }
}

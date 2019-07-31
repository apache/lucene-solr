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
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.value.IntValue.CastingIntValue;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.TrieIntField;

/**
 * An analytics wrapper for a single-valued {@link TrieIntField} or {@link IntPointField} with DocValues enabled.
 */
public class IntField extends AnalyticsField implements CastingIntValue {
  private NumericDocValues docValues;
  private int value;
  private boolean exists;

  public IntField(String fieldName) {
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
      value = (int)docValues.longValue();
    }
  }

  @Override
  public int getInt() {
    return value;
  }
  @Override
  public long getLong() {
    return (long)value;
  }
  @Override
  public float getFloat() {
    return (float)value;
  }
  @Override
  public double getDouble() {
    return (double)value;
  }
  @Override
  public String getString() {
    return exists ? Integer.toString(value) : null;
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
  public void streamInts(IntConsumer cons) {
    if (exists) {
      cons.accept(value);
    }
  }
  @Override
  public void streamLongs(LongConsumer cons) {
    if (exists) {
      cons.accept((long)value);
    }
  }
  @Override
  public void streamFloats(FloatConsumer cons) {
    if (exists) {
      cons.accept((float)value);
    }
  }
  @Override
  public void streamDoubles(DoubleConsumer cons) {
    if (exists) {
      cons.accept((double)value);
    }
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    if (exists) {
      cons.accept(Integer.toString(value));
    }
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    if (exists) {
      cons.accept(value);
    }
  }

  @Override
  public ExpressionComparator<Integer> getObjectComparator(String expression) {
    return new ExpressionComparator<>(expression);
  }
}

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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.value.DoubleValue.CastingDoubleValue;
import org.apache.solr.schema.DoublePointField;
import org.apache.solr.schema.TrieDoubleField;

/**
 * An analytics wrapper for a single-valued {@link TrieDoubleField} or {@link DoublePointField} with DocValues enabled.
 */
public class DoubleField extends AnalyticsField implements CastingDoubleValue {
  private NumericDocValues docValues;
  private double value;
  private boolean exists;

  public DoubleField(String fieldName) {
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
      value = Double.longBitsToDouble(docValues.longValue());
    }
  }

  @Override
  public double getDouble() {
    return value;
  }
  @Override
  public String getString() {
    return exists ? Double.toString(value) : null;
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
  public void streamDoubles(DoubleConsumer cons) {
    if (exists) {
      cons.accept(value);
    }
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    if (exists) {
      cons.accept(Double.toString(value));
    }
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    if (exists) {
      cons.accept(value);
    }
  }

  @Override
  public ExpressionComparator<Double> getObjectComparator(String expression) {
    return new ExpressionComparator<>(expression);
  }
}

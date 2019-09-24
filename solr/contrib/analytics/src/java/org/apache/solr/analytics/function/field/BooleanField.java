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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.analytics.facet.compare.ExpressionComparator;
import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.value.BooleanValue.CastingBooleanValue;
import org.apache.solr.schema.BoolField;

/**
 * An analytics wrapper for a single-valued {@link BoolField} with DocValues enabled.
 */
public class BooleanField extends AnalyticsField implements CastingBooleanValue {
  private SortedDocValues docValues;
  boolean value;
  boolean exists;
  int trueOrd;

  public BooleanField(String fieldName) {
    super(fieldName);
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = DocValues.getSorted(context.reader(), fieldName);

    // figure out what ord maps to true
    int numOrds = docValues.getValueCount();
    // if no values in the segment, default trueOrd to something other then -1 (missing)
    int trueOrd = -2;
    for (int i=0; i<numOrds; i++) {
      final BytesRef br = docValues.lookupOrd(i);
      if (br.length==1 && br.bytes[br.offset]=='T') {
        trueOrd = i;
        break;
      }
    }

    this.trueOrd = trueOrd;
  }

  @Override
  public void collect(int doc) throws IOException {
    exists = docValues.advanceExact(doc);
    if (exists) {
      value = trueOrd ==  docValues.ordValue();
    }
  }

  @Override
  public boolean getBoolean() {
    return value;
  }
  @Override
  public String getString() {
    return exists ? Boolean.toString(value) : null;
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
  public void streamBooleans(BooleanConsumer cons) {
    if (exists) {
      cons.accept(value);
    }
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    if (exists) {
      cons.accept(Boolean.toString(value));
    }
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    if (exists) {
      cons.accept(value);
    }
  }

  @Override
  public ExpressionComparator<Boolean> getObjectComparator(String expression) {
    return new ExpressionComparator<>(expression);
  }
}

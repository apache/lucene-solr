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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.value.FloatValueStream.CastingFloatValueStream;
import org.apache.solr.schema.FloatPointField;

/**
 * An analytics wrapper for a multi-valued {@link FloatPointField} with DocValues enabled.
 */
public class FloatMultiPointField extends AnalyticsField implements CastingFloatValueStream {
  private SortedNumericDocValues docValues;
  private int count;
  private float[] values;

  public FloatMultiPointField(String fieldName) {
    super(fieldName);
    count = 0;
    values = new float[initialArrayLength];
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
  }

  @Override
  public void collect(int doc) throws IOException {
    if (docValues.advanceExact(doc)) {
      count = docValues.docValueCount();
      resizeEmptyValues(count);
      for (int i = 0; i < count; ++i) {
        values[i] = NumericUtils.sortableIntToFloat((int)docValues.nextValue());
      }
    } else {
      count = 0;
    }
  }

  private void resizeEmptyValues(int count) {
    if (count > values.length) {
      values = new float[count];
    }
  }

  @Override
  public void streamFloats(FloatConsumer cons) {
    for (int i = 0; i < count; ++i) {
      cons.accept(values[i]);
    }
  }
  @Override
  public void streamDoubles(DoubleConsumer cons) {
    streamFloats(value -> cons.accept((double)value));
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    streamFloats(value -> cons.accept(Float.toString(value)));
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    streamFloats(value -> cons.accept(value));
  }
}

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
import java.util.function.LongConsumer;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.solr.analytics.value.LongValueStream.CastingLongValueStream;
import org.apache.solr.schema.LongPointField;

/**
 * An analytics wrapper for a multi-valued {@link LongPointField} with DocValues enabled.
 */
public class LongMultiPointField extends AnalyticsField implements CastingLongValueStream {
  private SortedNumericDocValues docValues;
  private int count;
  private long[] values;

  public LongMultiPointField(String fieldName) {
    super(fieldName);
    count = 0;
    values = new long[initialArrayLength];
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
        values[i] = docValues.nextValue();
      }
    } else {
      count = 0;
    }
  }

  private void resizeEmptyValues(int count) {
    if (count > values.length) {
      values = new long[count];
    }
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    for (int i = 0; i < count; ++i) {
      cons.accept(values[i]);
    }
  }
  @Override
  public void streamDoubles(DoubleConsumer cons) {
    streamLongs(value -> cons.accept((double)value));
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    streamLongs(value -> cons.accept(Long.toString(value)));
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    streamLongs(value -> cons.accept(value));
  }
}

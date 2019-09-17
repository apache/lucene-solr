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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.value.BooleanValueStream.CastingBooleanValueStream;
import org.apache.solr.schema.BoolField;


/**
 * An analytics wrapper for a multi-valued {@link BoolField} with DocValues enabled.
 */
public class BooleanMultiField extends AnalyticsField implements CastingBooleanValueStream {
  private SortedSetDocValues docValues;
  private int count;
  private boolean[] values;

  private int trueOrd;

  public BooleanMultiField(String fieldName) {
    super(fieldName);
    count = 0;
    values = new boolean[initialArrayLength];
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = DocValues.getSortedSet(context.reader(), fieldName);

    // figure out what ord maps to true
    long numOrds = docValues.getValueCount();
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
    count = 0;
    if (docValues.advanceExact(doc)) {
      int term;
      while ((term = (int)docValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        if (count == values.length) {
          resizeValues();
        }
        values[count++] = term == trueOrd;
      }
    }
  }

  private void resizeValues() {
    boolean[] newValues = new boolean[values.length*2];
    for (int i = 0; i < count; ++i) {
      newValues[i] = values[i];
    }
    values = newValues;
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    for (int i = 0; i < count; ++i) {
      cons.accept(values[i]);
    }
  }
  @Override
  public void streamStrings(Consumer<String> cons) {
    streamBooleans(value -> cons.accept(Boolean.toString(value)));
  }
  @Override
  public void streamObjects(Consumer<Object> cons) {
    streamBooleans(value -> cons.accept(value));
  }
}

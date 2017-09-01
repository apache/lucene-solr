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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.value.FloatValueStream.CastingFloatValueStream;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.solr.schema.TrieFloatField;

/**
 * An analytics wrapper for a multi-valued {@link TrieFloatField} with DocValues enabled.
 * @deprecated Trie fields are deprecated as of Solr 7.0
 */
@Deprecated
public class FloatMultiTrieField extends AnalyticsField implements CastingFloatValueStream {
  private SortedSetDocValues docValues;
  private int count;
  private float[] values;

  public FloatMultiTrieField(String fieldName) {
    super(fieldName);
    count = 0;
    values = new float[initialArrayLength];
  }
  
  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = DocValues.getSortedSet(context.reader(), fieldName);
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
        values[count++] = NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(docValues.lookupOrd(term)));
      }
    }
  }
  
  private void resizeValues() {
    float[] newValues = new float[values.length*2];
    for (int i = 0; i < count; ++i) {
      newValues[i] = values[i];
    }
    values = newValues;
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

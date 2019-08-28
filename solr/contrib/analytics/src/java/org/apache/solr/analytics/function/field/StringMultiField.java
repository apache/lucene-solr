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
import java.util.ArrayList;
import java.util.function.Consumer;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.analytics.value.StringValueStream.CastingStringValueStream;
import org.apache.solr.schema.StrField;

/**
 * An analytics wrapper for a multi-valued {@link StrField} with DocValues enabled.
 */
public class StringMultiField extends AnalyticsField implements CastingStringValueStream {
  private SortedSetDocValues docValues;
  private ArrayList<String> values;

  public StringMultiField(String fieldName) {
    super(fieldName);
    values = new ArrayList<>(initialArrayLength);
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    docValues = DocValues.getSortedSet(context.reader(), fieldName);
  }
  @Override
  public void collect(int doc) throws IOException {
    values.clear();
    if (docValues.advanceExact(doc)) {
      int term;
      while ((term = (int)docValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        values.add(docValues.lookupOrd(term).utf8ToString());
      }
    }
  }

  @Override
  public void streamStrings(Consumer<String> cons) {
    values.forEach(value -> cons.accept(value));
  }

  @Override
  public void streamObjects(Consumer<Object> cons) {
    values.forEach(value -> cons.accept(value));
  }

}

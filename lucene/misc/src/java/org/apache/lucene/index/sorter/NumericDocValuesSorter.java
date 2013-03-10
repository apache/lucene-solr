package org.apache.lucene.index.sorter;

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

import java.io.IOException;
import java.util.AbstractList;
import java.util.List;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.NumericDocValues;

/**
 * A {@link Sorter} which sorts documents according to their
 * {@link NumericDocValues}.
 * 
 * @lucene.experimental
 */
public class NumericDocValuesSorter extends Sorter {

  private final String fieldName;

  public NumericDocValuesSorter(final String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public int[] oldToNew(final AtomicReader reader) throws IOException {
    final NumericDocValues ndv = reader.getNumericDocValues(fieldName);
    final int maxDoc = reader.maxDoc();
    final int[] docs = new int[maxDoc];
    final List<Long> values = new AbstractList<Long>() {

      @Override
      public Long get(int doc) {
        return ndv.get(doc);
      }

      @Override
      public int size() {
        return reader.maxDoc();
      }
      
    };
    for (int i = 0; i < maxDoc; i++) {
      docs[i] = i;
    }
    return compute(docs, values);
  }
  
}

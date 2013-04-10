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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.NumericDocValues;

/**
 * A {@link Sorter} which sorts documents according to their
 * {@link NumericDocValues}. One can specify ascending or descending sort order.
 * 
 * @lucene.experimental
 */
public class NumericDocValuesSorter extends Sorter {

  private final String fieldName;
  private final boolean ascending;
  
  /** Constructor over the given field name, and ascending sort order. */
  public NumericDocValuesSorter(final String fieldName) {
    this(fieldName, true);
  }
  
  /**
   * Constructor over the given field name, and whether sorting should be
   * ascending ({@code true}) or descending ({@code false}).
   */
  public NumericDocValuesSorter(final String fieldName, boolean ascending) {
    this.fieldName = fieldName;
    this.ascending = ascending;
  }

  @Override
  public Sorter.DocMap sort(final AtomicReader reader) throws IOException {
    final NumericDocValues ndv = reader.getNumericDocValues(fieldName);
    final DocComparator comparator;
    if (ascending) {
      comparator = new DocComparator() {
        @Override
        public int compare(int docID1, int docID2) {
          final long v1 = ndv.get(docID1);
          final long v2 = ndv.get(docID2);
          return v1 < v2 ? -1 : v1 == v2 ? 0 : 1;
        }
      };
    } else {
      comparator = new DocComparator() {
        @Override
        public int compare(int docID1, int docID2) {
          final long v1 = ndv.get(docID1);
          final long v2 = ndv.get(docID2);
          return v1 > v2 ? -1 : v1 == v2 ? 0 : 1;
        }
      };
    }
    return sort(reader.maxDoc(), comparator);
  }
  
  @Override
  public String getID() {
    return "DocValues(" + fieldName + "," + (ascending ? "ascending" : "descending") + ")";
  }
  
}

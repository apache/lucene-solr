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
package org.apache.lucene.index;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedGrowableWriter;
import org.apache.lucene.util.packed.PagedMutable;


/**
 * A {@link DocValuesFieldUpdates} which holds updates of documents, of a single
 * {@link NumericDocValuesField}.
 * 
 * @lucene.experimental
 */
class NumericDocValuesFieldUpdates extends DocValuesFieldUpdates {

  // TODO: can't this just be NumericDocValues now?  avoid boxing the long value...
  final static class Iterator extends DocValuesFieldUpdates.Iterator {
    private final int size;
    private final PagedGrowableWriter values;
    private final PagedMutable docs;
    private long idx = 0; // long so we don't overflow if size == Integer.MAX_VALUE
    private int doc = -1;
    private Long value = null;
    private final long delGen;
    
    Iterator(int size, PagedGrowableWriter values, PagedMutable docs, long delGen) {
      this.size = size;
      this.values = values;
      this.docs = docs;
      this.delGen = delGen;
    }
    
    @Override
    Long value() {
      return value;
    }
    
    @Override
    int nextDoc() {
      if (idx >= size) {
        value = null;
        return doc = DocIdSetIterator.NO_MORE_DOCS;
      }
      doc = (int) docs.get(idx);
      ++idx;
      while (idx < size && docs.get(idx) == doc) {
        // scan forward to last update to this doc
        ++idx;
      }
      // idx points to the "next" element
      value = Long.valueOf(values.get(idx - 1));
      return doc;
    }
    
    @Override
    int doc() {
      return doc;
    }

    @Override
    long delGen() {
      return delGen;
    }
  }

  private final int bitsPerValue;
  private PagedMutable docs;
  private PagedGrowableWriter values;
  private int size;
  
  public NumericDocValuesFieldUpdates(long delGen, String field, int maxDoc) {
    super(maxDoc, delGen, field, DocValuesType.NUMERIC);
    bitsPerValue = PackedInts.bitsRequired(maxDoc - 1);
    docs = new PagedMutable(1, PAGE_SIZE, bitsPerValue, PackedInts.COMPACT);
    values = new PagedGrowableWriter(1, PAGE_SIZE, 1, PackedInts.FAST);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public synchronized void add(int doc, Object value) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }

    assert doc < maxDoc;
    
    // TODO: if the Sorter interface changes to take long indexes, we can remove that limitation
    if (size == Integer.MAX_VALUE) {
      throw new IllegalStateException("cannot support more than Integer.MAX_VALUE doc/value entries");
    }

    Long val = (Long) value;
    
    // grow the structures to have room for more elements
    if (docs.size() == size) {
      docs = docs.grow(size + 1);
      values = values.grow(size + 1);
    }
    
    docs.set(size, doc);
    values.set(size, val.longValue());
    ++size;
  }

  @Override
  public void finish() {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    // shrink wrap
    if (size < docs.size()) {
      docs = docs.resize(size);
      values = values.resize(size);
    }

    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        long tmpDoc = docs.get(j);
        docs.set(j, docs.get(i));
        docs.set(i, tmpDoc);
        
        long tmpVal = values.get(j);
        values.set(j, values.get(i));
        values.set(i, tmpVal);
      }

      @Override
      protected int compare(int i, int j) {
        // increasing docID order:
        // NOTE: we can have ties here, when the same docID was updated in the same segment, in which case we rely on sort being
        // stable and preserving original order so the last update to that docID wins
        return Integer.compare((int) docs.get(i), (int) docs.get(j));
      }
    }.sort(0, size);
  }

  @Override
  public Iterator iterator() {
    if (finished == false) {
      throw new IllegalStateException("call finish first");
    }
    return new Iterator(size, values, docs, delGen);
  }
  
  @Override
  public boolean any() {
    return size > 0;
  }

  @Override
  public long ramBytesUsed() {
    return values.ramBytesUsed()
      + docs.ramBytesUsed()
      + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
      + 2 * RamUsageEstimator.NUM_BYTES_INT
      + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  }
}

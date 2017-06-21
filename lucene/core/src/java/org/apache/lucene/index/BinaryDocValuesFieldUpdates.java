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


import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedGrowableWriter;
import org.apache.lucene.util.packed.PagedMutable;

/**
 * A {@link DocValuesFieldUpdates} which holds updates of documents, of a single
 * {@link BinaryDocValuesField}.
 * 
 * @lucene.experimental
 */
class BinaryDocValuesFieldUpdates extends DocValuesFieldUpdates {
  
  final static class Iterator extends DocValuesFieldUpdates.Iterator {
    private final int size;
    private final PagedGrowableWriter offsets;
    private final PagedGrowableWriter lengths;
    private final PagedMutable docs;
    private long idx = 0; // long so we don't overflow if size == Integer.MAX_VALUE
    private int doc = -1;
    private final BytesRef value;
    private int offset, length;
    private final long delGen;
    
    Iterator(int size, PagedGrowableWriter offsets, PagedGrowableWriter lengths, 
             PagedMutable docs, BytesRef values, long delGen) {
      this.offsets = offsets;
      this.size = size;
      this.lengths = lengths;
      this.docs = docs;
      value = values.clone();
      this.delGen = delGen;
    }
    
    @Override
    BytesRef value() {
      value.offset = offset;
      value.length = length;
      return value;
    }
    
    @Override
    int nextDoc() {
      if (idx >= size) {
        offset = -1;
        return doc = DocIdSetIterator.NO_MORE_DOCS;
      }
      doc = (int) docs.get(idx);
      ++idx;
      while (idx < size && docs.get(idx) == doc) {
        // scan forward to last update to this doc
        ++idx;
      }
      // idx points to the "next" element
      long prevIdx = idx - 1;
      // cannot change 'value' here because nextDoc is called before the
      // value is used, and it's a waste to clone the BytesRef when we
      // obtain the value
      offset = (int) offsets.get(prevIdx);
      length = (int) lengths.get(prevIdx);
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

  private PagedMutable docs;
  private PagedGrowableWriter offsets, lengths;
  private BytesRefBuilder values;
  private int size;
  private final int bitsPerValue;
  
  public BinaryDocValuesFieldUpdates(long delGen, String field, int maxDoc) {
    super(maxDoc, delGen, field, DocValuesType.BINARY);
    bitsPerValue = PackedInts.bitsRequired(maxDoc - 1);
    docs = new PagedMutable(1, PAGE_SIZE, bitsPerValue, PackedInts.COMPACT);
    offsets = new PagedGrowableWriter(1, PAGE_SIZE, 1, PackedInts.FAST);
    lengths = new PagedGrowableWriter(1, PAGE_SIZE, 1, PackedInts.FAST);
    values = new BytesRefBuilder();
  }

  @Override
  public int size() {
    return size;
  }

  // NOTE: we fully consume the incoming BytesRef so caller is free to reuse it after we return:
  @Override
  synchronized public void add(int doc, Object value) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }

    assert doc < maxDoc: "doc=" + doc + " maxDoc=" + maxDoc;

    // TODO: if the Sorter interface changes to take long indexes, we can remove that limitation
    if (size == Integer.MAX_VALUE) {
      throw new IllegalStateException("cannot support more than Integer.MAX_VALUE doc/value entries");
    }

    BytesRef val = (BytesRef) value;
    
    // grow the structures to have room for more elements
    if (docs.size() == size) {
      docs = docs.grow(size + 1);
      offsets = offsets.grow(size + 1);
      lengths = lengths.grow(size + 1);
    }
    
    docs.set(size, doc);
    offsets.set(size, values.length());
    lengths.set(size, val.length);
    values.append(val);
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
      offsets = offsets.resize(size);
      lengths = lengths.resize(size);
    }

    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        long tmpDoc = docs.get(j);
        docs.set(j, docs.get(i));
        docs.set(i, tmpDoc);
        
        long tmpOffset = offsets.get(j);
        offsets.set(j, offsets.get(i));
        offsets.set(i, tmpOffset);

        long tmpLength = lengths.get(j);
        lengths.set(j, lengths.get(i));
        lengths.set(i, tmpLength);
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
    return new Iterator(size, offsets, lengths, docs, values.get(), delGen);
  }

  @Override
  public boolean any() {
    return size > 0;
  }

  @Override
  public long ramBytesUsed() {
    return offsets.ramBytesUsed()
      + lengths.ramBytesUsed()
      + docs.ramBytesUsed()
      + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
      + 4 * RamUsageEstimator.NUM_BYTES_INT
      + 5 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
      + values.bytes().length;
  }
}

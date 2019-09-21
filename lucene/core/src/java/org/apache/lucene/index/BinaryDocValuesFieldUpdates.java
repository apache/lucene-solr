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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
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
final class BinaryDocValuesFieldUpdates extends DocValuesFieldUpdates {
  
  final static class Iterator extends DocValuesFieldUpdates.AbstractIterator {
    private final PagedGrowableWriter offsets;
    private final PagedGrowableWriter lengths;
    private final BytesRef value;
    private int offset, length;

    Iterator(int size, PagedGrowableWriter offsets, PagedGrowableWriter lengths, 
             PagedMutable docs, BytesRef values, long delGen) {
      super(size, docs, delGen);
      this.offsets = offsets;
      this.lengths = lengths;
      value = values.clone();
    }

    @Override
    BytesRef binaryValue() {
      value.offset = offset;
      value.length = length;
      return value;
    }

    @Override
    protected void set(long idx) {
      offset = (int) offsets.get(idx);
      length = (int) lengths.get(idx);
    }

    @Override
    long longValue() {
      throw new UnsupportedOperationException();
    }
  }

  private PagedGrowableWriter offsets, lengths;
  private BytesRefBuilder values;

  public BinaryDocValuesFieldUpdates(long delGen, String field, int maxDoc) {
    super(maxDoc, delGen, field, DocValuesType.BINARY);
    offsets = new PagedGrowableWriter(1, PAGE_SIZE, 1, PackedInts.FAST);
    lengths = new PagedGrowableWriter(1, PAGE_SIZE, 1, PackedInts.FAST);
    values = new BytesRefBuilder();
  }

  @Override
  public void add(int doc, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(int docId, DocValuesFieldUpdates.Iterator iterator) {
    add(docId, iterator.binaryValue());
  }

  @Override
  synchronized public void add(int doc, BytesRef value) {
    int index = add(doc);
    offsets.set(index, values.length());
    lengths.set(index, value.length);
    values.append(value);
  }

  @Override
  protected void swap(int i, int j) {
    super.swap(i, j);

    long tmpOffset = offsets.get(j);
    offsets.set(j, offsets.get(i));
    offsets.set(i, tmpOffset);

    long tmpLength = lengths.get(j);
    lengths.set(j, lengths.get(i));
    lengths.set(i, tmpLength);
  }

  @Override
  protected void grow(int size) {
    super.grow(size);
    offsets = offsets.grow(size);
    lengths = lengths.grow(size);
  }

  @Override
  protected void resize(int size) {
    super.resize(size);
    offsets = offsets.resize(size);
    lengths = lengths.resize(size);
  }

  @Override
  public Iterator iterator() {
    ensureFinished();
    return new Iterator(size, offsets, lengths, docs, values.get(), delGen);
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
      + offsets.ramBytesUsed()
      + lengths.ramBytesUsed()
      + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
      + 2 * Integer.BYTES
      + 3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
      + values.bytes().length;
  }
}

package org.apache.lucene.search.suggest;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.SorterTemplate;

final class BytesRefList {

  private final ByteBlockPool pool;
  private int[] offsets = new int[1];
  private int currentElement = 0;
  private int currentOffset = 0;

  public BytesRefList() {
    this(new ByteBlockPool(new ByteBlockPool.DirectAllocator()));
  }

  public BytesRefList(ByteBlockPool pool) {
    this.pool = pool;
    pool.nextBuffer();
  }

  public int append(BytesRef bytes) {
    if (currentElement >= offsets.length) {
      offsets = ArrayUtil.grow(offsets, offsets.length + 1);
    }
    pool.copy(bytes);
    offsets[currentElement++] = currentOffset;
    currentOffset += bytes.length;
    return currentElement;
  }

  public int size() {
    return currentElement;
  }

  public BytesRef get(BytesRef bytes, int pos) {
    if (currentElement > pos) {
      bytes.offset = offsets[pos];
      bytes.length = pos == currentElement - 1 ? currentOffset - bytes.offset
          : offsets[pos + 1] - bytes.offset;
      pool.copyFrom(bytes);
      return bytes;
    }
    throw new IndexOutOfBoundsException("index " + pos
        + " must be less than the size: " + currentElement);

  }

  public BytesRefIterator iterator() {
    final int numElements = currentElement;
    
    return new BytesRefIterator() {
      private final BytesRef spare = new BytesRef();
      private int pos = 0;

      @Override
      public BytesRef next() throws IOException {
        if (pos < numElements) {
          get(spare, pos++);
          return spare;
        }
        return null;
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return null;
      }
    };
  }
  
  public int[] sort(final Comparator<BytesRef> comp) {
    final int[] orderdEntries = new int[size()];
    for (int i = 0; i < orderdEntries.length; i++) {
      orderdEntries[i] = i;
    }
    new SorterTemplate() {
      @Override
      protected void swap(int i, int j) {
        final int o = orderdEntries[i];
        orderdEntries[i] = orderdEntries[j];
        orderdEntries[j] = o;
      }
      
      @Override
      protected int compare(int i, int j) {
        final int ord1 = orderdEntries[i], ord2 = orderdEntries[j];
        return comp.compare(get(scratch1, ord1), get(scratch2, ord2));
      }

      @Override
      protected void setPivot(int i) {
        final int ord = orderdEntries[i];
        get(pivot, ord);
      }
  
      @Override
      protected int comparePivot(int j) {
        final int ord = orderdEntries[j];
        return comp.compare(pivot, get(scratch2, ord));
      }
      
      private final BytesRef pivot = new BytesRef(),
        scratch1 = new BytesRef(), scratch2 = new BytesRef();
    }.quickSort(0, size() - 1);
    return orderdEntries;
  }
}

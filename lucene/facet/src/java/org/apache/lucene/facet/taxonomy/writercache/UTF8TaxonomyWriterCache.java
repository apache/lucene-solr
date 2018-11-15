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
package org.apache.lucene.facet.taxonomy.writercache;

import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;

/** A "cache" that never frees memory, and stores labels in a BytesRefHash (utf-8 encoding). */
public final class UTF8TaxonomyWriterCache implements TaxonomyWriterCache, Accountable {
  private final ThreadLocal<BytesRefBuilder> bytes = new ThreadLocal<BytesRefBuilder>() {
      @Override
      protected BytesRefBuilder initialValue() {
        return new BytesRefBuilder();
      }
    };

  private final Counter bytesUsed = Counter.newCounter();
  private final BytesRefHash map = new BytesRefHash(new ByteBlockPool(new DirectTrackingAllocator(bytesUsed)));

  private final static int PAGE_BITS = 16;
  private final static int PAGE_SIZE = 1 << PAGE_BITS;
  private final static int PAGE_MASK = PAGE_SIZE - 1;

  private volatile int[][] ordinals;

  // How many labels we are storing:
  private int count;

  // How many pages in ordinals we've allocated:
  private int pageCount;

  /** Sole constructor. */
  public UTF8TaxonomyWriterCache() {
    ordinals = new int[1][];
    ordinals[0] = new int[PAGE_SIZE];
  }

  @Override
  public int get(FacetLabel label) {
    BytesRef bytes = toBytes(label);
    int id;
    synchronized (this) {
      id = map.find(bytes);
    }
    if (id == -1) {
      return LabelToOrdinal.INVALID_ORDINAL;
    }
    int page = id >>> PAGE_BITS;
    int offset = id & PAGE_MASK;
    return ordinals[page][offset];
  }

  // Called only from assert
  private boolean assertSameOrdinal(FacetLabel label, int id, int ord) {
    id = -id - 1;
    int page = id >>> PAGE_BITS;
    int offset = id & PAGE_MASK;
    int oldOrd = ordinals[page][offset];
    if (oldOrd != ord) {
      throw new IllegalArgumentException("label " + label + " was already cached, with old ord=" + oldOrd + " versus new ord=" + ord);
    }
    return true;
  }

  @Override
  public boolean put(FacetLabel label, int ord) {
    BytesRef bytes = toBytes(label);
    int id;
    synchronized (this) {
      id = map.add(bytes);
      if (id < 0) {
        assert assertSameOrdinal(label, id, ord);
        return false;
      }
      assert id == count;
      int page = id >>> PAGE_BITS;
      int offset = id & PAGE_MASK;
      if (page == pageCount) {
        if (page == ordinals.length) {
          int[][] newOrdinals = new int[ArrayUtil.oversize(page+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)][];
          System.arraycopy(ordinals, 0, newOrdinals, 0, ordinals.length);
          ordinals = newOrdinals;
        }
        ordinals[page] = new int[PAGE_SIZE];
        pageCount++;
      }
      ordinals[page][offset] = ord;
      count++;

      // we never prune from the cache
      return false;
    }
  }

  @Override
  public boolean isFull() {
    // we are never full
    return false;
  }

  @Override
  public synchronized void clear() {
    map.clear();
    map.reinit();
    ordinals = new int[1][];
    ordinals[0] = new int[PAGE_SIZE];
    count = 0;
    pageCount = 0;
    assert bytesUsed.get() == 0;
  }
    
  /** How many labels are currently stored in the cache. */
  public int size() {
    return count;
  }
  
  @Override
  public synchronized long ramBytesUsed() {
    return bytesUsed.get() + pageCount * PAGE_SIZE * Integer.BYTES;
  }
    
  @Override
  public void close() {
  }

  private static final byte DELIM_CHAR = (byte) 0x1F;

  private BytesRef toBytes(FacetLabel label) {
    BytesRefBuilder bytes = this.bytes.get();
    bytes.clear();
    for (int i = 0; i < label.length; i++) {
      String part = label.components[i];
      if (i > 0) {
        bytes.append(DELIM_CHAR);
      }
      bytes.grow(bytes.length() + UnicodeUtil.maxUTF8Length(part.length()));
      bytes.setLength(UnicodeUtil.UTF16toUTF8(part, 0, part.length(), bytes.bytes(), bytes.length()));
    }
    return bytes.get();
  }
}

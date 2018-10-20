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
package org.apache.lucene.search;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.util.packed.PackedInts;

final class LongHashSet extends AbstractSet<Long> {

  private static final long MISSING = Long.MIN_VALUE;

  final long[] table;
  final int mask;
  final boolean hasMissingValue;
  final int size;
  final int hashCode;

  LongHashSet(long... values) {
    int tableSize = Math.toIntExact(values.length * 3L / 2);
    tableSize = 1 << PackedInts.bitsRequired(tableSize); // make it a power of 2
    assert tableSize >= values.length * 3L / 2;
    table = new long[tableSize];
    Arrays.fill(table, MISSING);
    mask = tableSize - 1;
    boolean hasMissingValue = false;
    int size = 0;
    int hashCode = 0;
    for (long value : values) {
      if (value == MISSING || add(value)) {
        if (value == MISSING) {
          hasMissingValue = true;
        }
        ++size;
        hashCode += Long.hashCode(value);
      }
    }
    this.hasMissingValue = hasMissingValue;
    this.size = size;
    this.hashCode = hashCode;
  }

  private boolean add(long l) {
    assert l != MISSING;
    final int slot = Long.hashCode(l) & mask;
    for (int i = slot; ; i = (i + 1) & mask) {
      if (table[i] == MISSING) {
        table[i] = l;
        return true;
      } else if (table[i] == l) {
        // already added
        return false;
      }
    }
  }

  boolean contains(long l) {
    if (l == MISSING) {
      return hasMissingValue;
    }
    final int slot = Long.hashCode(l) & mask;
    for (int i = slot; ; i = (i + 1) & mask) {
      if (table[i] == MISSING) {
        return false;
      } else if (table[i] == l) {
        return true;
      }
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj.getClass() == LongHashSet.class) {
      LongHashSet that = (LongHashSet) obj;
      if (hashCode != that.hashCode
          || size != that.size
          || hasMissingValue != that.hasMissingValue) {
        return false;
      }
      for (long v : table) {
        if (v != MISSING && that.contains(v) == false) {
          return false;
        }
      }
      return true;
    }
    return super.equals(obj);
  }

  @Override
  public boolean contains(Object o) {
    return o instanceof Long && contains(((Long) o).longValue());
  }

  @Override
  public Iterator<Long> iterator() {
    return new Iterator<Long>() {

      private boolean hasNext = hasMissingValue;
      private int i = -1;
      private long value = MISSING;

      @Override
      public boolean hasNext() {
        if (hasNext) {
          return true;
        }
        while (++i < table.length) {
          value = table[i];
          if (value != MISSING) {
            return hasNext = true;
          }
        }
        return false;
      }

      @Override
      public Long next() {
        if (hasNext() == false) {
          throw new NoSuchElementException();
        }
        hasNext = false;
        return value;
      }

    };
  }

}

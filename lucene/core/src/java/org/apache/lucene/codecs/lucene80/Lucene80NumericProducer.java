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

package org.apache.lucene.codecs.lucene80;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.composite.CompositeDocValuesProducer;
import org.apache.lucene.codecs.composite.CompositeFieldMetadata;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;

public class Lucene80NumericProducer implements CompositeDocValuesProducer.NumericProducer, CompositeDocValuesProducer.SortedNumericProducer {
  private final int maxDoc;

  private final Map<String, NumericEntry> numerics = new HashMap<>();
  private final Map<String, SortedNumericEntry> sortedNumerics = new HashMap<>();

  public Lucene80NumericProducer(int maxDoc) {
    this.maxDoc = maxDoc;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException {
    NumericEntry entry = getNumericEntry(field, compositeFieldMetadata.getMetaStartFP(), indexInput);
    return getNumeric(entry, indexInput);
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException {
    SortedNumericEntry sortedNumericEntry = getSortedNumericEntry(field, compositeFieldMetadata.getMetaStartFP(), indexInput);
    return getSortedNumeric(sortedNumericEntry, indexInput);
  }

  private NumericEntry getNumericEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    NumericEntry numericEntry = numerics.get(field.name);
    if (numericEntry != null) {
      return numericEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    numericEntry = new NumericEntry();
    readNumeric(clone, numericEntry);

    numerics.put(field.name, numericEntry);

    return numericEntry;
  }

  private SortedNumericEntry getSortedNumericEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    SortedNumericEntry sortedNumericEntry = sortedNumerics.get(field.name);
    if (sortedNumericEntry != null) {
      return sortedNumericEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    sortedNumericEntry = readSortedNumeric(clone);

    sortedNumerics.put(field.name, sortedNumericEntry);

    return sortedNumericEntry;
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = 0L;
    for (Accountable accountable : numerics.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    for (Accountable accountable : sortedNumerics.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    return ramBytesUsed;
  }

  static class NumericEntry implements Accountable {
    long[] table;
    int blockShift;
    byte bitsPerValue;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    long numValues;
    long minValue;
    long gcd;
    long valuesOffset;
    long valuesLength;
    long valueJumpTableOffset; // -1 if no jump-table

    @Override
    public long ramBytesUsed() {
      return table == null ? 0L : RamUsageEstimator.sizeOf(table);
    }
  }

  static class SortedNumericEntry extends NumericEntry {
    int numDocsWithField;
    DirectMonotonicReader.Meta addressesMeta;
    long addressesOffset;
    long addressesLength;

    @Override
    public long ramBytesUsed() {
      long ramBytesUsed = super.ramBytesUsed();
      ramBytesUsed += addressesMeta == null ? 0L : addressesMeta.ramBytesUsed();
      return ramBytesUsed;
    }
  }

  static NumericEntry readNumeric(IndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    readNumeric(meta, entry);
    return entry;
  }

  static void readNumeric(IndexInput meta, NumericEntry entry) throws IOException {
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numValues = meta.readLong();
    int tableSize = meta.readInt();
    if (tableSize > 256) {
      throw new CorruptIndexException("invalid table size: " + tableSize, meta);
    }
    if (tableSize >= 0) {
      entry.table = new long[tableSize];
      for (int i = 0; i < tableSize; ++i) {
        entry.table[i] = meta.readLong();
      }
    }
    if (tableSize < -1) {
      entry.blockShift = -2 - tableSize;
    } else {
      entry.blockShift = -1;
    }
    entry.bitsPerValue = meta.readByte();
    entry.minValue = meta.readLong();
    entry.gcd = meta.readLong();
    entry.valuesOffset = meta.readLong();
    entry.valuesLength = meta.readLong();
    entry.valueJumpTableOffset = meta.readLong();
  }

  static SortedNumericEntry readSortedNumeric(IndexInput meta) throws IOException {
    SortedNumericEntry entry = new SortedNumericEntry();
    readNumeric(meta, entry);
    entry.numDocsWithField = meta.readInt();
    if (entry.numDocsWithField != entry.numValues) {
      entry.addressesOffset = meta.readLong();
      final int blockShift = meta.readVInt();
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
      entry.addressesLength = meta.readLong();
    }
    return entry;
  }

  public SortedNumericDocValues getSortedNumeric(SortedNumericEntry entry, IndexInput data) throws IOException {
    if (entry.numValues == entry.numDocsWithField) {
      return DocValues.singleton(getNumeric(entry, data));
    }

    final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
    final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput);

    final LongValues values = getNumericValues(entry, data);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new SortedNumericDocValues() {

        int doc = -1;
        long start, end;
        int count;

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          doc = target;
          return true;
        }

        @Override
        public long nextValue() throws IOException {
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          return count;
        }
      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new SortedNumericDocValues() {

        boolean set;
        long start, end;
        int count;

        @Override
        public int nextDoc() throws IOException {
          set = false;
          return disi.nextDoc();
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          set = false;
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          set = false;
          return disi.advanceExact(target);
        }

        @Override
        public long nextValue() throws IOException {
          set();
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          set();
          return count;
        }

        private void set() {
          if (set == false) {
            final int index = disi.index();
            start = addresses.get(index);
            end = addresses.get(index + 1L);
            count = (int) (end - start);
            set = true;
          }
        }

      };
    }
  }

  private static abstract class DenseNumericDocValues extends NumericDocValues {

    final int maxDoc;
    int doc = -1;

    DenseNumericDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) {
      doc = target;
      return true;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

  }

  private static abstract class SparseNumericDocValues extends NumericDocValues {

    final IndexedDISI disi;

    SparseNumericDocValues(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public long cost() {
      return disi.cost();
    }
  }

  public NumericDocValues getNumeric(NumericEntry entry, IndexInput data) throws IOException {
    if (entry.docsWithFieldOffset == -2) {
      // empty
      return DocValues.emptyNumeric();
    } else if (entry.docsWithFieldOffset == -1) {
      // dense
      if (entry.bitsPerValue == 0) {
        return new DenseNumericDocValues(maxDoc) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }
        };
      } else {
        final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
        if (entry.blockShift >= 0) {
          // dense but split into blocks of different bits per value
          return new DenseNumericDocValues(maxDoc) {
            final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice, data);

            @Override
            public long longValue() throws IOException {
              return vBPVReader.getLongValue(doc);
            }
          };
        } else {
          final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new DenseNumericDocValues(maxDoc) {
              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(doc)];
              }
            };
          } else {
            final long mul = entry.gcd;
            final long delta = entry.minValue;
            return new DenseNumericDocValues(maxDoc) {
              @Override
              public long longValue() throws IOException {
                return mul * values.get(doc) + delta;
              }
            };
          }
        }
      }
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numValues);
      if (entry.bitsPerValue == 0) {
        return new SparseNumericDocValues(disi) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }
        };
      } else {
        final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
        if (entry.blockShift >= 0) {
          // sparse and split into blocks of different bits per value
          return new SparseNumericDocValues(disi) {
            final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice, data);

            @Override
            public long longValue() throws IOException {
              final int index = disi.index();
              return vBPVReader.getLongValue(index);
            }
          };
        } else {
          final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(disi.index())];
              }
            };
          } else {
            final long mul = entry.gcd;
            final long delta = entry.minValue;
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return mul * values.get(disi.index()) + delta;
              }
            };
          }
        }
      }
    }
  }

  private LongValues getNumericValues(NumericEntry entry, IndexInput data) throws IOException {
    if (entry.bitsPerValue == 0) {
      return new LongValues() {
        @Override
        public long get(long index) {
          return entry.minValue;
        }
      };
    } else {
      final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
      if (entry.blockShift >= 0) {
        return new LongValues() {
          final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice, data);

          @Override
          public long get(long index) {
            try {
              return vBPVReader.getLongValue(index);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
      } else {
        final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
        if (entry.table != null) {
          final long[] table = entry.table;
          return new LongValues() {
            @Override
            public long get(long index) {
              return table[(int) values.get(index)];
            }
          };
        } else if (entry.gcd != 1) {
          final long gcd = entry.gcd;
          final long minValue = entry.minValue;
          return new LongValues() {
            @Override
            public long get(long index) {
              return values.get(index) * gcd + minValue;
            }
          };
        } else if (entry.minValue != 0) {
          final long minValue = entry.minValue;
          return new LongValues() {
            @Override
            public long get(long index) {
              return values.get(index) + minValue;
            }
          };
        } else {
          return values;
        }
      }
    }
  }

  /**
   * Reader for longs split into blocks of different bits per values.
   * The longs are requested by index and must be accessed in monotonically increasing order.
   */
  // Note: The order requirement could be removed as the jump-tables allow for backwards iteration
  // Note 2: The rankSlice is only used if an advance of > 1 block is called. Its construction could be lazy
  private class VaryingBPVReader {
    final RandomAccessInput slice; // 2 slices to avoid cache thrashing when using rank
    final RandomAccessInput rankSlice;
    final NumericEntry entry;
    final int shift;
    final long mul;
    final int mask;

    long block = -1;
    long delta;
    long offset;
    long blockEndOffset;
    LongValues values;

    VaryingBPVReader(NumericEntry entry, RandomAccessInput slice, IndexInput data) throws IOException {
      this.entry = entry;
      this.slice = slice;
      this.rankSlice = entry.valueJumpTableOffset == -1 ? null :
          data.randomAccessSlice(entry.valueJumpTableOffset, data.length() - entry.valueJumpTableOffset);
      shift = entry.blockShift;
      mul = entry.gcd;
      mask = (1 << shift) - 1;
    }

    long getLongValue(long index) throws IOException {
      final long block = index >>> shift;
      if (this.block != block) {
        int bitsPerValue;
        do {
          // If the needed block is the one directly following the current block, it is cheaper to avoid the cache
          if (rankSlice != null && block != this.block + 1) {
            blockEndOffset = rankSlice.readLong(block * Long.BYTES) - entry.valuesOffset;
            this.block = block - 1;
          }
          offset = blockEndOffset;
          bitsPerValue = slice.readByte(offset++);
          delta = slice.readLong(offset);
          offset += Long.BYTES;
          if (bitsPerValue == 0) {
            blockEndOffset = offset;
          } else {
            final int length = slice.readInt(offset);
            offset += Integer.BYTES;
            blockEndOffset = offset + length;
          }
          this.block++;
        } while (this.block != block);
        values = bitsPerValue == 0 ? LongValues.ZEROES : DirectReader.getInstance(slice, bitsPerValue, offset);
      }
      return mul * values.get(index & mask) + delta;
    }
  }
}

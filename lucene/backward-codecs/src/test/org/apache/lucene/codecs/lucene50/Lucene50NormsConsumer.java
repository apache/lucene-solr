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
package org.apache.lucene.codecs.lucene50;


import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FilterIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.FormatAndBits;

import static org.apache.lucene.codecs.lucene50.Lucene50NormsFormat.VERSION_CURRENT;

/**
 * Writer for {@link Lucene50NormsFormat}
 * @deprecated Only for testing old 5.0-5.2 segments
 */
@Deprecated
final class Lucene50NormsConsumer extends NormsConsumer { 
  static final int BLOCK_SIZE = 1 << 14;
  
  // threshold for indirect encoding, computed as 1 - 1/log2(maxint)
  // norms are only read for matching postings... so this is the threshold
  // where n log n operations < maxdoc (e.g. it performs similar to other fields)
  static final float INDIRECT_THRESHOLD = 1 - 1 / 31F;

  IndexOutput data, meta;
  
  Lucene50NormsConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(meta, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
  
  // we explicitly use only certain bits per value and a specified format, so we statically check this will work
  static {
    assert PackedInts.Format.PACKED_SINGLE_BLOCK.isSupported(1);
    assert PackedInts.Format.PACKED_SINGLE_BLOCK.isSupported(2);
    assert PackedInts.Format.PACKED_SINGLE_BLOCK.isSupported(4);
  }

  @Override
  public void addNormsField(FieldInfo field, Iterable<Number> values) throws IOException {
    writeNormsField(field, values, 0);
  }
  
  private void writeNormsField(FieldInfo field, Iterable<Number> values, int level) throws IOException {
    assert level <= 1; // we only "recurse" once in the indirect case
    meta.writeVInt(field.number);
    NormMap uniqueValues = new NormMap();
    int count = 0;
    
    for (Number nv : values) {
      if (nv == null) {
        throw new IllegalStateException("illegal norms data for field " + field.name + ", got null for value: " + count);
      }
      final long v = nv.longValue();

      if (uniqueValues != null) {
        if (v >= Byte.MIN_VALUE && v <= Byte.MAX_VALUE) {
          if (uniqueValues.add((byte) v)) {
            if (uniqueValues.size > 256) {
              uniqueValues = null;
            }
          }
        } else {
          // anything outside an 8 bit float comes from a custom scorer, which is an extreme edge case
          uniqueValues = null;
        }
      }
      count++;
    }

    if (uniqueValues == null) {
      addDeltaCompressed(values, count);
    } else if (uniqueValues.size == 1) {
      // 0 bpv
      addConstant(uniqueValues.values[0]);
    } else {
      // small number of unique values: this is the typical case
      uniqueValues.optimizeOrdinals();
      
      int numCommonValues = -1;
      int commonValuesCount = 0;
      if (level == 0 && count > 256) {
        float threshold_count = count * INDIRECT_THRESHOLD;
        if (uniqueValues.freqs[0] > threshold_count) {
          numCommonValues = 1;
        } else if ((commonValuesCount = sum(uniqueValues.freqs, 0, 3)) > threshold_count && uniqueValues.size > 4) {
          numCommonValues = 3;
        } else if ((commonValuesCount = sum(uniqueValues.freqs, 0, 15)) > threshold_count && uniqueValues.size > 16) {
          numCommonValues = 15;
        }
      }

      if (numCommonValues == -1) {
        // no pattern in values, just find the most efficient way to pack the values
        FormatAndBits compression = fastestFormatAndBits(uniqueValues.size - 1);
        if (compression.bitsPerValue == 8) {
          addUncompressed(values, count);
        } else {
          addTableCompressed(values, compression, count, uniqueValues);
        }
        
      } else if (numCommonValues == 1) {
        byte commonValue = uniqueValues.values[0];
        if (commonValue == 0) {
          // if the common value is missing, don't waste RAM on a bitset, since we won't be searching those docs
          addIndirect(field, values, count, uniqueValues, 0);
        } else {
          // otherwise, write a sparse bitset, where 1 indicates 'uncommon value'.
          addPatchedBitset(field, values, count, uniqueValues);
        }
      } else {
        addPatchedTable(field, values, numCommonValues, commonValuesCount, count, uniqueValues);
      }
    }
  }
  
  private int sum(int[] freqs, int start, int end) {
    int accum = 0;
    for (int i = start; i < end; ++i) {
      accum += freqs[i];
    }
    return accum;
  }
  
  private FormatAndBits fastestFormatAndBits(int max) {
    // we only use bpv=1,2,4,8     
    PackedInts.Format format = PackedInts.Format.PACKED_SINGLE_BLOCK;
    int bitsPerValue = PackedInts.bitsRequired(max);
    if (bitsPerValue == 3) {
      bitsPerValue = 4;
    } else if (bitsPerValue > 4) {
      bitsPerValue = 8;
    }
    return new FormatAndBits(format, bitsPerValue);
  }
  
  private void addConstant(byte constant) throws IOException {
    meta.writeVInt(0);
    meta.writeByte(Lucene50NormsFormat.CONST_COMPRESSED);
    meta.writeLong(constant);
  }

  private void addUncompressed(Iterable<Number> values, int count) throws IOException {
    meta.writeVInt(count);
    meta.writeByte(Lucene50NormsFormat.UNCOMPRESSED); // uncompressed byte[]
    meta.writeLong(data.getFilePointer());
    for (Number nv : values) {
      data.writeByte(nv.byteValue());
    }
  }
  
  private void addTableCompressed(Iterable<Number> values, FormatAndBits compression, int count, NormMap uniqueValues) throws IOException {
    meta.writeVInt(count);
    meta.writeByte(Lucene50NormsFormat.TABLE_COMPRESSED); // table-compressed
    meta.writeLong(data.getFilePointer());

    writeTable(values, compression, count, uniqueValues, uniqueValues.size);
  }

  private void writeTable(Iterable<Number> values, FormatAndBits compression, int count, NormMap uniqueValues, int numOrds) throws IOException {
    data.writeVInt(PackedInts.VERSION_CURRENT);
    data.writeVInt(compression.format.getId());
    data.writeVInt(compression.bitsPerValue);
    
    data.writeVInt(numOrds);
    for (int i = 0; i < numOrds; i++) {
      data.writeByte(uniqueValues.values[i]);
    }

    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data, compression.format, count, compression.bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
    for(Number nv : values) {
      int ord = uniqueValues.ord(nv.byteValue());
      if (ord < numOrds) {
        writer.add(ord);
      } else {
        writer.add(numOrds); // collapses all ords >= numOrds into a single value
      }
    }
    writer.finish();
  }
  
  private void addDeltaCompressed(Iterable<Number> values, int count) throws IOException {
    meta.writeVInt(count);
    meta.writeByte(Lucene50NormsFormat.DELTA_COMPRESSED); // delta-compressed
    meta.writeLong(data.getFilePointer());
    data.writeVInt(PackedInts.VERSION_CURRENT);
    data.writeVInt(BLOCK_SIZE);

    final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
    for (Number nv : values) {
      writer.add(nv.longValue());
    }
    writer.finish();
  }
  
  // encodes only uncommon values in a sparse bitset
  // access is constant time, and the common case is predictable
  // exceptions nest either to CONST (if there are only 2 values), or INDIRECT (if there are > 2 values)
  private void addPatchedBitset(FieldInfo field, final Iterable<Number> values, int count, NormMap uniqueValues) throws IOException {
    int commonCount = uniqueValues.freqs[0];
    
    meta.writeVInt(count - commonCount);
    meta.writeByte(Lucene50NormsFormat.PATCHED_BITSET);
    meta.writeLong(data.getFilePointer());
    
    // write docs with value
    writeDocsWithValue(values, uniqueValues, 0);
    
    // write exceptions: only two cases make sense
    // bpv = 1 (folded into sparse bitset already)
    // bpv > 1 (add indirect exception table)
    meta.writeVInt(field.number);
    if (uniqueValues.size == 2) {
      // special case: implicit in bitset
      addConstant(uniqueValues.values[1]);
    } else {
      // exception table
      addIndirect(field, values, count, uniqueValues, 0);
    }
  }

  // encodes common values in a table, and the rest of the values as exceptions using INDIRECT.
  // the exceptions should not be accessed very often, since the values are uncommon
  private void addPatchedTable(FieldInfo field, final Iterable<Number> values, final int numCommonValues, int commonValuesCount, int count, final NormMap uniqueValues) throws IOException {
    meta.writeVInt(count);
    meta.writeByte(Lucene50NormsFormat.PATCHED_TABLE);
    meta.writeLong(data.getFilePointer());

    assert numCommonValues == 3 || numCommonValues == 15;
    FormatAndBits compression = fastestFormatAndBits(numCommonValues);
    
    writeTable(values, compression, count, uniqueValues, numCommonValues);

    meta.writeVInt(field.number);
    addIndirect(field, values, count - commonValuesCount, uniqueValues, numCommonValues);
  }
  
  // encodes values as sparse array: keys[] and values[]
  // access is log(N) where N = keys.length (slow!)
  // so this is only appropriate as an exception table for patched, or when common value is 0 (wont be accessed by searching) 
  private void addIndirect(FieldInfo field, final Iterable<Number> values, int count, final NormMap uniqueValues, final int minOrd) throws IOException {
    int commonCount = uniqueValues.freqs[minOrd];
    
    meta.writeVInt(count - commonCount);
    meta.writeByte(Lucene50NormsFormat.INDIRECT);
    meta.writeLong(data.getFilePointer());
    
    // write docs with value
    writeDocsWithValue(values, uniqueValues, minOrd);
    
    // write actual values
    writeNormsField(field, new Iterable<Number>() {
      @Override
      public Iterator<Number> iterator() {
        return new FilterIterator<Number, Number>(values.iterator()) {
          @Override
          protected boolean predicateFunction(Number value) {
            return uniqueValues.ord(value.byteValue()) > minOrd;
          }
        };
      }
    }, 1);
  }
  
  private void writeDocsWithValue(final Iterable<Number> values, NormMap uniqueValues, int minOrd) throws IOException {
    data.writeLong(uniqueValues.values[minOrd]);
    data.writeVInt(PackedInts.VERSION_CURRENT);
    data.writeVInt(BLOCK_SIZE);
    
    // write docs with value
    final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
    int doc = 0;
    for (Number n : values) {
      int ord = uniqueValues.ord(n.byteValue());
      if (ord > minOrd) {
        writer.add(doc);
      }
      doc++;
    }
    writer.finish();
  }
  
  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeVInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }

  // specialized deduplication of long->ord for norms: 99.99999% of the time this will be a single-byte range.
  static class NormMap {
    // we use short: at most we will add 257 values to this map before it's rejected as too big above.
    private final short[] ords = new short[256];
    final int[] freqs = new int[257];
    final byte[] values = new byte[257];
    int size;

    {
      Arrays.fill(ords, (short)-1);
    }

    // adds an item to the mapping. returns true if actually added
    public boolean add(byte l) {
      assert size <= 256; // once we add > 256 values, we nullify the map in addNumericField and don't use this strategy
      int index = (int)l + 128;
      short previous = ords[index];
      if (previous < 0) {
        short slot = (short)size;
        ords[index] = slot;
        freqs[slot]++;
        values[slot] = l;
        size++;
        return true;
      } else {
        freqs[previous]++;
        return false;
      }
    }

    public int ord(byte value) {
      return ords[(int)value + 128];
    }

    // reassign ordinals so higher frequencies have lower ordinals
    public void optimizeOrdinals() {
      new InPlaceMergeSorter() {
        @Override
        protected int compare(int i, int j) {
          return freqs[j] - freqs[i]; // sort descending
        }
        @Override
        protected void swap(int i, int j) {
          // swap ordinal i with ordinal j
          ords[(int)values[i] + 128] = (short)j;
          ords[(int)values[j] + 128] = (short)i;

          int tmpFreq = freqs[i];
          byte tmpValue = values[i];
          freqs[i] = freqs[j];
          values[i] = values[j];
          freqs[j] = tmpFreq;
          values[j] = tmpValue;
        }
      }.sort(0, size);
    }
  }

}

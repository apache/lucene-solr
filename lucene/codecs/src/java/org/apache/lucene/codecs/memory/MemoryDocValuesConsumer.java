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
package org.apache.lucene.codecs.memory;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts.FormatAndBits;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.VERSION_CURRENT;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.BLOCK_SIZE;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.BYTES;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.NUMBER;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.FST;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.SORTED_SET;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.SORTED_SET_SINGLETON;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.SORTED_NUMERIC;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.SORTED_NUMERIC_SINGLETON;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.DELTA_COMPRESSED;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.BLOCK_COMPRESSED;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.GCD_COMPRESSED;
import static org.apache.lucene.codecs.memory.MemoryDocValuesProducer.TABLE_COMPRESSED;

/**
 * Writer for {@link MemoryDocValuesFormat}
 */
class MemoryDocValuesConsumer extends DocValuesConsumer {
  IndexOutput data, meta;
  final int maxDoc;
  final float acceptableOverheadRatio;
  
  MemoryDocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension, float acceptableOverheadRatio) throws IOException {
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    maxDoc = state.segmentInfo.maxDoc();
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

  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    addNumericField(field, values, true);
  }

  void addNumericField(FieldInfo field, Iterable<Number> values, boolean optimizeStorage) throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(NUMBER);
    meta.writeLong(data.getFilePointer());
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    long blockSum = 0;
    long gcd = 0;
    boolean missing = false;
    // TODO: more efficient?
    HashSet<Long> uniqueValues = null;
    long count = 0;

    if (optimizeStorage) {
      uniqueValues = new HashSet<>();

      long currentBlockMin = Long.MAX_VALUE;
      long currentBlockMax = Long.MIN_VALUE;
      for (Number nv : values) {
        final long v;
        if (nv == null) {
          v = 0;
          missing = true;
        } else {
          v = nv.longValue();
        }

        if (gcd != 1) {
          if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
            // in that case v - minValue might overflow and make the GCD computation return
            // wrong results. Since these extreme values are unlikely, we just discard
            // GCD computation for them
            gcd = 1;
          } else if (count != 0) { // minValue needs to be set first
            gcd = MathUtil.gcd(gcd, v - minValue);
          }
        }

        currentBlockMin = Math.min(minValue, v);
        currentBlockMax = Math.max(maxValue, v);
        
        minValue = Math.min(minValue, v);
        maxValue = Math.max(maxValue, v);

        if (uniqueValues != null) {
          if (uniqueValues.add(v)) {
            if (uniqueValues.size() > 256) {
              uniqueValues = null;
            }
          }
        }

        ++count;
        if (count % BLOCK_SIZE == 0) {
          final long blockDelta = currentBlockMax - currentBlockMin;
          final int blockDeltaRequired = PackedInts.unsignedBitsRequired(blockDelta);
          final int blockBPV = PackedInts.fastestFormatAndBits(BLOCK_SIZE, blockDeltaRequired, acceptableOverheadRatio).bitsPerValue;
          blockSum += blockBPV;
          currentBlockMax = Long.MIN_VALUE;
          currentBlockMin = Long.MAX_VALUE;
        }
      }
    } else {
      for (Number nv : values) {
        long v = nv.longValue();
        maxValue = Math.max(v, maxValue);
        minValue = Math.min(v, minValue);
        count++;
      }
    }
    
    if (missing) {
      long start = data.getFilePointer();
      writeMissingBitset(values);
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    } else {
      meta.writeLong(-1L);
    }
    
    final long delta = maxValue - minValue;
    final int deltaRequired = delta < 0 ? 64 : PackedInts.bitsRequired(delta);
    final FormatAndBits deltaBPV = PackedInts.fastestFormatAndBits(maxDoc, deltaRequired, acceptableOverheadRatio);
        
    final FormatAndBits tableBPV;
    if (count < Integer.MAX_VALUE && uniqueValues != null) {
      tableBPV = PackedInts.fastestFormatAndBits(maxDoc, PackedInts.bitsRequired(uniqueValues.size()-1), acceptableOverheadRatio);
    } else {
      tableBPV = null;
    }
    
    final FormatAndBits gcdBPV;
    if (count < Integer.MAX_VALUE && gcd != 0 && gcd != 1) {
      final long gcdDelta = (maxValue - minValue) / gcd;
      final int gcdRequired = gcdDelta < 0 ? 64 : PackedInts.bitsRequired(gcdDelta);
      gcdBPV = PackedInts.fastestFormatAndBits(maxDoc, gcdRequired, acceptableOverheadRatio);
    } else {
      gcdBPV = null;
    }
    
    boolean doBlock = false;
    if (blockSum != 0) {
      int numBlocks = maxDoc / BLOCK_SIZE;
      float avgBPV = blockSum / (float)numBlocks;
      // just a heuristic, with tiny amounts of blocks our estimate is skewed as we ignore the final "incomplete" block.
      // with at least 4 blocks it's pretty accurate. The difference must also be significant (according to acceptable overhead).
      if (numBlocks >= 4 && (avgBPV+avgBPV*acceptableOverheadRatio) < deltaBPV.bitsPerValue) {
        doBlock = true;
      }
    }
    // blockpackedreader allows us to read in huge streams of ints
    if (count >= Integer.MAX_VALUE) {
      doBlock = true;
    }
    
    if (tableBPV != null && (tableBPV.bitsPerValue+tableBPV.bitsPerValue*acceptableOverheadRatio) < deltaBPV.bitsPerValue) {
      // small number of unique values
      meta.writeByte(TABLE_COMPRESSED); // table-compressed
      Long[] decode = uniqueValues.toArray(new Long[uniqueValues.size()]);
      final HashMap<Long,Integer> encode = new HashMap<>();
      int length = 1 << tableBPV.bitsPerValue;
      data.writeVInt(length);
      for (int i = 0; i < decode.length; i++) {
        data.writeLong(decode[i]);
        encode.put(decode[i], i);
      }
      for (int i = decode.length; i < length; i++) {
        data.writeLong(0);
      }
      
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeLong(count);
      data.writeVInt(tableBPV.format.getId());
      data.writeVInt(tableBPV.bitsPerValue);
      
      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data, tableBPV.format, (int)count, tableBPV.bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
      for(Number nv : values) {
        writer.add(encode.get(nv == null ? 0 : nv.longValue()));
      }
      writer.finish();
    } else if (gcdBPV != null && (gcdBPV.bitsPerValue+gcdBPV.bitsPerValue*acceptableOverheadRatio) < deltaBPV.bitsPerValue) {
      meta.writeByte(GCD_COMPRESSED);
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeLong(count);
      data.writeLong(minValue);
      data.writeLong(gcd);
      data.writeVInt(gcdBPV.format.getId());
      data.writeVInt(gcdBPV.bitsPerValue);

      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data, gcdBPV.format, (int)count, gcdBPV.bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
      for (Number nv : values) {
        long value = nv == null ? 0 : nv.longValue();
        writer.add((value - minValue) / gcd);
      }
      writer.finish();
    } else if (doBlock) {
      meta.writeByte(BLOCK_COMPRESSED); // block delta-compressed
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeLong(count);
      data.writeVInt(BLOCK_SIZE);
      final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
      for (Number nv : values) {
        writer.add(nv == null ? 0 : nv.longValue());
      }
      writer.finish();
    } else {
      meta.writeByte(DELTA_COMPRESSED); // delta-compressed
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeLong(count);
      final long minDelta = deltaBPV.bitsPerValue == 64 ? 0 : minValue;
      data.writeLong(minDelta);
      data.writeVInt(deltaBPV.format.getId());
      data.writeVInt(deltaBPV.bitsPerValue);

      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data, deltaBPV.format, (int)count, deltaBPV.bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
      for (Number nv : values) {
        long v = nv == null ? 0 : nv.longValue();
        writer.add(v - minDelta);
      }
      writer.finish();
    }
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
        CodecUtil.writeFooter(data);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      data = meta = null;
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, final Iterable<BytesRef> values) throws IOException {
    // write the byte[] data
    meta.writeVInt(field.number);
    meta.writeByte(BYTES);
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    final long startFP = data.getFilePointer();
    boolean missing = false;
    int upto = 0;
    for(BytesRef v : values) {
      final int length;
      if (v == null) {
        length = 0;
        missing = true;
      } else {
        length = v.length;
      }
      if (length > MemoryDocValuesFormat.MAX_BINARY_FIELD_LENGTH) {
        throw new IllegalArgumentException("DocValuesField \"" + field.name + "\" is too large, must be <= " + MemoryDocValuesFormat.MAX_BINARY_FIELD_LENGTH + " but got length=" + length + " v=" + v + "; upto=" + upto + " values=" + values);
      }
      upto++;
      minLength = Math.min(minLength, length);
      maxLength = Math.max(maxLength, length);
      if (v != null) {
        data.writeBytes(v.bytes, v.offset, v.length);
      }
    }
    meta.writeLong(startFP);
    meta.writeLong(data.getFilePointer() - startFP);
    if (missing) {
      long start = data.getFilePointer();
      writeMissingBitset(values);
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    } else {
      meta.writeLong(-1L);
    }
    meta.writeVInt(minLength);
    meta.writeVInt(maxLength);
    
    // if minLength == maxLength, it's a fixed-length byte[], we are done (the addresses are implicit)
    // otherwise, we need to record the length fields...
    if (minLength != maxLength) {
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeVInt(BLOCK_SIZE);

      final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
      long addr = 0;
      for (BytesRef v : values) {
        if (v != null) {
          addr += v.length;
        }
        writer.add(addr);
      }
      writer.finish();
    }
  }
  
  private void writeFST(FieldInfo field, Iterable<BytesRef> values) throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(FST);
    meta.writeLong(data.getFilePointer());
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<>(INPUT_TYPE.BYTE1, outputs);
    IntsRefBuilder scratch = new IntsRefBuilder();
    long ord = 0;
    for (BytesRef v : values) {
      builder.add(Util.toIntsRef(v, scratch), ord);
      ord++;
    }
    FST<Long> fst = builder.finish();
    if (fst != null) {
      fst.save(data);
    }
    meta.writeVLong(ord);
  }
  
  // TODO: in some cases representing missing with minValue-1 wouldn't take up additional space and so on,
  // but this is very simple, and algorithms only check this for values of 0 anyway (doesnt slow down normal decode)
  void writeMissingBitset(Iterable<?> values) throws IOException {
    long bits = 0;
    int count = 0;
    for (Object v : values) {
      if (count == 64) {
        data.writeLong(bits);
        count = 0;
        bits = 0;
      }
      if (v != null) {
        bits |= 1L << (count & 0x3f);
      }
      count++;
    }
    if (count > 0) {
      data.writeLong(bits);
    }
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    // write the ordinals as numerics
    addNumericField(field, docToOrd, false);
    
    // write the values as FST
    writeFST(field, values);
  }
  
  @Override
  public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
    meta.writeVInt(field.number);
    
    if (isSingleValued(docToValueCount)) {
      meta.writeByte(SORTED_NUMERIC_SINGLETON);
      addNumericField(field, singletonView(docToValueCount, values, null), true);
    } else {
      meta.writeByte(SORTED_NUMERIC);
      
      // write the addresses:
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeVInt(BLOCK_SIZE);
      meta.writeLong(data.getFilePointer());
      final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
      long addr = 0;
      writer.add(addr);
      for (Number v : docToValueCount) {
        addr += v.longValue();
        writer.add(addr);
      }
      writer.finish();
      long valueCount = writer.ord();
      meta.writeLong(valueCount);
      
      // write the values
      addNumericField(field, values, true);
    }
  }

  // note: this might not be the most efficient... but it's fairly simple
  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, final Iterable<Number> docToOrdCount, final Iterable<Number> ords) throws IOException {
    meta.writeVInt(field.number);
    
    if (isSingleValued(docToOrdCount)) {
      meta.writeByte(SORTED_SET_SINGLETON);
      addSortedField(field, values, singletonView(docToOrdCount, ords, -1L));
    } else {
      meta.writeByte(SORTED_SET);
      // write the ordinals as a binary field
      addBinaryField(field, new Iterable<BytesRef>() {
        @Override
        public Iterator<BytesRef> iterator() {
          return new SortedSetIterator(docToOrdCount.iterator(), ords.iterator());
        }
      });
      
      // write the values as FST
      writeFST(field, values);
    }
  }
  
  // per-document vint-encoded byte[]
  static class SortedSetIterator implements Iterator<BytesRef> {
    byte[] buffer = new byte[10];
    ByteArrayDataOutput out = new ByteArrayDataOutput();
    BytesRef ref = new BytesRef();
    
    final Iterator<Number> counts;
    final Iterator<Number> ords;
    
    SortedSetIterator(Iterator<Number> counts, Iterator<Number> ords) {
      this.counts = counts;
      this.ords = ords;
    }
    
    @Override
    public boolean hasNext() {
      return counts.hasNext();
    }

    @Override
    public BytesRef next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      
      int count = counts.next().intValue();
      int maxSize = count*9; // worst case
      if (maxSize > buffer.length) {
        buffer = ArrayUtil.grow(buffer, maxSize);
      }
      
      try {
        encodeValues(count);
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
      
      ref.bytes = buffer;
      ref.offset = 0;
      ref.length = out.getPosition();

      return ref;
    }
    
    // encodes count values to buffer
    private void encodeValues(int count) throws IOException {
      out.reset(buffer);
      long lastOrd = 0;
      for (int i = 0; i < count; i++) {
        long ord = ords.next().longValue();
        out.writeVLong(ord - lastOrd);
        lastOrd = ord;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

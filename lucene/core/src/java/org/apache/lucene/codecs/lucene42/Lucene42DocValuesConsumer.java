package org.apache.lucene.codecs.lucene42;

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
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.FormatAndBits;

/**
 * Writer for {@link Lucene42DocValuesFormat}
 */
class Lucene42DocValuesConsumer extends DocValuesConsumer {
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  
  static final byte NUMBER = 0;
  static final byte BYTES = 1;
  static final byte FST = 2;

  static final int BLOCK_SIZE = 4096;
  
  static final byte DELTA_COMPRESSED = 0;
  static final byte TABLE_COMPRESSED = 1;
  static final byte UNCOMPRESSED = 2;

  final IndexOutput data, meta;
  final int maxDoc;
  final float acceptableOverheadRatio;
  
  Lucene42DocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension, float acceptableOverheadRatio) throws IOException {
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    maxDoc = state.segmentInfo.getDocCount();
    boolean success = false;
    try {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeHeader(data, dataCodec, VERSION_CURRENT);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeHeader(meta, metaCodec, VERSION_CURRENT);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }
  
  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(NUMBER);
    meta.writeLong(data.getFilePointer());
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    // TODO: more efficient?
    HashSet<Long> uniqueValues = new HashSet<Long>();
    for(Number nv : values) {
      long v = nv.longValue();
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
      if (uniqueValues != null) {
        if (uniqueValues.add(v)) {
          if (uniqueValues.size() > 256) {
            uniqueValues = null;
          }
        }
      }
    }

    if (uniqueValues != null) {
      // small number of unique values
      final int bitsPerValue = PackedInts.bitsRequired(uniqueValues.size()-1);
      FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(maxDoc, bitsPerValue, acceptableOverheadRatio);
      if (formatAndBits.bitsPerValue == 8 && minValue >= Byte.MIN_VALUE && maxValue <= Byte.MAX_VALUE) {
        meta.writeByte(UNCOMPRESSED); // uncompressed
        for (Number nv : values) {
          data.writeByte((byte) nv.longValue());
        }
      } else {
        meta.writeByte(TABLE_COMPRESSED); // table-compressed
        Long[] decode = uniqueValues.toArray(new Long[uniqueValues.size()]);
        final HashMap<Long,Integer> encode = new HashMap<Long,Integer>();
        data.writeVInt(decode.length);
        for (int i = 0; i < decode.length; i++) {
          data.writeLong(decode[i]);
          encode.put(decode[i], i);
        }

        meta.writeVInt(PackedInts.VERSION_CURRENT);
        data.writeVInt(formatAndBits.format.getId());
        data.writeVInt(formatAndBits.bitsPerValue);

        final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data, formatAndBits.format, maxDoc, formatAndBits.bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
        for(Number nv : values) {
          writer.add(encode.get(nv.longValue()));
        }
        writer.finish();
      }
    } else {
      meta.writeByte(DELTA_COMPRESSED); // delta-compressed

      meta.writeVInt(PackedInts.VERSION_CURRENT);
      data.writeVInt(BLOCK_SIZE);

      final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
      for (Number nv : values) {
        writer.add(nv.longValue());
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
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
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
    for(BytesRef v : values) {
      minLength = Math.min(minLength, v.length);
      maxLength = Math.max(maxLength, v.length);
      data.writeBytes(v.bytes, v.offset, v.length);
    }
    meta.writeLong(startFP);
    meta.writeLong(data.getFilePointer() - startFP);
    meta.writeVInt(minLength);
    meta.writeVInt(maxLength);
    
    // if minLength == maxLength, its a fixed-length byte[], we are done (the addresses are implicit)
    // otherwise, we need to record the length fields...
    if (minLength != maxLength) {
      meta.writeVInt(PackedInts.VERSION_CURRENT);
      meta.writeVInt(BLOCK_SIZE);

      final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
      long addr = 0;
      for (BytesRef v : values) {
        addr += v.length;
        writer.add(addr);
      }
      writer.finish();
    }
  }
  
  private void writeFST(FieldInfo field, Iterable<BytesRef> values) throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(FST);
    meta.writeLong(data.getFilePointer());
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
    Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
    IntsRef scratch = new IntsRef();
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

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    // write the ordinals as numerics
    addNumericField(field, docToOrd);
    
    // write the values as FST
    writeFST(field, values);
  }

  // note: this might not be the most efficient... but its fairly simple
  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, final Iterable<Number> docToOrdCount, final Iterable<Number> ords) throws IOException {
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

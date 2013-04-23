package org.apache.lucene.codecs.diskdv;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.MonotonicBlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

/** writer for {@link DiskDocValuesFormat} */
public class DiskDocValuesConsumer extends DocValuesConsumer {

  static final int BLOCK_SIZE = 16384;

  /** Compressed using packed blocks of ints. */
  public static final int DELTA_COMPRESSED = 0;
  /** Compressed by computing the GCD. */
  public static final int GCD_COMPRESSED = 1;
  /** Compressed by giving IDs to unique values. */
  public static final int TABLE_COMPRESSED = 2;

  final IndexOutput data, meta;
  final int maxDoc;
  
  public DiskDocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeHeader(data, dataCodec, DiskDocValuesFormat.VERSION_CURRENT);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeHeader(meta, metaCodec, DiskDocValuesFormat.VERSION_CURRENT);
      maxDoc = state.segmentInfo.getDocCount();
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
    long count = 0;
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    long gcd = 0;
    // TODO: more efficient?
    HashSet<Long> uniqueValues = null;
    if (optimizeStorage) {
      uniqueValues = new HashSet<Long>();

      for (Number nv : values) {
        final long v = nv.longValue();

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
      }
    } else {
      for (@SuppressWarnings("unused") Number nv : values) {
        ++count;
      }
    }
    
    final long delta = maxValue - minValue;

    final int format;
    if (uniqueValues != null
        && (delta < 0L || PackedInts.bitsRequired(uniqueValues.size() - 1) < PackedInts.bitsRequired(delta))
        && count <= Integer.MAX_VALUE) {
      format = TABLE_COMPRESSED;
    } else if (gcd != 0 && gcd != 1) {
      format = GCD_COMPRESSED;
    } else {
      format = DELTA_COMPRESSED;
    }
    meta.writeVInt(field.number);
    meta.writeByte(DiskDocValuesFormat.NUMERIC);
    meta.writeVInt(format);
    meta.writeVInt(PackedInts.VERSION_CURRENT);
    meta.writeLong(data.getFilePointer());
    meta.writeVLong(count);
    meta.writeVInt(BLOCK_SIZE);

    switch (format) {
      case GCD_COMPRESSED:
        meta.writeLong(minValue);
        meta.writeLong(gcd);
        final BlockPackedWriter quotientWriter = new BlockPackedWriter(data, BLOCK_SIZE);
        for (Number nv : values) {
          quotientWriter.add((nv.longValue() - minValue) / gcd);
        }
        quotientWriter.finish();
        break;
      case DELTA_COMPRESSED:
        final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
        for (Number nv : values) {
          writer.add(nv.longValue());
        }
        writer.finish();
        break;
      case TABLE_COMPRESSED:
        final Long[] decode = uniqueValues.toArray(new Long[uniqueValues.size()]);
        final HashMap<Long,Integer> encode = new HashMap<Long,Integer>();
        meta.writeVInt(decode.length);
        for (int i = 0; i < decode.length; i++) {
          meta.writeLong(decode[i]);
          encode.put(decode[i], i);
        }
        final int bitsRequired = PackedInts.bitsRequired(uniqueValues.size() - 1);
        final PackedInts.Writer ordsWriter = PackedInts.getWriterNoHeader(data, PackedInts.Format.PACKED, (int) count, bitsRequired, PackedInts.DEFAULT_BUFFER_SIZE);
        for (Number nv : values) {
          ordsWriter.add(encode.get(nv.longValue()));
        }
        ordsWriter.finish();
        break;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, final Iterable<BytesRef> values) throws IOException {
    // write the byte[] data
    meta.writeVInt(field.number);
    meta.writeByte(DiskDocValuesFormat.BINARY);
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;
    final long startFP = data.getFilePointer();
    long count = 0;
    for(BytesRef v : values) {
      minLength = Math.min(minLength, v.length);
      maxLength = Math.max(maxLength, v.length);
      data.writeBytes(v.bytes, v.offset, v.length);
      count++;
    }
    meta.writeVInt(minLength);
    meta.writeVInt(maxLength);
    meta.writeVLong(count);
    meta.writeLong(startFP);
    
    // if minLength == maxLength, its a fixed-length byte[], we are done (the addresses are implicit)
    // otherwise, we need to record the length fields...
    if (minLength != maxLength) {
      meta.writeLong(data.getFilePointer());
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

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(DiskDocValuesFormat.SORTED);
    addBinaryField(field, values);
    addNumericField(field, docToOrd, false);
  }
  
  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
    meta.writeVInt(field.number);
    meta.writeByte(DiskDocValuesFormat.SORTED_SET);
    // write the ord -> byte[] as a binary field
    addBinaryField(field, values);
    // write the stream of ords as a numeric field
    // NOTE: we could return an iterator that delta-encodes these within a doc
    addNumericField(field, ords, false);
    
    // write the doc -> ord count as a absolute index to the stream
    meta.writeVInt(field.number);
    meta.writeByte(DiskDocValuesFormat.NUMERIC);
    meta.writeVInt(DELTA_COMPRESSED);
    meta.writeVInt(PackedInts.VERSION_CURRENT);
    meta.writeLong(data.getFilePointer());
    meta.writeVLong(maxDoc);
    meta.writeVInt(BLOCK_SIZE);

    final MonotonicBlockPackedWriter writer = new MonotonicBlockPackedWriter(data, BLOCK_SIZE);
    long addr = 0;
    for (Number v : docToOrdCount) {
      addr += v.longValue();
      writer.add(addr);
    }
    writer.finish();
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
}

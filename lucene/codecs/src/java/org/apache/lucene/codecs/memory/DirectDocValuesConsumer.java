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
import java.util.Iterator;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.VERSION_CURRENT;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.BYTES;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_NUMERIC;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_NUMERIC_SINGLETON;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_SET;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.SORTED_SET_SINGLETON;
import static org.apache.lucene.codecs.memory.DirectDocValuesProducer.NUMBER;

/**
 * Writer for {@link DirectDocValuesFormat}
 */

class DirectDocValuesConsumer extends DocValuesConsumer {
  IndexOutput data, meta;
  final int maxDoc;

  DirectDocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
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
    meta.writeVInt(field.number);
    meta.writeByte(NUMBER);
    addNumericFieldValues(field, values);
  }

  private void addNumericFieldValues(FieldInfo field, Iterable<Number> values) throws IOException {
    meta.writeLong(data.getFilePointer());
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    boolean missing = false;

    long count = 0;
    for (Number nv : values) {
      if (nv != null) {
        long v = nv.longValue();
        minValue = Math.min(minValue, v);
        maxValue = Math.max(maxValue, v);
      } else {
        missing = true;
      }
      count++;
      if (count >= DirectDocValuesFormat.MAX_SORTED_SET_ORDS) {
        throw new IllegalArgumentException("DocValuesField \"" + field.name + "\" is too large, must be <= " + DirectDocValuesFormat.MAX_SORTED_SET_ORDS + " values/total ords");
      }
    }
    meta.writeInt((int) count);
    
    if (missing) {
      long start = data.getFilePointer();
      writeMissingBitset(values);
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    } else {
      meta.writeLong(-1L);
    }

    byte byteWidth;
    if (minValue >= Byte.MIN_VALUE && maxValue <= Byte.MAX_VALUE) {
      byteWidth = 1;
    } else if (minValue >= Short.MIN_VALUE && maxValue <= Short.MAX_VALUE) {
      byteWidth = 2;
    } else if (minValue >= Integer.MIN_VALUE && maxValue <= Integer.MAX_VALUE) {
      byteWidth = 4;
    } else {
      byteWidth = 8;
    }
    meta.writeByte(byteWidth);

    for (Number nv : values) {
      long v;
      if (nv != null) {
        v = nv.longValue();
      } else {
        v = 0;
      }

      switch(byteWidth) {
      case 1:
        data.writeByte((byte) v);
        break;
      case 2:
        data.writeShort((short) v);
        break;
      case 4:
        data.writeInt((int) v);
        break;
      case 8:
        data.writeLong(v);
        break;
      }
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
    meta.writeVInt(field.number);
    meta.writeByte(BYTES);
    addBinaryFieldValues(field, values);
  }

  private void addBinaryFieldValues(FieldInfo field, final Iterable<BytesRef> values) throws IOException {
    // write the byte[] data
    final long startFP = data.getFilePointer();
    boolean missing = false;
    long totalBytes = 0;
    int count = 0;
    for(BytesRef v : values) {
      if (v != null) {
        data.writeBytes(v.bytes, v.offset, v.length);
        totalBytes += v.length;
        if (totalBytes > DirectDocValuesFormat.MAX_TOTAL_BYTES_LENGTH) {
          throw new IllegalArgumentException("DocValuesField \"" + field.name + "\" is too large, cannot have more than DirectDocValuesFormat.MAX_TOTAL_BYTES_LENGTH (" + DirectDocValuesFormat.MAX_TOTAL_BYTES_LENGTH + ") bytes");
        }
      } else {
        missing = true;
      }
      count++;
    }

    meta.writeLong(startFP);
    meta.writeInt((int) totalBytes);
    meta.writeInt(count);
    if (missing) {
      long start = data.getFilePointer();
      writeMissingBitset(values);
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    } else {
      meta.writeLong(-1L);
    }
    
    int addr = 0;
    for (BytesRef v : values) {
      data.writeInt(addr);
      if (v != null) {
        addr += v.length;
      }
    }
    data.writeInt(addr);
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
    meta.writeVInt(field.number);
    meta.writeByte(SORTED);

    // write the ordinals as numerics
    addNumericFieldValues(field, docToOrd);
    
    // write the values as binary
    addBinaryFieldValues(field, values);
  }
  
  @Override
  public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
    meta.writeVInt(field.number);
    if (isSingleValued(docToValueCount)) {
      meta.writeByte(SORTED_NUMERIC_SINGLETON);
      addNumericFieldValues(field, singletonView(docToValueCount, values, null));
    } else {
      meta.writeByte(SORTED_NUMERIC);

      // First write docToValueCounts, except we "aggregate" the
      // counts so they turn into addresses, and add a final
      // value = the total aggregate:
      addNumericFieldValues(field, countToAddressIterator(docToValueCount));

      // Write values for all docs, appended into one big
      // numerics:
      addNumericFieldValues(field, values);
    }
  }

  // note: this might not be the most efficient... but it's fairly simple
  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, final Iterable<Number> docToOrdCount, final Iterable<Number> ords) throws IOException {
    meta.writeVInt(field.number);
    
    if (isSingleValued(docToOrdCount)) {
      meta.writeByte(SORTED_SET_SINGLETON);
      // Write ordinals for all docs, appended into one big
      // numerics:
      addNumericFieldValues(field, singletonView(docToOrdCount, ords, -1L));
      
      // write the values as binary
      addBinaryFieldValues(field, values);
    } else {
      meta.writeByte(SORTED_SET);

      // First write docToOrdCounts, except we "aggregate" the
      // counts so they turn into addresses, and add a final
      // value = the total aggregate:
      addNumericFieldValues(field, countToAddressIterator(docToOrdCount));

      // Write ordinals for all docs, appended into one big
      // numerics:
      addNumericFieldValues(field, ords);
      
      // write the values as binary
      addBinaryFieldValues(field, values);
    }
  }
  
  /** 
   * Just aggregates the count values so they become
   * "addresses", and adds one more value in the end
   * (the final sum)
   */ 
  private Iterable<Number> countToAddressIterator(final Iterable<Number> counts) {
    return new Iterable<Number>() {
      @Override
      public Iterator<Number> iterator() {
        final Iterator<Number> iter = counts.iterator();
        
        return new Iterator<Number>() {
          
          long sum;
          boolean ended;
          
          @Override
          public boolean hasNext() {
            return iter.hasNext() || !ended;
          }
          
          @Override
          public Number next() {
            long toReturn = sum;
            
            if (iter.hasNext()) {
              Number n = iter.next();
              if (n != null) {
                sum += n.longValue();
              }
            } else if (!ended) {
              ended = true;
            } else {
              assert false;
            }
            
            return toReturn;
          }
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}

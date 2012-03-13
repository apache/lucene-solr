package org.apache.lucene.codecs.lucene40.values;

/**
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

import org.apache.lucene.codecs.DocValuesArraySource;
import org.apache.lucene.codecs.lucene40.values.FixedStraightBytesImpl.FixedBytesWriterBase;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Stores integers using {@link PackedInts}
 * 
 * @lucene.experimental
 * */
class PackedIntValues {

  private static final String CODEC_NAME = "PackedInts";
  private static final byte PACKED = 0x00;
  private static final byte FIXED_64 = 0x01;

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class PackedIntsWriter extends FixedBytesWriterBase {

    private long minValue;
    private long maxValue;
    private boolean started;
    private int lastDocId = -1;

    protected PackedIntsWriter(Directory dir, String id, Counter bytesUsed,
        IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context, Type.VAR_INTS);
      bytesRef = new BytesRef(8);
    }
    
    @Override
    public void finish(int docCount) throws IOException {
      boolean success = false;
      final IndexOutput dataOut = getOrCreateDataOut();
      try {
        if (!started) {
          minValue = maxValue = 0;
        }
        final long delta = maxValue - minValue;
        // if we exceed the range of positive longs we must switch to fixed
        // ints
        if (delta <= (maxValue >= 0 && minValue <= 0 ? Long.MAX_VALUE
            : Long.MAX_VALUE - 1) && delta >= 0) {
          dataOut.writeByte(PACKED);
          writePackedInts(dataOut, docCount);
          return; // done
        } else {
          dataOut.writeByte(FIXED_64);
        }
        writeData(dataOut);
        writeZeros(docCount - (lastDocID + 1), dataOut);
        success = true;
      } finally {
        resetPool();
        if (success) {
          IOUtils.close(dataOut);
        } else {
          IOUtils.closeWhileHandlingException(dataOut);
        }
      }
    }

    private void writePackedInts(IndexOutput datOut, int docCount) throws IOException {
      datOut.writeLong(minValue);
      
      // write a default value to recognize docs without a value for that
      // field
      final long defaultValue = maxValue >= 0 && minValue <= 0 ? 0 - minValue
          : ++maxValue - minValue;
      datOut.writeLong(defaultValue);
      PackedInts.Writer w = PackedInts.getWriter(datOut, docCount,
          PackedInts.bitsRequired(maxValue - minValue));
      for (int i = 0; i < lastDocID + 1; i++) {
        set(bytesRef, i);
        byte[] bytes = bytesRef.bytes;
        int offset = bytesRef.offset;
        long asLong =  
           (((long)(bytes[offset+0] & 0xff) << 56) |
            ((long)(bytes[offset+1] & 0xff) << 48) |
            ((long)(bytes[offset+2] & 0xff) << 40) |
            ((long)(bytes[offset+3] & 0xff) << 32) |
            ((long)(bytes[offset+4] & 0xff) << 24) |
            ((long)(bytes[offset+5] & 0xff) << 16) |
            ((long)(bytes[offset+6] & 0xff) <<  8) |
            ((long)(bytes[offset+7] & 0xff)));
        w.add(asLong == 0 ? defaultValue : asLong - minValue);
      }
      for (int i = lastDocID + 1; i < docCount; i++) {
        w.add(defaultValue);
      }
      w.finish();
    }
    
    @Override
    public void add(int docID, IndexableField docValue) throws IOException {
      final long v = docValue.numericValue().longValue();
      assert lastDocId < docID;
      if (!started) {
        started = true;
        minValue = maxValue = v;
      } else {
        if (v < minValue) {
          minValue = v;
        } else if (v > maxValue) {
          maxValue = v;
        }
      }
      lastDocId = docID;
      DocValuesArraySource.copyLong(bytesRef, v);
      bytesSpareField.setBytesValue(bytesRef);
      super.add(docID, bytesSpareField);
    }
  }

  /**
   * Opens all necessary files, but does not read any data in until you call
   * {@link #load}.
   */
  static class PackedIntsReader extends DocValues {
    private final IndexInput datIn;
    private final byte type;
    private final int numDocs;
    private final DocValuesArraySource values;

    protected PackedIntsReader(Directory dir, String id, int numDocs,
        IOContext context) throws IOException {
      datIn = dir.openInput(
                IndexFileNames.segmentFileName(id, Bytes.DV_SEGMENT_SUFFIX, DocValuesWriterBase.DATA_EXTENSION),
          context);
      this.numDocs = numDocs;
      boolean success = false;
      try {
        CodecUtil.checkHeader(datIn, CODEC_NAME, VERSION_START, VERSION_START);
        type = datIn.readByte();
        values = type == FIXED_64 ?  DocValuesArraySource.forType(Type.FIXED_INTS_64) : null;
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(datIn);
        }
      }
    }


    /**
     * Loads the actual values. You may call this more than once, eg if you
     * already previously loaded but then discarded the Source.
     */
    @Override
    public Source load() throws IOException {
      boolean success = false;
      final Source source;
      IndexInput input = null;
      try {
        input = (IndexInput) datIn.clone();
        
        if (values == null) {
          source = new PackedIntsSource(input, false);
        } else {
          source = values.newFromInput(input, numDocs);
        }
        success = true;
        return source;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(input, datIn);
        }
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      datIn.close();
    }


    @Override
    public Type getType() {
      return Type.VAR_INTS;
    }


    @Override
    public Source getDirectSource() throws IOException {
      return values != null ? new FixedStraightBytesImpl.DirectFixedStraightSource((IndexInput) datIn.clone(), 8, Type.FIXED_INTS_64) : new PackedIntsSource((IndexInput) datIn.clone(), true);
    }
  }

  
  static class PackedIntsSource extends Source {
    private final long minValue;
    private final long defaultValue;
    private final PackedInts.Reader values;

    public PackedIntsSource(IndexInput dataIn, boolean direct) throws IOException {
      super(Type.VAR_INTS);
      minValue = dataIn.readLong();
      defaultValue = dataIn.readLong();
      values = direct ? PackedInts.getDirectReader(dataIn) : PackedInts.getReader(dataIn);
    }
    
    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref.grow(8);
      DocValuesArraySource.copyLong(ref, getInt(docID));
      return ref;
    }

    @Override
    public long getInt(int docID) {
      // TODO -- can we somehow avoid 2X method calls
      // on each get? must push minValue down, and make
      // PackedInts implement Ints.Source
      assert docID >= 0;
      final long value = values.get(docID);
      return value == defaultValue ? 0 : minValue + value;
    }
  }

}
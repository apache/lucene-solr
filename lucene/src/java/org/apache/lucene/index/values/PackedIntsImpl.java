package org.apache.lucene.index.values;

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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Stores ints packed with fixed-bit precision.
 * 
 * @lucene.experimental
 * */
class PackedIntsImpl {

  private static final String CODEC_NAME = "PackedInts";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class IntsWriter extends Writer {

    // TODO: can we bulkcopy this on a merge?
    private LongsRef intsRef;
    private long[] docToValue;
    private long minValue;
    private long maxValue;
    private boolean started;
    private final String id;
    private final OpenBitSet defaultValues = new OpenBitSet(1);
    private int lastDocId = -1;
    private IndexOutput datOut;

    protected IntsWriter(Directory dir, String id, AtomicLong bytesUsed)
        throws IOException {
      super(bytesUsed);
      datOut = dir.createOutput(IndexFileNames.segmentFileName(id, "",
          DATA_EXTENSION));
      CodecUtil.writeHeader(datOut, CODEC_NAME, VERSION_CURRENT);
      this.id = id;
      docToValue = new long[1];
      bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_LONG); // TODO the bitset
                                                             // needs memory too
    }

    @Override
    public void add(int docID, long v) throws IOException {
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
      defaultValues.set(docID);
      lastDocId = docID;

      if (docID >= docToValue.length) {
        final long len = docToValue.length;
        docToValue = ArrayUtil.grow(docToValue, 1 + docID);
        defaultValues.ensureCapacity(docToValue.length);
        bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_LONG
            * ((docToValue.length) - len));
      }
      docToValue[docID] = v;
    }

    @Override
    public void finish(int docCount) throws IOException {
      try {
        if (!started) {
          minValue = maxValue = 0;
        }
        // TODO -- long can't work right since it's signed
        datOut.writeLong(minValue);
        // write a default value to recognize docs without a value for that
        // field
        final long defaultValue = ++maxValue - minValue;
        datOut.writeLong(defaultValue);
        PackedInts.Writer w = PackedInts.getWriter(datOut, docCount,
            PackedInts.bitsRequired(maxValue - minValue));
        final int firstDoc = defaultValues.nextSetBit(0);
        lastDocId++;
        if (firstDoc != -1) { 
          for (int i = 0; i < firstDoc; i++) {
            w.add(defaultValue); // fill with defaults until first bit set
          }

          for (int i = firstDoc; i < lastDocId;) {
            w.add(docToValue[i] - minValue);
            final int nextValue = defaultValues.nextSetBit(++i);
            for (; i < nextValue; i++) {
              w.add(defaultValue); // fill all gaps
            }
          }
        }
        for (int i = lastDocId; i < docCount; i++) {
          w.add(defaultValue);
        }
        w.finish();
      } finally {
        datOut.close();
        bytesUsed
            .addAndGet(-(RamUsageEstimator.NUM_BYTES_LONG * docToValue.length));
        docToValue = null;
      }

    }

    @Override
    protected void add(int docID) throws IOException {
      add(docID, intsRef.get());
    }

    @Override
    protected void setNextEnum(DocValuesEnum valuesEnum) {
      intsRef = valuesEnum.getInt();
    }

    @Override
    public void add(int docID, PerDocFieldValues docValues) throws IOException {
      add(docID, docValues.getInt());
    }

    @Override
    public void files(Collection<String> files) throws IOException {
      files.add(IndexFileNames.segmentFileName(id, "", DATA_EXTENSION));
    }
  }

  /**
   * Opens all necessary files, but does not read any data in until you call
   * {@link #load}.
   */
  static class IntsReader extends DocValues {
    private final IndexInput datIn;

    protected IntsReader(Directory dir, String id) throws IOException {
      datIn = dir.openInput(IndexFileNames.segmentFileName(id, "",
          Writer.DATA_EXTENSION));
      CodecUtil.checkHeader(datIn, CODEC_NAME, VERSION_START, VERSION_START);
    }

    /**
     * Loads the actual values. You may call this more than once, eg if you
     * already previously loaded but then discarded the Source.
     */
    @Override
    public Source load() throws IOException {
      return new IntsSource((IndexInput) datIn.clone());
    }

    private static class IntsSource extends Source {
      private final long minValue;
      private final long defaultValue;
      private final PackedInts.Reader values;

      public IntsSource(IndexInput dataIn) throws IOException {
        dataIn.seek(CodecUtil.headerLength(CODEC_NAME));
        minValue = dataIn.readLong();
        defaultValue = dataIn.readLong();
        values = PackedInts.getReader(dataIn);
        missingValue.longValue = minValue + defaultValue;
      }

      @Override
      public long getInt(int docID) {
        // TODO -- can we somehow avoid 2X method calls
        // on each get? must push minValue down, and make
        // PackedInts implement Ints.Source
        assert docID >= 0;
        return minValue + values.get(docID);
      }

      @Override
      public DocValuesEnum getEnum(AttributeSource attrSource)
          throws IOException {
        final MissingValue missing = getMissing();
        return new SourceEnum(attrSource, type(), this, values.size()) {
          @Override
          public int advance(int target) throws IOException {
            if (target >= numDocs)
              return pos = NO_MORE_DOCS;
            while (source.getInt(target) == missing.longValue) {
              if (++target >= numDocs) {
                return pos = NO_MORE_DOCS;
              }
            }
            intsRef.ints[intsRef.offset] = source.getInt(target);
            return pos = target;
          }
        };
      }

      @Override
      public ValueType type() {
        return ValueType.INTS;
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      datIn.close();
    }

    @Override
    public DocValuesEnum getEnum(AttributeSource source) throws IOException {
      return new IntsEnumImpl(source, (IndexInput) datIn.clone());
    }

    @Override
    public ValueType type() {
      return ValueType.INTS;
    }

  }

  private static final class IntsEnumImpl extends DocValuesEnum {
    private final PackedInts.ReaderIterator ints;
    private long minValue;
    private final IndexInput dataIn;
    private final long defaultValue;
    private final int maxDoc;
    private int pos = -1;

    private IntsEnumImpl(AttributeSource source, IndexInput dataIn)
        throws IOException {
      super(source, ValueType.INTS);
      intsRef.offset = 0;
      this.dataIn = dataIn;
      dataIn.seek(CodecUtil.headerLength(CODEC_NAME));
      minValue = dataIn.readLong();
      defaultValue = dataIn.readLong();
      this.ints = PackedInts.getReaderIterator(dataIn);
      maxDoc = ints.size();
    }

    @Override
    public void close() throws IOException {
      ints.close();
      dataIn.close();
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return pos = NO_MORE_DOCS;
      }
      long val = ints.advance(target);
      while (val == defaultValue) {
        if (++target >= maxDoc) {
          return pos = NO_MORE_DOCS;
        }
        val = ints.advance(target);
      }
      intsRef.ints[0] = minValue + val;
      intsRef.offset = 0; // can we skip this?
      return pos = target;
    }

    @Override
    public int docID() {
      return pos;
    }

    @Override
    public int nextDoc() throws IOException {
      if (pos >= maxDoc) {
        return pos = NO_MORE_DOCS;
      }
      return advance(pos + 1);
    }
  }
}
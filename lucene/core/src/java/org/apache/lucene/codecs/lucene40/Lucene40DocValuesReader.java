package org.apache.lucene.codecs.lucene40;

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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosReader.LegacyDocValuesType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Reads the 4.0 format of norms/docvalues
 * @lucene.experimental
 * @deprecated Only for reading old 4.0 and 4.1 segments
 */
@Deprecated
final class Lucene40DocValuesReader extends DocValuesProducer {
  private final Directory dir;
  private final SegmentReadState state;
  private final String legacyKey;
  private static final String segmentSuffix = "dv";

  // ram instances we have already loaded
  private final Map<Integer,NumericDocValues> numericInstances =
      new HashMap<>();
  private final Map<Integer,BinaryDocValues> binaryInstances =
      new HashMap<>();
  private final Map<Integer,SortedDocValues> sortedInstances =
      new HashMap<>();

  private final AtomicLong ramBytesUsed;

  Lucene40DocValuesReader(SegmentReadState state, String filename, String legacyKey) throws IOException {
    this.state = state;
    this.legacyKey = legacyKey;
    this.dir = new CompoundFileDirectory(state.directory, filename, state.context, false);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOf(getClass()));
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues instance = numericInstances.get(field.number);
    if (instance == null) {
      String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
      IndexInput input = dir.openInput(fileName, state.context);
      boolean success = false;
      try {
        switch(LegacyDocValuesType.valueOf(field.getAttribute(legacyKey))) {
          case VAR_INTS:
            instance = loadVarIntsField(field, input);
            break;
          case FIXED_INTS_8:
            instance = loadByteField(field, input);
            break;
          case FIXED_INTS_16:
            instance = loadShortField(field, input);
            break;
          case FIXED_INTS_32:
            instance = loadIntField(field, input);
            break;
          case FIXED_INTS_64:
            instance = loadLongField(field, input);
            break;
          case FLOAT_32:
            instance = loadFloatField(field, input);
            break;
          case FLOAT_64:
            instance = loadDoubleField(field, input);
            break;
          default:
            throw new AssertionError();
        }
        CodecUtil.checkEOF(input);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(input);
        } else {
          IOUtils.closeWhileHandlingException(input);
        }
      }
      numericInstances.put(field.number, instance);
    }
    return instance;
  }

  private NumericDocValues loadVarIntsField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.VAR_INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.VAR_INTS_VERSION_START,
                                 Lucene40DocValuesFormat.VAR_INTS_VERSION_CURRENT);
    byte header = input.readByte();
    if (header == Lucene40DocValuesFormat.VAR_INTS_FIXED_64) {
      int maxDoc = state.segmentInfo.getDocCount();
      final long values[] = new long[maxDoc];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readLong();
      }
      ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          return values[docID];
        }
      };
    } else if (header == Lucene40DocValuesFormat.VAR_INTS_PACKED) {
      final long minValue = input.readLong();
      final long defaultValue = input.readLong();
      final PackedInts.Reader reader = PackedInts.getReader(input);
      ramBytesUsed.addAndGet(reader.ramBytesUsed());
      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          final long value = reader.get(docID);
          if (value == defaultValue) {
            return 0;
          } else {
            return minValue + value;
          }
        }
      };
    } else {
      throw new CorruptIndexException("invalid VAR_INTS header byte: " + header + " (resource=" + input + ")");
    }
  }

  private NumericDocValues loadByteField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 1) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize);
    }
    int maxDoc = state.segmentInfo.getDocCount();
    final byte values[] = new byte[maxDoc];
    input.readBytes(values, 0, values.length);
    ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadShortField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 2) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize);
    }
    int maxDoc = state.segmentInfo.getDocCount();
    final short values[] = new short[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readShort();
    }
    ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadIntField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 4) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize);
    }
    int maxDoc = state.segmentInfo.getDocCount();
    final int values[] = new int[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readInt();
    }
    ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadLongField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.INTS_CODEC_NAME,
                                 Lucene40DocValuesFormat.INTS_VERSION_START,
                                 Lucene40DocValuesFormat.INTS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 8) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize);
    }
    int maxDoc = state.segmentInfo.getDocCount();
    final long values[] = new long[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readLong();
    }
    ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadFloatField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.FLOATS_CODEC_NAME,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_START,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 4) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize);
    }
    int maxDoc = state.segmentInfo.getDocCount();
    final int values[] = new int[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readInt();
    }
    ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  private NumericDocValues loadDoubleField(FieldInfo field, IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, Lucene40DocValuesFormat.FLOATS_CODEC_NAME,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_START,
                                 Lucene40DocValuesFormat.FLOATS_VERSION_CURRENT);
    int valueSize = input.readInt();
    if (valueSize != 8) {
      throw new CorruptIndexException("invalid valueSize: " + valueSize);
    }
    int maxDoc = state.segmentInfo.getDocCount();
    final long values[] = new long[maxDoc];
    for (int i = 0; i < values.length; i++) {
      values[i] = input.readLong();
    }
    ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return values[docID];
      }
    };
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryDocValues instance = binaryInstances.get(field.number);
    if (instance == null) {
      switch(LegacyDocValuesType.valueOf(field.getAttribute(legacyKey))) {
        case BYTES_FIXED_STRAIGHT:
          instance = loadBytesFixedStraight(field);
          break;
        case BYTES_VAR_STRAIGHT:
          instance = loadBytesVarStraight(field);
          break;
        case BYTES_FIXED_DEREF:
          instance = loadBytesFixedDeref(field);
          break;
        case BYTES_VAR_DEREF:
          instance = loadBytesVarDeref(field);
          break;
        default:
          throw new AssertionError();
      }
      binaryInstances.put(field.number, instance);
    }
    return instance;
  }

  private BinaryDocValues loadBytesFixedStraight(FieldInfo field) throws IOException {
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    IndexInput input = dir.openInput(fileName, state.context);
    boolean success = false;
    try {
      CodecUtil.checkHeader(input, Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_CODEC_NAME,
                                   Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_FIXED_STRAIGHT_VERSION_CURRENT);
      final int fixedLength = input.readInt();
      PagedBytes bytes = new PagedBytes(16);
      bytes.copy(input, fixedLength * (long)state.segmentInfo.getDocCount());
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      CodecUtil.checkEOF(input);
      success = true;
      ramBytesUsed.addAndGet(bytesReader.ramBytesUsed());
      return new BinaryDocValues() {

        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          bytesReader.fillSlice(term, fixedLength * (long)docID, fixedLength);
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(input);
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }

  private BinaryDocValues loadBytesVarStraight(FieldInfo field) throws IOException {
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
    IndexInput data = null;
    IndexInput index = null;
    boolean success = false;
    try {
      data = dir.openInput(dataName, state.context);
      CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_CODEC_NAME_DAT,
                                  Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_START,
                                  Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_CURRENT);
      index = dir.openInput(indexName, state.context);
      CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_CODEC_NAME_IDX,
                                   Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_VAR_STRAIGHT_VERSION_CURRENT);
      long totalBytes = index.readVLong();
      PagedBytes bytes = new PagedBytes(16);
      bytes.copy(data, totalBytes);
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      final PackedInts.Reader reader = PackedInts.getReader(index);
      CodecUtil.checkEOF(data);
      CodecUtil.checkEOF(index);
      success = true;
      ramBytesUsed.addAndGet(bytesReader.ramBytesUsed() + reader.ramBytesUsed());
      return new BinaryDocValues() {
        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          long startAddress = reader.get(docID);
          long endAddress = reader.get(docID+1);
          bytesReader.fillSlice(term, startAddress, (int)(endAddress - startAddress));
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(data, index);
      } else {
        IOUtils.closeWhileHandlingException(data, index);
      }
    }
  }

  private BinaryDocValues loadBytesFixedDeref(FieldInfo field) throws IOException {
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
    IndexInput data = null;
    IndexInput index = null;
    boolean success = false;
    try {
      data = dir.openInput(dataName, state.context);
      CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_FIXED_DEREF_CODEC_NAME_DAT,
                                  Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_START,
                                  Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_CURRENT);
      index = dir.openInput(indexName, state.context);
      CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_FIXED_DEREF_CODEC_NAME_IDX,
                                   Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_FIXED_DEREF_VERSION_CURRENT);

      final int fixedLength = data.readInt();
      final int valueCount = index.readInt();
      PagedBytes bytes = new PagedBytes(16);
      bytes.copy(data, fixedLength * (long) valueCount);
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      final PackedInts.Reader reader = PackedInts.getReader(index);
      CodecUtil.checkEOF(data);
      CodecUtil.checkEOF(index);
      ramBytesUsed.addAndGet(bytesReader.ramBytesUsed() + reader.ramBytesUsed());
      success = true;
      return new BinaryDocValues() {
        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          final long offset = fixedLength * reader.get(docID);
          bytesReader.fillSlice(term, offset, fixedLength);
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(data, index);
      } else {
        IOUtils.closeWhileHandlingException(data, index);
      }
    }
  }

  private BinaryDocValues loadBytesVarDeref(FieldInfo field) throws IOException {
    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
    String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
    IndexInput data = null;
    IndexInput index = null;
    boolean success = false;
    try {
      data = dir.openInput(dataName, state.context);
      CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_VAR_DEREF_CODEC_NAME_DAT,
                                  Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_START,
                                  Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_CURRENT);
      index = dir.openInput(indexName, state.context);
      CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_VAR_DEREF_CODEC_NAME_IDX,
                                   Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_START,
                                   Lucene40DocValuesFormat.BYTES_VAR_DEREF_VERSION_CURRENT);

      final long totalBytes = index.readLong();
      final PagedBytes bytes = new PagedBytes(16);
      bytes.copy(data, totalBytes);
      final PagedBytes.Reader bytesReader = bytes.freeze(true);
      final PackedInts.Reader reader = PackedInts.getReader(index);
      CodecUtil.checkEOF(data);
      CodecUtil.checkEOF(index);
      ramBytesUsed.addAndGet(bytesReader.ramBytesUsed() + reader.ramBytesUsed());
      success = true;
      return new BinaryDocValues() {
        
        @Override
        public BytesRef get(int docID) {
          final BytesRef term = new BytesRef();
          long startAddress = reader.get(docID);
          BytesRef lengthBytes = new BytesRef();
          bytesReader.fillSlice(lengthBytes, startAddress, 1);
          byte code = lengthBytes.bytes[lengthBytes.offset];
          if ((code & 128) == 0) {
            // length is 1 byte
            bytesReader.fillSlice(term, startAddress + 1, (int) code);
          } else {
            bytesReader.fillSlice(lengthBytes, startAddress + 1, 1);
            int length = ((code & 0x7f) << 8) | (lengthBytes.bytes[lengthBytes.offset] & 0xff);
            bytesReader.fillSlice(term, startAddress + 2, length);
          }
          return term;
        }
      };
    } finally {
      if (success) {
        IOUtils.close(data, index);
      } else {
        IOUtils.closeWhileHandlingException(data, index);
      }
    }
  }

  @Override
  public synchronized SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedDocValues instance = sortedInstances.get(field.number);
    if (instance == null) {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "dat");
      String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name + "_" + Integer.toString(field.number), segmentSuffix, "idx");
      IndexInput data = null;
      IndexInput index = null;
      boolean success = false;
      try {
        data = dir.openInput(dataName, state.context);
        index = dir.openInput(indexName, state.context);
        switch(LegacyDocValuesType.valueOf(field.getAttribute(legacyKey))) {
          case BYTES_FIXED_SORTED:
            instance = loadBytesFixedSorted(field, data, index);
            break;
          case BYTES_VAR_SORTED:
            instance = loadBytesVarSorted(field, data, index);
            break;
          default:
            throw new AssertionError();
        }
        CodecUtil.checkEOF(data);
        CodecUtil.checkEOF(index);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data, index);
        } else {
          IOUtils.closeWhileHandlingException(data, index);
        }
      }
      sortedInstances.put(field.number, instance);
    }
    return instance;
  }

  private SortedDocValues loadBytesFixedSorted(FieldInfo field, IndexInput data, IndexInput index) throws IOException {
    CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_FIXED_SORTED_CODEC_NAME_DAT,
                                Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_START,
                                Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_CURRENT);
    CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_FIXED_SORTED_CODEC_NAME_IDX,
                                 Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_START,
                                 Lucene40DocValuesFormat.BYTES_FIXED_SORTED_VERSION_CURRENT);

    final int fixedLength = data.readInt();
    final int valueCount = index.readInt();

    PagedBytes bytes = new PagedBytes(16);
    bytes.copy(data, fixedLength * (long) valueCount);
    final PagedBytes.Reader bytesReader = bytes.freeze(true);
    final PackedInts.Reader reader = PackedInts.getReader(index);
    ramBytesUsed.addAndGet(bytesReader.ramBytesUsed() + reader.ramBytesUsed());

    return correctBuggyOrds(new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return (int) reader.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        final BytesRef term = new BytesRef();
        bytesReader.fillSlice(term, fixedLength * (long) ord, fixedLength);
        return term;
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }
    });
  }

  private SortedDocValues loadBytesVarSorted(FieldInfo field, IndexInput data, IndexInput index) throws IOException {
    CodecUtil.checkHeader(data, Lucene40DocValuesFormat.BYTES_VAR_SORTED_CODEC_NAME_DAT,
                                Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_START,
                                Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_CURRENT);
    CodecUtil.checkHeader(index, Lucene40DocValuesFormat.BYTES_VAR_SORTED_CODEC_NAME_IDX,
                                 Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_START,
                                 Lucene40DocValuesFormat.BYTES_VAR_SORTED_VERSION_CURRENT);

    long maxAddress = index.readLong();
    PagedBytes bytes = new PagedBytes(16);
    bytes.copy(data, maxAddress);
    final PagedBytes.Reader bytesReader = bytes.freeze(true);
    final PackedInts.Reader addressReader = PackedInts.getReader(index);
    final PackedInts.Reader ordsReader = PackedInts.getReader(index);

    final int valueCount = addressReader.size() - 1;
    ramBytesUsed.addAndGet(bytesReader.ramBytesUsed() + addressReader.ramBytesUsed() + ordsReader.ramBytesUsed());

    return correctBuggyOrds(new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return (int)ordsReader.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        final BytesRef term = new BytesRef();
        long startAddress = addressReader.get(ord);
        long endAddress = addressReader.get(ord+1);
        bytesReader.fillSlice(term, startAddress, (int)(endAddress - startAddress));
        return term;
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }
    });
  }

  // detects and corrects LUCENE-4717 in old indexes
  private SortedDocValues correctBuggyOrds(final SortedDocValues in) {
    final int maxDoc = state.segmentInfo.getDocCount();
    for (int i = 0; i < maxDoc; i++) {
      if (in.getOrd(i) == 0) {
        return in; // ok
      }
    }

    // we had ord holes, return an ord-shifting-impl that corrects the bug
    return new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return in.getOrd(docID) - 1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return in.lookupOrd(ord+1);
      }

      @Override
      public int getValueCount() {
        return in.getValueCount() - 1;
      }
    };
  }
  
  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    throw new IllegalStateException("Lucene 4.0 does not support SortedNumeric: how did you pull this off?");
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    throw new IllegalStateException("Lucene 4.0 does not support SortedSet: how did you pull this off?");
  }

  @Override
  public Bits getDocsWithField(FieldInfo field) throws IOException {
    return new Bits.MatchAllBits(state.segmentInfo.getDocCount());
  }

  @Override
  public void close() throws IOException {
    dir.close();
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }

  @Override
  public void checkIntegrity() throws IOException {
  }
}

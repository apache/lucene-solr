package org.apache.lucene.codecs.memory;

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
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Reader for {@link DirectDocValuesFormat}
 */

class DirectDocValuesProducer extends DocValuesProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<Integer,NumericEntry> numerics = new HashMap<Integer,NumericEntry>();
  private final Map<Integer,BinaryEntry> binaries = new HashMap<Integer,BinaryEntry>();
  private final Map<Integer,SortedEntry> sorteds = new HashMap<Integer,SortedEntry>();
  private final Map<Integer,SortedSetEntry> sortedSets = new HashMap<Integer,SortedSetEntry>();
  private final IndexInput data;
  
  // ram instances we have already loaded
  private final Map<Integer,NumericDocValues> numericInstances = 
      new HashMap<Integer,NumericDocValues>();
  private final Map<Integer,BinaryDocValues> binaryInstances =
      new HashMap<Integer,BinaryDocValues>();
  private final Map<Integer,SortedDocValues> sortedInstances =
      new HashMap<Integer,SortedDocValues>();
  private final Map<Integer,SortedSetRawValues> sortedSetInstances =
      new HashMap<Integer,SortedSetRawValues>();
  private final Map<Integer,Bits> docsWithFieldInstances = new HashMap<Integer,Bits>();
  
  private final int maxDoc;
  private final AtomicLong ramBytesUsed;
  
  static final byte NUMBER = 0;
  static final byte BYTES = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
    
  DirectDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.getDocCount();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    IndexInput in = state.directory.openInput(metaName, state.context);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    boolean success = false;
    final int version;
    try {
      version = CodecUtil.checkHeader(in, metaCodec, 
                                      VERSION_START,
                                      VERSION_CURRENT);
      readFields(in);

      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }

    success = false;
    try {
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.openInput(dataName, state.context);
      final int version2 = CodecUtil.checkHeader(data, dataCodec, 
                                                 VERSION_START,
                                                 VERSION_CURRENT);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch");
      }

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }
  }

  private NumericEntry readNumericEntry(IndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    entry.offset = meta.readLong();
    entry.count = meta.readInt();
    entry.missingOffset = meta.readLong();
    if (entry.missingOffset != -1) {
      entry.missingBytes = meta.readLong();
    } else {
      entry.missingBytes = 0;
    }
    entry.byteWidth = meta.readByte();

    return entry;
  }

  private BinaryEntry readBinaryEntry(IndexInput meta) throws IOException {
    BinaryEntry entry = new BinaryEntry();
    entry.offset = meta.readLong();
    entry.numBytes = meta.readInt();
    entry.count = meta.readInt();
    entry.missingOffset = meta.readLong();
    if (entry.missingOffset != -1) {
      entry.missingBytes = meta.readLong();
    } else {
      entry.missingBytes = 0;
    }

    return entry;
  }

  private SortedEntry readSortedEntry(IndexInput meta) throws IOException {
    SortedEntry entry = new SortedEntry();
    entry.docToOrd = readNumericEntry(meta);
    entry.values = readBinaryEntry(meta);
    return entry;
  }

  private SortedSetEntry readSortedSetEntry(IndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    entry.docToOrdAddress = readNumericEntry(meta);
    entry.ords = readNumericEntry(meta);
    entry.values = readBinaryEntry(meta);
    return entry;
  }

  private void readFields(IndexInput meta) throws IOException {
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      int fieldType = meta.readByte();
      if (fieldType == NUMBER) {
        numerics.put(fieldNumber, readNumericEntry(meta));
      } else if (fieldType == BYTES) {
        binaries.put(fieldNumber, readBinaryEntry(meta));
      } else if (fieldType == SORTED) {
        sorteds.put(fieldNumber, readSortedEntry(meta));
      } else if (fieldType == SORTED_SET) {
        sortedSets.put(fieldNumber, readSortedSetEntry(meta));
      } else {
        throw new CorruptIndexException("invalid entry type: " + fieldType + ", input=" + meta);
      }
      fieldNumber = meta.readVInt();
    }
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValues instance = numericInstances.get(field.number);
    if (instance == null) {
      // Lazy load
      instance = loadNumeric(numerics.get(field.number));
      numericInstances.put(field.number, instance);
    }
    return instance;
  }
  
  private NumericDocValues loadNumeric(NumericEntry entry) throws IOException {
    data.seek(entry.offset + entry.missingBytes);
    switch (entry.byteWidth) {
    case 1:
      {
        final byte[] values = new byte[entry.count];
        data.readBytes(values, 0, entry.count);
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
        return new NumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
      }

    case 2:
      {
        final short[] values = new short[entry.count];
        for(int i=0;i<entry.count;i++) {
          values[i] = data.readShort();
        }
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
        return new NumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
      }

    case 4:
      {
        final int[] values = new int[entry.count];
        for(int i=0;i<entry.count;i++) {
          values[i] = data.readInt();
        }
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
        return new NumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
      }

    case 8:
      {
        final long[] values = new long[entry.count];
        for(int i=0;i<entry.count;i++) {
          values[i] = data.readLong();
        }
        ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(values));
        return new NumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
      }
    
    default:
      throw new AssertionError();
    }
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryDocValues instance = binaryInstances.get(field.number);
    if (instance == null) {
      // Lazy load
      instance = loadBinary(binaries.get(field.number));
      binaryInstances.put(field.number, instance);
    }
    return instance;
  }
  
  private BinaryDocValues loadBinary(BinaryEntry entry) throws IOException {
    data.seek(entry.offset);
    final byte[] bytes = new byte[entry.numBytes];
    data.readBytes(bytes, 0, entry.numBytes);
    data.seek(entry.offset + entry.numBytes + entry.missingBytes);

    final int[] address = new int[entry.count+1];
    for(int i=0;i<entry.count;i++) {
      address[i] = data.readInt();
    }
    address[entry.count] = data.readInt();

    ramBytesUsed.addAndGet(RamUsageEstimator.sizeOf(bytes) + RamUsageEstimator.sizeOf(address));

    return new BinaryDocValues() {
      @Override
      public void get(int docID, BytesRef result) {
        result.bytes = bytes;
        result.offset = address[docID];
        result.length = address[docID+1] - result.offset;
      };
    };
  }
  
  @Override
  public synchronized SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedDocValues instance = sortedInstances.get(field.number);
    if (instance == null) {
      // Lazy load
      instance = loadSorted(field);
      sortedInstances.put(field.number, instance);
    }
    return instance;
  }

  private SortedDocValues loadSorted(FieldInfo field) throws IOException {
    final SortedEntry entry = sorteds.get(field.number);
    final NumericDocValues docToOrd = loadNumeric(entry.docToOrd);
    final BinaryDocValues values = loadBinary(entry.values);

    return new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        return (int) docToOrd.get(docID);
      }

      @Override
      public void lookupOrd(int ord, BytesRef result) {
        values.get(ord, result);
      }

      @Override
      public int getValueCount() {
        return entry.values.count;
      }

      // Leave lookupTerm to super's binary search

      // Leave termsEnum to super
    };
  }

  @Override
  public synchronized SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetRawValues instance = sortedSetInstances.get(field.number);
    final SortedSetEntry entry = sortedSets.get(field.number);
    if (instance == null) {
      // Lazy load
      instance = loadSortedSet(entry);
      sortedSetInstances.put(field.number, instance);
    }

    final NumericDocValues docToOrdAddress = instance.docToOrdAddress;
    final NumericDocValues ords = instance.ords;
    final BinaryDocValues values = instance.values;

    // Must make a new instance since the iterator has state:
    return new SortedSetDocValues() {
      int ordUpto;
      int ordLimit;

      @Override
      public long nextOrd() {
        if (ordUpto == ordLimit) {
          return NO_MORE_ORDS;
        } else {
          return ords.get(ordUpto++);
        }
      }
      
      @Override
      public void setDocument(int docID) {
        ordUpto = (int) docToOrdAddress.get(docID);
        ordLimit = (int) docToOrdAddress.get(docID+1);
      }

      @Override
      public void lookupOrd(long ord, BytesRef result) {
        values.get((int) ord, result);
      }

      @Override
      public long getValueCount() {
        return entry.values.count;
      }

      // Leave lookupTerm to super's binary search

      // Leave termsEnum to super
    };
  }
  
  private SortedSetRawValues loadSortedSet(SortedSetEntry entry) throws IOException {
    SortedSetRawValues instance = new SortedSetRawValues();
    instance.docToOrdAddress = loadNumeric(entry.docToOrdAddress);
    instance.ords = loadNumeric(entry.ords);
    instance.values = loadBinary(entry.values);
    return instance;
  }

  private Bits getMissingBits(int fieldNumber, final long offset, final long length) throws IOException {
    if (offset == -1) {
      return new Bits.MatchAllBits(maxDoc);
    } else {
      Bits instance;
      synchronized(this) {
        instance = docsWithFieldInstances.get(fieldNumber);
        if (instance == null) {
          IndexInput data = this.data.clone();
          data.seek(offset);
          assert length % 8 == 0;
          long bits[] = new long[(int) length >> 3];
          for (int i = 0; i < bits.length; i++) {
            bits[i] = data.readLong();
          }
          instance = new FixedBitSet(bits, maxDoc);
          docsWithFieldInstances.put(fieldNumber, instance);
        }
      }
      return instance;
    }
  }
  
  @Override
  public Bits getDocsWithField(FieldInfo field) throws IOException {
    switch(field.getDocValuesType()) {
      case SORTED_SET:
        return new SortedSetDocsWithField(getSortedSet(field), maxDoc);
      case SORTED:
        return new SortedDocsWithField(getSorted(field), maxDoc);
      case BINARY:
        BinaryEntry be = binaries.get(field.number);
        return getMissingBits(field.number, be.missingOffset, be.missingBytes);
      case NUMERIC:
        NumericEntry ne = numerics.get(field.number);
        return getMissingBits(field.number, ne.missingOffset, ne.missingBytes);
      default: 
        throw new AssertionError();
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  static class SortedSetRawValues {
    NumericDocValues docToOrdAddress;
    NumericDocValues ords;
    BinaryDocValues values;
  }

  static class NumericEntry {
    long offset;
    int count;
    long missingOffset;
    long missingBytes;
    byte byteWidth;
    int packedIntsVersion;
  }

  static class BinaryEntry {
    long offset;
    long missingOffset;
    long missingBytes;
    int count;
    int numBytes;
    int minLength;
    int maxLength;
    int packedIntsVersion;
    int blockSize;
  }
  
  static class SortedEntry {
    NumericEntry docToOrd;
    BinaryEntry values;
  }

  static class SortedSetEntry {
    NumericEntry docToOrdAddress;
    NumericEntry ords;
    BinaryEntry values;
  }
  
  static class FSTEntry {
    long offset;
    long numOrds;
  }
}

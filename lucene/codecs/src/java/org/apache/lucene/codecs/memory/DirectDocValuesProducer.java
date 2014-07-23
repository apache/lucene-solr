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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
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
  private final Map<Integer,NumericEntry> numerics = new HashMap<>();
  private final Map<Integer,BinaryEntry> binaries = new HashMap<>();
  private final Map<Integer,SortedEntry> sorteds = new HashMap<>();
  private final Map<Integer,SortedSetEntry> sortedSets = new HashMap<>();
  private final Map<Integer,SortedNumericEntry> sortedNumerics = new HashMap<>();
  private final IndexInput data;
  
  // ram instances we have already loaded
  private final Map<Integer,NumericDocValues> numericInstances = 
      new HashMap<>();
  private final Map<Integer,BinaryRawValues> binaryInstances =
      new HashMap<>();
  private final Map<Integer,SortedRawValues> sortedInstances =
      new HashMap<>();
  private final Map<Integer,SortedSetRawValues> sortedSetInstances =
      new HashMap<>();
  private final Map<Integer,SortedNumericRawValues> sortedNumericInstances =
      new HashMap<>();
  private final Map<Integer,Bits> docsWithFieldInstances = new HashMap<>();
  
  private final int maxDoc;
  private final AtomicLong ramBytesUsed;
  private final int version;
  
  static final byte NUMBER = 0;
  static final byte BYTES = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;
  static final byte SORTED_SET_SINGLETON = 4;
  static final byte SORTED_NUMERIC = 5;
  static final byte SORTED_NUMERIC_SINGLETON = 6;

  static final int VERSION_START = 2;
  static final int VERSION_CURRENT = VERSION_START;
    
  DirectDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.getDocCount();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    boolean success = false;
    try {
      version = CodecUtil.checkHeader(in, metaCodec, 
                                      VERSION_START,
                                      VERSION_CURRENT);
      readFields(in);

      CodecUtil.checkFooter(in);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    success = false;
    try {
      final int version2 = CodecUtil.checkHeader(data, dataCodec, 
                                                 VERSION_START,
                                                 VERSION_CURRENT);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch");
      }
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

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

  private SortedSetEntry readSortedSetEntry(IndexInput meta, boolean singleton) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    if (singleton == false) {
      entry.docToOrdAddress = readNumericEntry(meta);
    }
    entry.ords = readNumericEntry(meta);
    entry.values = readBinaryEntry(meta);
    return entry;
  }
  
  private SortedNumericEntry readSortedNumericEntry(IndexInput meta, boolean singleton) throws IOException {
    SortedNumericEntry entry = new SortedNumericEntry();
    if (singleton == false) {
      entry.docToAddress = readNumericEntry(meta);
    }
    entry.values = readNumericEntry(meta);
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
        SortedEntry entry = readSortedEntry(meta);
        sorteds.put(fieldNumber, entry);
        binaries.put(fieldNumber, entry.values);
      } else if (fieldType == SORTED_SET) {
        SortedSetEntry entry = readSortedSetEntry(meta, false);
        sortedSets.put(fieldNumber, entry);
        binaries.put(fieldNumber, entry.values);
      } else if (fieldType == SORTED_SET_SINGLETON) {
        SortedSetEntry entry = readSortedSetEntry(meta, true);
        sortedSets.put(fieldNumber, entry);
        binaries.put(fieldNumber, entry.values);
      } else if (fieldType == SORTED_NUMERIC) {
        SortedNumericEntry entry = readSortedNumericEntry(meta, false);
        sortedNumerics.put(fieldNumber, entry);
      } else if (fieldType == SORTED_NUMERIC_SINGLETON) {
        SortedNumericEntry entry = readSortedNumericEntry(meta, true);
        sortedNumerics.put(fieldNumber, entry);
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
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
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
    BinaryRawValues instance = binaryInstances.get(field.number);
    if (instance == null) {
      // Lazy load
      instance = loadBinary(binaries.get(field.number));
      binaryInstances.put(field.number, instance);
    }
    final byte[] bytes = instance.bytes;
    final int[] address = instance.address;

    return new BinaryDocValues() {
      final BytesRef term = new BytesRef();

      @Override
      public BytesRef get(int docID) {
        term.bytes = bytes;
        term.offset = address[docID];
        term.length = address[docID+1] - term.offset;
        return term;
      }
    };
  }
  
  private BinaryRawValues loadBinary(BinaryEntry entry) throws IOException {
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

    BinaryRawValues values = new BinaryRawValues();
    values.bytes = bytes;
    values.address = address;
    return values;
  }
  
  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final SortedEntry entry = sorteds.get(field.number);
    SortedRawValues instance;
    synchronized (this) {
      instance = sortedInstances.get(field.number);
      if (instance == null) {
        // Lazy load
        instance = loadSorted(field);
        sortedInstances.put(field.number, instance);
      }
    }
    return newSortedInstance(instance.docToOrd, getBinary(field), entry.values.count);
  }
  
  private SortedDocValues newSortedInstance(final NumericDocValues docToOrd, final BinaryDocValues values, final int count) {
    return new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        return (int) docToOrd.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return values.get(ord);
      }

      @Override
      public int getValueCount() {
        return count;
      }

      // Leave lookupTerm to super's binary search

      // Leave termsEnum to super
    };
  }

  private SortedRawValues loadSorted(FieldInfo field) throws IOException {
    final SortedEntry entry = sorteds.get(field.number);
    final NumericDocValues docToOrd = loadNumeric(entry.docToOrd);
    final SortedRawValues values = new SortedRawValues();
    values.docToOrd = docToOrd;
    return values;
  }

  @Override
  public synchronized SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedNumericRawValues instance = sortedNumericInstances.get(field.number);
    final SortedNumericEntry entry = sortedNumerics.get(field.number);
    if (instance == null) {
      // Lazy load
      instance = loadSortedNumeric(entry);
      sortedNumericInstances.put(field.number, instance);
    }
    
    if (entry.docToAddress == null) {
      final NumericDocValues single = instance.values;
      final Bits docsWithField = getMissingBits(field.number, entry.values.missingOffset, entry.values.missingBytes);
      return DocValues.singleton(single, docsWithField);
    } else {
      final NumericDocValues docToAddress = instance.docToAddress;
      final NumericDocValues values = instance.values;
      
      return new SortedNumericDocValues() {
        int valueStart;
        int valueLimit;
        
        @Override
        public void setDocument(int doc) {
          valueStart = (int) docToAddress.get(doc);
          valueLimit = (int) docToAddress.get(doc+1);
        }
        
        @Override
        public long valueAt(int index) {
          return values.get(valueStart + index);
        }
        
        @Override
        public int count() {
          return valueLimit - valueStart;
        }
      };
    }
  }
  
  private SortedNumericRawValues loadSortedNumeric(SortedNumericEntry entry) throws IOException {
    SortedNumericRawValues instance = new SortedNumericRawValues();
    if (entry.docToAddress != null) {
      instance.docToAddress = loadNumeric(entry.docToAddress);
    }
    instance.values = loadNumeric(entry.values);
    return instance;
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

    if (instance.docToOrdAddress == null) {
      SortedDocValues sorted = newSortedInstance(instance.ords, getBinary(field), entry.values.count);
      return DocValues.singleton(sorted);
    } else {
      final NumericDocValues docToOrdAddress = instance.docToOrdAddress;
      final NumericDocValues ords = instance.ords;
      final BinaryDocValues values = getBinary(field);
      
      // Must make a new instance since the iterator has state:
      return new RandomAccessOrds() {
        int ordStart;
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
          ordStart = ordUpto = (int) docToOrdAddress.get(docID);
          ordLimit = (int) docToOrdAddress.get(docID+1);
        }
        
        @Override
        public BytesRef lookupOrd(long ord) {
          return values.get((int) ord);
        }
        
        @Override
        public long getValueCount() {
          return entry.values.count;
        }
        
        @Override
        public long ordAt(int index) {
          return ords.get(ordStart + index);
        }
        
        @Override
        public int cardinality() {
          return ordLimit - ordStart;
        }
        
        // Leave lookupTerm to super's binary search
        
        // Leave termsEnum to super
      };
    }
  }
  
  private SortedSetRawValues loadSortedSet(SortedSetEntry entry) throws IOException {
    SortedSetRawValues instance = new SortedSetRawValues();
    if (entry.docToOrdAddress != null) {
      instance.docToOrdAddress = loadNumeric(entry.docToOrdAddress);
    }
    instance.ords = loadNumeric(entry.ords);
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
        return DocValues.docsWithValue(getSortedSet(field), maxDoc);
      case SORTED_NUMERIC:
        return DocValues.docsWithValue(getSortedNumeric(field), maxDoc);
      case SORTED:
        return DocValues.docsWithValue(getSorted(field), maxDoc);
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

  static class BinaryRawValues {
    byte[] bytes;
    int[] address;
  }

  static class SortedRawValues {
    NumericDocValues docToOrd;
  }
  
  static class SortedNumericRawValues {
    NumericDocValues docToAddress;
    NumericDocValues values;
  }

  static class SortedSetRawValues {
    NumericDocValues docToOrdAddress;
    NumericDocValues ords;
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
  
  static class SortedNumericEntry {
    NumericEntry docToAddress;
    NumericEntry values;
  }
  
  static class FSTEntry {
    long offset;
    long numOrds;
  }
}

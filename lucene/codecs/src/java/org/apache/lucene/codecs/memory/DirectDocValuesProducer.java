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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
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
  private final Map<String,NumericEntry> numerics = new HashMap<>();
  private final Map<String,BinaryEntry> binaries = new HashMap<>();
  private final Map<String,SortedEntry> sorteds = new HashMap<>();
  private final Map<String,SortedSetEntry> sortedSets = new HashMap<>();
  private final Map<String,SortedNumericEntry> sortedNumerics = new HashMap<>();
  private final IndexInput data;
  
  // ram instances we have already loaded
  private final Map<String,NumericRawValues> numericInstances = new HashMap<>();
  private final Map<String,BinaryRawValues> binaryInstances = new HashMap<>();
  private final Map<String,SortedRawValues> sortedInstances = new HashMap<>();
  private final Map<String,SortedSetRawValues> sortedSetInstances = new HashMap<>();
  private final Map<String,SortedNumericRawValues> sortedNumericInstances = new HashMap<>();
  private final Map<String,FixedBitSet> docsWithFieldInstances = new HashMap<>();
  
  private final int numEntries;
  
  private final int maxDoc;
  private final AtomicLong ramBytesUsed;
  private final int version;
  
  private final boolean merging;
  
  static final byte NUMBER = 0;
  static final byte BYTES = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;
  static final byte SORTED_SET_SINGLETON = 4;
  static final byte SORTED_NUMERIC = 5;
  static final byte SORTED_NUMERIC_SINGLETON = 6;

  static final int VERSION_START = 3;
  static final int VERSION_CURRENT = VERSION_START;
  
  // clone for merge: when merging we don't do any instances.put()s
  DirectDocValuesProducer(DirectDocValuesProducer original) throws IOException {
    assert Thread.holdsLock(original);
    numerics.putAll(original.numerics);
    binaries.putAll(original.binaries);
    sorteds.putAll(original.sorteds);
    sortedSets.putAll(original.sortedSets);
    sortedNumerics.putAll(original.sortedNumerics);
    data = original.data.clone();
    
    numericInstances.putAll(original.numericInstances);
    binaryInstances.putAll(original.binaryInstances);
    sortedInstances.putAll(original.sortedInstances);
    sortedSetInstances.putAll(original.sortedSetInstances);
    sortedNumericInstances.putAll(original.sortedNumericInstances);
    docsWithFieldInstances.putAll(original.docsWithFieldInstances);
    
    numEntries = original.numEntries;
    maxDoc = original.maxDoc;
    ramBytesUsed = new AtomicLong(original.ramBytesUsed.get());
    version = original.version;
    merging = true;
  }
    
  DirectDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    merging = false;
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    boolean success = false;
    try {
      version = CodecUtil.checkIndexHeader(in, metaCodec, VERSION_START, VERSION_CURRENT, 
                                                 state.segmentInfo.getId(), state.segmentSuffix);
      numEntries = readFields(in, state.fieldInfos);

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
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec, VERSION_START, VERSION_CURRENT,
                                                              state.segmentInfo.getId(), state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
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

  private int readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int numEntries = 0;
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      numEntries++;
      FieldInfo info = infos.fieldInfo(fieldNumber);
      int fieldType = meta.readByte();
      if (fieldType == NUMBER) {
        numerics.put(info.name, readNumericEntry(meta));
      } else if (fieldType == BYTES) {
        binaries.put(info.name, readBinaryEntry(meta));
      } else if (fieldType == SORTED) {
        SortedEntry entry = readSortedEntry(meta);
        sorteds.put(info.name, entry);
        binaries.put(info.name, entry.values);
      } else if (fieldType == SORTED_SET) {
        SortedSetEntry entry = readSortedSetEntry(meta, false);
        sortedSets.put(info.name, entry);
        binaries.put(info.name, entry.values);
      } else if (fieldType == SORTED_SET_SINGLETON) {
        SortedSetEntry entry = readSortedSetEntry(meta, true);
        sortedSets.put(info.name, entry);
        binaries.put(info.name, entry.values);
      } else if (fieldType == SORTED_NUMERIC) {
        SortedNumericEntry entry = readSortedNumericEntry(meta, false);
        sortedNumerics.put(info.name, entry);
      } else if (fieldType == SORTED_NUMERIC_SINGLETON) {
        SortedNumericEntry entry = readSortedNumericEntry(meta, true);
        sortedNumerics.put(info.name, entry);
      } else {
        throw new CorruptIndexException("invalid entry type: " + fieldType + ", field= " + info.name, meta);
      }
      fieldNumber = meta.readVInt();
    }
    return numEntries;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }
  
  @Override
  public synchronized Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    resources.addAll(Accountables.namedAccountables("numeric field", numericInstances));
    resources.addAll(Accountables.namedAccountables("binary field", binaryInstances));
    resources.addAll(Accountables.namedAccountables("sorted field", sortedInstances));
    resources.addAll(Accountables.namedAccountables("sorted set field", sortedSetInstances));
    resources.addAll(Accountables.namedAccountables("sorted numeric field", sortedNumericInstances));
    resources.addAll(Accountables.namedAccountables("missing bitset field", docsWithFieldInstances));
    return Collections.unmodifiableList(resources);
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(entries=" + numEntries + ")";
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data.clone());
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericRawValues instance = numericInstances.get(field.name);
    NumericEntry ne = numerics.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadNumeric(ne);
      if (!merging) {
        numericInstances.put(field.name, instance);
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
      }
    }
    return new LegacyNumericDocValuesWrapper(getMissingBits(field, ne.missingOffset, ne.missingBytes), instance.numerics);
  }
  
  private NumericRawValues loadNumeric(NumericEntry entry) throws IOException {
    NumericRawValues ret = new NumericRawValues();
    IndexInput data = this.data.clone();
    data.seek(entry.offset + entry.missingBytes);
    switch (entry.byteWidth) {
    case 1:
      {
        final byte[] values = new byte[entry.count];
        data.readBytes(values, 0, entry.count);
        ret.bytesUsed = RamUsageEstimator.sizeOf(values);
        ret.numerics = new LegacyNumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
        return ret;
      }

    case 2:
      {
        final short[] values = new short[entry.count];
        for(int i=0;i<entry.count;i++) {
          values[i] = data.readShort();
        }
        ret.bytesUsed = RamUsageEstimator.sizeOf(values);
        ret.numerics = new LegacyNumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
        return ret;
      }

    case 4:
      {
        final int[] values = new int[entry.count];
        for(int i=0;i<entry.count;i++) {
          values[i] = data.readInt();
        }
        ret.bytesUsed = RamUsageEstimator.sizeOf(values);
        ret.numerics = new LegacyNumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
        return ret;
      }

    case 8:
      {
        final long[] values = new long[entry.count];
        for(int i=0;i<entry.count;i++) {
          values[i] = data.readLong();
        }
        ret.bytesUsed = RamUsageEstimator.sizeOf(values);
        ret.numerics = new LegacyNumericDocValues() {
          @Override
          public long get(int idx) {
            return values[idx];
          }
        };
        return ret;
      }
    
    default:
      throw new AssertionError();
    }
  }

  private synchronized LegacyBinaryDocValues getLegacyBinary(FieldInfo field) throws IOException {
    BinaryRawValues instance = binaryInstances.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadBinary(binaries.get(field.name));
      if (!merging) {
        binaryInstances.put(field.name, instance);
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
      }
    }
    final byte[] bytes = instance.bytes;
    final int[] address = instance.address;

    return new LegacyBinaryDocValues() {
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
  
  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryEntry be = binaries.get(field.name);
    return new LegacyBinaryDocValuesWrapper(getMissingBits(field, be.missingOffset, be.missingBytes), getLegacyBinary(field));
  }
  
  private BinaryRawValues loadBinary(BinaryEntry entry) throws IOException {
    IndexInput data = this.data.clone();
    data.seek(entry.offset);
    final byte[] bytes = new byte[entry.numBytes];
    data.readBytes(bytes, 0, entry.numBytes);
    data.seek(entry.offset + entry.numBytes + entry.missingBytes);

    final int[] address = new int[entry.count+1];
    for(int i=0;i<entry.count;i++) {
      address[i] = data.readInt();
    }
    address[entry.count] = data.readInt();

    BinaryRawValues values = new BinaryRawValues();
    values.bytes = bytes;
    values.address = address;
    return values;
  }
  
  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final SortedEntry entry = sorteds.get(field.name);
    SortedRawValues instance;
    synchronized (this) {
      instance = sortedInstances.get(field.name);
      if (instance == null) {
        // Lazy load
        instance = loadSorted(field);
        if (!merging) {
          sortedInstances.put(field.name, instance);
          ramBytesUsed.addAndGet(instance.ramBytesUsed());
        }
      }
    }
    return new LegacySortedDocValuesWrapper(newSortedInstance(instance.docToOrd.numerics, getLegacyBinary(field), entry.values.count), maxDoc);
  }
  
  private LegacySortedDocValues newSortedInstance(final LegacyNumericDocValues docToOrd, final LegacyBinaryDocValues values, final int count) {
    return new LegacySortedDocValues() {

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
    final SortedEntry entry = sorteds.get(field.name);
    final NumericRawValues docToOrd = loadNumeric(entry.docToOrd);
    final SortedRawValues values = new SortedRawValues();
    values.docToOrd = docToOrd;
    return values;
  }

  @Override
  public synchronized SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedNumericRawValues instance = sortedNumericInstances.get(field.name);
    final SortedNumericEntry entry = sortedNumerics.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadSortedNumeric(entry);
      if (!merging) {
        sortedNumericInstances.put(field.name, instance);
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
      }
    }
    
    if (entry.docToAddress == null) {
      final LegacyNumericDocValues single = instance.values.numerics;
      final Bits docsWithField = getMissingBits(field, entry.values.missingOffset, entry.values.missingBytes);
      return DocValues.singleton(new LegacyNumericDocValuesWrapper(docsWithField, single));
    } else {
      final LegacyNumericDocValues docToAddress = instance.docToAddress.numerics;
      final LegacyNumericDocValues values = instance.values.numerics;
      
      return new LegacySortedNumericDocValuesWrapper(new LegacySortedNumericDocValues() {
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
        }, maxDoc);
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
    SortedSetRawValues instance = sortedSetInstances.get(field.name);
    final SortedSetEntry entry = sortedSets.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadSortedSet(entry);
      if (!merging) {
        sortedSetInstances.put(field.name, instance);
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
      }
    }

    if (instance.docToOrdAddress == null) {
      LegacySortedDocValues sorted = newSortedInstance(instance.ords.numerics, getLegacyBinary(field), entry.values.count);
      return DocValues.singleton(new LegacySortedDocValuesWrapper(sorted, maxDoc));
    } else {
      final LegacyNumericDocValues docToOrdAddress = instance.docToOrdAddress.numerics;
      final LegacyNumericDocValues ords = instance.ords.numerics;
      final LegacyBinaryDocValues values = getLegacyBinary(field);
      
      // Must make a new instance since the iterator has state:
      return new LegacySortedSetDocValuesWrapper(new LegacySortedSetDocValues() {
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
        
        // Leave lookupTerm to super's binary search
        
        // Leave termsEnum to super
        }, maxDoc);
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

  private Bits getMissingBits(FieldInfo field, final long offset, final long length) throws IOException {
    if (offset == -1) {
      return new Bits.MatchAllBits(maxDoc);
    } else {
      FixedBitSet instance;
      synchronized(this) {
        instance = docsWithFieldInstances.get(field.name);
        if (instance == null) {
          IndexInput data = this.data.clone();
          data.seek(offset);
          assert length % 8 == 0;
          long bits[] = new long[(int) length >> 3];
          for (int i = 0; i < bits.length; i++) {
            bits[i] = data.readLong();
          }
          instance = new FixedBitSet(bits, maxDoc);
          if (!merging) {
            docsWithFieldInstances.put(field.name, instance);
            ramBytesUsed.addAndGet(instance.ramBytesUsed());
          }
        }
      }
      return instance;
    }
  }
  
  @Override
  public synchronized DocValuesProducer getMergeInstance() throws IOException {
    return new DirectDocValuesProducer(this);
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  static class BinaryRawValues implements Accountable {
    byte[] bytes;
    int[] address;
    
    @Override
    public long ramBytesUsed() {
      long bytesUsed = RamUsageEstimator.sizeOf(bytes);
      if (address != null) {
        bytesUsed += RamUsageEstimator.sizeOf(address);
      }
      return bytesUsed;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      List<Accountable> resources = new ArrayList<>();
      if (address != null) {
        resources.add(Accountables.namedAccountable("addresses", RamUsageEstimator.sizeOf(address)));
      }
      resources.add(Accountables.namedAccountable("bytes", RamUsageEstimator.sizeOf(bytes)));
      return Collections.unmodifiableList(resources);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }
  
  static class NumericRawValues implements Accountable {
    LegacyNumericDocValues numerics;
    long bytesUsed;
    
    @Override
    public long ramBytesUsed() {
      return bytesUsed;
    }
    
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  static class SortedRawValues implements Accountable {
    NumericRawValues docToOrd;

    @Override
    public long ramBytesUsed() {
      return docToOrd.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
      return docToOrd.getChildResources();
    }
    
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }
  
  static class SortedNumericRawValues implements Accountable {
    NumericRawValues docToAddress;
    NumericRawValues values;
    
    @Override
    public long ramBytesUsed() {
      long bytesUsed = values.ramBytesUsed();
      if (docToAddress != null) {
        bytesUsed += docToAddress.ramBytesUsed();
      }
      return bytesUsed;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      List<Accountable> resources = new ArrayList<>();
      if (docToAddress != null) {
        resources.add(Accountables.namedAccountable("addresses", docToAddress));
      }
      resources.add(Accountables.namedAccountable("values", values));
      return Collections.unmodifiableList(resources);
    }
    
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  static class SortedSetRawValues implements Accountable {
    NumericRawValues docToOrdAddress;
    NumericRawValues ords;

    @Override
    public long ramBytesUsed() {
      long bytesUsed = ords.ramBytesUsed();
      if (docToOrdAddress != null) {
        bytesUsed += docToOrdAddress.ramBytesUsed();
      }
      return bytesUsed;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      List<Accountable> resources = new ArrayList<>();
      if (docToOrdAddress != null) {
        resources.add(Accountables.namedAccountable("addresses", docToOrdAddress));
      }
      resources.add(Accountables.namedAccountable("ordinals", ords));
      return Collections.unmodifiableList(resources);
    }
    
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
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

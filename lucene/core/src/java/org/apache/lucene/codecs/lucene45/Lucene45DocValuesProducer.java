package org.apache.lucene.codecs.lucene45;

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

import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.BINARY_FIXED_UNCOMPRESSED;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.BINARY_PREFIX_COMPRESSED;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.BINARY_VARIABLE_UNCOMPRESSED;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.DELTA_COMPRESSED;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.GCD_COMPRESSED;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.SORTED_SET_SINGLE_VALUED_SORTED;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.SORTED_SET_WITH_ADDRESSES;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer.TABLE_COMPRESSED;
import static org.apache.lucene.codecs.lucene45.Lucene45DocValuesFormat.VERSION_SORTED_SET_SINGLE_VALUE_OPTIMIZED;

import java.io.Closeable; // javadocs
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SingletonSortedSetDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.BlockPackedReader;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.apache.lucene.util.packed.PackedInts;

/** reader for {@link Lucene45DocValuesFormat} */
public class Lucene45DocValuesProducer extends DocValuesProducer implements Closeable {
  private final Map<Integer,NumericEntry> numerics;
  private final Map<Integer,BinaryEntry> binaries;
  private final Map<Integer,SortedSetEntry> sortedSets;
  private final Map<Integer,NumericEntry> ords;
  private final Map<Integer,NumericEntry> ordIndexes;
  private final IndexInput data;
  private final int maxDoc;
  private final int version;

  // memory-resident structures
  private final Map<Integer,MonotonicBlockPackedReader> addressInstances = new HashMap<Integer,MonotonicBlockPackedReader>();
  private final Map<Integer,MonotonicBlockPackedReader> ordIndexInstances = new HashMap<Integer,MonotonicBlockPackedReader>();
  
  /** expert: instantiates a new reader */
  protected Lucene45DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    IndexInput in = state.directory.openInput(metaName, state.context);
    this.maxDoc = state.segmentInfo.getDocCount();
    boolean success = false;
    try {
      version = CodecUtil.checkHeader(in, metaCodec, 
                                      Lucene45DocValuesFormat.VERSION_START,
                                      Lucene45DocValuesFormat.VERSION_CURRENT);
      numerics = new HashMap<Integer,NumericEntry>();
      ords = new HashMap<Integer,NumericEntry>();
      ordIndexes = new HashMap<Integer,NumericEntry>();
      binaries = new HashMap<Integer,BinaryEntry>();
      sortedSets = new HashMap<Integer,SortedSetEntry>();
      readFields(in, state.fieldInfos);

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
                                                 Lucene45DocValuesFormat.VERSION_START,
                                                 Lucene45DocValuesFormat.VERSION_CURRENT);
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

  private void readSortedField(int fieldNumber, IndexInput meta, FieldInfos infos) throws IOException {
    // sorted = binary + numeric
    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sorted entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene45DocValuesFormat.BINARY) {
      throw new CorruptIndexException("sorted entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    BinaryEntry b = readBinaryEntry(meta);
    binaries.put(fieldNumber, b);
    
    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sorted entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene45DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sorted entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    NumericEntry n = readNumericEntry(meta);
    ords.put(fieldNumber, n);
  }

  private void readSortedSetFieldWithAddresses(int fieldNumber, IndexInput meta, FieldInfos infos) throws IOException {
    // sortedset = binary + numeric (addresses) + ordIndex
    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene45DocValuesFormat.BINARY) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    BinaryEntry b = readBinaryEntry(meta);
    binaries.put(fieldNumber, b);

    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene45DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    NumericEntry n1 = readNumericEntry(meta);
    ords.put(fieldNumber, n1);

    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene45DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    NumericEntry n2 = readNumericEntry(meta);
    ordIndexes.put(fieldNumber, n2);
  }

  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      byte type = meta.readByte();
      if (type == Lucene45DocValuesFormat.NUMERIC) {
        numerics.put(fieldNumber, readNumericEntry(meta));
      } else if (type == Lucene45DocValuesFormat.BINARY) {
        BinaryEntry b = readBinaryEntry(meta);
        binaries.put(fieldNumber, b);
      } else if (type == Lucene45DocValuesFormat.SORTED) {
        readSortedField(fieldNumber, meta, infos);
      } else if (type == Lucene45DocValuesFormat.SORTED_SET) {
        SortedSetEntry ss = readSortedSetEntry(meta);
        sortedSets.put(fieldNumber, ss);
        if (ss.format == SORTED_SET_WITH_ADDRESSES) {
          readSortedSetFieldWithAddresses(fieldNumber, meta, infos);
        } else if (ss.format == SORTED_SET_SINGLE_VALUED_SORTED) {
          if (meta.readVInt() != fieldNumber) {
            throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
          }
          if (meta.readByte() != Lucene45DocValuesFormat.SORTED) {
            throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
          }
          readSortedField(fieldNumber, meta, infos);
        } else {
          throw new AssertionError();
        }
      } else {
        throw new CorruptIndexException("invalid type: " + type + ", resource=" + meta);
      }
      fieldNumber = meta.readVInt();
    }
  }
  
  static NumericEntry readNumericEntry(IndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    entry.format = meta.readVInt();
    entry.missingOffset = meta.readLong();
    entry.packedIntsVersion = meta.readVInt();
    entry.offset = meta.readLong();
    entry.count = meta.readVLong();
    entry.blockSize = meta.readVInt();
    switch(entry.format) {
      case GCD_COMPRESSED:
        entry.minValue = meta.readLong();
        entry.gcd = meta.readLong();
        break;
      case TABLE_COMPRESSED:
        if (entry.count > Integer.MAX_VALUE) {
          throw new CorruptIndexException("Cannot use TABLE_COMPRESSED with more than MAX_VALUE values, input=" + meta);
        }
        final int uniqueValues = meta.readVInt();
        if (uniqueValues > 256) {
          throw new CorruptIndexException("TABLE_COMPRESSED cannot have more than 256 distinct values, input=" + meta);
        }
        entry.table = new long[uniqueValues];
        for (int i = 0; i < uniqueValues; ++i) {
          entry.table[i] = meta.readLong();
        }
        break;
      case DELTA_COMPRESSED:
        break;
      default:
        throw new CorruptIndexException("Unknown format: " + entry.format + ", input=" + meta);
    }
    return entry;
  }
  
  static BinaryEntry readBinaryEntry(IndexInput meta) throws IOException {
    BinaryEntry entry = new BinaryEntry();
    entry.format = meta.readVInt();
    entry.missingOffset = meta.readLong();
    entry.minLength = meta.readVInt();
    entry.maxLength = meta.readVInt();
    entry.count = meta.readVLong();
    entry.offset = meta.readLong();
    switch(entry.format) {
      case BINARY_FIXED_UNCOMPRESSED:
        break;
      case BINARY_PREFIX_COMPRESSED:
        entry.addressInterval = meta.readVInt();
        entry.addressesOffset = meta.readLong();
        entry.packedIntsVersion = meta.readVInt();
        entry.blockSize = meta.readVInt();
        break;
      case BINARY_VARIABLE_UNCOMPRESSED:
        entry.addressesOffset = meta.readLong();
        entry.packedIntsVersion = meta.readVInt();
        entry.blockSize = meta.readVInt();
        break;
      default:
        throw new CorruptIndexException("Unknown format: " + entry.format + ", input=" + meta);
    }
    return entry;
  }

  SortedSetEntry readSortedSetEntry(IndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    if (version >= VERSION_SORTED_SET_SINGLE_VALUE_OPTIMIZED) {
      entry.format = meta.readVInt();
    } else {
      entry.format = SORTED_SET_WITH_ADDRESSES;
    }
    if (entry.format != SORTED_SET_SINGLE_VALUED_SORTED && entry.format != SORTED_SET_WITH_ADDRESSES) {
      throw new CorruptIndexException("Unknown format: " + entry.format + ", input=" + meta);
    }
    return entry;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.number);
    return getNumeric(entry);
  }
  
  @Override
  public long ramBytesUsed() {
    long sizeInBytes = 0;    
    for(MonotonicBlockPackedReader monotonicBlockPackedReader: addressInstances.values()) {
      sizeInBytes += Integer.SIZE + monotonicBlockPackedReader.ramBytesUsed();
    }
    for(MonotonicBlockPackedReader monotonicBlockPackedReader: ordIndexInstances.values()) {
      sizeInBytes += Integer.SIZE + monotonicBlockPackedReader.ramBytesUsed();
    }
    return sizeInBytes;
  }
  
  LongValues getNumeric(NumericEntry entry) throws IOException {
    final IndexInput data = this.data.clone();
    data.seek(entry.offset);

    switch (entry.format) {
      case DELTA_COMPRESSED:
        final BlockPackedReader reader = new BlockPackedReader(data, entry.packedIntsVersion, entry.blockSize, entry.count, true);
        return reader;
      case GCD_COMPRESSED:
        final long min = entry.minValue;
        final long mult = entry.gcd;
        final BlockPackedReader quotientReader = new BlockPackedReader(data, entry.packedIntsVersion, entry.blockSize, entry.count, true);
        return new LongValues() {
          @Override
          public long get(long id) {
            return min + mult * quotientReader.get(id);
          }
        };
      case TABLE_COMPRESSED:
        final long table[] = entry.table;
        final int bitsRequired = PackedInts.bitsRequired(table.length - 1);
        final PackedInts.Reader ords = PackedInts.getDirectReaderNoHeader(data, PackedInts.Format.PACKED, entry.packedIntsVersion, (int) entry.count, bitsRequired);
        return new LongValues() {
          @Override
          public long get(long id) {
            return table[(int) ords.get((int) id)];
          }
        };
      default:
        throw new AssertionError();
    }
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryEntry bytes = binaries.get(field.number);
    switch(bytes.format) {
      case BINARY_FIXED_UNCOMPRESSED:
        return getFixedBinary(field, bytes);
      case BINARY_VARIABLE_UNCOMPRESSED:
        return getVariableBinary(field, bytes);
      case BINARY_PREFIX_COMPRESSED:
        return getCompressedBinary(field, bytes);
      default:
        throw new AssertionError();
    }
  }
  
  private BinaryDocValues getFixedBinary(FieldInfo field, final BinaryEntry bytes) {
    final IndexInput data = this.data.clone();

    return new LongBinaryDocValues() {
      @Override
      public void get(long id, BytesRef result) {
        long address = bytes.offset + id * bytes.maxLength;
        try {
          data.seek(address);
          // NOTE: we could have one buffer, but various consumers (e.g. FieldComparatorSource) 
          // assume "they" own the bytes after calling this!
          final byte[] buffer = new byte[bytes.maxLength];
          data.readBytes(buffer, 0, buffer.length);
          result.bytes = buffer;
          result.offset = 0;
          result.length = buffer.length;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  /** returns an address instance for variable-length binary values.
   *  @lucene.internal */
  protected MonotonicBlockPackedReader getAddressInstance(IndexInput data, FieldInfo field, BinaryEntry bytes) throws IOException {
    final MonotonicBlockPackedReader addresses;
    synchronized (addressInstances) {
      MonotonicBlockPackedReader addrInstance = addressInstances.get(field.number);
      if (addrInstance == null) {
        data.seek(bytes.addressesOffset);
        addrInstance = new MonotonicBlockPackedReader(data, bytes.packedIntsVersion, bytes.blockSize, bytes.count, false);
        addressInstances.put(field.number, addrInstance);
      }
      addresses = addrInstance;
    }
    return addresses;
  }
  
  private BinaryDocValues getVariableBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
    final IndexInput data = this.data.clone();
    
    final MonotonicBlockPackedReader addresses = getAddressInstance(data, field, bytes);

    return new LongBinaryDocValues() {
      @Override
      public void get(long id, BytesRef result) {
        long startAddress = bytes.offset + (id == 0 ? 0 : addresses.get(id-1));
        long endAddress = bytes.offset + addresses.get(id);
        int length = (int) (endAddress - startAddress);
        try {
          data.seek(startAddress);
          // NOTE: we could have one buffer, but various consumers (e.g. FieldComparatorSource) 
          // assume "they" own the bytes after calling this!
          final byte[] buffer = new byte[length];
          data.readBytes(buffer, 0, buffer.length);
          result.bytes = buffer;
          result.offset = 0;
          result.length = length;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  /** returns an address instance for prefix-compressed binary values. 
   * @lucene.internal */
  protected MonotonicBlockPackedReader getIntervalInstance(IndexInput data, FieldInfo field, BinaryEntry bytes) throws IOException {
    final MonotonicBlockPackedReader addresses;
    final long interval = bytes.addressInterval;
    synchronized (addressInstances) {
      MonotonicBlockPackedReader addrInstance = addressInstances.get(field.number);
      if (addrInstance == null) {
        data.seek(bytes.addressesOffset);
        final long size;
        if (bytes.count % interval == 0) {
          size = bytes.count / interval;
        } else {
          size = 1L + bytes.count / interval;
        }
        addrInstance = new MonotonicBlockPackedReader(data, bytes.packedIntsVersion, bytes.blockSize, size, false);
        addressInstances.put(field.number, addrInstance);
      }
      addresses = addrInstance;
    }
    return addresses;
  }


  private BinaryDocValues getCompressedBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
    final IndexInput data = this.data.clone();

    final MonotonicBlockPackedReader addresses = getIntervalInstance(data, field, bytes);
    
    return new CompressedBinaryDocValues(bytes, addresses, data);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final int valueCount = (int) binaries.get(field.number).count;
    final BinaryDocValues binary = getBinary(field);
    NumericEntry entry = ords.get(field.number);
    IndexInput data = this.data.clone();
    data.seek(entry.offset);
    final BlockPackedReader ordinals = new BlockPackedReader(data, entry.packedIntsVersion, entry.blockSize, entry.count, true);
    
    return new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        return (int) ordinals.get(docID);
      }

      @Override
      public void lookupOrd(int ord, BytesRef result) {
        binary.get(ord, result);
      }

      @Override
      public int getValueCount() {
        return valueCount;
      }

      @Override
      public int lookupTerm(BytesRef key) {
        if (binary instanceof CompressedBinaryDocValues) {
          return (int) ((CompressedBinaryDocValues)binary).lookupTerm(key);
        } else {
        return super.lookupTerm(key);
        }
      }

      @Override
      public TermsEnum termsEnum() {
        if (binary instanceof CompressedBinaryDocValues) {
          return ((CompressedBinaryDocValues)binary).getTermsEnum();
        } else {
          return super.termsEnum();
        }
      }
    };
  }
  
  /** returns an address instance for sortedset ordinal lists
   * @lucene.internal */
  protected MonotonicBlockPackedReader getOrdIndexInstance(IndexInput data, FieldInfo field, NumericEntry entry) throws IOException {
    final MonotonicBlockPackedReader ordIndex;
    synchronized (ordIndexInstances) {
      MonotonicBlockPackedReader ordIndexInstance = ordIndexInstances.get(field.number);
      if (ordIndexInstance == null) {
        data.seek(entry.offset);
        ordIndexInstance = new MonotonicBlockPackedReader(data, entry.packedIntsVersion, entry.blockSize, entry.count, false);
        ordIndexInstances.put(field.number, ordIndexInstance);
      }
      ordIndex = ordIndexInstance;
    }
    return ordIndex;
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetEntry ss = sortedSets.get(field.number);
    if (ss.format == SORTED_SET_SINGLE_VALUED_SORTED) {
      final SortedDocValues values = getSorted(field);
      return new SingletonSortedSetDocValues(values);
    } else if (ss.format != SORTED_SET_WITH_ADDRESSES) {
      throw new AssertionError();
    }

    final IndexInput data = this.data.clone();
    final long valueCount = binaries.get(field.number).count;
    // we keep the byte[]s and list of ords on disk, these could be large
    final LongBinaryDocValues binary = (LongBinaryDocValues) getBinary(field);
    final LongValues ordinals = getNumeric(ords.get(field.number));
    // but the addresses to the ord stream are in RAM
    final MonotonicBlockPackedReader ordIndex = getOrdIndexInstance(data, field, ordIndexes.get(field.number));
    
    return new SortedSetDocValues() {
      long offset;
      long endOffset;
      
      @Override
      public long nextOrd() {
        if (offset == endOffset) {
          return NO_MORE_ORDS;
        } else {
          long ord = ordinals.get(offset);
          offset++;
          return ord;
        }
      }

      @Override
      public void setDocument(int docID) {
        offset = (docID == 0 ? 0 : ordIndex.get(docID-1));
        endOffset = ordIndex.get(docID);
      }

      @Override
      public void lookupOrd(long ord, BytesRef result) {
        binary.get(ord, result);
      }

      @Override
      public long getValueCount() {
        return valueCount;
      }
      
      @Override
      public long lookupTerm(BytesRef key) {
        if (binary instanceof CompressedBinaryDocValues) {
          return ((CompressedBinaryDocValues)binary).lookupTerm(key);
        } else {
          return super.lookupTerm(key);
        }
      }

      @Override
      public TermsEnum termsEnum() {
        if (binary instanceof CompressedBinaryDocValues) {
          return ((CompressedBinaryDocValues)binary).getTermsEnum();
        } else {
          return super.termsEnum();
        }
      }
    };
  }
  
  private Bits getMissingBits(final long offset) throws IOException {
    if (offset == -1) {
      return new Bits.MatchAllBits(maxDoc);
    } else {
      final IndexInput in = data.clone();
      return new Bits() {

        @Override
        public boolean get(int index) {
          try {
            in.seek(offset + (index >> 3));
            return (in.readByte() & (1 << (index & 7))) != 0;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };
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
        return getMissingBits(be.missingOffset);
      case NUMERIC:
        NumericEntry ne = numerics.get(field.number);
        return getMissingBits(ne.missingOffset);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
  
  /** metadata entry for a numeric docvalues field */
  protected static class NumericEntry {
    private NumericEntry() {}
    /** offset to the bitset representing docsWithField, or -1 if no documents have missing values */
    long missingOffset;
    /** offset to the actual numeric values */
    public long offset;

    int format;
    /** packed ints version used to encode these numerics */
    public int packedIntsVersion;
    /** count of values written */
    public long count;
    /** packed ints blocksize */
    public int blockSize;
    
    long minValue;
    long gcd;
    long table[];
  }
  
  /** metadata entry for a binary docvalues field */
  protected static class BinaryEntry {
    private BinaryEntry() {}
    /** offset to the bitset representing docsWithField, or -1 if no documents have missing values */
    long missingOffset;
    /** offset to the actual binary values */
    long offset;

    int format;
    /** count of values written */
    public long count;
    int minLength;
    int maxLength;
    /** offset to the addressing data that maps a value to its slice of the byte[] */
    public long addressesOffset;
    /** interval of shared prefix chunks (when using prefix-compressed binary) */
    public long addressInterval;
    /** packed ints version used to encode addressing information */
    public int packedIntsVersion;
    /** packed ints blocksize */
    public int blockSize;
  }

  /** metadata entry for a sorted-set docvalues field */
  protected static class SortedSetEntry {
    private SortedSetEntry() {}
    int format;
  }

  // internally we compose complex dv (sorted/sortedset) from other ones
  static abstract class LongBinaryDocValues extends BinaryDocValues {
    @Override
    public final void get(int docID, BytesRef result) {
      get((long)docID, result);
    }
    
    abstract void get(long id, BytesRef Result);
  }
  
  // in the compressed case, we add a few additional operations for
  // more efficient reverse lookup and enumeration
  static class CompressedBinaryDocValues extends LongBinaryDocValues {
    final BinaryEntry bytes;
    final long interval;
    final long numValues;
    final long numIndexValues;
    final MonotonicBlockPackedReader addresses;
    final IndexInput data;
    final TermsEnum termsEnum;
    
    public CompressedBinaryDocValues(BinaryEntry bytes, MonotonicBlockPackedReader addresses, IndexInput data) throws IOException {
      this.bytes = bytes;
      this.interval = bytes.addressInterval;
      this.addresses = addresses;
      this.data = data;
      this.numValues = bytes.count;
      this.numIndexValues = addresses.size();
      this.termsEnum = getTermsEnum(data);
    }
    
    @Override
    public void get(long id, BytesRef result) {
      try {
        termsEnum.seekExact(id);
        BytesRef term = termsEnum.term();
        result.bytes = term.bytes;
        result.offset = term.offset;
        result.length = term.length;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    long lookupTerm(BytesRef key) {
      try {
        SeekStatus status = termsEnum.seekCeil(key);
        if (status == SeekStatus.END) {
          return -numValues-1;
        } else if (status == SeekStatus.FOUND) {
          return termsEnum.ord();
        } else {
          return -termsEnum.ord()-1;
        }
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
    }
    
    TermsEnum getTermsEnum() {
      try {
        return getTermsEnum(data.clone());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    private TermsEnum getTermsEnum(final IndexInput input) throws IOException {
      input.seek(bytes.offset);
      
      return new TermsEnum() {
        private long currentOrd = -1;
        // TODO: maxLength is negative when all terms are merged away...
        private final BytesRef termBuffer = new BytesRef(bytes.maxLength < 0 ? 0 : bytes.maxLength);
        private final BytesRef term = new BytesRef(); // TODO: paranoia?

        @Override
        public BytesRef next() throws IOException {
          if (doNext() == null) {
            return null;
          } else {
            setTerm();
            return term;
          }
        }
        
        private BytesRef doNext() throws IOException {
          if (++currentOrd >= numValues) {
            return null;
          } else {
            int start = input.readVInt();
            int suffix = input.readVInt();
            input.readBytes(termBuffer.bytes, start, suffix);
            termBuffer.length = start + suffix;
            return termBuffer;
          }
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
          // binary-search just the index values to find the block,
          // then scan within the block
          long low = 0;
          long high = numIndexValues-1;

          while (low <= high) {
            long mid = (low + high) >>> 1;
            doSeek(mid * interval);
            int cmp = termBuffer.compareTo(text);

            if (cmp < 0) {
              low = mid + 1;
            } else if (cmp > 0) {
              high = mid - 1;
            } else {
              // we got lucky, found an indexed term
              setTerm();
              return SeekStatus.FOUND;
            }
          }
          
          if (numIndexValues == 0) {
            return SeekStatus.END;
          }
          
          // block before insertion point
          long block = low-1;
          doSeek(block < 0 ? -1 : block * interval);
          
          while (doNext() != null) {
            int cmp = termBuffer.compareTo(text);
            if (cmp == 0) {
              setTerm();
              return SeekStatus.FOUND;
            } else if (cmp > 0) {
              setTerm();
              return SeekStatus.NOT_FOUND;
            }
          }
          
          return SeekStatus.END;
        }

        @Override
        public void seekExact(long ord) throws IOException {
          doSeek(ord);
          setTerm();
        }
        
        private void doSeek(long ord) throws IOException {
          long block = ord / interval;

          if (ord >= currentOrd && block == currentOrd / interval) {
            // seek within current block
          } else {
            // position before start of block
            currentOrd = ord - ord % interval - 1;
            input.seek(bytes.offset + addresses.get(block));
          }
          
          while (currentOrd < ord) {
            doNext();
          }
        }
        
        private void setTerm() {
          // TODO: is there a cleaner way
          term.bytes = new byte[termBuffer.length];
          term.offset = 0;
          term.copyBytes(termBuffer);
        }

        @Override
        public BytesRef term() throws IOException {
          return term;
        }

        @Override
        public long ord() throws IOException {
          return currentOrd;
        }
        
        @Override
        public Comparator<BytesRef> getComparator() {
          return BytesRef.getUTF8SortedAsUnicodeComparator();
        }

        @Override
        public int docFreq() throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public long totalTermFreq() throws IOException {
          return -1;
        }

        @Override
        public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}

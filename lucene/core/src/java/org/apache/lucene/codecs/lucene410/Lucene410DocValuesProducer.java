package org.apache.lucene.codecs.lucene410;

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

import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.BINARY_FIXED_UNCOMPRESSED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.BINARY_PREFIX_COMPRESSED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.BINARY_VARIABLE_UNCOMPRESSED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.DELTA_COMPRESSED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.GCD_COMPRESSED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.MONOTONIC_COMPRESSED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.SORTED_SINGLE_VALUED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.SORTED_WITH_ADDRESSES;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.TABLE_COMPRESSED;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.INTERVAL_SHIFT;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.INTERVAL_COUNT;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.INTERVAL_MASK;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.REVERSE_INTERVAL_SHIFT;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.REVERSE_INTERVAL_MASK;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.BLOCK_INTERVAL_SHIFT;
import static org.apache.lucene.codecs.lucene410.Lucene410DocValuesConsumer.BLOCK_INTERVAL_MASK;

import java.io.Closeable; // javadocs
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;

/** reader for {@link Lucene410DocValuesFormat} */
class Lucene410DocValuesProducer extends DocValuesProducer implements Closeable {
  private final Map<Integer,NumericEntry> numerics;
  private final Map<Integer,BinaryEntry> binaries;
  private final Map<Integer,SortedSetEntry> sortedSets;
  private final Map<Integer,SortedSetEntry> sortedNumerics;
  private final Map<Integer,NumericEntry> ords;
  private final Map<Integer,NumericEntry> ordIndexes;
  private final AtomicLong ramBytesUsed;
  private final IndexInput data;
  private final int maxDoc;
  private final int version;

  // memory-resident structures
  private final Map<Integer,MonotonicBlockPackedReader> addressInstances = new HashMap<>();
  private final Map<Integer,MonotonicBlockPackedReader> ordIndexInstances = new HashMap<>();
  private final Map<Integer,ReverseTermsIndex> reverseIndexInstances = new HashMap<>();
  
  /** expert: instantiates a new reader */
  Lucene410DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
    this.maxDoc = state.segmentInfo.getDocCount();
    boolean success = false;
    try {
      version = CodecUtil.checkHeader(in, metaCodec, 
                                      Lucene410DocValuesFormat.VERSION_START,
                                      Lucene410DocValuesFormat.VERSION_CURRENT);
      numerics = new HashMap<>();
      ords = new HashMap<>();
      ordIndexes = new HashMap<>();
      binaries = new HashMap<>();
      sortedSets = new HashMap<>();
      sortedNumerics = new HashMap<>();
      readFields(in, state.fieldInfos);

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
                                                 Lucene410DocValuesFormat.VERSION_START,
                                                 Lucene410DocValuesFormat.VERSION_CURRENT);
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
    
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
  }

  private void readSortedField(int fieldNumber, IndexInput meta, FieldInfos infos) throws IOException {
    // sorted = binary + numeric
    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sorted entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene410DocValuesFormat.BINARY) {
      throw new CorruptIndexException("sorted entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    BinaryEntry b = readBinaryEntry(meta);
    binaries.put(fieldNumber, b);
    
    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sorted entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene410DocValuesFormat.NUMERIC) {
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
    if (meta.readByte() != Lucene410DocValuesFormat.BINARY) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    BinaryEntry b = readBinaryEntry(meta);
    binaries.put(fieldNumber, b);

    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene410DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    NumericEntry n1 = readNumericEntry(meta);
    ords.put(fieldNumber, n1);

    if (meta.readVInt() != fieldNumber) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    if (meta.readByte() != Lucene410DocValuesFormat.NUMERIC) {
      throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
    }
    NumericEntry n2 = readNumericEntry(meta);
    ordIndexes.put(fieldNumber, n2);
  }

  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      if (infos.fieldInfo(fieldNumber) == null) {
        // trickier to validate more: because we re-use for norms, because we use multiple entries
        // for "composite" types like sortedset, etc.
        throw new CorruptIndexException("Invalid field number: " + fieldNumber + " (resource=" + meta + ")");
      }
      byte type = meta.readByte();
      if (type == Lucene410DocValuesFormat.NUMERIC) {
        numerics.put(fieldNumber, readNumericEntry(meta));
      } else if (type == Lucene410DocValuesFormat.BINARY) {
        BinaryEntry b = readBinaryEntry(meta);
        binaries.put(fieldNumber, b);
      } else if (type == Lucene410DocValuesFormat.SORTED) {
        readSortedField(fieldNumber, meta, infos);
      } else if (type == Lucene410DocValuesFormat.SORTED_SET) {
        SortedSetEntry ss = readSortedSetEntry(meta);
        sortedSets.put(fieldNumber, ss);
        if (ss.format == SORTED_WITH_ADDRESSES) {
          readSortedSetFieldWithAddresses(fieldNumber, meta, infos);
        } else if (ss.format == SORTED_SINGLE_VALUED) {
          if (meta.readVInt() != fieldNumber) {
            throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
          }
          if (meta.readByte() != Lucene410DocValuesFormat.SORTED) {
            throw new CorruptIndexException("sortedset entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
          }
          readSortedField(fieldNumber, meta, infos);
        } else {
          throw new AssertionError();
        }
      } else if (type == Lucene410DocValuesFormat.SORTED_NUMERIC) {
        SortedSetEntry ss = readSortedSetEntry(meta);
        sortedNumerics.put(fieldNumber, ss);
        if (meta.readVInt() != fieldNumber) {
          throw new CorruptIndexException("sortednumeric entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
        }
        if (meta.readByte() != Lucene410DocValuesFormat.NUMERIC) {
          throw new CorruptIndexException("sortednumeric entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
        }
        numerics.put(fieldNumber, readNumericEntry(meta));
        if (ss.format == SORTED_WITH_ADDRESSES) {
          if (meta.readVInt() != fieldNumber) {
            throw new CorruptIndexException("sortednumeric entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
          }
          if (meta.readByte() != Lucene410DocValuesFormat.NUMERIC) {
            throw new CorruptIndexException("sortednumeric entry for field: " + fieldNumber + " is corrupt (resource=" + meta + ")");
          }
          NumericEntry ordIndex = readNumericEntry(meta);
          ordIndexes.put(fieldNumber, ordIndex);
        } else if (ss.format != SORTED_SINGLE_VALUED) {
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
    entry.offset = meta.readLong();
    entry.count = meta.readVLong();
    switch(entry.format) {
      case GCD_COMPRESSED:
        entry.minValue = meta.readLong();
        entry.gcd = meta.readLong();
        entry.bitsPerValue = meta.readVInt();
        break;
      case TABLE_COMPRESSED:
        final int uniqueValues = meta.readVInt();
        if (uniqueValues > 256) {
          throw new CorruptIndexException("TABLE_COMPRESSED cannot have more than 256 distinct values, input=" + meta);
        }
        entry.table = new long[uniqueValues];
        for (int i = 0; i < uniqueValues; ++i) {
          entry.table[i] = meta.readLong();
        }
        entry.bitsPerValue = meta.readVInt();
        break;
      case DELTA_COMPRESSED:
        entry.minValue = meta.readLong();
        entry.bitsPerValue = meta.readVInt();
        break;
      case MONOTONIC_COMPRESSED:
        entry.packedIntsVersion = meta.readVInt();
        entry.blockSize = meta.readVInt();
        break;
      default:
        throw new CorruptIndexException("Unknown format: " + entry.format + ", input=" + meta);
    }
    entry.endOffset = meta.readLong();
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
        entry.addressesOffset = meta.readLong();
        entry.packedIntsVersion = meta.readVInt();
        entry.blockSize = meta.readVInt();
        entry.reverseIndexOffset = meta.readLong();
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
    entry.format = meta.readVInt();
    if (entry.format != SORTED_SINGLE_VALUED && entry.format != SORTED_WITH_ADDRESSES) {
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
    return ramBytesUsed.get();
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  LongValues getNumeric(NumericEntry entry) throws IOException {
    RandomAccessInput slice = this.data.randomAccessSlice(entry.offset, entry.endOffset - entry.offset);
    switch (entry.format) {
      case DELTA_COMPRESSED:
        final long delta = entry.minValue;
        final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
        return new LongValues() {
          @Override
          public long get(long id) {
            return delta + values.get(id);
          }
        };
      case GCD_COMPRESSED:
        final long min = entry.minValue;
        final long mult = entry.gcd;
        final LongValues quotientReader = DirectReader.getInstance(slice, entry.bitsPerValue);
        return new LongValues() {
          @Override
          public long get(long id) {
            return min + mult * quotientReader.get(id);
          }
        };
      case TABLE_COMPRESSED:
        final long table[] = entry.table;
        final LongValues ords = DirectReader.getInstance(slice, entry.bitsPerValue);
        return new LongValues() {
          @Override
          public long get(long id) {
            return table[(int) ords.get(id)];
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
  
  private BinaryDocValues getFixedBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
    final IndexInput data = this.data.slice("fixed-binary", bytes.offset, bytes.count * bytes.maxLength);

    final BytesRef term = new BytesRef(bytes.maxLength);
    final byte[] buffer = term.bytes;
    final int length = term.length = bytes.maxLength;
    
    return new LongBinaryDocValues() {
      @Override
      public BytesRef get(long id) {
        try {
          data.seek(id * length);
          data.readBytes(buffer, 0, buffer.length);
          return term;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  /** returns an address instance for variable-length binary values. */
  private synchronized MonotonicBlockPackedReader getAddressInstance(FieldInfo field, BinaryEntry bytes) throws IOException {
    MonotonicBlockPackedReader addresses = addressInstances.get(field.number);
    if (addresses == null) {
      data.seek(bytes.addressesOffset);
      addresses = MonotonicBlockPackedReader.of(data, bytes.packedIntsVersion, bytes.blockSize, bytes.count+1, false);
      addressInstances.put(field.number, addresses);
      ramBytesUsed.addAndGet(addresses.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_INT);
    }
    return addresses;
  }
  
  private BinaryDocValues getVariableBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
    final MonotonicBlockPackedReader addresses = getAddressInstance(field, bytes);

    final IndexInput data = this.data.slice("var-binary", bytes.offset, bytes.addressesOffset - bytes.offset);
    final BytesRef term = new BytesRef(Math.max(0, bytes.maxLength));
    final byte buffer[] = term.bytes;
    
    return new LongBinaryDocValues() {      
      @Override
      public BytesRef get(long id) {
        long startAddress = addresses.get(id);
        long endAddress = addresses.get(id+1);
        int length = (int) (endAddress - startAddress);
        try {
          data.seek(startAddress);
          data.readBytes(buffer, 0, length);
          term.length = length;
          return term;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  /** returns an address instance for prefix-compressed binary values. */
  private synchronized MonotonicBlockPackedReader getIntervalInstance(FieldInfo field, BinaryEntry bytes) throws IOException {
    MonotonicBlockPackedReader addresses = addressInstances.get(field.number);
    if (addresses == null) {
      data.seek(bytes.addressesOffset);
      final long size = (bytes.count + INTERVAL_MASK) >>> INTERVAL_SHIFT;
      addresses = MonotonicBlockPackedReader.of(data, bytes.packedIntsVersion, bytes.blockSize, size, false);
      addressInstances.put(field.number, addresses);
      ramBytesUsed.addAndGet(addresses.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_INT);
    }
    return addresses;
  }
  
  /** returns a reverse lookup instance for prefix-compressed binary values. */
  private synchronized ReverseTermsIndex getReverseIndexInstance(FieldInfo field, BinaryEntry bytes) throws IOException {
    ReverseTermsIndex index = reverseIndexInstances.get(field.number);
    if (index == null) {
      index = new ReverseTermsIndex();
      data.seek(bytes.reverseIndexOffset);
      long size = (bytes.count + REVERSE_INTERVAL_MASK) >>> REVERSE_INTERVAL_SHIFT;
      index.termAddresses = MonotonicBlockPackedReader.of(data, bytes.packedIntsVersion, bytes.blockSize, size, false);
      long dataSize = data.readVLong();
      PagedBytes pagedBytes = new PagedBytes(15);
      pagedBytes.copy(data, dataSize);
      index.terms = pagedBytes.freeze(true);
      reverseIndexInstances.put(field.number, index);
      ramBytesUsed.addAndGet(index.termAddresses.ramBytesUsed() + index.terms.ramBytesUsed());
    }
    return index;
  }

  private BinaryDocValues getCompressedBinary(FieldInfo field, final BinaryEntry bytes) throws IOException {
    final MonotonicBlockPackedReader addresses = getIntervalInstance(field, bytes);
    final ReverseTermsIndex index = getReverseIndexInstance(field, bytes);
    assert addresses.size() > 0; // we don't have to handle empty case
    IndexInput slice = data.slice("terms", bytes.offset, bytes.addressesOffset - bytes.offset);
    return new CompressedBinaryDocValues(bytes, addresses, index, slice);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    final int valueCount = (int) binaries.get(field.number).count;
    final BinaryDocValues binary = getBinary(field);
    NumericEntry entry = ords.get(field.number);
    final LongValues ordinals = getNumeric(entry);
    return new SortedDocValues() {

      @Override
      public int getOrd(int docID) {
        return (int) ordinals.get(docID);
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return binary.get(ord);
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
  
  /** returns an address instance for sortedset ordinal lists */
  private synchronized MonotonicBlockPackedReader getOrdIndexInstance(FieldInfo field, NumericEntry entry) throws IOException {
    MonotonicBlockPackedReader instance = ordIndexInstances.get(field.number);
    if (instance == null) {
      data.seek(entry.offset);
      instance = MonotonicBlockPackedReader.of(data, entry.packedIntsVersion, entry.blockSize, entry.count+1, false);
      ordIndexInstances.put(field.number, instance);
      ramBytesUsed.addAndGet(instance.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_INT);
    }
    return instance;
  }
  
  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedSetEntry ss = sortedNumerics.get(field.number);
    NumericEntry numericEntry = numerics.get(field.number);
    final LongValues values = getNumeric(numericEntry);
    if (ss.format == SORTED_SINGLE_VALUED) {
      final Bits docsWithField = getMissingBits(numericEntry.missingOffset);
      return DocValues.singleton(values, docsWithField);
    } else if (ss.format == SORTED_WITH_ADDRESSES) {
      final MonotonicBlockPackedReader ordIndex = getOrdIndexInstance(field, ordIndexes.get(field.number));
      
      return new SortedNumericDocValues() {
        long startOffset;
        long endOffset;
        
        @Override
        public void setDocument(int doc) {
          startOffset = ordIndex.get(doc);
          endOffset = ordIndex.get(doc+1L);
        }

        @Override
        public long valueAt(int index) {
          return values.get(startOffset + index);
        }

        @Override
        public int count() {
          return (int) (endOffset - startOffset);
        }
      };
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetEntry ss = sortedSets.get(field.number);
    if (ss.format == SORTED_SINGLE_VALUED) {
      final SortedDocValues values = getSorted(field);
      return DocValues.singleton(values);
    } else if (ss.format != SORTED_WITH_ADDRESSES) {
      throw new AssertionError();
    }

    final long valueCount = binaries.get(field.number).count;
    // we keep the byte[]s and list of ords on disk, these could be large
    final LongBinaryDocValues binary = (LongBinaryDocValues) getBinary(field);
    final LongValues ordinals = getNumeric(ords.get(field.number));
    // but the addresses to the ord stream are in RAM
    final MonotonicBlockPackedReader ordIndex = getOrdIndexInstance(field, ordIndexes.get(field.number));
    
    return new RandomAccessOrds() {
      long startOffset;
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
        startOffset = offset = ordIndex.get(docID);
        endOffset = ordIndex.get(docID+1L);
      }

      @Override
      public BytesRef lookupOrd(long ord) {
        return binary.get(ord);
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

      @Override
      public long ordAt(int index) {
        return ordinals.get(startOffset + index);
      }

      @Override
      public int cardinality() {
        return (int) (endOffset - startOffset);
      }
    };
  }
  
  private Bits getMissingBits(final long offset) throws IOException {
    if (offset == -1) {
      return new Bits.MatchAllBits(maxDoc);
    } else {
      int length = (int) ((maxDoc + 7L) >>> 3);
      final RandomAccessInput in = data.randomAccessSlice(offset, length);
      return new Bits() {
        @Override
        public boolean get(int index) {
          try {
            return (in.readByte(index >> 3) & (1 << (index & 7))) != 0;
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
        return DocValues.docsWithValue(getSortedSet(field), maxDoc);
      case SORTED_NUMERIC:
        return DocValues.docsWithValue(getSortedNumeric(field), maxDoc);
      case SORTED:
        return DocValues.docsWithValue(getSorted(field), maxDoc);
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
  static class NumericEntry {
    private NumericEntry() {}
    /** offset to the bitset representing docsWithField, or -1 if no documents have missing values */
    long missingOffset;
    /** offset to the actual numeric values */
    public long offset;
    /** end offset to the actual numeric values */
    public long endOffset;
    /** bits per value used to pack the numeric values */
    public int bitsPerValue;

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
  static class BinaryEntry {
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
    /** offset to the reverse index */
    public long reverseIndexOffset;
    /** packed ints version used to encode addressing information */
    public int packedIntsVersion;
    /** packed ints blocksize */
    public int blockSize;
  }

  /** metadata entry for a sorted-set docvalues field */
  static class SortedSetEntry {
    private SortedSetEntry() {}
    int format;
  }

  // internally we compose complex dv (sorted/sortedset) from other ones
  static abstract class LongBinaryDocValues extends BinaryDocValues {
    @Override
    public final BytesRef get(int docID) {
      return get((long)docID);
    }
    
    abstract BytesRef get(long id);
  }
  
  // used for reverse lookup to a small range of blocks
  static class ReverseTermsIndex {
    public MonotonicBlockPackedReader termAddresses;
    public PagedBytes.Reader terms;
  }
  
  //in the compressed case, we add a few additional operations for
  //more efficient reverse lookup and enumeration
  static final class CompressedBinaryDocValues extends LongBinaryDocValues {    
    final long numValues;
    final long numIndexValues;
    final int maxTermLength;
    final MonotonicBlockPackedReader addresses;
    final IndexInput data;
    final CompressedBinaryTermsEnum termsEnum;
    final PagedBytes.Reader reverseTerms;
    final MonotonicBlockPackedReader reverseAddresses;
    final long numReverseIndexValues;
    
    public CompressedBinaryDocValues(BinaryEntry bytes, MonotonicBlockPackedReader addresses, ReverseTermsIndex index, IndexInput data) throws IOException {
      this.maxTermLength = bytes.maxLength;
      this.numValues = bytes.count;
      this.addresses = addresses;
      this.numIndexValues = addresses.size();
      this.data = data;
      this.reverseTerms = index.terms;
      this.reverseAddresses = index.termAddresses;
      this.numReverseIndexValues = reverseAddresses.size();
      this.termsEnum = getTermsEnum(data);
    }
    
    @Override
    public BytesRef get(long id) {
      try {
        termsEnum.seekExact(id);
        return termsEnum.term();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    long lookupTerm(BytesRef key) {
      try {
        switch (termsEnum.seekCeil(key)) {
          case FOUND: return termsEnum.ord();
          case NOT_FOUND: return -termsEnum.ord()-1;
          default: return -numValues-1;
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
    
    private CompressedBinaryTermsEnum getTermsEnum(IndexInput input) throws IOException {
      return new CompressedBinaryTermsEnum(input);
    }
    
    class CompressedBinaryTermsEnum extends TermsEnum {
      private long currentOrd = -1;
      // offset to the start of the current block 
      private long currentBlockStart;
      private final IndexInput input;
      // delta from currentBlockStart to start of each term
      private final int offsets[] = new int[INTERVAL_COUNT];
      private final byte buffer[] = new byte[2*INTERVAL_COUNT-1];
      
      private final BytesRef term = new BytesRef(maxTermLength);
      private final BytesRef firstTerm = new BytesRef(maxTermLength);
      private final BytesRef scratch = new BytesRef();
      
      CompressedBinaryTermsEnum(IndexInput input) throws IOException {
        this.input = input;
        input.seek(0);
      }
      
      private void readHeader() throws IOException {
        firstTerm.length = input.readVInt();
        input.readBytes(firstTerm.bytes, 0, firstTerm.length);
        input.readBytes(buffer, 0, INTERVAL_COUNT-1);
        if (buffer[0] == -1) {
          readShortAddresses();
        } else {
          readByteAddresses();
        }
        currentBlockStart = input.getFilePointer();
      }
      
      // read single byte addresses: each is delta - 2
      // (shared prefix byte and length > 0 are both implicit)
      private void readByteAddresses() throws IOException {
        int addr = 0;
        for (int i = 1; i < offsets.length; i++) {
          addr += 2 + (buffer[i-1] & 0xFF);
          offsets[i] = addr;
        }
      }
      
      // read double byte addresses: each is delta - 2
      // (shared prefix byte and length > 0 are both implicit)
      private void readShortAddresses() throws IOException {
        input.readBytes(buffer, INTERVAL_COUNT-1, INTERVAL_COUNT);
        int addr = 0;
        for (int i = 1; i < offsets.length; i++) {
          int x = i<<1;
          addr += 2 + ((buffer[x-1] << 8) | (buffer[x] & 0xFF));
          offsets[i] = addr;
        }
      }
      
      // set term to the first term
      private void readFirstTerm() throws IOException {
        term.length = firstTerm.length;
        System.arraycopy(firstTerm.bytes, firstTerm.offset, term.bytes, 0, term.length);
      }
      
      // read term at offset, delta encoded from first term
      private void readTerm(int offset) throws IOException {
        int start = input.readByte() & 0xFF;
        System.arraycopy(firstTerm.bytes, firstTerm.offset, term.bytes, 0, start);
        int suffix = offsets[offset] - offsets[offset-1] - 1;
        input.readBytes(term.bytes, start, suffix);
        term.length = start + suffix;
      }
      
      @Override
      public BytesRef next() throws IOException {
        currentOrd++;
        if (currentOrd >= numValues) {
          return null;
        } else { 
          int offset = (int) (currentOrd & INTERVAL_MASK);
          if (offset == 0) {
            // switch to next block
            readHeader();
            readFirstTerm();
          } else {
            readTerm(offset);
          }
          return term;
        }
      }
      
      // binary search reverse index to find smaller 
      // range of blocks to search
      long binarySearchIndex(BytesRef text) throws IOException {
        long low = 0;
        long high = numReverseIndexValues - 1;
        while (low <= high) {
          long mid = (low + high) >>> 1;
          reverseTerms.fill(scratch, reverseAddresses.get(mid));
          int cmp = scratch.compareTo(text);
          
          if (cmp < 0) {
            low = mid + 1;
          } else if (cmp > 0) {
            high = mid - 1;
          } else {
            return mid;
          }
        }
        return high;
      }
      
      // binary search against first term in block range 
      // to find term's block
      long binarySearchBlock(BytesRef text, long low, long high) throws IOException {       
        while (low <= high) {
          long mid = (low + high) >>> 1;
          input.seek(addresses.get(mid));
          term.length = input.readVInt();
          input.readBytes(term.bytes, 0, term.length);
          int cmp = term.compareTo(text);
          
          if (cmp < 0) {
            low = mid + 1;
          } else if (cmp > 0) {
            high = mid - 1;
          } else {
            return mid;
          }
        }
        return high;
      }
      
      @Override
      public SeekStatus seekCeil(BytesRef text) throws IOException {
        // locate block: narrow to block range with index, then search blocks
        final long block;
        long indexPos = binarySearchIndex(text);
        if (indexPos < 0) {
          block = 0;
        } else {
          long low = indexPos << BLOCK_INTERVAL_SHIFT;
          long high = Math.min(numIndexValues - 1, low + BLOCK_INTERVAL_MASK);
          block = Math.max(low, binarySearchBlock(text, low, high));
        }
        
        // position before block, then scan to term.
        input.seek(addresses.get(block));
        currentOrd = (block << INTERVAL_SHIFT) - 1;
        
        while (next() != null) {
          int cmp = term.compareTo(text);
          if (cmp == 0) {
            return SeekStatus.FOUND;
          } else if (cmp > 0) {
            return SeekStatus.NOT_FOUND;
          }
        }
        return SeekStatus.END;
      }
      
      @Override
      public void seekExact(long ord) throws IOException {
        long block = ord >>> INTERVAL_SHIFT;
        if (block != currentOrd >>> INTERVAL_SHIFT) {
          // switch to different block
          input.seek(addresses.get(block));
          readHeader();
        }
        
        currentOrd = ord;
        
        int offset = (int) (ord & INTERVAL_MASK);
        if (offset == 0) {
          readFirstTerm();
        } else {
          input.seek(currentBlockStart + offsets[offset-1]);
          readTerm(offset);
        }
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

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }
    }
  }
}

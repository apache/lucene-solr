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
package org.apache.lucene.codecs.lucene80;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;

/** reader for {@link Lucene80DocValuesFormat} */
final class Lucene80DocValuesProducer extends DocValuesProducer implements Closeable {
  private final Map<String,NumericEntry> numerics = new HashMap<>();
  private final Map<String,BinaryEntry> binaries = new HashMap<>();
  private final Map<String,SortedEntry> sorted = new HashMap<>();
  private final Map<String,SortedSetEntry> sortedSets = new HashMap<>();
  private final Map<String,SortedNumericEntry> sortedNumerics = new HashMap<>();
  private long ramBytesUsed;
  private final IndexInput data;
  private final int maxDoc;
  private int version = -1;

  /** expert: instantiates a new reader */
  Lucene80DocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    this.maxDoc = state.segmentInfo.maxDoc();
    ramBytesUsed = RamUsageEstimator.shallowSizeOfInstance(getClass());

    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;
      
      try {
        version =
            CodecUtil.checkIndexHeader(
                in,
                metaCodec,
                Lucene80DocValuesFormat.VERSION_START,
                Lucene80DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);

        readFields(state.segmentInfo.name, in, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    boolean success = false;
    try {
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec,
                                                 Lucene80DocValuesFormat.VERSION_START,
                                                 Lucene80DocValuesFormat.VERSION_CURRENT,
                                                 state.segmentInfo.getId(),
                                                 state.segmentSuffix);
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

  private void readFields(String segmentName, ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      byte type = meta.readByte();
      if (type == Lucene80DocValuesFormat.NUMERIC) {
        numerics.put(info.name, readNumeric(meta));
      } else if (type == Lucene80DocValuesFormat.BINARY) {
        final boolean compressed;
        if (version >= Lucene80DocValuesFormat.VERSION_CONFIGURABLE_COMPRESSION) {
          String value = info.getAttribute(Lucene80DocValuesFormat.MODE_KEY);
          if (value == null) {
            throw new IllegalStateException(
                "missing value for "
                    + Lucene80DocValuesFormat.MODE_KEY
                    + " for field: "
                    + info.name
                    + " in segment: "
                    + segmentName);
          }
          Lucene80DocValuesFormat.Mode mode = Lucene80DocValuesFormat.Mode.valueOf(value);
          compressed = mode == Lucene80DocValuesFormat.Mode.BEST_COMPRESSION;
        } else {
          compressed = version >= Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED;
        }
        binaries.put(info.name, readBinary(meta, compressed));
      } else if (type == Lucene80DocValuesFormat.SORTED) {
        sorted.put(info.name, readSorted(meta));
      } else if (type == Lucene80DocValuesFormat.SORTED_SET) {
        sortedSets.put(info.name, readSortedSet(meta));
      } else if (type == Lucene80DocValuesFormat.SORTED_NUMERIC) {
        sortedNumerics.put(info.name, readSortedNumeric(meta));
      } else {
        throw new CorruptIndexException("invalid type: " + type, meta);
      }
    }
  }

  private NumericEntry readNumeric(ChecksumIndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    readNumeric(meta, entry);
    return entry;
  }

  private void readNumeric(ChecksumIndexInput meta, NumericEntry entry) throws IOException {
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numValues = meta.readLong();
    int tableSize = meta.readInt();
    if (tableSize > 256) {
      throw new CorruptIndexException("invalid table size: " + tableSize, meta);
    }
    if (tableSize >= 0) {
      entry.table = new long[tableSize];
      ramBytesUsed += RamUsageEstimator.sizeOf(entry.table);
      for (int i = 0; i < tableSize; ++i) {
        entry.table[i] = meta.readLong();
      }
    }
    if (tableSize < -1) {
      entry.blockShift = -2 - tableSize;
    } else {
      entry.blockShift = -1;
    }
    entry.bitsPerValue = meta.readByte();
    entry.minValue = meta.readLong();
    entry.gcd = meta.readLong();
    entry.valuesOffset = meta.readLong();
    entry.valuesLength = meta.readLong();
    entry.valueJumpTableOffset = meta.readLong();
  }

  private BinaryEntry readBinary(IndexInput meta, boolean compressed) throws IOException {
    final BinaryEntry entry = new BinaryEntry();
    entry.compressed = compressed;
    entry.dataOffset = meta.readLong();
    entry.dataLength = meta.readLong();
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numDocsWithField = meta.readInt();
    entry.minLength = meta.readInt();
    entry.maxLength = meta.readInt();
    if ((entry.compressed && entry.numDocsWithField > 0) ||  entry.minLength < entry.maxLength) {
      entry.addressesOffset = meta.readLong();

      // Old count of uncompressed addresses 
      long numAddresses = entry.numDocsWithField + 1L;
      // New count of compressed addresses - the number of compresseed blocks
      if (entry.compressed) {
        entry.numCompressedChunks = meta.readVInt();
        entry.docsPerChunkShift = meta.readVInt();
        entry.maxUncompressedChunkSize = meta.readVInt();
        numAddresses = entry.numCompressedChunks;
      }      

      final int blockShift = meta.readVInt();
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);
      ramBytesUsed += entry.addressesMeta.ramBytesUsed();
      entry.addressesLength = meta.readLong();
    }
    return entry;
  }

  private SortedEntry readSorted(ChecksumIndexInput meta) throws IOException {
    SortedEntry entry = new SortedEntry();
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numDocsWithField = meta.readInt();
    entry.bitsPerValue = meta.readByte();
    entry.ordsOffset = meta.readLong();
    entry.ordsLength = meta.readLong();
    readTermDict(meta, entry);
    return entry;
  }

  private SortedSetEntry readSortedSet(ChecksumIndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    byte multiValued = meta.readByte();
    switch (multiValued) {
      case 0: // singlevalued
        entry.singleValueEntry = readSorted(meta);
        return entry;
      case 1: // multivalued
        break;
      default:
        throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
    }
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.bitsPerValue = meta.readByte();
    entry.ordsOffset = meta.readLong();
    entry.ordsLength = meta.readLong();
    entry.numDocsWithField = meta.readInt();
    entry.addressesOffset = meta.readLong();
    final int blockShift = meta.readVInt();
    entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
    ramBytesUsed += entry.addressesMeta.ramBytesUsed();
    entry.addressesLength = meta.readLong();
    readTermDict(meta, entry);
    return entry;
  }

  private static void readTermDict(ChecksumIndexInput meta, TermsDictEntry entry) throws IOException {
    entry.termsDictSize = meta.readVLong();
    int termsDictBlockCode = meta.readInt();
    if (Lucene80DocValuesFormat.TERMS_DICT_BLOCK_LZ4_CODE == termsDictBlockCode) {
      // This is a LZ4 compressed block.
      entry.compressed = true;
      entry.termsDictBlockShift = Lucene80DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;
    } else {
      entry.termsDictBlockShift = termsDictBlockCode;
    }

    final int blockShift = meta.readInt();
    final long addressesSize = (entry.termsDictSize + (1L << entry.termsDictBlockShift) - 1) >>> entry.termsDictBlockShift;
    entry.termsAddressesMeta = DirectMonotonicReader.loadMeta(meta, addressesSize, blockShift);
    entry.maxTermLength = meta.readInt();
    // Read one more int for compressed term dict.
    if (entry.compressed) {
      entry.maxBlockLength = meta.readInt();
    }
    entry.termsDataOffset = meta.readLong();
    entry.termsDataLength = meta.readLong();
    entry.termsAddressesOffset = meta.readLong();
    entry.termsAddressesLength = meta.readLong();
    entry.termsDictIndexShift = meta.readInt();
    final long indexSize = (entry.termsDictSize + (1L << entry.termsDictIndexShift) - 1) >>> entry.termsDictIndexShift;
    entry.termsIndexAddressesMeta = DirectMonotonicReader.loadMeta(meta, 1 + indexSize, blockShift);
    entry.termsIndexOffset = meta.readLong();
    entry.termsIndexLength = meta.readLong();
    entry.termsIndexAddressesOffset = meta.readLong();
    entry.termsIndexAddressesLength = meta.readLong();
  }

  private SortedNumericEntry readSortedNumeric(ChecksumIndexInput meta) throws IOException {
    SortedNumericEntry entry = new SortedNumericEntry();
    readNumeric(meta, entry);
    entry.numDocsWithField = meta.readInt();
    if (entry.numDocsWithField != entry.numValues) {
      entry.addressesOffset = meta.readLong();
      final int blockShift = meta.readVInt();
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
      ramBytesUsed += entry.addressesMeta.ramBytesUsed();
      entry.addressesLength = meta.readLong();
    }
    return entry;
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  private static class NumericEntry {
    long[] table;
    int blockShift;
    byte bitsPerValue;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    long numValues;
    long minValue;
    long gcd;
    long valuesOffset;
    long valuesLength;
    long valueJumpTableOffset; // -1 if no jump-table
  }

  private static class BinaryEntry {
    boolean compressed;
    long dataOffset;
    long dataLength;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    int numDocsWithField;
    int minLength;
    int maxLength;
    long addressesOffset;
    long addressesLength;
    DirectMonotonicReader.Meta addressesMeta;
    int numCompressedChunks;
    int docsPerChunkShift;
    int maxUncompressedChunkSize;
  }

  private static class TermsDictEntry {
    long termsDictSize;
    int termsDictBlockShift;
    DirectMonotonicReader.Meta termsAddressesMeta;
    int maxTermLength;
    long termsDataOffset;
    long termsDataLength;
    long termsAddressesOffset;
    long termsAddressesLength;
    int termsDictIndexShift;
    DirectMonotonicReader.Meta termsIndexAddressesMeta;
    long termsIndexOffset;
    long termsIndexLength;
    long termsIndexAddressesOffset;
    long termsIndexAddressesLength;

    boolean compressed;
    int maxBlockLength;
  }

  private static class SortedEntry extends TermsDictEntry {
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    int numDocsWithField;
    byte bitsPerValue;
    long ordsOffset;
    long ordsLength;
  }

  private static class SortedSetEntry extends TermsDictEntry {
    SortedEntry singleValueEntry;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    int numDocsWithField;
    byte bitsPerValue;
    long ordsOffset;
    long ordsLength;
    DirectMonotonicReader.Meta addressesMeta;
    long addressesOffset;
    long addressesLength;
  }

  private static class SortedNumericEntry extends NumericEntry {
    int numDocsWithField;
    DirectMonotonicReader.Meta addressesMeta;
    long addressesOffset;
    long addressesLength;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericEntry entry = numerics.get(field.name);
    return getNumeric(entry);
  }

  private static abstract class DenseNumericDocValues extends NumericDocValues {

    final int maxDoc;
    int doc = -1;

    DenseNumericDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) {
      doc = target;
      return true;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

  }

  private static abstract class SparseNumericDocValues extends NumericDocValues {

    final IndexedDISI disi;

    SparseNumericDocValues(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public long cost() {
      return disi.cost();
    }
  }

  private NumericDocValues getNumeric(NumericEntry entry) throws IOException {
    if (entry.docsWithFieldOffset == -2) {
      // empty
      return DocValues.emptyNumeric();
    } else if (entry.docsWithFieldOffset == -1) {
      // dense
      if (entry.bitsPerValue == 0) {
        return new DenseNumericDocValues(maxDoc) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }
        };
      } else {
        final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
        if (entry.blockShift >= 0) {
          // dense but split into blocks of different bits per value
          return new DenseNumericDocValues(maxDoc) {
            final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

            @Override
            public long longValue() throws IOException {
              return vBPVReader.getLongValue(doc);
            }
          };
        } else {
          final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new DenseNumericDocValues(maxDoc) {
              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(doc)];
              }
            };
          } else {
            final long mul = entry.gcd;
            final long delta = entry.minValue;
            return new DenseNumericDocValues(maxDoc) {
              @Override
              public long longValue() throws IOException {
                return mul * values.get(doc) + delta;
              }
            };
          }
        }
      }
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numValues);
      if (entry.bitsPerValue == 0) {
        return new SparseNumericDocValues(disi) {
          @Override
          public long longValue() throws IOException {
            return entry.minValue;
          }
        };
      } else {
        final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
        if (entry.blockShift >= 0) {
          // sparse and split into blocks of different bits per value
          return new SparseNumericDocValues(disi) {
            final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

            @Override
            public long longValue() throws IOException {
              final int index = disi.index();
              return vBPVReader.getLongValue(index);
            }
          };
        } else {
          final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
          if (entry.table != null) {
            final long[] table = entry.table;
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return table[(int) values.get(disi.index())];
              }
            };
          } else {
            final long mul = entry.gcd;
            final long delta = entry.minValue;
            return new SparseNumericDocValues(disi) {
              @Override
              public long longValue() throws IOException {
                return mul * values.get(disi.index()) + delta;
              }
            };
          }
        }
      }
    }
  }

  private LongValues getNumericValues(NumericEntry entry) throws IOException {
    if (entry.bitsPerValue == 0) {
      return new LongValues() {
        @Override
        public long get(long index) {
          return entry.minValue;
        }
      };
    } else {
      final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
      if (entry.blockShift >= 0) {
        return new LongValues() {
          final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);
          @Override
          public long get(long index) {
            try {
              return vBPVReader.getLongValue(index);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
      } else {
        final LongValues values = DirectReader.getInstance(slice, entry.bitsPerValue);
        if (entry.table != null) {
          final long[] table = entry.table;
          return new LongValues() {
            @Override
            public long get(long index) {
              return table[(int) values.get(index)];
            }
          };
        } else if (entry.gcd != 1) {
          final long gcd = entry.gcd;
          final long minValue = entry.minValue;
          return new LongValues() {
            @Override
            public long get(long index) {
              return values.get(index) * gcd + minValue;
            }
          };
        } else if (entry.minValue != 0) {
          final long minValue = entry.minValue;
          return new LongValues() {
            @Override
            public long get(long index) {
              return values.get(index) + minValue;
            }
          };
        } else {
          return values;
        }
      }
    }
  }

  private static abstract class DenseBinaryDocValues extends BinaryDocValues {

    final int maxDoc;
    int doc = -1;

    DenseBinaryDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      doc = target;
      return true;
    }
  }

  private static abstract class SparseBinaryDocValues extends BinaryDocValues {

    final IndexedDISI disi;

    SparseBinaryDocValues(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public long cost() {
      return disi.cost();
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }
  }
  
  private BinaryDocValues getUncompressedBinary(BinaryEntry entry) throws IOException {
    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptyBinary();
    }

    final IndexInput bytesSlice = data.slice("fixed-binary", entry.dataOffset, entry.dataLength);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      if (entry.minLength == entry.maxLength) {
        // fixed length
        final int length = entry.maxLength;
        return new DenseBinaryDocValues(maxDoc) {
          final BytesRef bytes = new BytesRef(new byte[length], 0, length);

          @Override
          public BytesRef binaryValue() throws IOException {
            bytesSlice.seek((long) doc * length);
            bytesSlice.readBytes(bytes.bytes, 0, length);
            return bytes;
          }
        };
      } else {
        // variable length
        final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
        return new DenseBinaryDocValues(maxDoc) {
          final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

          @Override
          public BytesRef binaryValue() throws IOException {
            long startOffset = addresses.get(doc);
            bytes.length = (int) (addresses.get(doc + 1L) - startOffset);
            bytesSlice.seek(startOffset);
            bytesSlice.readBytes(bytes.bytes, 0, bytes.length);
            return bytes;
          }
        };
      }
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      if (entry.minLength == entry.maxLength) {
        // fixed length
        final int length = entry.maxLength;
        return new SparseBinaryDocValues(disi) {
          final BytesRef bytes = new BytesRef(new byte[length], 0, length);

          @Override
          public BytesRef binaryValue() throws IOException {
            bytesSlice.seek((long) disi.index() * length);
            bytesSlice.readBytes(bytes.bytes, 0, length);
            return bytes;
          }
        };
      } else {
        // variable length
        final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
        return new SparseBinaryDocValues(disi) {
          final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

          @Override
          public BytesRef binaryValue() throws IOException {
            final int index = disi.index();
            long startOffset = addresses.get(index);
            bytes.length = (int) (addresses.get(index + 1L) - startOffset);
            bytesSlice.seek(startOffset);
            bytesSlice.readBytes(bytes.bytes, 0, bytes.length);
            return bytes;
          }
        };
      }
    }
  }  
  
  // Decompresses blocks of binary values to retrieve content
  class BinaryDecoder {
    
    private final LongValues addresses;
    private final IndexInput compressedData;
    // Cache of last uncompressed block 
    private long lastBlockId = -1;
    private final int []uncompressedDocStarts;
    private int uncompressedBlockLength = 0;        
    private final byte[] uncompressedBlock;
    private final BytesRef uncompressedBytesRef;
    private final int docsPerChunk;
    private final int docsPerChunkShift;
    
    public BinaryDecoder(LongValues addresses, IndexInput compressedData, int biggestUncompressedBlockSize, int docsPerChunkShift) {
      super();
      this.addresses = addresses;
      this.compressedData = compressedData;
      // pre-allocate a byte array large enough for the biggest uncompressed block needed.
      this.uncompressedBlock = new byte[biggestUncompressedBlockSize];
      uncompressedBytesRef = new BytesRef(uncompressedBlock);
      this.docsPerChunk = 1 << docsPerChunkShift;
      this.docsPerChunkShift = docsPerChunkShift;
      uncompressedDocStarts = new int[docsPerChunk + 1];
      
    }


    BytesRef decode(int docNumber) throws IOException {
      int blockId = docNumber >> docsPerChunkShift; 
      int docInBlockId = docNumber % docsPerChunk;
      assert docInBlockId < docsPerChunk;
      
      
      // already read and uncompressed?
      if (blockId != lastBlockId) {
        lastBlockId = blockId;
        long blockStartOffset = addresses.get(blockId);
        compressedData.seek(blockStartOffset);
        
        uncompressedBlockLength = 0;        

        int onlyLength = -1;
        for (int i = 0; i < docsPerChunk; i++) {
          if (i == 0) {
            // The first length value is special. It is shifted and has a bit to denote if
            // all other values are the same length
            int lengthPlusSameInd = compressedData.readVInt();
            int sameIndicator = lengthPlusSameInd & 1;
            int firstValLength = lengthPlusSameInd >>>1;
            if (sameIndicator == 1) {
              onlyLength = firstValLength;
            }
            uncompressedBlockLength += firstValLength;            
          } else {
            if (onlyLength == -1) {
              // Various lengths are stored - read each from disk
              uncompressedBlockLength += compressedData.readVInt();            
            } else {
              // Only one length 
              uncompressedBlockLength += onlyLength;
            }
          }
          uncompressedDocStarts[i+1] = uncompressedBlockLength;
        }
        
        if (uncompressedBlockLength == 0) {
          uncompressedBytesRef.offset = 0;
          uncompressedBytesRef.length = 0;
          return uncompressedBytesRef;
        }
        
        assert uncompressedBlockLength <= uncompressedBlock.length;
        LZ4.decompress(compressedData, uncompressedBlockLength, uncompressedBlock, 0);
      }
      
      uncompressedBytesRef.offset = uncompressedDocStarts[docInBlockId];        
      uncompressedBytesRef.length = uncompressedDocStarts[docInBlockId +1] - uncompressedBytesRef.offset;
      return uncompressedBytesRef;
    }    
  }
  

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryEntry entry = binaries.get(field.name);
    if (entry.compressed) {
      return getCompressedBinary(entry);
    } else {
      return getUncompressedBinary(entry);
    }
  }

  private BinaryDocValues getCompressedBinary(BinaryEntry entry) throws IOException { 
    
    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptyBinary();
    }
    if (entry.docsWithFieldOffset == -1) {
      // dense
      final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
      final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
      return new DenseBinaryDocValues(maxDoc) {
        BinaryDecoder decoder = new BinaryDecoder(addresses, data.clone(), entry.maxUncompressedChunkSize, entry.docsPerChunkShift);

        @Override
        public BytesRef binaryValue() throws IOException {          
          return decoder.decode(doc);
        }
      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
      final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);
      return new SparseBinaryDocValues(disi) {
        BinaryDecoder decoder = new BinaryDecoder(addresses, data.clone(), entry.maxUncompressedChunkSize, entry.docsPerChunkShift);

        @Override
        public BytesRef binaryValue() throws IOException {
          return decoder.decode(disi.index());
        }
      };
    }
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedEntry entry = sorted.get(field.name);
    return getSorted(entry);
  }

  private SortedDocValues getSorted(SortedEntry entry) throws IOException {
    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptySorted();
    }

    final LongValues ords;
    if (entry.bitsPerValue == 0) {
      ords = new LongValues() {
        @Override
        public long get(long index) {
          return 0L;
        }
      };
    } else {
      final RandomAccessInput slice = data.randomAccessSlice(entry.ordsOffset, entry.ordsLength);
      ords = DirectReader.getInstance(slice, entry.bitsPerValue);
    }

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new BaseSortedDocValues(entry, data) {

        int doc = -1;

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) {
          doc = target;
          return true;
        }

        @Override
        public int ordValue() {
          return (int) ords.get(doc);
        }
      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new BaseSortedDocValues(entry, data) {

        @Override
        public int nextDoc() throws IOException {
          return disi.nextDoc();
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return disi.advanceExact(target);
        }

        @Override
        public int ordValue() {
          return (int) ords.get(disi.index());
        }
      };
    }
  }

  private static abstract class BaseSortedDocValues extends SortedDocValues {

    final SortedEntry entry;
    final IndexInput data;
    final TermsEnum termsEnum;

    BaseSortedDocValues(SortedEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      this.data = data;
      this.termsEnum = termsEnum();
    }

    @Override
    public int getValueCount() {
      return Math.toIntExact(entry.termsDictSize);
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public int lookupTerm(BytesRef key) throws IOException {
      SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return Math.toIntExact(termsEnum.ord());
        default:
          return Math.toIntExact(-1L - termsEnum.ord());
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry, data);
    }
  }

  private static abstract class BaseSortedSetDocValues extends SortedSetDocValues {

    final SortedSetEntry entry;
    final IndexInput data;
    final TermsEnum termsEnum;

    BaseSortedSetDocValues(SortedSetEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      this.data = data;
      this.termsEnum = termsEnum();
    }

    @Override
    public long getValueCount() {
      return entry.termsDictSize;
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public long lookupTerm(BytesRef key) throws IOException {
      SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return termsEnum.ord();
        default:
          return -1L - termsEnum.ord();
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry, data);
    }
  }

  private static class TermsDict extends BaseTermsEnum {
    static final int LZ4_DECOMPRESSOR_PADDING = 7;

    final TermsDictEntry entry;
    final LongValues blockAddresses;
    final IndexInput bytes;
    final long blockMask;
    final LongValues indexAddresses;
    final IndexInput indexBytes;
    final BytesRef term;
    long ord = -1;

    BytesRef blockBuffer = null;
    ByteArrayDataInput blockInput = null;
    long currentCompressedBlockStart = -1;
    long currentCompressedBlockEnd = -1;

    TermsDict(TermsDictEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      RandomAccessInput addressesSlice = data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);
      blockAddresses = DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addressesSlice);
      bytes = data.slice("terms", entry.termsDataOffset, entry.termsDataLength);
      blockMask = (1L << entry.termsDictBlockShift) - 1;
      RandomAccessInput indexAddressesSlice = data.randomAccessSlice(entry.termsIndexAddressesOffset, entry.termsIndexAddressesLength);
      indexAddresses = DirectMonotonicReader.getInstance(entry.termsIndexAddressesMeta, indexAddressesSlice);
      indexBytes = data.slice("terms-index", entry.termsIndexOffset, entry.termsIndexLength);
      term = new BytesRef(entry.maxTermLength);

      if (entry.compressed) {
        // add 7 padding bytes can help decompression run faster.
        int bufferSize = entry.maxBlockLength + LZ4_DECOMPRESSOR_PADDING;
        blockBuffer = new BytesRef(new byte[bufferSize], 0, bufferSize);
      }
    }

    @Override
    public BytesRef next() throws IOException {
      if (++ord >= entry.termsDictSize) {
        return null;
      }

      if ((ord & blockMask) == 0L) {
        if (this.entry.compressed) {
          decompressBlock();
        } else {
          term.length = bytes.readVInt();
          bytes.readBytes(term.bytes, 0, term.length);
        }
      } else {
        DataInput input = this.entry.compressed ? blockInput : bytes;
        final int token = Byte.toUnsignedInt(input.readByte());
        int prefixLength = token & 0x0F;
        int suffixLength = 1 + (token >>> 4);
        if (prefixLength == 15) {
          prefixLength += input.readVInt();
        }
        if (suffixLength == 16) {
          suffixLength += input.readVInt();
        }
        term.length = prefixLength + suffixLength;
        input.readBytes(term.bytes, prefixLength, suffixLength);
      }
      return term;
    }

    @Override
    public void seekExact(long ord) throws IOException {
      if (ord < 0 || ord >= entry.termsDictSize) {
        throw new IndexOutOfBoundsException();
      }
      final long blockIndex = ord >>> entry.termsDictBlockShift;
      final long blockAddress = blockAddresses.get(blockIndex);
      bytes.seek(blockAddress);
      this.ord = (blockIndex << entry.termsDictBlockShift) - 1;
      do {
        next();
      } while (this.ord < ord);
    }

    private BytesRef getTermFromIndex(long index) throws IOException {
      assert index >= 0 && index <= (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      final long start = indexAddresses.get(index);
      term.length = (int) (indexAddresses.get(index + 1) - start);
      indexBytes.seek(start);
      indexBytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekTermsIndex(BytesRef text) throws IOException {
      long lo = 0L;
      long hi = (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      while (lo <= hi) {
        final long mid = (lo + hi) >>> 1;
        getTermFromIndex(mid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }

      assert hi < 0 || getTermFromIndex(hi).compareTo(text) <= 0;
      assert hi == ((entry.termsDictSize - 1) >>> entry.termsDictIndexShift) || getTermFromIndex(hi + 1).compareTo(text) > 0;

      return hi;
    }

    private BytesRef getFirstTermFromBlock(long block) throws IOException {
      assert block >= 0 && block <= (entry.termsDictSize - 1) >>> entry.termsDictBlockShift;
      final long blockAddress = blockAddresses.get(block);
      bytes.seek(blockAddress);
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekBlock(BytesRef text) throws IOException {
      long index = seekTermsIndex(text);
      if (index == -1L) {
        return -1L;
      }

      long ordLo = index << entry.termsDictIndexShift;
      long ordHi = Math.min(entry.termsDictSize, ordLo + (1L << entry.termsDictIndexShift)) - 1L;

      long blockLo = ordLo >>> entry.termsDictBlockShift;
      long blockHi = ordHi >>> entry.termsDictBlockShift;

      while (blockLo <= blockHi) {
        final long blockMid = (blockLo + blockHi) >>> 1;
        getFirstTermFromBlock(blockMid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          blockLo = blockMid + 1;
        } else {
          blockHi = blockMid - 1;
        }
      }

      assert blockHi < 0 || getFirstTermFromBlock(blockHi).compareTo(text) <= 0;
      assert blockHi == ((entry.termsDictSize - 1) >>> entry.termsDictBlockShift) || getFirstTermFromBlock(blockHi + 1).compareTo(text) > 0;

      return blockHi;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      final long block = seekBlock(text);
      if (block == -1) {
        // before the first term
        seekExact(0L);
        return SeekStatus.NOT_FOUND;
      }
      final long blockAddress = blockAddresses.get(block);
      this.ord = block << entry.termsDictBlockShift;
      bytes.seek(blockAddress);
      if (this.entry.compressed) {
        decompressBlock();
      } else {
        term.length = bytes.readVInt();
        bytes.readBytes(term.bytes, 0, term.length);
      }

      while (true) {
        int cmp = term.compareTo(text);
        if (cmp == 0) {
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          return SeekStatus.NOT_FOUND;
        }
        if (next() == null) {
          return SeekStatus.END;
        }
      }
    }

    private void decompressBlock() throws IOException {
      // The first term is kept uncompressed, so no need to decompress block if only
      // look up the first term when doing seek block.
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      long offset = bytes.getFilePointer();
      if (offset < entry.termsDataLength - 1) {
        // Avoid decompress again if we are reading a same block.
        if (currentCompressedBlockStart != offset) {
          int decompressLength = bytes.readVInt();
          // Decompress the remaining of current block
          LZ4.decompress(bytes, decompressLength, blockBuffer.bytes, 0);
          currentCompressedBlockStart = offset;
          currentCompressedBlockEnd = bytes.getFilePointer();
        } else {
          // Skip decompression but need to re-seek to block end.
          bytes.seek(currentCompressedBlockEnd);
        }

        // Reset the buffer.
        blockInput = new ByteArrayDataInput(blockBuffer.bytes, 0, blockBuffer.length);
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public long ord() throws IOException {
      return ord;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return -1L;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedNumericEntry entry = sortedNumerics.get(field.name);
    if (entry.numValues == entry.numDocsWithField) {
      return DocValues.singleton(getNumeric(entry));
    }

    final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
    final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput);

    final LongValues values = getNumericValues(entry);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new SortedNumericDocValues() {

        int doc = -1;
        long start, end;
        int count;

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          doc = target;
          return true;
        }

        @Override
        public long nextValue() throws IOException {
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          return count;
        }
      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new SortedNumericDocValues() {

        boolean set;
        long start, end;
        int count;

        @Override
        public int nextDoc() throws IOException {
          set = false;
          return disi.nextDoc();
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          set = false;
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          set = false;
          return disi.advanceExact(target);
        }

        @Override
        public long nextValue() throws IOException {
          set();
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          set();
          return count;
        }

        private void set() {
          if (set == false) {
            final int index = disi.index();
            start = addresses.get(index);
            end = addresses.get(index + 1L);
            count = (int) (end - start);
            set = true;
          }
        }

      };
    }
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetEntry entry = sortedSets.get(field.name);
    if (entry.singleValueEntry != null) {
      return DocValues.singleton(getSorted(entry.singleValueEntry));
    }

    final RandomAccessInput slice = data.randomAccessSlice(entry.ordsOffset, entry.ordsLength);
    final LongValues ords = DirectReader.getInstance(slice, entry.bitsPerValue);

    final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
    final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new BaseSortedSetDocValues(entry, data) {

        int doc = -1;
        long start;
        long end;

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          doc = target;
          return true;
        }

        @Override
        public long nextOrd() throws IOException {
          if (start == end) {
            return NO_MORE_ORDS;
          }
          return ords.get(start++);
        }

      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new BaseSortedSetDocValues(entry, data) {

        boolean set;
        long start;
        long end = 0;

        @Override
        public int nextDoc() throws IOException {
          set = false;
          return disi.nextDoc();
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          set = false;
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          set = false;
          return disi.advanceExact(target);
        }

        @Override
        public long nextOrd() throws IOException {
          if (set == false) {
            final int index = disi.index();
            final long start = addresses.get(index);
            this.start = start + 1;
            end = addresses.get(index + 1L);
            set = true;
            return ords.get(start);
          } else if (start == end) {
            return NO_MORE_ORDS;
          } else {
            return ords.get(start++);
          }
        }

      };
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  /**
   * Reader for longs split into blocks of different bits per values.
   * The longs are requested by index and must be accessed in monotonically increasing order.
   */
  // Note: The order requirement could be removed as the jump-tables allow for backwards iteration
  // Note 2: The rankSlice is only used if an advance of > 1 block is called. Its construction could be lazy
  private class VaryingBPVReader {
    final RandomAccessInput slice; // 2 slices to avoid cache thrashing when using rank
    final RandomAccessInput rankSlice;
    final NumericEntry entry;
    final int shift;
    final long mul;
    final int mask;

    long block = -1;
    long delta;
    long offset;
    long blockEndOffset;
    LongValues values;

    VaryingBPVReader(NumericEntry entry, RandomAccessInput slice) throws IOException {
      this.entry = entry;
      this.slice = slice;
      this.rankSlice = entry.valueJumpTableOffset == -1 ? null :
          data.randomAccessSlice(entry.valueJumpTableOffset, data.length()-entry.valueJumpTableOffset);
      shift = entry.blockShift;
      mul = entry.gcd;
      mask = (1 << shift) - 1;
    }

    long getLongValue(long index) throws IOException {
      final long block = index >>> shift;
      if (this.block != block) {
        int bitsPerValue;
        do {
          // If the needed block is the one directly following the current block, it is cheaper to avoid the cache
          if (rankSlice != null && block != this.block+1) {
            blockEndOffset = rankSlice.readLong(block*Long.BYTES)-entry.valuesOffset;
            this.block = block-1;
          }
          offset = blockEndOffset;
          bitsPerValue = slice.readByte(offset++);
          delta = slice.readLong(offset);
          offset += Long.BYTES;
          if (bitsPerValue == 0) {
            blockEndOffset = offset;
          } else {
            final int length = slice.readInt(offset);
            offset += Integer.BYTES;
            blockEndOffset = offset + length;
          }
          this.block++;
        } while (this.block != block);
        values = bitsPerValue == 0 ? LongValues.ZEROES : DirectReader.getInstance(slice, bitsPerValue, offset);
      }
      return mul * values.get(index & mask) + delta;
    }
  }
}

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.composite.CompositeDocValuesProducer;
import org.apache.lucene.codecs.composite.CompositeFieldMetadata;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;

public class Lucene80BinaryProducer implements CompositeDocValuesProducer.BinaryProducer{
  private final int maxDoc;
  private final Map<String, BinaryEntry> binaries = new HashMap<>();

  public Lucene80BinaryProducer(int maxDoc) {
    this.maxDoc = maxDoc;
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException {
    BinaryEntry entry = getBinaryEntry(field, compositeFieldMetadata.getMetaStartFP(), indexInput);
    return getBinaryFromEntry(entry, indexInput, maxDoc);
  }

  private BinaryEntry getBinaryEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    BinaryEntry binaryEntry = binaries.get(field.name);
    if (binaryEntry != null) {
      return binaryEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    binaryEntry = readBinary(clone, Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED);

    binaries.put(field.name, binaryEntry);
    return binaryEntry;
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = 0L;
    for (Accountable accountable : binaries.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    return ramBytesUsed;
  }

  static BinaryEntry readBinary(IndexInput meta, int version) throws IOException {
    BinaryEntry entry = new BinaryEntry();
    entry.dataOffset = meta.readLong();
    entry.dataLength = meta.readLong();
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numDocsWithField = meta.readInt();
    entry.minLength = meta.readInt();
    entry.maxLength = meta.readInt();
    if ((version >= Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED && entry.numDocsWithField > 0) || entry.minLength < entry.maxLength) {
      entry.addressesOffset = meta.readLong();

      // Old count of uncompressed addresses
      long numAddresses = entry.numDocsWithField + 1L;
      // New count of compressed addresses - the number of compresseed blocks
      if (version >= Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED) {
        entry.numCompressedChunks = meta.readVInt();
        entry.docsPerChunkShift = meta.readVInt();
        entry.maxUncompressedChunkSize = meta.readVInt();
        numAddresses = entry.numCompressedChunks;
      }

      final int blockShift = meta.readVInt();
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);
      entry.addressesLength = meta.readLong();
    }
    return entry;
  }

  static class BinaryEntry implements Accountable {
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

    @Override
    public long ramBytesUsed() {
      return addressesMeta == null ? 0L : addressesMeta.ramBytesUsed();
    }
  }

  public BinaryDocValues getBinaryFromEntry(BinaryEntry entry, IndexInput data, int version) throws IOException {
    if (version < Lucene80DocValuesFormat.VERSION_BIN_COMPRESSED) {
      return getUncompressedBinary(entry, data);
    }

    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptyBinary();
    }
    if (entry.docsWithFieldOffset == -1) {
      // dense
      final RandomAccessInput addressesData = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
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
      final RandomAccessInput addressesData = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
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

  // BWC - old binary format
  private BinaryDocValues getUncompressedBinary(BinaryEntry entry, IndexInput data) throws IOException {
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
        final RandomAccessInput addressesData = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
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
        final RandomAccessInput addressesData = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
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
    private final int[] uncompressedDocStarts;
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
            int firstValLength = lengthPlusSameInd >>> 1;
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
          uncompressedDocStarts[i + 1] = uncompressedBlockLength;
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
      uncompressedBytesRef.length = uncompressedDocStarts[docInBlockId + 1] - uncompressedBytesRef.offset;
      return uncompressedBytesRef;
    }
  }
}
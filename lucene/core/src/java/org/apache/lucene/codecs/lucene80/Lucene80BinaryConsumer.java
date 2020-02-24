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
import java.util.Arrays;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.composite.CompositeDocValuesConsumer;
import org.apache.lucene.codecs.composite.CompositeFieldMetadata;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import static org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;

public class Lucene80BinaryConsumer implements CompositeDocValuesConsumer.BinaryConsumer{
  private final SegmentWriteState state;
  private final int maxDoc;

  public Lucene80BinaryConsumer(SegmentWriteState state) {
    this.state = state;
    this.maxDoc = state.segmentInfo.maxDoc();
  }

  @Override
  public CompositeFieldMetadata addBinary(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException {
    ByteBuffersDataOutput delegate = ByteBuffersDataOutput.newResettableInstance();
    ByteBuffersIndexOutput metadataRamBuffer = new ByteBuffersIndexOutput(delegate, "meta", "meta");
    addBinary(field, valuesProducer, indexOutput, metadataRamBuffer);
    long metaStartFP = indexOutput.getFilePointer();
    delegate.copyTo(indexOutput);
    return new CompositeFieldMetadata(field.number, DocValuesType.BINARY, metaStartFP);
  }

  public void addBinary(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput data, IndexOutput meta) throws IOException {
    try (CompressedBinaryBlockWriter blockWriter = new CompressedBinaryBlockWriter(data, meta)){
      BinaryDocValues values = valuesProducer.getBinary(field);
      long start = data.getFilePointer();
      meta.writeLong(start); // dataOffset
      int numDocsWithField = 0;
      int minLength = Integer.MAX_VALUE;
      int maxLength = 0;
      for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
        numDocsWithField++;
        BytesRef v = values.binaryValue();
        blockWriter.addDoc(doc, v);
        int length = v.length;
        minLength = Math.min(length, minLength);
        maxLength = Math.max(length, maxLength);
      }
      blockWriter.flushData();

      assert numDocsWithField <= maxDoc;
      meta.writeLong(data.getFilePointer() - start); // dataLength

      if (numDocsWithField == 0) {
        meta.writeLong(-2); // docsWithFieldOffset
        meta.writeLong(0L); // docsWithFieldLength
        meta.writeShort((short) -1); // jumpTableEntryCount
        meta.writeByte((byte) -1);   // denseRankPower
      } else if (numDocsWithField == maxDoc) {
        meta.writeLong(-1); // docsWithFieldOffset
        meta.writeLong(0L); // docsWithFieldLength
        meta.writeShort((short) -1); // jumpTableEntryCount
        meta.writeByte((byte) -1);   // denseRankPower
      } else {
        long offset = data.getFilePointer();
        meta.writeLong(offset); // docsWithFieldOffset
        values = valuesProducer.getBinary(field);
        final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
        meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
        meta.writeShort(jumpTableEntryCount);
        meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      }

      meta.writeInt(numDocsWithField);
      meta.writeInt(minLength);
      meta.writeInt(maxLength);

      blockWriter.writeMetaData();

    }
  }

  class CompressedBinaryBlockWriter implements Closeable {
    final LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
    private final IndexOutput data;
    private final IndexOutput meta;
    int uncompressedBlockLength = 0;
    int maxUncompressedBlockLength = 0;
    int numDocsInCurrentBlock = 0;
    final int[] docLengths = new int[Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK];
    byte[] block = BytesRef.EMPTY_BYTES;
    int totalChunks = 0;
    long maxPointer = 0;
    final long blockAddressesStart;

    private final IndexOutput tempBinaryOffsets;


    public CompressedBinaryBlockWriter(IndexOutput data, IndexOutput meta) throws IOException {
      this.data = data;
      this.meta = meta;
      tempBinaryOffsets = state.directory.createTempOutput(state.segmentInfo.name, "binary_pointers", state.context);
      boolean success = false;
      try {
        CodecUtil.writeHeader(tempBinaryOffsets, Lucene80DocValuesFormat.META_CODEC + "FilePointers", Lucene80DocValuesFormat.VERSION_CURRENT);
        blockAddressesStart = data.getFilePointer();
        success = true;
      } finally {
        if (success == false) {
          IOUtils.closeWhileHandlingException(this); //self-close because constructor caller can't
        }
      }
    }

    void addDoc(int doc, BytesRef v) throws IOException {
      docLengths[numDocsInCurrentBlock] = v.length;
      block = ArrayUtil.grow(block, uncompressedBlockLength + v.length);
      System.arraycopy(v.bytes, v.offset, block, uncompressedBlockLength, v.length);
      uncompressedBlockLength += v.length;
      numDocsInCurrentBlock++;
      if (numDocsInCurrentBlock == Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK) {
        flushData();
      }
    }

    private void flushData() throws IOException {
      if (numDocsInCurrentBlock > 0) {
        // Write offset to this block to temporary offsets file
        totalChunks++;
        long thisBlockStartPointer = data.getFilePointer();

        // Optimisation - check if all lengths are same
        boolean allLengthsSame = true;
        for (int i = 1; i < Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK; i++) {
          if (docLengths[i] != docLengths[i-1]) {
            allLengthsSame = false;
            break;
          }
        }
        if (allLengthsSame) {
          // Only write one value shifted. Steal a bit to indicate all other lengths are the same
          int onlyOneLength = (docLengths[0] <<1) | 1;
          data.writeVInt(onlyOneLength);
        } else {
          for (int i = 0; i < Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK; i++) {
            if (i == 0) {
              // Write first value shifted and steal a bit to indicate other lengths are to follow
              int multipleLengths = (docLengths[0] <<1);
              data.writeVInt(multipleLengths);
            } else {
              data.writeVInt(docLengths[i]);
            }
          }
        }
        maxUncompressedBlockLength = Math.max(maxUncompressedBlockLength, uncompressedBlockLength);
        LZ4.compress(block, 0, uncompressedBlockLength, data, ht);
        numDocsInCurrentBlock = 0;
        // Ensure initialized with zeroes because full array is always written
        Arrays.fill(docLengths, 0);
        uncompressedBlockLength = 0;
        maxPointer = data.getFilePointer();
        tempBinaryOffsets.writeVLong(maxPointer - thisBlockStartPointer);
      }
    }

    void writeMetaData() throws IOException {
      if (totalChunks == 0) {
        return;
      }

      long startDMW = data.getFilePointer();
      meta.writeLong(startDMW);

      meta.writeVInt(totalChunks);
      meta.writeVInt(Lucene80DocValuesFormat.BINARY_BLOCK_SHIFT);
      meta.writeVInt(maxUncompressedBlockLength);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);


      CodecUtil.writeFooter(tempBinaryOffsets);
      IOUtils.close(tempBinaryOffsets);
      //write the compressed block offsets info to the meta file by reading from temp file
      try (ChecksumIndexInput filePointersIn = state.directory.openChecksumInput(tempBinaryOffsets.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(filePointersIn, Lucene80DocValuesFormat.META_CODEC + "FilePointers", Lucene80DocValuesFormat.VERSION_CURRENT,
            Lucene80DocValuesFormat.VERSION_CURRENT);
        Throwable priorE = null;
        try {
          final DirectMonotonicWriter filePointers = DirectMonotonicWriter.getInstance(meta, data, totalChunks, DIRECT_MONOTONIC_BLOCK_SHIFT);
          long fp = blockAddressesStart;
          for (int i = 0; i < totalChunks; ++i) {
            filePointers.add(fp);
            fp += filePointersIn.readVLong();
          }
          if (maxPointer < fp) {
            throw new CorruptIndexException("File pointers don't add up ("+fp+" vs expected "+maxPointer+")", filePointersIn);
          }
          filePointers.finish();
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(filePointersIn, priorE);
        }
      }
      // Write the length of the DMW block in the data
      meta.writeLong(data.getFilePointer() - startDMW);
    }

    @Override
    public void close() throws IOException {
      if (tempBinaryOffsets != null) {
        IOUtils.close(tempBinaryOffsets);
        state.directory.deleteFile(tempBinaryOffsets.getName());
      }
    }
  }
}
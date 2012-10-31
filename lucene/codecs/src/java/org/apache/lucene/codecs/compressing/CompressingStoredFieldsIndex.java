package org.apache.lucene.codecs.compressing;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * A format for the stored fields index file (.fdx).
 * <p>
 * These formats allow different memory/speed trade-offs to locate documents
 * into the fields data file (.fdt).
 * @lucene.experimental
 */
public enum CompressingStoredFieldsIndex {

  /**
   * This format stores the document index on disk using 64-bits pointers to
   * the start offsets of chunks in the fields data file.
   * <p>
   * This format has no memory overhead and requires at most 1 disk seek to
   * locate a document in the fields data file. Use this fields index in
   * memory-constrained environments.
   */
  DISK_DOC(0) {
    @Override
    Writer newWriter(IndexOutput out) {
      return new DiskDocFieldsIndexWriter(out);
    }
    @Override
    Reader newReader(IndexInput in, SegmentInfo si) throws IOException {
      return new DiskDocFieldsIndexReader(in, si);
    }
  },

  /**
   * For every chunk of compressed documents, this index stores the first doc
   * ID of the chunk as well as the start offset of the chunk.
   * <p>
   * This fields index uses a very compact in-memory representation (up to
   * <code>12 * numChunks</code> bytes, but likely much less) and requires no
   * disk seek to locate a document in the fields data file. Unless you are
   * working with very little memory, you should use this instance.
   */
  MEMORY_CHUNK(1) {
    @Override
    Writer newWriter(IndexOutput out) throws IOException {
      return new MemoryChunkFieldsIndexWriter(out);
    }
    @Override
    Reader newReader(IndexInput in, SegmentInfo si) throws IOException {
      return new MemoryChunkFieldsIndexReader(in, si);
    }
  };

  /**
   * Retrieve a {@link CompressingStoredFieldsIndex} according to its
   * <code>ID</code>.
   */
  public static CompressingStoredFieldsIndex byId(int id) {
    for (CompressingStoredFieldsIndex idx : CompressingStoredFieldsIndex.values()) {
      if (idx.getId() == id) {
        return idx;
      }
    }
    throw new IllegalArgumentException("Unknown id: " + id);
  }

  private final int id;

  private CompressingStoredFieldsIndex(int id) {
    this.id = id;
  }

  /**
   * Returns an ID for this compression mode. Should be unique across
   * {@link CompressionMode}s as it is used for serialization and
   * unserialization.
   */
  public final int getId() {
    return id;
  }

  abstract Writer newWriter(IndexOutput out) throws IOException;

  abstract Reader newReader(IndexInput in, SegmentInfo si) throws IOException;

  static abstract class Writer implements Closeable {

    protected final IndexOutput fieldsIndexOut;

    Writer(IndexOutput indexOutput) {
      this.fieldsIndexOut = indexOutput;
    }

    /** Write the index file for a chunk of <code>numDocs</code> docs starting
     *  at offset <code>startPointer</code>. */
    abstract void writeIndex(int numDocs, long startPointer) throws IOException;

    /** Finish writing an index file of <code>numDocs</code> documents. */
    abstract void finish(int numDocs) throws IOException;

    @Override
    public void close() throws IOException {
      fieldsIndexOut.close();
    }

  }

  private static class DiskDocFieldsIndexWriter extends Writer {

    final long startOffset;

    DiskDocFieldsIndexWriter(IndexOutput fieldsIndexOut) {
      super(fieldsIndexOut);
      startOffset = fieldsIndexOut.getFilePointer();
    }

    @Override
    void writeIndex(int numDocs, long startPointer) throws IOException {
      for (int i = 0; i < numDocs; ++i) {
        fieldsIndexOut.writeLong(startPointer);
      }
    }

    @Override
    void finish(int numDocs) throws IOException {
      if (startOffset + ((long) numDocs) * 8 != fieldsIndexOut.getFilePointer()) {
        // see Lucene40StoredFieldsWriter#finish
        throw new RuntimeException((fieldsIndexOut.getFilePointer() - startOffset)/8 + " fdx size mismatch: docCount is " + numDocs + " but fdx file size is " + fieldsIndexOut.getFilePointer() + " file=" + fieldsIndexOut.toString() + "; now aborting this merge to prevent index corruption");
      }
    }

  }

  private static class MemoryChunkFieldsIndexWriter extends Writer {

    static final int BLOCK_SIZE = 1024; // number of chunks to serialize at once

    static long moveSignToLowOrderBit(long n) {
      return (n >> 63) ^ (n << 1);
    }

    int totalDocs;
    int blockDocs;
    int blockChunks;
    long firstStartPointer;
    long maxStartPointer;
    final int[] docBaseDeltas;
    final long[] startPointerDeltas;

    MemoryChunkFieldsIndexWriter(IndexOutput indexOutput) throws IOException {
      super(indexOutput);
      reset();
      totalDocs = 0;
      docBaseDeltas = new int[BLOCK_SIZE];
      startPointerDeltas = new long[BLOCK_SIZE];
      fieldsIndexOut.writeVInt(PackedInts.VERSION_CURRENT);
    }

    private void reset() {
      blockChunks = 0;
      blockDocs = 0;
      firstStartPointer = -1; // means unset
    }

    private void writeBlock() throws IOException {
      assert blockChunks > 0;
      fieldsIndexOut.writeVInt(blockChunks);

      // The trick here is that we only store the difference from the average start
      // pointer or doc base, this helps save bits per value.
      // And in order to prevent a few chunks that would be far from the average to
      // raise the number of bits per value for all of them, we only encode blocks
      // of 1024 chunks at once
      // See LUCENE-4512

      // doc bases
      final int avgChunkDocs;
      if (blockChunks == 1) {
        avgChunkDocs = 0;
      } else {
        avgChunkDocs = Math.round((float) (blockDocs - docBaseDeltas[blockChunks - 1]) / (blockChunks - 1));
      }
      fieldsIndexOut.writeVInt(totalDocs - blockDocs); // docBase
      fieldsIndexOut.writeVInt(avgChunkDocs);
      int docBase = 0;
      long maxDelta = 0;
      for (int i = 0; i < blockChunks; ++i) {
        final int delta = docBase - avgChunkDocs * i;
        maxDelta |= moveSignToLowOrderBit(delta);
        docBase += docBaseDeltas[i];
      }

      final int bitsPerDocBase = PackedInts.bitsRequired(maxDelta);
      fieldsIndexOut.writeVInt(bitsPerDocBase);
      PackedInts.Writer writer = PackedInts.getWriterNoHeader(fieldsIndexOut,
          PackedInts.Format.PACKED, blockChunks, bitsPerDocBase, 1);
      docBase = 0;
      for (int i = 0; i < blockChunks; ++i) {
        final long delta = docBase - avgChunkDocs * i;
        assert PackedInts.bitsRequired(moveSignToLowOrderBit(delta)) <= writer.bitsPerValue();
        writer.add(moveSignToLowOrderBit(delta));
        docBase += docBaseDeltas[i];
      }
      writer.finish();

      // start pointers
      fieldsIndexOut.writeVLong(firstStartPointer);
      final long avgChunkSize;
      if (blockChunks == 1) {
        avgChunkSize = 0;
      } else {
        avgChunkSize = (maxStartPointer - firstStartPointer) / (blockChunks - 1);
      }
      fieldsIndexOut.writeVLong(avgChunkSize);
      long startPointer = 0;
      maxDelta = 0;
      for (int i = 0; i < blockChunks; ++i) {
        startPointer += startPointerDeltas[i];
        final long delta = startPointer - avgChunkSize * i;
        maxDelta |= moveSignToLowOrderBit(delta);
      }

      final int bitsPerStartPointer = PackedInts.bitsRequired(maxDelta);
      fieldsIndexOut.writeVInt(bitsPerStartPointer);
      writer = PackedInts.getWriterNoHeader(fieldsIndexOut, PackedInts.Format.PACKED,
          blockChunks, bitsPerStartPointer, 1);
      startPointer = 0;
      for (int i = 0; i < blockChunks; ++i) {
        startPointer += startPointerDeltas[i];
        final long delta = startPointer - avgChunkSize * i;
        assert PackedInts.bitsRequired(moveSignToLowOrderBit(delta)) <= writer.bitsPerValue();
        writer.add(moveSignToLowOrderBit(delta));
      }
      writer.finish();
    }

    @Override
    void writeIndex(int numDocs, long startPointer) throws IOException {
      if (blockChunks == BLOCK_SIZE) {
        writeBlock();
        reset();
      }

      if (firstStartPointer == -1) {
        firstStartPointer = maxStartPointer = startPointer;
      }
      assert firstStartPointer > 0 && startPointer >= firstStartPointer;

      docBaseDeltas[blockChunks] = numDocs;
      startPointerDeltas[blockChunks] = startPointer - maxStartPointer;

      ++blockChunks;
      blockDocs += numDocs;
      totalDocs += numDocs;
      maxStartPointer = startPointer;
    }

    @Override
    void finish(int numDocs) throws IOException {
      if (numDocs != totalDocs) {
        throw new IllegalStateException("Expected " + numDocs + " docs, but got " + totalDocs);
      }
      if (blockChunks > 0) {
        writeBlock();
      }
      fieldsIndexOut.writeVInt(0); // end marker
    }

  }

  static abstract class Reader implements Cloneable, Closeable {

    protected final IndexInput fieldsIndexIn;

    Reader(IndexInput fieldsIndexIn) {
      this.fieldsIndexIn = fieldsIndexIn;
    }

    /** Get the start pointer of the compressed block that contains docID */
    abstract long getStartPointer(int docID) throws IOException;

    public void close() throws IOException {
      IOUtils.close(fieldsIndexIn);
    }

    public abstract Reader clone();
  }

  private static class DiskDocFieldsIndexReader extends Reader {

    final long startPointer;

    DiskDocFieldsIndexReader(IndexInput fieldsIndexIn, SegmentInfo si) throws CorruptIndexException {
      this(fieldsIndexIn, fieldsIndexIn.getFilePointer());
      final long indexSize = fieldsIndexIn.length() - fieldsIndexIn.getFilePointer();
      final int numDocs = (int) (indexSize >> 3);
      // Verify two sources of "maxDoc" agree:
      if (numDocs != si.getDocCount()) {
        throw new CorruptIndexException("doc counts differ for segment " + si + ": fieldsReader shows " + numDocs + " but segmentInfo shows " + si.getDocCount());
      }
    }

    private DiskDocFieldsIndexReader(IndexInput fieldsIndexIn, long startPointer) {
      super(fieldsIndexIn);
      this.startPointer = startPointer;
    }

    @Override
    long getStartPointer(int docID) throws IOException {
      fieldsIndexIn.seek(startPointer + docID * 8L);
      return fieldsIndexIn.readLong();
    }

    @Override
    public Reader clone() {
      return new DiskDocFieldsIndexReader(fieldsIndexIn.clone(), startPointer);
    }

  }

  private static class MemoryChunkFieldsIndexReader extends Reader {

    static long moveLowOrderBitToSign(long n) {
      return ((n >>> 1) ^ -(n & 1));
    }

    private final int maxDoc;
    private final int[] docBases;
    private final long[] startPointers;
    private final int[] avgChunkDocs;
    private final long[] avgChunkSizes;
    private final PackedInts.Reader[] docBasesDeltas; // delta from the avg
    private final PackedInts.Reader[] startPointersDeltas; // delta from the avg

    MemoryChunkFieldsIndexReader(IndexInput fieldsIndexIn, SegmentInfo si) throws IOException {
      super(fieldsIndexIn);
      maxDoc = si.getDocCount();
      int[] docBases = new int[16];
      long[] startPointers = new long[16];
      int[] avgChunkDocs = new int[16];
      long[] avgChunkSizes = new long[16];
      PackedInts.Reader[] docBasesDeltas = new PackedInts.Reader[16];
      PackedInts.Reader[] startPointersDeltas = new PackedInts.Reader[16];

      final int packedIntsVersion = fieldsIndexIn.readVInt();

      int blockCount = 0;

      for (;;) {
        final int numChunks = fieldsIndexIn.readVInt();
        if (numChunks == 0) {
          break;
        }
        if (blockCount == docBases.length) {
          final int newSize = ArrayUtil.oversize(blockCount + 1, 8);
          docBases = Arrays.copyOf(docBases, newSize);
          startPointers = Arrays.copyOf(startPointers, newSize);
          avgChunkDocs = Arrays.copyOf(avgChunkDocs, newSize);
          avgChunkSizes = Arrays.copyOf(avgChunkSizes, newSize);
          docBasesDeltas = Arrays.copyOf(docBasesDeltas, newSize);
          startPointersDeltas = Arrays.copyOf(startPointersDeltas, newSize);
        }

        // doc bases
        docBases[blockCount] = fieldsIndexIn.readVInt();
        avgChunkDocs[blockCount] = fieldsIndexIn.readVInt();
        final int bitsPerDocBase = fieldsIndexIn.readVInt();
        if (bitsPerDocBase > 32) {
          throw new CorruptIndexException("Corrupted");
        }
        docBasesDeltas[blockCount] = PackedInts.getReaderNoHeader(fieldsIndexIn, PackedInts.Format.PACKED, packedIntsVersion, numChunks, bitsPerDocBase);

        // start pointers
        startPointers[blockCount] = fieldsIndexIn.readVLong();
        avgChunkSizes[blockCount] = fieldsIndexIn.readVLong();
        final int bitsPerStartPointer = fieldsIndexIn.readVInt();
        if (bitsPerStartPointer > 64) {
          throw new CorruptIndexException("Corrupted");
        }
        startPointersDeltas[blockCount] = PackedInts.getReaderNoHeader(fieldsIndexIn, PackedInts.Format.PACKED, packedIntsVersion, numChunks, bitsPerStartPointer);

        ++blockCount;
      }

      this.docBases = Arrays.copyOf(docBases, blockCount);
      this.startPointers = Arrays.copyOf(startPointers, blockCount);
      this.avgChunkDocs = Arrays.copyOf(avgChunkDocs, blockCount);
      this.avgChunkSizes = Arrays.copyOf(avgChunkSizes, blockCount);
      this.docBasesDeltas = Arrays.copyOf(docBasesDeltas, blockCount);
      this.startPointersDeltas = Arrays.copyOf(startPointersDeltas, blockCount);
    }

    private MemoryChunkFieldsIndexReader(MemoryChunkFieldsIndexReader other) {
      super(null);
      this.maxDoc = other.maxDoc;
      this.docBases = other.docBases;
      this.startPointers = other.startPointers;
      this.avgChunkDocs = other.avgChunkDocs;
      this.avgChunkSizes = other.avgChunkSizes;
      this.docBasesDeltas = other.docBasesDeltas;
      this.startPointersDeltas = other.startPointersDeltas;
    }

    private int block(int docID) {
      int lo = 0, hi = docBases.length - 1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final int midValue = docBases[mid];
        if (midValue == docID) {
          return mid;
        } else if (midValue < docID) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      return hi;
    }

    private int relativeDocBase(int block, int relativeChunk) {
      final int expected = avgChunkDocs[block] * relativeChunk;
      final long delta = moveLowOrderBitToSign(docBasesDeltas[block].get(relativeChunk));
      return expected + (int) delta;
    }

    private long relativeStartPointer(int block, int relativeChunk) {
      final long expected = avgChunkSizes[block] * relativeChunk;
      final long delta = moveLowOrderBitToSign(startPointersDeltas[block].get(relativeChunk));
      return expected + delta;
    }

    private int relativeChunk(int block, int relativeDoc) {
      int lo = 0, hi = docBasesDeltas[block].size() - 1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final int midValue = relativeDocBase(block, mid);
        if (midValue == relativeDoc) {
          return mid;
        } else if (midValue < relativeDoc) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      return hi;
    }

    @Override
    long getStartPointer(int docID) {
      if (docID < 0 || docID >= maxDoc) {
        throw new IllegalArgumentException("docID out of range [0-" + maxDoc + "]: " + docID);
      }
      final int block = block(docID);
      final int relativeChunk = relativeChunk(block, docID - docBases[block]);
      return startPointers[block] + relativeStartPointer(block, relativeChunk);
    }

    @Override
    public Reader clone() {
      if (fieldsIndexIn == null) {
        return this;
      } else {
        return new MemoryChunkFieldsIndexReader(this);
      }
    }

  }

}

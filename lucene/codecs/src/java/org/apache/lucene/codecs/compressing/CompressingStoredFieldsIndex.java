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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.GrowableWriter;
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
   * locate a document in the fields data file. Use this format in
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
   * For every document in the segment, this format stores the offset of the
   * compressed chunk that contains it in the fields data file.
   * <p>
   * This fields index format requires at most <code>8 * numDocs</code> bytes
   * of memory. Locating a document in the fields data file requires no disk
   * seek. Use this format when blocks are very likely to contain few
   * documents (in particular when <code>chunkSize = 1</code>).
   */
  MEMORY_DOC(1) {
    @Override
    Writer newWriter(IndexOutput out) throws IOException {
      return new ChunksFieldsIndexWriter(out);
    }
    @Override
    Reader newReader(IndexInput in, SegmentInfo si) throws IOException {
      return new MemoryDocFieldsIndexReader(in, si);
    }
  },

  /**
   * For every chunk of compressed documents, this format stores the first doc
   * ID of the chunk as well as the start offset of the chunk.
   * <p>
   * This fields index format require at most
   * <code>12 * numChunks</code> bytes of memory. Locating a document in the
   * fields data file requires no disk seek. Use this format when chunks are
   * likely to contain several documents.
   */
  MEMORY_CHUNK(2) {
    @Override
    Writer newWriter(IndexOutput out) throws IOException {
      return new ChunksFieldsIndexWriter(out);
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

  private static class ChunksFieldsIndexWriter extends Writer {

    int numChunks;
    long maxStartPointer;
    GrowableWriter docBaseDeltas;
    GrowableWriter startPointerDeltas;

    ChunksFieldsIndexWriter(IndexOutput indexOutput) {
      super(indexOutput);
      numChunks = 0;
      maxStartPointer = 0;
      docBaseDeltas = new GrowableWriter(2, 128, PackedInts.COMPACT);
      startPointerDeltas = new GrowableWriter(5, 128, PackedInts.COMPACT);
    }

    @Override
    void writeIndex(int numDocs, long startPointer) throws IOException {
      if (numChunks == docBaseDeltas.size()) {
        final int newSize = ArrayUtil.oversize(numChunks + 1, 1);
        docBaseDeltas = docBaseDeltas.resize(newSize);
        startPointerDeltas = startPointerDeltas.resize(newSize);
      }
      docBaseDeltas.set(numChunks, numDocs);
      startPointerDeltas.set(numChunks, startPointer - maxStartPointer);

      ++numChunks;
      maxStartPointer = startPointer;
    }

    @Override
    void finish(int numDocs) throws IOException {
      if (numChunks != docBaseDeltas.size()) {
        docBaseDeltas = docBaseDeltas.resize(numChunks);
        startPointerDeltas = startPointerDeltas.resize(numChunks);
      }
      fieldsIndexOut.writeVInt(numChunks);
      fieldsIndexOut.writeByte((byte) PackedInts.bitsRequired(maxStartPointer));
      docBaseDeltas.save(fieldsIndexOut);
      startPointerDeltas.save(fieldsIndexOut);
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
      if (fieldsIndexIn != null) {
        fieldsIndexIn.close();
      }
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

  private static class MemoryDocFieldsIndexReader extends Reader {

    private final PackedInts.Reader startPointers;

    MemoryDocFieldsIndexReader(IndexInput fieldsIndexIn, SegmentInfo si) throws IOException {
      super(fieldsIndexIn);
      final int numChunks = fieldsIndexIn.readVInt();
      final int bitsPerStartPointer = fieldsIndexIn.readByte() & 0xFF;
      if (bitsPerStartPointer > 64) {
        throw new CorruptIndexException("Corrupted");
      }

      final PackedInts.Reader chunkDocs = PackedInts.getReader(fieldsIndexIn);
      if (chunkDocs.size() != numChunks) {
        throw new CorruptIndexException("Expected " + numChunks + " chunks, but got " + chunkDocs.size());
      }

      final PackedInts.ReaderIterator startPointerDeltas = PackedInts.getReaderIterator(fieldsIndexIn, PackedInts.DEFAULT_BUFFER_SIZE);
      if (startPointerDeltas.size() != numChunks) {
        throw new CorruptIndexException("Expected " + numChunks + " chunks, but got " + startPointerDeltas.size());
      }
      final PackedInts.Mutable startPointers = PackedInts.getMutable(si.getDocCount(), bitsPerStartPointer, PackedInts.COMPACT);
      int docID = 0;
      long startPointer = 0;
      for (int i = 0; i < numChunks; ++i) {
        startPointer += startPointerDeltas.next();
        final int chunkDocCount = (int) chunkDocs.get(i);
        for (int j = 0; j < chunkDocCount; ++j) {
          startPointers.set(docID++, startPointer);
        }
      }
      if (docID != si.getDocCount()) {
        throw new CorruptIndexException("Expected " + si.getDocCount() + " docs, got " + docID);
      }

      this.startPointers = startPointers;
    }

    private MemoryDocFieldsIndexReader(PackedInts.Reader startPointers) {
      super(null);
      this.startPointers = startPointers;
    }

    @Override
    long getStartPointer(int docID) throws IOException {
      return startPointers.get(docID);
    }

    @Override
    public Reader clone() {
      if (fieldsIndexIn == null) {
        return this;
      } else {
        return new MemoryDocFieldsIndexReader(startPointers);
      }
    }

  }

  private static class MemoryChunkFieldsIndexReader extends Reader {

    private final PackedInts.Reader docBases;
    private final PackedInts.Reader startPointers;

     MemoryChunkFieldsIndexReader(IndexInput fieldsIndexIn, SegmentInfo si) throws IOException {
      super(fieldsIndexIn);
      final int numChunks = fieldsIndexIn.readVInt();
      final int bitsPerStartPointer = fieldsIndexIn.readByte() & 0xFF;
      if (bitsPerStartPointer > 64) {
        throw new CorruptIndexException("Corrupted");
      }

      final PackedInts.ReaderIterator docBaseDeltas = PackedInts.getReaderIterator(fieldsIndexIn, PackedInts.DEFAULT_BUFFER_SIZE);
      if (docBaseDeltas.size() != numChunks) {
        throw new CorruptIndexException("Expected " + numChunks + " chunks, but got " + docBaseDeltas.size());
      }
      final PackedInts.Mutable docBases = PackedInts.getMutable(numChunks, PackedInts.bitsRequired(Math.max(0, si.getDocCount() - 1)), PackedInts.COMPACT);

      int docBase = 0;
      for (int i = 0; i < numChunks; ++i) {
        docBases.set(i, docBase);
        docBase += docBaseDeltas.next();
      }
      if (docBase != si.getDocCount()) {
        throw new CorruptIndexException("Expected " + si.getDocCount() + " docs, got " + docBase);
      }

      final PackedInts.ReaderIterator startPointerDeltas = PackedInts.getReaderIterator(fieldsIndexIn, PackedInts.DEFAULT_BUFFER_SIZE);
      if (startPointerDeltas.size() != numChunks) {
        throw new CorruptIndexException("Expected " + numChunks + " chunks, but got " + startPointerDeltas.size());
      }
      final PackedInts.Mutable startPointers = PackedInts.getMutable(numChunks, bitsPerStartPointer, PackedInts.COMPACT);
      long startPointer = 0;
      for (int i = 0; i < numChunks; ++i) {
        startPointer += startPointerDeltas.next();
        startPointers.set(i, startPointer);
      }

      this.docBases = docBases;
      this.startPointers = startPointers;
    }

    private MemoryChunkFieldsIndexReader(PackedInts.Reader docBases, PackedInts.Reader startPointers) {
      super(null);
      this.docBases = docBases;
      this.startPointers = startPointers;
    }

    @Override
    long getStartPointer(int docID) {
      assert docBases.size() > 0;
      int lo = 0, hi = docBases.size() - 1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final long midValue = docBases.get(mid);
        if (midValue == docID) {
          return startPointers.get(mid);
        } else if (midValue < docID) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      return startPointers.get(hi);
    }

    @Override
    public Reader clone() {
      if (fieldsIndexIn == null) {
        return this;
      } else {
        return new MemoryChunkFieldsIndexReader(docBases, startPointers);
      }
    }

  }

}

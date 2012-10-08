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

import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.BYTE_ARR;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.CODEC_NAME_DAT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.*;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.HEADER_LENGTH_DAT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.HEADER_LENGTH_IDX;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_DOUBLE;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_FLOAT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_INT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_LONG;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.STRING;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.TYPE_MASK;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.VERSION_CURRENT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.VERSION_START;
import static org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsWriter.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsWriter.FIELDS_INDEX_EXTENSION;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

final class CompressingStoredFieldsReader extends StoredFieldsReader {

  private final FieldInfos fieldInfos;
  private final CompressingStoredFieldsIndex.Reader indexReader;
  private final IndexInput fieldsStream;
  private final int packedIntsVersion;
  private final CompressionMode compressionMode;
  private final Uncompressor uncompressor;
  private final BytesRef bytes;
  private final int numDocs;
  private boolean closed;
  
  // used by clone
  private CompressingStoredFieldsReader(CompressingStoredFieldsReader reader) {
    this.fieldInfos = reader.fieldInfos;
    this.fieldsStream = reader.fieldsStream.clone();
    this.indexReader = reader.indexReader.clone();
    this.packedIntsVersion = reader.packedIntsVersion;
    this.compressionMode = reader.compressionMode;
    this.uncompressor = reader.uncompressor.clone();
    this.numDocs = reader.numDocs;
    this.bytes = new BytesRef(reader.bytes.bytes.length);
    this.closed = false;
  }
  
  public CompressingStoredFieldsReader(Directory d, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    numDocs = si.getDocCount();
    IndexInput indexStream = null;
    try {
      fieldsStream = d.openInput(IndexFileNames.segmentFileName(segment, "", FIELDS_EXTENSION), context);
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, "", FIELDS_INDEX_EXTENSION);
      indexStream = d.openInput(indexStreamFN, context);

      CodecUtil.checkHeader(indexStream, CODEC_NAME_IDX, VERSION_START, VERSION_CURRENT);
      CodecUtil.checkHeader(fieldsStream, CODEC_NAME_DAT, VERSION_START, VERSION_CURRENT);
      assert HEADER_LENGTH_DAT == fieldsStream.getFilePointer();
      assert HEADER_LENGTH_IDX == indexStream.getFilePointer();

      final int storedFieldsIndexId = indexStream.readVInt();
      final CompressingStoredFieldsIndex storedFieldsIndex = CompressingStoredFieldsIndex.byId(storedFieldsIndexId);
      indexReader = storedFieldsIndex.newReader(indexStream, si);
      indexStream = null;

      packedIntsVersion = fieldsStream.readVInt();
      final int compressionModeId = fieldsStream.readVInt();
      compressionMode = CompressionMode.byId(compressionModeId);
      uncompressor = compressionMode.newUncompressor();
      this.bytes = new BytesRef();

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this, indexStream);
      }
    }
  }

  /**
   * @throws AlreadyClosedException if this FieldsReader is closed
   */
  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this FieldsReader is closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      IOUtils.close(fieldsStream, indexReader);
      closed = true;
    }
  }

  private static void readField(ByteArrayDataInput in, StoredFieldVisitor visitor, FieldInfo info, int bits) throws IOException {
    switch (bits & TYPE_MASK) {
      case BYTE_ARR:
        int length = in.readVInt();
        byte[] data = new byte[length];
        in.readBytes(data, 0, length);
        visitor.binaryField(info, data);
        break;
      case STRING:
        length = in.readVInt();
        data = new byte[length];
        in.readBytes(data, 0, length);
        visitor.stringField(info, new String(data, IOUtils.CHARSET_UTF_8));
        break;
      case NUMERIC_INT:
        visitor.intField(info, in.readInt());
        break;
      case NUMERIC_FLOAT:
        visitor.floatField(info, Float.intBitsToFloat(in.readInt()));
        break;
      case NUMERIC_LONG:
        visitor.longField(info, in.readLong());
        break;
      case NUMERIC_DOUBLE:
        visitor.doubleField(info, Double.longBitsToDouble(in.readLong()));
        break;
      default:
        throw new AssertionError("Unknown type flag: " + Integer.toHexString(bits));
    }
  }

  private static void skipField(ByteArrayDataInput in, int bits) throws IOException {
    switch (bits & TYPE_MASK) {
      case BYTE_ARR:
      case STRING:
        final int length = in.readVInt();
        in.skipBytes(length);
        break;
      case NUMERIC_INT:
      case NUMERIC_FLOAT:
        in.readInt();
        break;
      case NUMERIC_LONG:
      case NUMERIC_DOUBLE:
        in.readLong();
        break;
      default:
        throw new AssertionError("Unknown type flag: " + Integer.toHexString(bits));
    }
  }

  @Override
  public void visitDocument(int docID, StoredFieldVisitor visitor)
      throws IOException {
    fieldsStream.seek(indexReader.getStartPointer(docID));

    final int docBase = fieldsStream.readVInt();
    final int chunkDocs = fieldsStream.readVInt();
    final int bitsPerValue = fieldsStream.readVInt();
    if (docID < docBase
        || docID >= docBase + chunkDocs
        || docBase + chunkDocs > numDocs
        || bitsPerValue > 31) {
      throw new CorruptIndexException("Corrupted: docID=" + docID
          + ", docBase=" + docBase + ", chunkDocs=" + chunkDocs
          + ", numDocs=" + numDocs + ", bitsPerValue=" + bitsPerValue);
    }

    final long filePointer = fieldsStream.getFilePointer();
    final PackedInts.ReaderIterator lengths = PackedInts.getReaderIteratorNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerValue, 1);
    int offset = 0;
    for (int i = docBase; i < docID; ++i) {
      offset += lengths.next();
    }
    final int length = (int) lengths.next();
    // skip the last values
    fieldsStream.seek(filePointer + (PackedInts.Format.PACKED.nblocks(bitsPerValue, chunkDocs) << 3));

    uncompressor.uncompress(fieldsStream, offset, length, bytes);

    final ByteArrayDataInput documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);
    final int numFields = documentInput.readVInt();
    for (int fieldIDX = 0; fieldIDX < numFields; fieldIDX++) {
      final long infoAndBits = documentInput.readVLong();
      final int fieldNumber = (int) (infoAndBits >>> TYPE_BITS);
      FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);

      final int bits = (int) (infoAndBits & TYPE_MASK);
      assert bits <= NUMERIC_DOUBLE: "bits=" + Integer.toHexString(bits);

      switch(visitor.needsField(fieldInfo)) {
        case YES:
          readField(documentInput, visitor, fieldInfo, bits);
          break;
        case NO: 
          skipField(documentInput, bits);
          break;
        case STOP: 
          return;
      }
    }
  }

  @Override
  public StoredFieldsReader clone() {
    ensureOpen();
    return new CompressingStoredFieldsReader(this);
  }

  CompressionMode getCompressionMode() {
    return compressionMode;
  }

  ChunkIterator chunkIterator(int startDocID) throws IOException {
    ensureOpen();
    fieldsStream.seek(indexReader.getStartPointer(startDocID));
    return new ChunkIterator();
  }

  final class ChunkIterator {

    BytesRef bytes;
    int docBase;
    int chunkDocs;
    int[] lengths;

    private ChunkIterator() {
      this.docBase = -1;
      bytes = new BytesRef();
      lengths = new int[0];
    }

    private int readHeader() throws IOException {
      final int docBase = fieldsStream.readVInt();
      final int chunkDocs = fieldsStream.readVInt();
      final int bitsPerValue = fieldsStream.readVInt();
      if (docBase < this.docBase + this.chunkDocs
          || docBase + chunkDocs > numDocs
          || bitsPerValue > 31) {
        throw new CorruptIndexException("Corrupted: current docBase=" + this.docBase
            + ", current numDocs=" + this.chunkDocs + ", new docBase=" + docBase
            + ", new numDocs=" + chunkDocs + ", bitsPerValue=" + bitsPerValue);
      }
      this.docBase = docBase;
      this.chunkDocs = chunkDocs;
      return bitsPerValue;
    }

    /**
     * Return the uncompressed size of the chunk
     */
    int chunkSize() {
      int sum = 0;
      for (int i = 0; i < chunkDocs; ++i) {
        sum += lengths[i];
      }
      return sum;
    }

    /**
     * Go to the chunk containing the provided doc ID.
     */
    void next(int doc) throws IOException {
      assert doc >= docBase + chunkDocs : doc + " " + docBase + " " + chunkDocs;
      // try next chunk
      int bitsPerValue = readHeader();
      if (docBase + chunkDocs <= doc) {
        // doc is not in the next chunk, use seek to skip to the next document chunk
        fieldsStream.seek(indexReader.getStartPointer(doc));
        bitsPerValue = readHeader();
      }
      if (doc < docBase
          || doc >= docBase + chunkDocs) {
        throw new CorruptIndexException("Corrupted: docID=" + doc
            + ", docBase=" + docBase + ", chunkDocs=" + chunkDocs);
      }

      // decode lengths
      if (lengths.length < chunkDocs) {
        lengths = new int[ArrayUtil.oversize(chunkDocs, 4)];
      }
      final PackedInts.ReaderIterator iterator = PackedInts.getReaderIteratorNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerValue, 0);
      for (int i = 0; i < chunkDocs; ++i) {
        lengths[i] = (int) iterator.next();
      }
    }

    /**
     * Uncompress the chunk.
     */
    void uncompress() throws IOException {
      // uncompress data
      uncompressor.uncompress(fieldsStream, bytes);
      if (bytes.length != chunkSize()) {
        throw new CorruptIndexException("Corrupted: expected chunk size = " + chunkSize() + ", got " + bytes.length);
      }
    }

    /**
     * Copy compressed data.
     */
    void copyCompressedData(DataOutput out) throws IOException {
      uncompressor.copyCompressedData(fieldsStream, out);
    }

  }

}

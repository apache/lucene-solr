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
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.CODEC_SFX_DAT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.CODEC_SFX_IDX;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_DOUBLE;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_FLOAT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_INT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.NUMERIC_LONG;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.STRING;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.TYPE_BITS;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.TYPE_MASK;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.VERSION_CURRENT;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.VERSION_START;
import static org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsWriter.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsWriter.FIELDS_INDEX_EXTENSION;

import java.io.IOException;
import java.util.Arrays;

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

/**
 * {@link StoredFieldsReader} impl for {@link CompressingStoredFieldsFormat}.
 * @lucene.experimental
 */
public final class CompressingStoredFieldsReader extends StoredFieldsReader {

  private final FieldInfos fieldInfos;
  private final CompressingStoredFieldsIndexReader indexReader;
  private final IndexInput fieldsStream;
  private final int packedIntsVersion;
  private final CompressionMode compressionMode;
  private final Decompressor decompressor;
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
    this.decompressor = reader.decompressor.clone();
    this.numDocs = reader.numDocs;
    this.bytes = new BytesRef(reader.bytes.bytes.length);
    this.closed = false;
  }

  /** Sole constructor. */
  public CompressingStoredFieldsReader(Directory d, SegmentInfo si, String segmentSuffix, FieldInfos fn,
      IOContext context, String formatName, CompressionMode compressionMode) throws IOException {
    this.compressionMode = compressionMode;
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    numDocs = si.getDocCount();
    IndexInput indexStream = null;
    try {
      fieldsStream = d.openInput(IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION), context);
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_INDEX_EXTENSION);
      indexStream = d.openInput(indexStreamFN, context);

      final String codecNameIdx = formatName + CODEC_SFX_IDX;
      final String codecNameDat = formatName + CODEC_SFX_DAT;
      CodecUtil.checkHeader(indexStream, codecNameIdx, VERSION_START, VERSION_CURRENT);
      CodecUtil.checkHeader(fieldsStream, codecNameDat, VERSION_START, VERSION_CURRENT);
      assert CodecUtil.headerLength(codecNameDat) == fieldsStream.getFilePointer();
      assert CodecUtil.headerLength(codecNameIdx) == indexStream.getFilePointer();

      indexReader = new CompressingStoredFieldsIndexReader(indexStream, si);
      indexStream = null;

      packedIntsVersion = fieldsStream.readVInt();
      decompressor = compressionMode.newDecompressor();
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

  /** 
   * Close the underlying {@link IndexInput}s.
   */
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
    if (docID < docBase
        || docID >= docBase + chunkDocs
        || docBase + chunkDocs > numDocs) {
      throw new CorruptIndexException("Corrupted: docID=" + docID
          + ", docBase=" + docBase + ", chunkDocs=" + chunkDocs
          + ", numDocs=" + numDocs);
    }

    final int numStoredFields, offset, length, totalLength;
    if (chunkDocs == 1) {
      numStoredFields = fieldsStream.readVInt();
      offset = 0;
      length = fieldsStream.readVInt();
      totalLength = length;
    } else {
      final int bitsPerStoredFields = fieldsStream.readVInt();
      if (bitsPerStoredFields == 0) {
        numStoredFields = fieldsStream.readVInt();
      } else if (bitsPerStoredFields > 31) {
        throw new CorruptIndexException("bitsPerStoredFields=" + bitsPerStoredFields);
      } else {
        final long filePointer = fieldsStream.getFilePointer();
        final PackedInts.Reader reader = PackedInts.getDirectReaderNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerStoredFields);
        numStoredFields = (int) (reader.get(docID - docBase));
        fieldsStream.seek(filePointer + PackedInts.Format.PACKED.byteCount(packedIntsVersion, chunkDocs, bitsPerStoredFields));
      }

      final int bitsPerLength = fieldsStream.readVInt();
      if (bitsPerLength == 0) {
        length = fieldsStream.readVInt();
        offset = (docID - docBase) * length;
        totalLength = chunkDocs * length;
      } else if (bitsPerStoredFields > 31) {
        throw new CorruptIndexException("bitsPerLength=" + bitsPerLength);
      } else {
        final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerLength, 1);
        int off = 0;
        for (int i = 0; i < docID - docBase; ++i) {
          off += it.next();
        }
        offset = off;
        length = (int) it.next();
        off += length;
        for (int i = docID - docBase + 1; i < chunkDocs; ++i) {
          off += it.next();
        }
        totalLength = off;
      }
    }

    if ((length == 0) != (numStoredFields == 0)) {
      throw new CorruptIndexException("length=" + length + ", numStoredFields=" + numStoredFields);
    }
    if (numStoredFields == 0) {
      // nothing to do
      return;
    }

    decompressor.decompress(fieldsStream, totalLength, offset, length, bytes);
    assert bytes.length == length;

    final ByteArrayDataInput documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);
    for (int fieldIDX = 0; fieldIDX < numStoredFields; fieldIDX++) {
      final long infoAndBits = documentInput.readVLong();
      final int fieldNumber = (int) (infoAndBits >>> TYPE_BITS);
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);

      final int bits = (int) (infoAndBits & TYPE_MASK);
      assert bits <= NUMERIC_DOUBLE: "bits=" + Integer.toHexString(bits);

      switch(visitor.needsField(fieldInfo)) {
        case YES:
          readField(documentInput, visitor, fieldInfo, bits);
          assert documentInput.getPosition() <= bytes.offset + bytes.length : documentInput.getPosition() + " " + bytes.offset + bytes.length;
          break;
        case NO:
          skipField(documentInput, bits);
          assert documentInput.getPosition() <= bytes.offset + bytes.length : documentInput.getPosition() + " " + bytes.offset + bytes.length;
          break;
        case STOP:
          return;
      }
    }
    assert documentInput.getPosition() == bytes.offset + bytes.length : documentInput.getPosition() + " " + bytes.offset + " " + bytes.length;
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
    int[] numStoredFields;
    int[] lengths;

    private ChunkIterator() {
      this.docBase = -1;
      bytes = new BytesRef();
      numStoredFields = new int[1];
      lengths = new int[1];
    }

    /**
     * Return the decompressed size of the chunk
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
      fieldsStream.seek(indexReader.getStartPointer(doc));

      final int docBase = fieldsStream.readVInt();
      final int chunkDocs = fieldsStream.readVInt();
      if (docBase < this.docBase + this.chunkDocs
          || docBase + chunkDocs > numDocs) {
        throw new CorruptIndexException("Corrupted: current docBase=" + this.docBase
            + ", current numDocs=" + this.chunkDocs + ", new docBase=" + docBase
            + ", new numDocs=" + chunkDocs);
      }
      this.docBase = docBase;
      this.chunkDocs = chunkDocs;

      if (chunkDocs > numStoredFields.length) {
        final int newLength = ArrayUtil.oversize(chunkDocs, 4);
        numStoredFields = new int[newLength];
        lengths = new int[newLength];
      }

      if (chunkDocs == 1) {
        numStoredFields[0] = fieldsStream.readVInt();
        lengths[0] = fieldsStream.readVInt();
      } else {
        final int bitsPerStoredFields = fieldsStream.readVInt();
        if (bitsPerStoredFields == 0) {
          Arrays.fill(numStoredFields, 0, chunkDocs, fieldsStream.readVInt());
        } else if (bitsPerStoredFields > 31) {
          throw new CorruptIndexException("bitsPerStoredFields=" + bitsPerStoredFields);
        } else {
          final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerStoredFields, 1);
          for (int i = 0; i < chunkDocs; ++i) {
            numStoredFields[i] = (int) it.next();
          }
        }

        final int bitsPerLength = fieldsStream.readVInt();
        if (bitsPerLength == 0) {
          Arrays.fill(lengths, 0, chunkDocs, fieldsStream.readVInt());
        } else if (bitsPerLength > 31) {
          throw new CorruptIndexException("bitsPerLength=" + bitsPerLength);
        } else {
          final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerLength, 1);
          for (int i = 0; i < chunkDocs; ++i) {
            lengths[i] = (int) it.next();
          }
        }
      }
    }

    /**
     * Decompress the chunk.
     */
    void decompress() throws IOException {
      // decompress data
      final int chunkSize = chunkSize();
      decompressor.decompress(fieldsStream, chunkSize, 0, chunkSize, bytes);
      if (bytes.length != chunkSize) {
        throw new CorruptIndexException("Corrupted: expected chunk size = " + chunkSize() + ", got " + bytes.length);
      }
    }

    /**
     * Copy compressed data.
     */
    void copyCompressedData(DataOutput out) throws IOException {
      final long chunkEnd = docBase + chunkDocs == numDocs
          ? fieldsStream.length()
          : indexReader.getStartPointer(docBase + chunkDocs);
      out.copyBytes(fieldsStream, chunkEnd - fieldsStream.getFilePointer());
    }

  }

}

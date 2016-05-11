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
package org.apache.lucene.codecs.lucene41;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link StoredFieldsReader} impl for {@code Lucene41StoredFieldsFormat}.
 * @deprecated only for reading old segments
 */
@Deprecated
final class Lucene41StoredFieldsReader extends StoredFieldsReader {

  // Do not reuse the decompression buffer when there is more than 32kb to decompress
  private static final int BUFFER_REUSE_THRESHOLD = 1 << 15;
  
  static final int         STRING = 0x00;
  static final int       BYTE_ARR = 0x01;
  static final int    NUMERIC_INT = 0x02;
  static final int  NUMERIC_FLOAT = 0x03;
  static final int   NUMERIC_LONG = 0x04;
  static final int NUMERIC_DOUBLE = 0x05;
  
  static final String CODEC_SFX_IDX = "Index";
  static final String CODEC_SFX_DAT = "Data";
  
  static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE);
  static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);
  
  static final int VERSION_START = 0;
  static final int VERSION_BIG_CHUNKS = 1;
  static final int VERSION_CHECKSUM = 2;
  static final int VERSION_CURRENT = VERSION_CHECKSUM;
  
  /** Extension of stored fields file */
  public static final String FIELDS_EXTENSION = "fdt";
  
  /** Extension of stored fields index file */
  public static final String FIELDS_INDEX_EXTENSION = "fdx";

  private final int version;
  private final FieldInfos fieldInfos;
  private final Lucene41StoredFieldsIndexReader indexReader;
  private final long maxPointer;
  private final IndexInput fieldsStream;
  private final int chunkSize;
  private final int packedIntsVersion;
  private final CompressionMode compressionMode;
  private final Decompressor decompressor;
  private final BytesRef bytes;
  private final int numDocs;
  private boolean closed;

  // used by clone
  private Lucene41StoredFieldsReader(Lucene41StoredFieldsReader reader) {
    this.version = reader.version;
    this.fieldInfos = reader.fieldInfos;
    this.fieldsStream = reader.fieldsStream.clone();
    this.indexReader = reader.indexReader.clone();
    this.maxPointer = reader.maxPointer;
    this.chunkSize = reader.chunkSize;
    this.packedIntsVersion = reader.packedIntsVersion;
    this.compressionMode = reader.compressionMode;
    this.decompressor = reader.decompressor.clone();
    this.numDocs = reader.numDocs;
    this.bytes = new BytesRef(reader.bytes.bytes.length);
    this.closed = false;
  }

  /** Sole constructor. */
  public Lucene41StoredFieldsReader(Directory d, SegmentInfo si, String segmentSuffix, FieldInfos fn,
      IOContext context, String formatName, CompressionMode compressionMode) throws IOException {
    this.compressionMode = compressionMode;
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    numDocs = si.maxDoc();
    ChecksumIndexInput indexStream = null;
    try {
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_INDEX_EXTENSION);
      final String fieldsStreamFN = IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION);
      // Load the index into memory
      indexStream = d.openChecksumInput(indexStreamFN, context);
      final String codecNameIdx = formatName + CODEC_SFX_IDX;
      version = CodecUtil.checkHeader(indexStream, codecNameIdx, VERSION_START, VERSION_CURRENT);
      assert CodecUtil.headerLength(codecNameIdx) == indexStream.getFilePointer();
      indexReader = new Lucene41StoredFieldsIndexReader(indexStream, si);

      long maxPointer = -1;
      
      if (version >= VERSION_CHECKSUM) {
        maxPointer = indexStream.readVLong();
        CodecUtil.checkFooter(indexStream);
      } else {
        CodecUtil.checkEOF(indexStream);
      }
      indexStream.close();
      indexStream = null;

      // Open the data file and read metadata
      fieldsStream = d.openInput(fieldsStreamFN, context);
      if (version >= VERSION_CHECKSUM) {
        if (maxPointer + CodecUtil.footerLength() != fieldsStream.length()) {
          throw new CorruptIndexException("Invalid fieldsStream maxPointer (file truncated?): maxPointer=" + maxPointer + ", length=" + fieldsStream.length(), fieldsStream);
        }
      } else {
        maxPointer = fieldsStream.length();
      }
      this.maxPointer = maxPointer;
      final String codecNameDat = formatName + CODEC_SFX_DAT;
      final int fieldsVersion = CodecUtil.checkHeader(fieldsStream, codecNameDat, VERSION_START, VERSION_CURRENT);
      if (version != fieldsVersion) {
        throw new CorruptIndexException("Version mismatch between stored fields index and data: " + version + " != " + fieldsVersion, fieldsStream);
      }
      assert CodecUtil.headerLength(codecNameDat) == fieldsStream.getFilePointer();

      if (version >= VERSION_BIG_CHUNKS) {
        chunkSize = fieldsStream.readVInt();
      } else {
        chunkSize = -1;
      }
      packedIntsVersion = fieldsStream.readVInt();
      decompressor = compressionMode.newDecompressor();
      this.bytes = new BytesRef();
      
      if (version >= VERSION_CHECKSUM) {
        // NOTE: data file is too costly to verify checksum against all the bytes on open,
        // but for now we at least verify proper structure of the checksum footer: which looks
        // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
        // such as file truncation.
        CodecUtil.retrieveChecksum(fieldsStream);
      }

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
      IOUtils.close(fieldsStream);
      closed = true;
    }
  }

  private static void readField(DataInput in, StoredFieldVisitor visitor, FieldInfo info, int bits) throws IOException {
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
        visitor.stringField(info, data);
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

  private static void skipField(DataInput in, int bits) throws IOException {
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
          + ", numDocs=" + numDocs, fieldsStream);
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
        throw new CorruptIndexException("bitsPerStoredFields=" + bitsPerStoredFields, fieldsStream);
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
        throw new CorruptIndexException("bitsPerLength=" + bitsPerLength, fieldsStream);
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
      throw new CorruptIndexException("length=" + length + ", numStoredFields=" + numStoredFields, fieldsStream);
    }
    if (numStoredFields == 0) {
      // nothing to do
      return;
    }

    final DataInput documentInput;
    if (version >= VERSION_BIG_CHUNKS && totalLength >= 2 * chunkSize) {
      assert chunkSize > 0;
      assert offset < chunkSize;

      decompressor.decompress(fieldsStream, chunkSize, offset, Math.min(length, chunkSize - offset), bytes);
      documentInput = new DataInput() {

        int decompressed = bytes.length;

        void fillBuffer() throws IOException {
          assert decompressed <= length;
          if (decompressed == length) {
            throw new EOFException();
          }
          final int toDecompress = Math.min(length - decompressed, chunkSize);
          decompressor.decompress(fieldsStream, toDecompress, 0, toDecompress, bytes);
          decompressed += toDecompress;
        }

        @Override
        public byte readByte() throws IOException {
          if (bytes.length == 0) {
            fillBuffer();
          }
          --bytes.length;
          return bytes.bytes[bytes.offset++];
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
          while (len > bytes.length) {
            System.arraycopy(bytes.bytes, bytes.offset, b, offset, bytes.length);
            len -= bytes.length;
            offset += bytes.length;
            fillBuffer();
          }
          System.arraycopy(bytes.bytes, bytes.offset, b, offset, len);
          bytes.offset += len;
          bytes.length -= len;
        }

      };
    } else {
      final BytesRef bytes = totalLength <= BUFFER_REUSE_THRESHOLD ? this.bytes : new BytesRef();
      decompressor.decompress(fieldsStream, totalLength, offset, length, bytes);
      assert bytes.length == length;
      documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);
    }

    for (int fieldIDX = 0; fieldIDX < numStoredFields; fieldIDX++) {
      final long infoAndBits = documentInput.readVLong();
      final int fieldNumber = (int) (infoAndBits >>> TYPE_BITS);
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);

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
    return new Lucene41StoredFieldsReader(this);
  }

  @Override
  public long ramBytesUsed() {
    return indexReader.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.singleton(Accountables.namedAccountable("stored field index", indexReader));
  }

  @Override
  public void checkIntegrity() throws IOException {
    if (version >= VERSION_CHECKSUM) {
      CodecUtil.checksumEntireFile(fieldsStream);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(mode=" + compressionMode + ",chunksize=" + chunkSize + ")";
  }
}

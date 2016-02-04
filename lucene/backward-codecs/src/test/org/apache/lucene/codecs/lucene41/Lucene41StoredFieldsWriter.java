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

import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.BYTE_ARR;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.CODEC_SFX_DAT;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.CODEC_SFX_IDX;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.FIELDS_INDEX_EXTENSION;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.NUMERIC_DOUBLE;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.NUMERIC_FLOAT;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.NUMERIC_INT;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.NUMERIC_LONG;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.STRING;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.TYPE_BITS;
import static org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsReader.VERSION_CURRENT;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.codecs.compressing.GrowableByteArrayDataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Writer for 4.1 stored fields format for testing
 * @deprecated for test purposes only
 */
@Deprecated
final class Lucene41StoredFieldsWriter extends StoredFieldsWriter {

  // hard limit on the maximum number of documents per chunk
  static final int MAX_DOCUMENTS_PER_CHUNK = 128;

  private final Directory directory;
  private final String segment;
  private final String segmentSuffix;
  private Lucene41StoredFieldsIndexWriter indexWriter;
  private IndexOutput fieldsStream;

  private final Compressor compressor;
  private final int chunkSize;

  private final GrowableByteArrayDataOutput bufferedDocs;
  private int[] numStoredFields; // number of stored fields
  private int[] endOffsets; // end offsets in bufferedDocs
  private int docBase; // doc ID at the beginning of the chunk
  private int numBufferedDocs; // docBase + numBufferedDocs == current doc ID

  /** Sole constructor. */
  public Lucene41StoredFieldsWriter(Directory directory, SegmentInfo si, String segmentSuffix, IOContext context,
      String formatName, CompressionMode compressionMode, int chunkSize) throws IOException {
    assert directory != null;
    this.directory = directory;
    this.segment = si.name;
    this.segmentSuffix = segmentSuffix;
    this.compressor = compressionMode.newCompressor();
    this.chunkSize = chunkSize;
    this.docBase = 0;
    this.bufferedDocs = new GrowableByteArrayDataOutput(chunkSize);
    this.numStoredFields = new int[16];
    this.endOffsets = new int[16];
    this.numBufferedDocs = 0;

    boolean success = false;
    IndexOutput indexStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_INDEX_EXTENSION), 
                                                                     context);
    try {
      fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION),
                                                    context);

      final String codecNameIdx = formatName + CODEC_SFX_IDX;
      final String codecNameDat = formatName + CODEC_SFX_DAT;
      CodecUtil.writeHeader(indexStream, codecNameIdx, VERSION_CURRENT);
      CodecUtil.writeHeader(fieldsStream, codecNameDat, VERSION_CURRENT);
      assert CodecUtil.headerLength(codecNameDat) == fieldsStream.getFilePointer();
      assert CodecUtil.headerLength(codecNameIdx) == indexStream.getFilePointer();

      indexWriter = new Lucene41StoredFieldsIndexWriter(indexStream);
      indexStream = null;

      fieldsStream.writeVInt(chunkSize);
      fieldsStream.writeVInt(PackedInts.VERSION_CURRENT);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(fieldsStream, indexStream, indexWriter);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(fieldsStream, indexWriter);
    } finally {
      fieldsStream = null;
      indexWriter = null;
    }
  }

  private int numStoredFieldsInDoc;

  @Override
  public void startDocument() throws IOException {
  }

  @Override
  public void finishDocument() throws IOException {
    if (numBufferedDocs == this.numStoredFields.length) {
      final int newLength = ArrayUtil.oversize(numBufferedDocs + 1, 4);
      this.numStoredFields = Arrays.copyOf(this.numStoredFields, newLength);
      endOffsets = Arrays.copyOf(endOffsets, newLength);
    }
    this.numStoredFields[numBufferedDocs] = numStoredFieldsInDoc;
    numStoredFieldsInDoc = 0;
    endOffsets[numBufferedDocs] = bufferedDocs.length;
    ++numBufferedDocs;
    if (triggerFlush()) {
      flush();
    }
  }

  private static void saveInts(int[] values, int length, DataOutput out) throws IOException {
    assert length > 0;
    if (length == 1) {
      out.writeVInt(values[0]);
    } else {
      boolean allEqual = true;
      for (int i = 1; i < length; ++i) {
        if (values[i] != values[0]) {
          allEqual = false;
          break;
        }
      }
      if (allEqual) {
        out.writeVInt(0);
        out.writeVInt(values[0]);
      } else {
        long max = 0;
        for (int i = 0; i < length; ++i) {
          max |= values[i];
        }
        final int bitsRequired = PackedInts.bitsRequired(max);
        out.writeVInt(bitsRequired);
        final PackedInts.Writer w = PackedInts.getWriterNoHeader(out, PackedInts.Format.PACKED, length, bitsRequired, 1);
        for (int i = 0; i < length; ++i) {
          w.add(values[i]);
        }
        w.finish();
      }
    }
  }

  private void writeHeader(int docBase, int numBufferedDocs, int[] numStoredFields, int[] lengths) throws IOException {
    // save docBase and numBufferedDocs
    fieldsStream.writeVInt(docBase);
    fieldsStream.writeVInt(numBufferedDocs);

    // save numStoredFields
    saveInts(numStoredFields, numBufferedDocs, fieldsStream);

    // save lengths
    saveInts(lengths, numBufferedDocs, fieldsStream);
  }

  private boolean triggerFlush() {
    return bufferedDocs.length >= chunkSize || // chunks of at least chunkSize bytes
        numBufferedDocs >= MAX_DOCUMENTS_PER_CHUNK;
  }

  private void flush() throws IOException {
    indexWriter.writeIndex(numBufferedDocs, fieldsStream.getFilePointer());

    // transform end offsets into lengths
    final int[] lengths = endOffsets;
    for (int i = numBufferedDocs - 1; i > 0; --i) {
      lengths[i] = endOffsets[i] - endOffsets[i - 1];
      assert lengths[i] >= 0;
    }
    writeHeader(docBase, numBufferedDocs, numStoredFields, lengths);

    // compress stored fields to fieldsStream
    if (bufferedDocs.length >= 2 * chunkSize) {
      // big chunk, slice it
      for (int compressed = 0; compressed < bufferedDocs.length; compressed += chunkSize) {
        compressor.compress(bufferedDocs.bytes, compressed, Math.min(chunkSize, bufferedDocs.length - compressed), fieldsStream);
      }
    } else {
      compressor.compress(bufferedDocs.bytes, 0, bufferedDocs.length, fieldsStream);
    }

    // reset
    docBase += numBufferedDocs;
    numBufferedDocs = 0;
    bufferedDocs.length = 0;
  }

  @Override
  public void writeField(FieldInfo info, IndexableField field)
      throws IOException {

    ++numStoredFieldsInDoc;

    int bits = 0;
    final BytesRef bytes;
    final String string;

    Number number = field.numericValue();
    if (number != null) {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        bits = NUMERIC_INT;
      } else if (number instanceof Long) {
        bits = NUMERIC_LONG;
      } else if (number instanceof Float) {
        bits = NUMERIC_FLOAT;
      } else if (number instanceof Double) {
        bits = NUMERIC_DOUBLE;
      } else {
        throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
      }
      string = null;
      bytes = null;
    } else {
      bytes = field.binaryValue();
      if (bytes != null) {
        bits = BYTE_ARR;
        string = null;
      } else {
        bits = STRING;
        string = field.stringValue();
        if (string == null) {
          throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
        }
      }
    }

    final long infoAndBits = (((long) info.number) << TYPE_BITS) | bits;
    bufferedDocs.writeVLong(infoAndBits);

    if (bytes != null) {
      bufferedDocs.writeVInt(bytes.length);
      bufferedDocs.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    } else if (string != null) {
      bufferedDocs.writeString(field.stringValue());
    } else {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        bufferedDocs.writeInt(number.intValue());
      } else if (number instanceof Long) {
        bufferedDocs.writeLong(number.longValue());
      } else if (number instanceof Float) {
        bufferedDocs.writeInt(Float.floatToIntBits(number.floatValue()));
      } else if (number instanceof Double) {
        bufferedDocs.writeLong(Double.doubleToLongBits(number.doubleValue()));
      } else {
        throw new AssertionError("Cannot get here");
      }
    }
  }

  @Override
  public void finish(FieldInfos fis, int numDocs) throws IOException {
    if (numBufferedDocs > 0) {
      flush();
    } else {
      assert bufferedDocs.length == 0;
    }
    if (docBase != numDocs) {
      throw new RuntimeException("Wrote " + docBase + " docs, finish called with numDocs=" + numDocs);
    }
    indexWriter.finish(numDocs, fieldsStream.getFilePointer());
    CodecUtil.writeFooter(fieldsStream);
    assert bufferedDocs.length == 0;
  }
}

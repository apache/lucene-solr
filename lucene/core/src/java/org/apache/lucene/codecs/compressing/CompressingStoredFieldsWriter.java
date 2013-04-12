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

import static org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsWriter.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsWriter.FIELDS_INDEX_EXTENSION;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsReader.ChunkIterator;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link StoredFieldsWriter} impl for {@link CompressingStoredFieldsFormat}.
 * @lucene.experimental
 */
public final class CompressingStoredFieldsWriter extends StoredFieldsWriter {

  // hard limit on the maximum number of documents per chunk
  static final int MAX_DOCUMENTS_PER_CHUNK = 128;

  static final int         STRING = 0x00;
  static final int       BYTE_ARR = 0x01;
  static final int    NUMERIC_INT = 0x02;
  static final int  NUMERIC_FLOAT = 0x03;
  static final int   NUMERIC_LONG = 0x04;
  static final int NUMERIC_DOUBLE = 0x05;

  static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE);
  static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);

  static final String CODEC_SFX_IDX = "Index";
  static final String CODEC_SFX_DAT = "Data";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  private final Directory directory;
  private final String segment;
  private final String segmentSuffix;
  private CompressingStoredFieldsIndexWriter indexWriter;
  private IndexOutput fieldsStream;

  private final CompressionMode compressionMode;
  private final Compressor compressor;
  private final int chunkSize;

  private final GrowableByteArrayDataOutput bufferedDocs;
  private int[] numStoredFields; // number of stored fields
  private int[] endOffsets; // end offsets in bufferedDocs
  private int docBase; // doc ID at the beginning of the chunk
  private int numBufferedDocs; // docBase + numBufferedDocs == current doc ID

  /** Sole constructor. */
  public CompressingStoredFieldsWriter(Directory directory, SegmentInfo si, String segmentSuffix, IOContext context,
      String formatName, CompressionMode compressionMode, int chunkSize) throws IOException {
    assert directory != null;
    this.directory = directory;
    this.segment = si.name;
    this.segmentSuffix = segmentSuffix;
    this.compressionMode = compressionMode;
    this.compressor = compressionMode.newCompressor();
    this.chunkSize = chunkSize;
    this.docBase = 0;
    this.bufferedDocs = new GrowableByteArrayDataOutput(chunkSize);
    this.numStoredFields = new int[16];
    this.endOffsets = new int[16];
    this.numBufferedDocs = 0;

    boolean success = false;
    IndexOutput indexStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_INDEX_EXTENSION), context);
    try {
      fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION), context);

      final String codecNameIdx = formatName + CODEC_SFX_IDX;
      final String codecNameDat = formatName + CODEC_SFX_DAT;
      CodecUtil.writeHeader(indexStream, codecNameIdx, VERSION_CURRENT);
      CodecUtil.writeHeader(fieldsStream, codecNameDat, VERSION_CURRENT);
      assert CodecUtil.headerLength(codecNameDat) == fieldsStream.getFilePointer();
      assert CodecUtil.headerLength(codecNameIdx) == indexStream.getFilePointer();

      indexWriter = new CompressingStoredFieldsIndexWriter(indexStream);
      indexStream = null;

      fieldsStream.writeVInt(PackedInts.VERSION_CURRENT);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(indexStream);
        abort();
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

  @Override
  public void startDocument(int numStoredFields) throws IOException {
    if (numBufferedDocs == this.numStoredFields.length) {
      final int newLength = ArrayUtil.oversize(numBufferedDocs + 1, 4);
      this.numStoredFields = Arrays.copyOf(this.numStoredFields, newLength);
      endOffsets = Arrays.copyOf(endOffsets, newLength);
    }
    this.numStoredFields[numBufferedDocs] = numStoredFields;
    ++numBufferedDocs;
  }

  @Override
  public void finishDocument() throws IOException {
    endOffsets[numBufferedDocs - 1] = bufferedDocs.length;
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
    compressor.compress(bufferedDocs.bytes, 0, bufferedDocs.length, fieldsStream);

    // reset
    docBase += numBufferedDocs;
    numBufferedDocs = 0;
    bufferedDocs.length = 0;
  }

  @Override
  public void writeField(FieldInfo info, IndexableField field)
      throws IOException {
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
  public void abort() {
    IOUtils.closeWhileHandlingException(this);
    IOUtils.deleteFilesIgnoringExceptions(directory,
        IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION),
        IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_INDEX_EXTENSION));
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
    indexWriter.finish(numDocs);
    assert bufferedDocs.length == 0;
  }

  @Override
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0;
    int idx = 0;

    for (AtomicReader reader : mergeState.readers) {
      final SegmentReader matchingSegmentReader = mergeState.matchingSegmentReaders[idx++];
      CompressingStoredFieldsReader matchingFieldsReader = null;
      if (matchingSegmentReader != null) {
        final StoredFieldsReader fieldsReader = matchingSegmentReader.getFieldsReader();
        // we can only bulk-copy if the matching reader is also a CompressingStoredFieldsReader
        if (fieldsReader != null && fieldsReader instanceof CompressingStoredFieldsReader) {
          matchingFieldsReader = (CompressingStoredFieldsReader) fieldsReader;
        }
      }

      final int maxDoc = reader.maxDoc();
      final Bits liveDocs = reader.getLiveDocs();

      if (matchingFieldsReader == null) {
        // naive merge...
        for (int i = nextLiveDoc(0, liveDocs, maxDoc); i < maxDoc; i = nextLiveDoc(i + 1, liveDocs, maxDoc)) {
          Document doc = reader.document(i);
          addDocument(doc, mergeState.fieldInfos);
          ++docCount;
          mergeState.checkAbort.work(300);
        }
      } else {
        int docID = nextLiveDoc(0, liveDocs, maxDoc);
        if (docID < maxDoc) {
          // not all docs were deleted
          final ChunkIterator it = matchingFieldsReader.chunkIterator(docID);
          int[] startOffsets = new int[0];
          do {
            // go to the next chunk that contains docID
            it.next(docID);
            // transform lengths into offsets
            if (startOffsets.length < it.chunkDocs) {
              startOffsets = new int[ArrayUtil.oversize(it.chunkDocs, 4)];
            }
            for (int i = 1; i < it.chunkDocs; ++i) {
              startOffsets[i] = startOffsets[i - 1] + it.lengths[i - 1];
            }

            if (compressionMode == matchingFieldsReader.getCompressionMode() // same compression mode
                && numBufferedDocs == 0 // starting a new chunk
                && startOffsets[it.chunkDocs - 1] < chunkSize // chunk is small enough
                && startOffsets[it.chunkDocs - 1] + it.lengths[it.chunkDocs - 1] >= chunkSize // chunk is large enough
                && nextDeletedDoc(it.docBase, liveDocs, it.docBase + it.chunkDocs) == it.docBase + it.chunkDocs) { // no deletion in the chunk
              assert docID == it.docBase;

              // no need to decompress, just copy data
              indexWriter.writeIndex(it.chunkDocs, fieldsStream.getFilePointer());
              writeHeader(this.docBase, it.chunkDocs, it.numStoredFields, it.lengths);
              it.copyCompressedData(fieldsStream);
              this.docBase += it.chunkDocs;
              docID = nextLiveDoc(it.docBase + it.chunkDocs, liveDocs, maxDoc);
              docCount += it.chunkDocs;
              mergeState.checkAbort.work(300 * it.chunkDocs);
            } else {
              // decompress
              it.decompress();
              if (startOffsets[it.chunkDocs - 1] + it.lengths[it.chunkDocs - 1] != it.bytes.length) {
                throw new CorruptIndexException("Corrupted: expected chunk size=" + startOffsets[it.chunkDocs - 1] + it.lengths[it.chunkDocs - 1] + ", got " + it.bytes.length);
              }
              // copy non-deleted docs
              for (; docID < it.docBase + it.chunkDocs; docID = nextLiveDoc(docID + 1, liveDocs, maxDoc)) {
                final int diff = docID - it.docBase;
                startDocument(it.numStoredFields[diff]);
                bufferedDocs.writeBytes(it.bytes.bytes, it.bytes.offset + startOffsets[diff], it.lengths[diff]);
                finishDocument();
                ++docCount;
                mergeState.checkAbort.work(300);
              }
            }
          } while (docID < maxDoc);
        }
      }
    }
    finish(mergeState.fieldInfos, docCount);
    return docCount;
  }

  private static int nextLiveDoc(int doc, Bits liveDocs, int maxDoc) {
    if (liveDocs == null) {
      return doc;
    }
    while (doc < maxDoc && !liveDocs.get(doc)) {
      ++doc;
    }
    return doc;
  }

  private static int nextDeletedDoc(int doc, Bits liveDocs, int maxDoc) {
    if (liveDocs == null) {
      return maxDoc;
    }
    while (doc < maxDoc && liveDocs.get(doc)) {
      ++doc;
    }
    return doc;
  }

}

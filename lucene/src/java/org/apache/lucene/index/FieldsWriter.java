package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

final class FieldsWriter {
  // NOTE: bit 0 is free here!  You can steal it!
  static final int FIELD_IS_BINARY = 1 << 1;

  // the old bit 1 << 2 was compressed, is now left out

  private static final int _NUMERIC_BIT_SHIFT = 3;
  static final int FIELD_IS_NUMERIC_MASK = 0x07 << _NUMERIC_BIT_SHIFT;

  static final int FIELD_IS_NUMERIC_INT = 1 << _NUMERIC_BIT_SHIFT;
  static final int FIELD_IS_NUMERIC_LONG = 2 << _NUMERIC_BIT_SHIFT;
  static final int FIELD_IS_NUMERIC_FLOAT = 3 << _NUMERIC_BIT_SHIFT;
  static final int FIELD_IS_NUMERIC_DOUBLE = 4 << _NUMERIC_BIT_SHIFT;
  // currently unused: static final int FIELD_IS_NUMERIC_SHORT = 5 << _NUMERIC_BIT_SHIFT;
  // currently unused: static final int FIELD_IS_NUMERIC_BYTE = 6 << _NUMERIC_BIT_SHIFT;

  // the next possible bits are: 1 << 6; 1 << 7
  
  // Lucene 3.0: Removal of compressed fields
  static final int FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS = 2;

  // Lucene 3.2: NumericFields are stored in binary format
  static final int FORMAT_LUCENE_3_2_NUMERIC_FIELDS = 3;

  // NOTE: if you introduce a new format, make it 1 higher
  // than the current one, and always change this if you
  // switch to a new format!
  static final int FORMAT_CURRENT = FORMAT_LUCENE_3_2_NUMERIC_FIELDS;

  // when removing support for old versions, leave the last supported version here
  static final int FORMAT_MINIMUM = FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS;

  // If null - we were supplied with streams, if notnull - we manage them ourselves
  private Directory directory;
  private String segment;
  private IndexOutput fieldsStream;
  private IndexOutput indexStream;

  FieldsWriter(Directory directory, String segment, IOContext context) throws IOException {
    this.directory = directory;
    this.segment = segment;

    boolean success = false;
    try {
      fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_EXTENSION), context);
      indexStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_INDEX_EXTENSION), context);

      fieldsStream.writeInt(FORMAT_CURRENT);
      indexStream.writeInt(FORMAT_CURRENT);

      success = true;
    } finally {
      if (!success) {
        abort();
      }
    }
  }

  FieldsWriter(IndexOutput fdx, IndexOutput fdt) {
    directory = null;
    segment = null;
    fieldsStream = fdt;
    indexStream = fdx;
  }

  void setFieldsStream(IndexOutput stream) {
    this.fieldsStream = stream;
  }

  // Writes the contents of buffer into the fields stream
  // and adds a new entry for this document into the index
  // stream.  This assumes the buffer was already written
  // in the correct fields format.
  void startDocument(int numStoredFields) throws IOException {
    indexStream.writeLong(fieldsStream.getFilePointer());
    fieldsStream.writeVInt(numStoredFields);
  }

  void skipDocument() throws IOException {
    indexStream.writeLong(fieldsStream.getFilePointer());
    fieldsStream.writeVInt(0);
  }

  void close() throws IOException {
    if (directory != null) {
      try {
        IOUtils.closeSafely(false, fieldsStream, indexStream);
      } finally {
        fieldsStream = indexStream = null;
      }
    }
  }

  void abort() {
    if (directory != null) {
      try {
        close();
      } catch (IOException ignored) {
      }
      try {
        directory.deleteFile(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_EXTENSION));
      } catch (IOException ignored) {
      }
      try {
        directory.deleteFile(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_INDEX_EXTENSION));
      } catch (IOException ignored) {
      }
    }
  }

  final void writeField(int fieldNumber, IndexableField field) throws IOException {
    fieldsStream.writeVInt(fieldNumber);
    int bits = 0;
    final BytesRef bytes;
    final String string;
    // TODO: maybe a field should serialize itself?
    // this way we don't bake into indexer all these
    // specific encodings for different fields?  and apps
    // can customize...
    if (field.numeric()) {
      switch (field.numericDataType()) {
        case INT:
          bits |= FIELD_IS_NUMERIC_INT; break;
        case LONG:
          bits |= FIELD_IS_NUMERIC_LONG; break;
        case FLOAT:
          bits |= FIELD_IS_NUMERIC_FLOAT; break;
        case DOUBLE:
          bits |= FIELD_IS_NUMERIC_DOUBLE; break;
        default:
          assert false : "Should never get here";
      }
      string = null;
      bytes = null;
    } else {
      bytes = field.binaryValue();
      if (bytes != null) {
        bits |= FIELD_IS_BINARY;
        string = null;
      } else {
        string = field.stringValue();
      }
    }

    fieldsStream.writeByte((byte) bits);

    if (bytes != null) {
      fieldsStream.writeVInt(bytes.length);
      fieldsStream.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    } else if (string != null) {
      fieldsStream.writeString(field.stringValue());
    } else {
      final Number n = field.numericValue();
      if (n == null) {
        throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
      }
      switch (field.numericDataType()) {
        case INT:
          fieldsStream.writeInt(n.intValue()); break;
        case LONG:
          fieldsStream.writeLong(n.longValue()); break;
        case FLOAT:
          fieldsStream.writeInt(Float.floatToIntBits(n.floatValue())); break;
        case DOUBLE:
          fieldsStream.writeLong(Double.doubleToLongBits(n.doubleValue())); break;
        default:
          assert false : "Should never get here";
      }
    }
  }

  /** Bulk write a contiguous series of documents.  The
   *  lengths array is the length (in bytes) of each raw
   *  document.  The stream IndexInput is the
   *  fieldsStream from which we should bulk-copy all
   *  bytes. */
  final void addRawDocuments(IndexInput stream, int[] lengths, int numDocs) throws IOException {
    long position = fieldsStream.getFilePointer();
    long start = position;
    for(int i=0;i<numDocs;i++) {
      indexStream.writeLong(position);
      position += lengths[i];
    }
    fieldsStream.copyBytes(stream, position-start);
    assert fieldsStream.getFilePointer() == position;
  }

  final void addDocument(Iterable<? extends IndexableField> doc, FieldInfos fieldInfos) throws IOException {
    indexStream.writeLong(fieldsStream.getFilePointer());

    int storedCount = 0;
    for (IndexableField field : doc) {
      if (field.stored()) {
        storedCount++;
      }
    }
    fieldsStream.writeVInt(storedCount);

    for (IndexableField field : doc) {
      if (field.stored()) {
        writeField(fieldInfos.fieldNumber(field.name()), field);
      }
    }
  }
}

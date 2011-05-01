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
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

final class FieldsWriter {
  static final byte FIELD_IS_TOKENIZED = 0x1;
  static final byte FIELD_IS_BINARY = 0x2;

  // Lucene 3.0: Removal of compressed fields
  static final int FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS = 2;

  // NOTE: if you introduce a new format, make it 1 higher
  // than the current one, and always change this if you
  // switch to a new format!
  static final int FORMAT_CURRENT = FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS;

  // when removing support for old versions, leave the last supported version here
  static final int FORMAT_MINIMUM = FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS;

  // If null - we were supplied with streams, if notnull - we manage them ourselves
  private Directory directory;
  private String segment;
  private IndexOutput fieldsStream;
  private IndexOutput indexStream;

  FieldsWriter(Directory directory, String segment) throws IOException {
    this.directory = directory;
    this.segment = segment;

    boolean success = false;
    try {
      fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_EXTENSION));
      indexStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.FIELDS_INDEX_EXTENSION));

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
        IOUtils.closeSafely(fieldsStream, indexStream);
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

  final void writeField(int fieldNumber, Fieldable field) throws IOException {
    fieldsStream.writeVInt(fieldNumber);
    byte bits = 0;
    if (field.isTokenized())
      bits |= FieldsWriter.FIELD_IS_TOKENIZED;
    if (field.isBinary())
      bits |= FieldsWriter.FIELD_IS_BINARY;

    fieldsStream.writeByte(bits);

    if (field.isBinary()) {
      final byte[] data;
      final int len;
      final int offset;
      data = field.getBinaryValue();
      len = field.getBinaryLength();
      offset =  field.getBinaryOffset();

      fieldsStream.writeVInt(len);
      fieldsStream.writeBytes(data, offset, len);
    }
    else {
      fieldsStream.writeString(field.stringValue());
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

  final void addDocument(Document doc, FieldInfos fieldInfos) throws IOException {
    indexStream.writeLong(fieldsStream.getFilePointer());

    int storedCount = 0;
    List<Fieldable> fields = doc.getFields();
    for (Fieldable field : fields) {
      if (field.isStored())
          storedCount++;
    }
    fieldsStream.writeVInt(storedCount);


    for (Fieldable field : fields) {
      if (field.isStored())
        writeField(fieldInfos.fieldNumber(field.name()), field);
    }
  }
}

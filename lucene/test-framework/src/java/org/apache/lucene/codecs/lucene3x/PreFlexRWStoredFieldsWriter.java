package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** @lucene.experimental */
final class PreFlexRWStoredFieldsWriter extends StoredFieldsWriter {
  private final Directory directory;
  private final String segment;
  private IndexOutput fieldsStream;
  private IndexOutput indexStream;

  public PreFlexRWStoredFieldsWriter(Directory directory, String segment, IOContext context) throws IOException {
    assert directory != null;
    this.directory = directory;
    this.segment = segment;

    boolean success = false;
    try {
      fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene3xStoredFieldsReader.FIELDS_EXTENSION), context);
      indexStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", Lucene3xStoredFieldsReader.FIELDS_INDEX_EXTENSION), context);

      fieldsStream.writeInt(Lucene3xStoredFieldsReader.FORMAT_CURRENT);
      indexStream.writeInt(Lucene3xStoredFieldsReader.FORMAT_CURRENT);

      success = true;
    } finally {
      if (!success) {
        abort();
      }
    }
  }

  // Writes the contents of buffer into the fields stream
  // and adds a new entry for this document into the index
  // stream.  This assumes the buffer was already written
  // in the correct fields format.
  public void startDocument(int numStoredFields) throws IOException {
    indexStream.writeLong(fieldsStream.getFilePointer());
    fieldsStream.writeVInt(numStoredFields);
  }

  public void close() throws IOException {
    try {
      IOUtils.close(fieldsStream, indexStream);
    } finally {
      fieldsStream = indexStream = null;
    }
  }

  public void abort() {
    try {
      close();
    } catch (Throwable ignored) {}
    IOUtils.deleteFilesIgnoringExceptions(directory,
        IndexFileNames.segmentFileName(segment, "", Lucene3xStoredFieldsReader.FIELDS_EXTENSION),
        IndexFileNames.segmentFileName(segment, "", Lucene3xStoredFieldsReader.FIELDS_INDEX_EXTENSION));
  }

  public void writeField(FieldInfo info, IndexableField field) throws IOException {
    fieldsStream.writeVInt(info.number);
    int bits = 0;
    final BytesRef bytes;
    final String string;
    // TODO: maybe a field should serialize itself?
    // this way we don't bake into indexer all these
    // specific encodings for different fields?  and apps
    // can customize...

    Number number = field.numericValue();
    if (number != null) {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        bits |= Lucene3xStoredFieldsReader.FIELD_IS_NUMERIC_INT;
      } else if (number instanceof Long) {
        bits |= Lucene3xStoredFieldsReader.FIELD_IS_NUMERIC_LONG;
      } else if (number instanceof Float) {
        bits |= Lucene3xStoredFieldsReader.FIELD_IS_NUMERIC_FLOAT;
      } else if (number instanceof Double) {
        bits |= Lucene3xStoredFieldsReader.FIELD_IS_NUMERIC_DOUBLE;
      } else {
        throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
      }
      string = null;
      bytes = null;
    } else {
      bytes = field.binaryValue();
      if (bytes != null) {
        bits |= Lucene3xStoredFieldsReader.FIELD_IS_BINARY;
        string = null;
      } else {
        string = field.stringValue();
        if (string == null) {
          throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
        }
      }
    }

    fieldsStream.writeByte((byte) bits);

    if (bytes != null) {
      fieldsStream.writeVInt(bytes.length);
      fieldsStream.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    } else if (string != null) {
      fieldsStream.writeString(field.stringValue());
    } else {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        fieldsStream.writeInt(number.intValue());
      } else if (number instanceof Long) {
        fieldsStream.writeLong(number.longValue());
      } else if (number instanceof Float) {
        fieldsStream.writeInt(Float.floatToIntBits(number.floatValue()));
      } else if (number instanceof Double) {
        fieldsStream.writeLong(Double.doubleToLongBits(number.doubleValue()));
      } else {
        assert false;
      }
    }
  }

  @Override
  public void finish(FieldInfos fis, int numDocs) throws IOException {
    if (4+((long) numDocs)*8 != indexStream.getFilePointer())
      // This is most likely a bug in Sun JRE 1.6.0_04/_05;
      // we detect that the bug has struck, here, and
      // throw an exception to prevent the corruption from
      // entering the index.  See LUCENE-1282 for
      // details.
      throw new RuntimeException("fdx size mismatch: docCount is " + numDocs + " but fdx file size is " + indexStream.getFilePointer() + " file=" + indexStream.toString() + "; now aborting this merge to prevent index corruption");
  }
}

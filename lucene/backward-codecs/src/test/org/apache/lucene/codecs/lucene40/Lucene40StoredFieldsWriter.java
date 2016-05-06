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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsReader.*;


/**
 * Writer for 4.0 stored fields format for testing
 * @deprecated for test purposes only
 */
@Deprecated
final class Lucene40StoredFieldsWriter extends StoredFieldsWriter {

  private final Directory directory;
  private final String segment;
  private IndexOutput fieldsStream;
  private IndexOutput indexStream;
  private final RAMOutputStream fieldsBuffer = new RAMOutputStream();

  /** Sole constructor. */
  public Lucene40StoredFieldsWriter(Directory directory, String segment, IOContext context) throws IOException {
    assert directory != null;
    this.directory = directory;
    this.segment = segment;

    boolean success = false;
    try {
      fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", FIELDS_EXTENSION), context);
      indexStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", FIELDS_INDEX_EXTENSION), context);

      CodecUtil.writeHeader(fieldsStream, CODEC_NAME_DAT, VERSION_CURRENT);
      CodecUtil.writeHeader(indexStream, CODEC_NAME_IDX, VERSION_CURRENT);
      assert HEADER_LENGTH_DAT == fieldsStream.getFilePointer();
      assert HEADER_LENGTH_IDX == indexStream.getFilePointer();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  int numStoredFields;

  // Writes the contents of buffer into the fields stream
  // and adds a new entry for this document into the index
  // stream.  This assumes the buffer was already written
  // in the correct fields format.
  @Override
  public void startDocument() throws IOException {
    indexStream.writeLong(fieldsStream.getFilePointer());
  }

  @Override
  public void finishDocument() throws IOException {
    fieldsStream.writeVInt(numStoredFields);
    fieldsBuffer.writeTo(fieldsStream);
    fieldsBuffer.reset();
    numStoredFields = 0;
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(fieldsStream, indexStream);
    } finally {
      fieldsStream = indexStream = null;
    }
  }

  @Override
  public void writeField(FieldInfo info, IndexableField field) throws IOException {
    numStoredFields++;

    fieldsBuffer.writeVInt(info.number);
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
        bits |= FIELD_IS_NUMERIC_INT;
      } else if (number instanceof Long) {
        bits |= FIELD_IS_NUMERIC_LONG;
      } else if (number instanceof Float) {
        bits |= FIELD_IS_NUMERIC_FLOAT;
      } else if (number instanceof Double) {
        bits |= FIELD_IS_NUMERIC_DOUBLE;
      } else {
        throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
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
        if (string == null) {
          throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
        }
      }
    }

    fieldsBuffer.writeByte((byte) bits);

    if (bytes != null) {
      fieldsBuffer.writeVInt(bytes.length);
      fieldsBuffer.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    } else if (string != null) {
      fieldsBuffer.writeString(field.stringValue());
    } else {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        fieldsBuffer.writeInt(number.intValue());
      } else if (number instanceof Long) {
        fieldsBuffer.writeLong(number.longValue());
      } else if (number instanceof Float) {
        fieldsBuffer.writeInt(Float.floatToIntBits(number.floatValue()));
      } else if (number instanceof Double) {
        fieldsBuffer.writeLong(Double.doubleToLongBits(number.doubleValue()));
      } else {
        throw new AssertionError("Cannot get here");
      }
    }
  }

  @Override
  public void finish(FieldInfos fis, int numDocs) {
    long indexFP = indexStream.getFilePointer();
    if (HEADER_LENGTH_IDX+((long) numDocs)*8 != indexFP)
      // This is most likely a bug in Sun JRE 1.6.0_04/_05;
      // we detect that the bug has struck, here, and
      // throw an exception to prevent the corruption from
      // entering the index.  See LUCENE-1282 for
      // details.
      throw new RuntimeException("fdx size mismatch: docCount is " + numDocs + " but fdx file size is " + indexFP + " (wrote numDocs=" + ((indexFP-HEADER_LENGTH_IDX)/8.0) + " file=" + indexStream.toString() + "; now aborting this merge to prevent index corruption");
  }
}

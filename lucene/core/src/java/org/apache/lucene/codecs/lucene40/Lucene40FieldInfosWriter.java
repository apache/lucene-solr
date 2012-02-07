package org.apache.lucene.codecs.lucene40;

/**
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
import java.io.IOException;

import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/**
 * @lucene.experimental
 */
public class Lucene40FieldInfosWriter extends FieldInfosWriter {
  
  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "fnm";
  
  // per-field codec support, records index values for fields
  static final int FORMAT_START = -4;

  // whenever you add a new format, make it 1 smaller (negative version logic)!
  static final int FORMAT_CURRENT = FORMAT_START;
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_OFFSETS_IN_POSTINGS = 0x4;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  static final byte OMIT_POSITIONS = -128;
  
  @Override
  public void write(Directory directory, String segmentName, FieldInfos infos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", FIELD_INFOS_EXTENSION);
    IndexOutput output = directory.createOutput(fileName, context);
    try {
      output.writeVInt(FORMAT_CURRENT);
      output.writeVInt(infos.size());
      for (FieldInfo fi : infos) {
        assert fi.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !fi.storePayloads;
        byte bits = 0x0;
        if (fi.isIndexed) bits |= IS_INDEXED;
        if (fi.storeTermVector) bits |= STORE_TERMVECTOR;
        if (fi.omitNorms) bits |= OMIT_NORMS;
        if (fi.storePayloads) bits |= STORE_PAYLOADS;
        if (fi.indexOptions == IndexOptions.DOCS_ONLY) {
          bits |= OMIT_TERM_FREQ_AND_POSITIONS;
        } else if (fi.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
          bits |= STORE_OFFSETS_IN_POSTINGS;
        } else if (fi.indexOptions == IndexOptions.DOCS_AND_FREQS) {
          bits |= OMIT_POSITIONS;
        }
        output.writeString(fi.name);
        output.writeInt(fi.number);
        output.writeByte(bits);

        // pack the DV types in one byte
        final byte dv = docValuesByte(fi.getDocValuesType());
        final byte nrm = docValuesByte(fi.getNormType());
        assert (dv & (~0xF)) == 0 && (nrm & (~0x0F)) == 0;
        byte val = (byte) (0xff & ((nrm << 4) | dv));
        output.writeByte(val);
      }
    } finally {
      output.close();
    }
  }

  public byte docValuesByte(Type type) {
    if (type == null) {
      return 0;
    } else {
      switch(type) {
      case VAR_INTS:
        return 1;
      case FLOAT_32:
        return 2;
      case FLOAT_64:
        return 3;
      case BYTES_FIXED_STRAIGHT:
        return 4;
      case BYTES_FIXED_DEREF:
        return 5;
      case BYTES_VAR_STRAIGHT:
        return 6;
      case BYTES_VAR_DEREF:
        return 7;
      case FIXED_INTS_16:
        return 8;
      case FIXED_INTS_32:
        return 9;
      case FIXED_INTS_64:
        return 10;
      case FIXED_INTS_8:
        return 11;
      case BYTES_FIXED_SORTED:
        return 12;
      case BYTES_VAR_SORTED:
        return 13;
      default:
        throw new IllegalStateException("unhandled indexValues type " + type);
      }
    }
  }
  
}

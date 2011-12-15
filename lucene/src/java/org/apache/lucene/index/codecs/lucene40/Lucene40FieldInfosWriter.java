package org.apache.lucene.index.codecs.lucene40;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.codecs.FieldInfosWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/**
 * @lucene.experimental
 */
public class Lucene40FieldInfosWriter extends FieldInfosWriter {
  
  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "fnm";
  
  // First used in 2.9; prior to 2.9 there was no format header
  static final int FORMAT_START = -2;
  // First used in 3.4: omit only positional information
  static final int FORMAT_OMIT_POSITIONS = -3;
  // per-field codec support, records index values for fields
  static final int FORMAT_FLEX = -4;

  // whenever you add a new format, make it 1 smaller (negative version logic)!
  static final int FORMAT_CURRENT = FORMAT_FLEX;
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x4;
  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x8;
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
        assert fi.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS || !fi.storePayloads;
        byte bits = 0x0;
        if (fi.isIndexed) bits |= IS_INDEXED;
        if (fi.storeTermVector) bits |= STORE_TERMVECTOR;
        if (fi.storePositionWithTermVector) bits |= STORE_POSITIONS_WITH_TERMVECTOR;
        if (fi.storeOffsetWithTermVector) bits |= STORE_OFFSET_WITH_TERMVECTOR;
        if (fi.omitNorms) bits |= OMIT_NORMS;
        if (fi.storePayloads) bits |= STORE_PAYLOADS;
        if (fi.indexOptions == IndexOptions.DOCS_ONLY)
          bits |= OMIT_TERM_FREQ_AND_POSITIONS;
        else if (fi.indexOptions == IndexOptions.DOCS_AND_FREQS)
          bits |= OMIT_POSITIONS;
        output.writeString(fi.name);
        output.writeInt(fi.number);
        output.writeByte(bits);

        final byte b;

        if (!fi.hasDocValues()) {
          b = 0;
        } else {
          switch(fi.getDocValuesType()) {
          case VAR_INTS:
            b = 1;
            break;
          case FLOAT_32:
            b = 2;
            break;
          case FLOAT_64:
            b = 3;
            break;
          case BYTES_FIXED_STRAIGHT:
            b = 4;
            break;
          case BYTES_FIXED_DEREF:
            b = 5;
            break;
          case BYTES_VAR_STRAIGHT:
            b = 6;
            break;
          case BYTES_VAR_DEREF:
            b = 7;
            break;
          case FIXED_INTS_16:
            b = 8;
            break;
          case FIXED_INTS_32:
            b = 9;
            break;
          case FIXED_INTS_64:
            b = 10;
            break;
          case FIXED_INTS_8:
            b = 11;
            break;
          case BYTES_FIXED_SORTED:
            b = 12;
            break;
          case BYTES_VAR_SORTED:
            b = 13;
            break;
          default:
            throw new IllegalStateException("unhandled indexValues type " + fi.getDocValuesType());
          }
        }
        output.writeByte(b);
      }
    } finally {
      output.close();
    }
  }
  
}

package org.apache.lucene.codecs.lucene3x;

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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/**
 * @lucene.internal
 * @lucene.experimental
 */
class PreFlexRWFieldInfosWriter extends FieldInfosWriter {
  // TODO move to test-framework preflex RW?
  
  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "fnm";
  
  // First used in 2.9; prior to 2.9 there was no format header
  static final int FORMAT_START = -2;
  // First used in 3.4: omit only positional information
  static final int FORMAT_OMIT_POSITIONS = -3;
  
  static final int FORMAT_PREFLEX_RW = Integer.MIN_VALUE;

  // whenever you add a new format, make it 1 smaller (negative version logic)!
  static final int FORMAT_CURRENT = FORMAT_OMIT_POSITIONS;
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  static final byte OMIT_POSITIONS = -128;
  
  @Override
  public void write(Directory directory, String segmentName, FieldInfos infos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", FIELD_INFOS_EXTENSION);
    IndexOutput output = directory.createOutput(fileName, context);
    try {
      output.writeVInt(FORMAT_PREFLEX_RW);
      output.writeVInt(infos.size());
      for (FieldInfo fi : infos) {
        assert fi.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS || !fi.storePayloads;
        byte bits = 0x0;
        if (fi.isIndexed) bits |= IS_INDEXED;
        if (fi.storeTermVector) bits |= STORE_TERMVECTOR;
        if (fi.omitNorms) bits |= OMIT_NORMS;
        if (fi.storePayloads) bits |= STORE_PAYLOADS;
        if (fi.indexOptions == IndexOptions.DOCS_ONLY) {
          bits |= OMIT_TERM_FREQ_AND_POSITIONS;
        } else if (fi.indexOptions == IndexOptions.DOCS_AND_FREQS) {
          bits |= OMIT_POSITIONS;
        }
        output.writeString(fi.name);
        /*
         * we need to write the field number since IW tries
         * to stabelize the field numbers across segments so the
         * FI ordinal is not necessarily equivalent to the field number 
         */
        output.writeInt(fi.number);
        output.writeByte(bits);
        if (fi.isIndexed && !fi.omitNorms) {
          // to allow null norm types we need to indicate if norms are written 
          // only in RW case
          output.writeByte((byte) (fi.getNormType() == null ? 0 : 1));
        }
      }
    } finally {
      output.close();
    }
  }
  
}

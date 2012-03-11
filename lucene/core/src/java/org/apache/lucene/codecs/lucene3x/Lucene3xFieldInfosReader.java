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
import java.util.Set;

import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * @lucene.experimental
 * @deprecated
 */
@Deprecated
class Lucene3xFieldInfosReader extends FieldInfosReader {
  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "fnm";
  
  // First used in 2.9; prior to 2.9 there was no format header
  static final int FORMAT_START = -2;
  // First used in 3.4: omit only positional information
  static final int FORMAT_OMIT_POSITIONS = -3;
  static final int FORMAT_MINIMUM = FORMAT_START;
  static final int FORMAT_CURRENT = FORMAT_OMIT_POSITIONS;
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  static final byte OMIT_POSITIONS = -128;

  @Override
  public FieldInfos read(Directory directory, String segmentName, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", FIELD_INFOS_EXTENSION);
    IndexInput input = directory.openInput(fileName, iocontext);

    boolean hasVectors = false;
    boolean hasFreq = false;
    boolean hasProx = false;
    
    try {
      final int format = input.readVInt();

      if (format > FORMAT_MINIMUM) {
        throw new IndexFormatTooOldException(input, format, FORMAT_MINIMUM, FORMAT_CURRENT);
      }
      if (format < FORMAT_CURRENT) {
        throw new IndexFormatTooNewException(input, format, FORMAT_MINIMUM, FORMAT_CURRENT);
      }

      final int size = input.readVInt(); //read in the size
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        String name = input.readString();
        final int fieldNumber = i;
        byte bits = input.readByte();
        boolean isIndexed = (bits & IS_INDEXED) != 0;
        boolean storeTermVector = (bits & STORE_TERMVECTOR) != 0;
        boolean omitNorms = (bits & OMIT_NORMS) != 0;
        boolean storePayloads = (bits & STORE_PAYLOADS) != 0;
        final IndexOptions indexOptions;
        if ((bits & OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_ONLY;
        } else if ((bits & OMIT_POSITIONS) != 0) {
          if (format <= FORMAT_OMIT_POSITIONS) {
            indexOptions = IndexOptions.DOCS_AND_FREQS;
          } else {
            throw new CorruptIndexException("Corrupt fieldinfos, OMIT_POSITIONS set but format=" + format + " (resource: " + input + ")");
          }
        } else {
          indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }

        // LUCENE-3027: past indices were able to write
        // storePayloads=true when omitTFAP is also true,
        // which is invalid.  We correct that, here:
        if (indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
          storePayloads = false;
        }
        hasVectors |= storeTermVector;
        hasProx |= isIndexed && indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        hasFreq |= isIndexed && indexOptions != IndexOptions.DOCS_ONLY;
        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          omitNorms, storePayloads, indexOptions, null, isIndexed && !omitNorms? Type.FIXED_INTS_8 : null);
      }

      if (input.getFilePointer() != input.length()) {
        throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length() + " (resource: " + input + ")");
      }
      return new FieldInfos(infos, hasFreq, hasProx, hasVectors);
    } finally {
      input.close();
    }
  }
  
  public static void files(SegmentInfo info, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", FIELD_INFOS_EXTENSION));
  }
}

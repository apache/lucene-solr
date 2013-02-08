package org.apache.lucene.codecs.lucene3x;
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
import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo.DocValuesType;
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
 * @lucene.internal
 * @lucene.experimental
 */
class PreFlexRWFieldInfosReader extends FieldInfosReader {
  static final int FORMAT_MINIMUM = PreFlexRWFieldInfosWriter.FORMAT_START;

  @Override
  public FieldInfos read(Directory directory, String segmentName, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", PreFlexRWFieldInfosWriter.FIELD_INFOS_EXTENSION);
    IndexInput input = directory.openInput(fileName, iocontext);
    
    try {
      final int format = input.readVInt();

      if (format > FORMAT_MINIMUM) {
        throw new IndexFormatTooOldException(input, format, FORMAT_MINIMUM, PreFlexRWFieldInfosWriter.FORMAT_CURRENT);
      }
      if (format < PreFlexRWFieldInfosWriter.FORMAT_CURRENT && format != PreFlexRWFieldInfosWriter.FORMAT_PREFLEX_RW) {
        throw new IndexFormatTooNewException(input, format, FORMAT_MINIMUM, PreFlexRWFieldInfosWriter.FORMAT_CURRENT);
      }

      final int size = input.readVInt(); //read in the size
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        String name = input.readString();
        final int fieldNumber = format == PreFlexRWFieldInfosWriter.FORMAT_PREFLEX_RW ? input.readInt() : i;
        byte bits = input.readByte();
        boolean isIndexed = (bits & PreFlexRWFieldInfosWriter.IS_INDEXED) != 0;
        boolean storeTermVector = (bits & PreFlexRWFieldInfosWriter.STORE_TERMVECTOR) != 0;
        boolean omitNorms = (bits & PreFlexRWFieldInfosWriter.OMIT_NORMS) != 0;
        boolean storePayloads = (bits & PreFlexRWFieldInfosWriter.STORE_PAYLOADS) != 0;
        final IndexOptions indexOptions;
        if (!isIndexed) {
          indexOptions = null;
        } else if ((bits & PreFlexRWFieldInfosWriter.OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_ONLY;
        } else if ((bits & PreFlexRWFieldInfosWriter.OMIT_POSITIONS) != 0) {
          if (format <= PreFlexRWFieldInfosWriter.FORMAT_OMIT_POSITIONS) {
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
        
        DocValuesType normType = isIndexed && !omitNorms ? DocValuesType.NUMERIC : null;
        if (format == PreFlexRWFieldInfosWriter.FORMAT_PREFLEX_RW && normType != null) {
          // RW can have norms but doesn't write them
          normType = input.readByte() != 0 ? DocValuesType.NUMERIC : null;
        }
        
        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          omitNorms, storePayloads, indexOptions, null, normType, null);
      }

      if (input.getFilePointer() != input.length()) {
        throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length() + " (resource: " + input + ")");
      }
      return new FieldInfos(infos);
    } finally {
      input.close();
    }
  }
  
  public static void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", PreFlexRWFieldInfosWriter.FIELD_INFOS_EXTENSION));
  }
}

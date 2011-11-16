package org.apache.lucene.index.codecs;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.values.ValueType;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

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

/**
 * @lucene.experimental
 */
public class DefaultFieldInfosReader extends FieldInfosReader {

  static final int FORMAT_MINIMUM = DefaultFieldInfosWriter.FORMAT_START;

  @Override
  public FieldInfos read(Directory directory, String segmentName, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", DefaultFieldInfosWriter.FIELD_INFOS_EXTENSION);
    IndexInput input = directory.openInput(fileName, iocontext);

    boolean hasVectors = false;
    boolean hasFreq = false;
    boolean hasProx = false;
    
    try {
      final int format = input.readVInt();

      if (format > FORMAT_MINIMUM) {
        throw new IndexFormatTooOldException(input, format, FORMAT_MINIMUM, DefaultFieldInfosWriter.FORMAT_CURRENT);
      }
      if (format < DefaultFieldInfosWriter.FORMAT_CURRENT) {
        throw new IndexFormatTooNewException(input, format, FORMAT_MINIMUM, DefaultFieldInfosWriter.FORMAT_CURRENT);
      }

      final int size = input.readVInt(); //read in the size
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        String name = input.readString();
        final int fieldNumber = format <= DefaultFieldInfosWriter.FORMAT_FLEX? input.readInt():i;
        byte bits = input.readByte();
        boolean isIndexed = (bits & DefaultFieldInfosWriter.IS_INDEXED) != 0;
        boolean storeTermVector = (bits & DefaultFieldInfosWriter.STORE_TERMVECTOR) != 0;
        boolean storePositionsWithTermVector = (bits & DefaultFieldInfosWriter.STORE_POSITIONS_WITH_TERMVECTOR) != 0;
        boolean storeOffsetWithTermVector = (bits & DefaultFieldInfosWriter.STORE_OFFSET_WITH_TERMVECTOR) != 0;
        boolean omitNorms = (bits & DefaultFieldInfosWriter.OMIT_NORMS) != 0;
        boolean storePayloads = (bits & DefaultFieldInfosWriter.STORE_PAYLOADS) != 0;
        final IndexOptions indexOptions;
        if ((bits & DefaultFieldInfosWriter.OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_ONLY;
        } else if ((bits & DefaultFieldInfosWriter.OMIT_POSITIONS) != 0) {
          if (format <= DefaultFieldInfosWriter.FORMAT_OMIT_POSITIONS) {
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
        ValueType docValuesType = null;
        if (format <= DefaultFieldInfosWriter.FORMAT_FLEX) {
          final byte b = input.readByte();
          switch(b) {
            case 0:
              docValuesType = null;
              break;
            case 1:
              docValuesType = ValueType.VAR_INTS;
              break;
            case 2:
              docValuesType = ValueType.FLOAT_32;
              break;
            case 3:
              docValuesType = ValueType.FLOAT_64;
              break;
            case 4:
              docValuesType = ValueType.BYTES_FIXED_STRAIGHT;
              break;
            case 5:
              docValuesType = ValueType.BYTES_FIXED_DEREF;
              break;
            case 6:
              docValuesType = ValueType.BYTES_VAR_STRAIGHT;
              break;
            case 7:
              docValuesType = ValueType.BYTES_VAR_DEREF;
              break;
            case 8:
              docValuesType = ValueType.FIXED_INTS_16;
              break;
            case 9:
              docValuesType = ValueType.FIXED_INTS_32;
              break;
            case 10:
              docValuesType = ValueType.FIXED_INTS_64;
              break;
            case 11:
              docValuesType = ValueType.FIXED_INTS_8;
              break;
            case 12:
              docValuesType = ValueType.BYTES_FIXED_SORTED;
              break;
            case 13:
              docValuesType = ValueType.BYTES_VAR_SORTED;
              break;
        
            default:
              throw new IllegalStateException("unhandled indexValues type " + b);
          }
        }
        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          storePositionsWithTermVector, storeOffsetWithTermVector, 
          omitNorms, storePayloads, indexOptions, docValuesType);
      }

      if (input.getFilePointer() != input.length()) {
        throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length() + " (resource: " + input + ")");
      }
      
      return new FieldInfos(infos, hasFreq, hasProx, hasVectors);
    } finally {
      input.close();
    }
  }
  
  public static void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", DefaultFieldInfosWriter.FIELD_INFOS_EXTENSION));
  }
}

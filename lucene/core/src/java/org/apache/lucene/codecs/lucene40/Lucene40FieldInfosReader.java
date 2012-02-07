package org.apache.lucene.codecs.lucene40;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.DocValues;
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
public class Lucene40FieldInfosReader extends FieldInfosReader {

  static final int FORMAT_MINIMUM = Lucene40FieldInfosWriter.FORMAT_START;

  @Override
  public FieldInfos read(Directory directory, String segmentName, IOContext iocontext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, "", Lucene40FieldInfosWriter.FIELD_INFOS_EXTENSION);
    IndexInput input = directory.openInput(fileName, iocontext);

    boolean hasVectors = false;
    boolean hasFreq = false;
    boolean hasProx = false;
    
    try {
      final int format = input.readVInt();

      if (format > FORMAT_MINIMUM) {
        throw new IndexFormatTooOldException(input, format, FORMAT_MINIMUM, Lucene40FieldInfosWriter.FORMAT_CURRENT);
      }
      if (format < Lucene40FieldInfosWriter.FORMAT_CURRENT) {
        throw new IndexFormatTooNewException(input, format, FORMAT_MINIMUM, Lucene40FieldInfosWriter.FORMAT_CURRENT);
      }

      final int size = input.readVInt(); //read in the size
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        String name = input.readString();
        final int fieldNumber = input.readInt();
        byte bits = input.readByte();
        boolean isIndexed = (bits & Lucene40FieldInfosWriter.IS_INDEXED) != 0;
        boolean storeTermVector = (bits & Lucene40FieldInfosWriter.STORE_TERMVECTOR) != 0;
        boolean omitNorms = (bits & Lucene40FieldInfosWriter.OMIT_NORMS) != 0;
        boolean storePayloads = (bits & Lucene40FieldInfosWriter.STORE_PAYLOADS) != 0;
        final IndexOptions indexOptions;
        if ((bits & Lucene40FieldInfosWriter.OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_ONLY;
        } else if ((bits & Lucene40FieldInfosWriter.OMIT_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_AND_FREQS;
        } else if ((bits & Lucene40FieldInfosWriter.STORE_OFFSETS_IN_POSTINGS) != 0) {
          indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else {
          indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }

        // LUCENE-3027: past indices were able to write
        // storePayloads=true when omitTFAP is also true,
        // which is invalid.  We correct that, here:
        if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
          storePayloads = false;
        }
        hasVectors |= storeTermVector;
        hasProx |= isIndexed && indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        hasFreq |= isIndexed && indexOptions != IndexOptions.DOCS_ONLY;
        // DV Types are packed in one byte
        byte val = input.readByte();
        final DocValues.Type docValuesType = getDocValuesType((byte) (val & 0x0F));
        final DocValues.Type normsType = getDocValuesType((byte) ((val >>> 4) & 0x0F));
        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          omitNorms, storePayloads, indexOptions, docValuesType, normsType);
      }

      if (input.getFilePointer() != input.length()) {
        throw new CorruptIndexException("did not read all bytes from file \"" + fileName + "\": read " + input.getFilePointer() + " vs size " + input.length() + " (resource: " + input + ")");
      }
      
      return new FieldInfos(infos, hasFreq, hasProx, hasVectors);
    } finally {
      input.close();
    }
  }

  public DocValues.Type getDocValuesType(
      final byte b) {
    switch(b) {
      case 0:
        return null;
      case 1:
        return DocValues.Type.VAR_INTS;
      case 2:
        return DocValues.Type.FLOAT_32;
      case 3:
        return DocValues.Type.FLOAT_64;
      case 4:
        return DocValues.Type.BYTES_FIXED_STRAIGHT;
      case 5:
        return DocValues.Type.BYTES_FIXED_DEREF;
      case 6:
        return DocValues.Type.BYTES_VAR_STRAIGHT;
      case 7:
        return DocValues.Type.BYTES_VAR_DEREF;
      case 8:
        return DocValues.Type.FIXED_INTS_16;
      case 9:
        return DocValues.Type.FIXED_INTS_32;
      case 10:
        return DocValues.Type.FIXED_INTS_64;
      case 11:
        return DocValues.Type.FIXED_INTS_8;
      case 12:
        return DocValues.Type.BYTES_FIXED_SORTED;
      case 13:
        return DocValues.Type.BYTES_VAR_SORTED;
      default:
        throw new IllegalStateException("unhandled indexValues type " + b);
    }
  }
  
  public static void files(SegmentInfo info, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", Lucene40FieldInfosWriter.FIELD_INFOS_EXTENSION));
  }
}

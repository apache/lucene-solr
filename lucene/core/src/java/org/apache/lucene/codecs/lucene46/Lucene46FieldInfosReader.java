package org.apache.lucene.codecs.lucene46;

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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 4.6 FieldInfos reader.
 * 
 * @lucene.experimental
 * @see Lucene46FieldInfosFormat
 */
final class Lucene46FieldInfosReader extends FieldInfosReader {

  /** Sole constructor. */
  public Lucene46FieldInfosReader() {
  }

  @Override
  public FieldInfos read(Directory directory, String segmentName, String segmentSuffix, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentName, segmentSuffix, Lucene46FieldInfosFormat.EXTENSION);
    ChecksumIndexInput input = directory.openChecksumInput(fileName, context);
    
    boolean success = false;
    try {
      int codecVersion = CodecUtil.checkHeader(input, Lucene46FieldInfosFormat.CODEC_NAME, 
                                                      Lucene46FieldInfosFormat.FORMAT_START, 
                                                      Lucene46FieldInfosFormat.FORMAT_CURRENT);

      final int size = input.readVInt(); //read in the size
      FieldInfo infos[] = new FieldInfo[size];

      for (int i = 0; i < size; i++) {
        String name = input.readString();
        final int fieldNumber = input.readVInt();
        if (fieldNumber < 0) {
          throw new CorruptIndexException("invalid field number for field: " + name + ", fieldNumber=" + fieldNumber + " (resource=" + input + ")");
        }
        byte bits = input.readByte();
        boolean isIndexed = (bits & Lucene46FieldInfosFormat.IS_INDEXED) != 0;
        boolean storeTermVector = (bits & Lucene46FieldInfosFormat.STORE_TERMVECTOR) != 0;
        boolean omitNorms = (bits & Lucene46FieldInfosFormat.OMIT_NORMS) != 0;
        boolean storePayloads = (bits & Lucene46FieldInfosFormat.STORE_PAYLOADS) != 0;
        final IndexOptions indexOptions;
        if (!isIndexed) {
          indexOptions = null;
        } else if ((bits & Lucene46FieldInfosFormat.OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_ONLY;
        } else if ((bits & Lucene46FieldInfosFormat.OMIT_POSITIONS) != 0) {
          indexOptions = IndexOptions.DOCS_AND_FREQS;
        } else if ((bits & Lucene46FieldInfosFormat.STORE_OFFSETS_IN_POSTINGS) != 0) {
          indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else {
          indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }

        // DV Types are packed in one byte
        byte val = input.readByte();
        final DocValuesType docValuesType = getDocValuesType(input, (byte) (val & 0x0F));
        final DocValuesType normsType = getDocValuesType(input, (byte) ((val >>> 4) & 0x0F));
        final long dvGen = input.readLong();
        final Map<String,String> attributes = input.readStringStringMap();
        infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, 
          omitNorms, storePayloads, indexOptions, docValuesType, normsType, dvGen, Collections.unmodifiableMap(attributes));
      }
      
      if (codecVersion >= Lucene46FieldInfosFormat.FORMAT_CHECKSUM) {
        CodecUtil.checkFooter(input);
      } else {
        CodecUtil.checkEOF(input);
      }
      FieldInfos fieldInfos = new FieldInfos(infos);
      success = true;
      return fieldInfos;
    } finally {
      if (success) {
        input.close();
      } else {
        IOUtils.closeWhileHandlingException(input);
      }
    }
  }
  
  private static DocValuesType getDocValuesType(IndexInput input, byte b) throws IOException {
    if (b == 0) {
      return null;
    } else if (b == 1) {
      return DocValuesType.NUMERIC;
    } else if (b == 2) {
      return DocValuesType.BINARY;
    } else if (b == 3) {
      return DocValuesType.SORTED;
    } else if (b == 4) {
      return DocValuesType.SORTED_SET;
    } else if (b == 5) {
      return DocValuesType.SORTED_NUMERIC;
    } else {
      throw new CorruptIndexException("invalid docvalues byte: " + b + " (resource=" + input + ")");
    }
  }
}

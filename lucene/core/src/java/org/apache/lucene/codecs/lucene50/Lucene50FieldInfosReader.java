package org.apache.lucene.codecs.lucene50;

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
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

/**
 * Lucene 5.0 FieldInfos reader.
 * 
 * @lucene.experimental
 * @see Lucene50FieldInfosFormat
 */
final class Lucene50FieldInfosReader extends FieldInfosReader {

  /** Sole constructor. */
  public Lucene50FieldInfosReader() {
  }

  @Override
  public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, Lucene50FieldInfosFormat.EXTENSION);
    try (ChecksumIndexInput input = directory.openChecksumInput(fileName, context)) {
      Throwable priorE = null;
      FieldInfo infos[] = null;
      try {
        CodecUtil.checkSegmentHeader(input, Lucene50FieldInfosFormat.CODEC_NAME, 
                                     Lucene50FieldInfosFormat.FORMAT_START, 
                                     Lucene50FieldInfosFormat.FORMAT_CURRENT,
                                     segmentInfo.getId());
        
        final int size = input.readVInt(); //read in the size
        infos = new FieldInfo[size];
        
        for (int i = 0; i < size; i++) {
          String name = input.readString();
          final int fieldNumber = input.readVInt();
          if (fieldNumber < 0) {
            throw new CorruptIndexException("invalid field number for field: " + name + ", fieldNumber=" + fieldNumber, input);
          }
          byte bits = input.readByte();
          boolean isIndexed = (bits & Lucene50FieldInfosFormat.IS_INDEXED) != 0;
          boolean storeTermVector = (bits & Lucene50FieldInfosFormat.STORE_TERMVECTOR) != 0;
          boolean omitNorms = (bits & Lucene50FieldInfosFormat.OMIT_NORMS) != 0;
          boolean storePayloads = (bits & Lucene50FieldInfosFormat.STORE_PAYLOADS) != 0;
          final IndexOptions indexOptions;
          if (!isIndexed) {
            indexOptions = null;
          } else if ((bits & Lucene50FieldInfosFormat.OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
            indexOptions = IndexOptions.DOCS_ONLY;
          } else if ((bits & Lucene50FieldInfosFormat.OMIT_POSITIONS) != 0) {
            indexOptions = IndexOptions.DOCS_AND_FREQS;
          } else if ((bits & Lucene50FieldInfosFormat.STORE_OFFSETS_IN_POSTINGS) != 0) {
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
          try {
            infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, omitNorms, storePayloads, 
                                     indexOptions, docValuesType, normsType, dvGen, Collections.unmodifiableMap(attributes));
            infos[i].checkConsistency();
          } catch (IllegalStateException e) {
            throw new CorruptIndexException("invalid fieldinfo for field: " + name + ", fieldNumber=" + fieldNumber, input, e);
          }
        }
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
      return new FieldInfos(infos);
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
      throw new CorruptIndexException("invalid docvalues byte: " + b, input);
    }
  }
}

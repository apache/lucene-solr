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
package org.apache.lucene.codecs.lucene86;


import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDReader;

/** Reads point values previously written with {@link Lucene86PointsWriter} */
public class Lucene86PointsReader extends PointsReader implements Closeable {
  final IndexInput indexIn, dataIn;
  final SegmentReadState readState;
  final Map<Integer,BKDReader> readers = new HashMap<>();

  /** Sole constructor */
  public Lucene86PointsReader(SegmentReadState readState) throws IOException {
    this.readState = readState;

    String metaFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name,
        readState.segmentSuffix,
        Lucene86PointsFormat.META_EXTENSION);
    String indexFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name,
        readState.segmentSuffix,
        Lucene86PointsFormat.INDEX_EXTENSION);
    String dataFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name,
        readState.segmentSuffix,
        Lucene86PointsFormat.DATA_EXTENSION);

    boolean success = false;
    try {
      indexIn = readState.directory.openInput(indexFileName, readState.context);
      CodecUtil.checkIndexHeader(indexIn,
          Lucene86PointsFormat.INDEX_CODEC_NAME,
          Lucene86PointsFormat.VERSION_START,
          Lucene86PointsFormat.VERSION_CURRENT,
          readState.segmentInfo.getId(),
          readState.segmentSuffix);
      CodecUtil.retrieveChecksum(indexIn);

      dataIn = readState.directory.openInput(dataFileName, readState.context);
      CodecUtil.checkIndexHeader(dataIn,
          Lucene86PointsFormat.DATA_CODEC_NAME,
          Lucene86PointsFormat.VERSION_START,
          Lucene86PointsFormat.VERSION_CURRENT,
          readState.segmentInfo.getId(),
          readState.segmentSuffix);
      CodecUtil.retrieveChecksum(dataIn);

      try (ChecksumIndexInput metaIn = readState.directory.openChecksumInput(metaFileName, readState.context)) {
        Throwable priorE = null;
        try {
          CodecUtil.checkIndexHeader(metaIn,
              Lucene86PointsFormat.META_CODEC_NAME,
              Lucene86PointsFormat.VERSION_START,
              Lucene86PointsFormat.VERSION_CURRENT,
              readState.segmentInfo.getId(),
              readState.segmentSuffix);

          while (true) {
            int fieldNumber = metaIn.readInt();
            if (fieldNumber == -1) {
              break;
            } else if (fieldNumber < 0) {
              throw new CorruptIndexException("Illegal field number: " + fieldNumber, metaIn);
            }
            BKDReader reader = new BKDReader(metaIn, indexIn, dataIn);
            readers.put(fieldNumber, reader);
          }
        } catch (Throwable t) {
          priorE = t;
        } finally {
          CodecUtil.checkFooter(metaIn, priorE);
        }
      }
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }

  }

  /** Returns the underlying {@link BKDReader}.
   *
   * @lucene.internal */
  @Override
  public PointValues getValues(String fieldName) {
    FieldInfo fieldInfo = readState.fieldInfos.fieldInfo(fieldName);
    if (fieldInfo == null) {
      throw new IllegalArgumentException("field=\"" + fieldName + "\" is unrecognized");
    }
    if (fieldInfo.getPointDimensionCount() == 0) {
      throw new IllegalArgumentException("field=\"" + fieldName + "\" did not index point values");
    }

    return readers.get(fieldInfo.number);
  }

  @Override
  public long ramBytesUsed() {
    return 0L;
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(indexIn);
    CodecUtil.checksumEntireFile(dataIn);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(indexIn, dataIn);
    // Free up heap:
    readers.clear();
  }

}
  

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
package org.apache.lucene.codecs.lucene60;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PointReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDReader;

/** Reads point values previously written with {@link Lucene60PointWriter} */
public class Lucene60PointReader extends PointReader implements Closeable {
  final IndexInput dataIn;
  final SegmentReadState readState;
  final Map<Integer,BKDReader> readers = new HashMap<>();

  /** Sole constructor */
  public Lucene60PointReader(SegmentReadState readState) throws IOException {
    this.readState = readState;


    String indexFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name,
                                                          readState.segmentSuffix,
                                                          Lucene60PointFormat.INDEX_EXTENSION);

    Map<Integer,Long> fieldToFileOffset = new HashMap<>();

    // Read index file
    try (ChecksumIndexInput indexIn = readState.directory.openChecksumInput(indexFileName, readState.context)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(indexIn,
                                   Lucene60PointFormat.META_CODEC_NAME,
                                   Lucene60PointFormat.INDEX_VERSION_START,
                                   Lucene60PointFormat.INDEX_VERSION_START,
                                   readState.segmentInfo.getId(),
                                   readState.segmentSuffix);
        int count = indexIn.readVInt();
        for(int i=0;i<count;i++) {
          int fieldNumber = indexIn.readVInt();
          long fp = indexIn.readVLong();
          fieldToFileOffset.put(fieldNumber, fp);
        }
      } catch (Throwable t) {
        priorE = t;
      } finally {
        CodecUtil.checkFooter(indexIn, priorE);
      }
    }

    String dataFileName = IndexFileNames.segmentFileName(readState.segmentInfo.name,
                                                         readState.segmentSuffix,
                                                         Lucene60PointFormat.DATA_EXTENSION);
    boolean success = false;
    dataIn = readState.directory.openInput(dataFileName, readState.context);
    try {

      CodecUtil.checkIndexHeader(dataIn,
                                 Lucene60PointFormat.DATA_CODEC_NAME,
                                 Lucene60PointFormat.DATA_VERSION_START,
                                 Lucene60PointFormat.DATA_VERSION_START,
                                 readState.segmentInfo.getId(),
                                 readState.segmentSuffix);

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(dataIn);

      for(Map.Entry<Integer,Long> ent : fieldToFileOffset.entrySet()) {
        int fieldNumber = ent.getKey();
        long fp = ent.getValue();
        dataIn.seek(fp);
        BKDReader reader = new BKDReader(dataIn);
        readers.put(fieldNumber, reader);
      }

      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private BKDReader getBKDReader(String fieldName) {
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
  public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
    BKDReader bkdReader = getBKDReader(fieldName);

    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this point field were deleted in this segment:
      return;
    }

    bkdReader.intersect(visitor);
  }

  @Override
  public long ramBytesUsed() {
    long sizeInBytes = 0;
    for(BKDReader reader : readers.values()) {
      sizeInBytes += reader.ramBytesUsed();
    }
    return sizeInBytes;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    for(Map.Entry<Integer,BKDReader> ent : readers.entrySet()) {
      resources.add(Accountables.namedAccountable(readState.fieldInfos.fieldInfo(ent.getKey()).name,
                                                  ent.getValue()));
    }
    return Collections.unmodifiableList(resources);
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(dataIn);
  }

  @Override
  public void close() throws IOException {
    dataIn.close();
    // Free up heap:
    readers.clear();
  }

  @Override
  public byte[] getMinPackedValue(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this point field were deleted in this segment:
      return null;
    }

    return bkdReader.getMinPackedValue();
  }

  @Override
  public byte[] getMaxPackedValue(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this point field were deleted in this segment:
      return null;
    }

    return bkdReader.getMaxPackedValue();
  }

  @Override
  public int getNumDimensions(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this point field were deleted in this segment:
      return 0;
    }
    return bkdReader.getNumDimensions();
  }

  @Override
  public int getBytesPerDimension(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this point field were deleted in this segment:
      return 0;
    }
    return bkdReader.getBytesPerDimension();
  }

  @Override
  public long size(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this point field were deleted in this segment:
      return 0;
    }
    return bkdReader.getPointCount();
  }

  @Override
  public int getDocCount(String fieldName) {
    BKDReader bkdReader = getBKDReader(fieldName);
    if (bkdReader == null) {
      // Schema ghost corner case!  This field did index points in the past, but
      // now all docs having this point field were deleted in this segment:
      return 0;
    }
    return bkdReader.getDocCount();
  }
}
  

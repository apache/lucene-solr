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

import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Lucene3x ReadOnly TermVectorsFormat implementation
 * @deprecated (4.0) This is only used to read indexes created
 * before 4.0.
 * @lucene.experimental
 */
@Deprecated
class Lucene3xTermVectorsFormat extends TermVectorsFormat {

  @Override
  public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(Lucene3xSegmentInfoFormat.getDocStoreSegment(segmentInfo), "", Lucene3xTermVectorsReader.VECTORS_FIELDS_EXTENSION);

    // Unfortunately, for 3.x indices, each segment's
    // FieldInfos can lie about hasVectors (claim it's true
    // when really it's false).... so we have to carefully
    // check if the files really exist before trying to open
    // them (4.x has fixed this):
    final boolean exists;
    if (Lucene3xSegmentInfoFormat.getDocStoreOffset(segmentInfo) != -1 && Lucene3xSegmentInfoFormat.getDocStoreIsCompoundFile(segmentInfo)) {
      String cfxFileName = IndexFileNames.segmentFileName(Lucene3xSegmentInfoFormat.getDocStoreSegment(segmentInfo), "", Lucene3xCodec.COMPOUND_FILE_STORE_EXTENSION);
      if (segmentInfo.dir.fileExists(cfxFileName)) {
        Directory cfsDir = new CompoundFileDirectory(segmentInfo.dir, cfxFileName, context, false);
        try {
          exists = cfsDir.fileExists(fileName);
        } finally {
          cfsDir.close();
        }
      } else {
        exists = false;
      }
    } else {
      exists = directory.fileExists(fileName);
    }

    if (!exists) {
      // 3x's FieldInfos sometimes lies and claims a segment
      // has vectors when it doesn't:
      return null;
    } else {
      return new Lucene3xTermVectorsReader(directory, segmentInfo, fieldInfos, context);
    }
  }

  @Override
  public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
}

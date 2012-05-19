package org.apache.lucene.codecs.lucene40;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.SegmentInfosReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 4.0 implementation of {@link SegmentInfosReader}.
 * 
 * @see Lucene40SegmentInfosFormat
 * @lucene.experimental
 */
public class Lucene40SegmentInfosReader extends SegmentInfosReader {

  @Override
  public SegmentInfo read(Directory dir, String segment) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene40SegmentInfosFormat.SI_EXTENSION);
    final IndexInput input = dir.openInput(fileName, IOContext.READONCE);
    boolean success = false;
    try {
      final String version = input.readString();
      final int docCount = input.readInt();
        // this is still written in 4.0 if we open a 3.x and upgrade the SI
      final int docStoreOffset = input.readInt();
      final String docStoreSegment;
      final boolean docStoreIsCompoundFile;
      if (docStoreOffset != -1) { 
        docStoreSegment = input.readString();
        docStoreIsCompoundFile = input.readByte() == SegmentInfo.YES;
      } else {
        docStoreSegment = segment;
        docStoreIsCompoundFile = false;
      }
      final int numNormGen = input.readInt();
      final Map<Integer,Long> normGen;
      if (numNormGen == SegmentInfo.NO) {
        normGen = null;
      } else {
        normGen = new HashMap<Integer, Long>();
        for(int j=0;j<numNormGen;j++) {
          normGen.put(input.readInt(), input.readLong());
        }
      }
      final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

      final int delCount = input.readInt();
      assert delCount <= docCount;
      final Map<String,String> diagnostics = input.readStringStringMap();

      success = true;
      return new SegmentInfo(dir, version, segment, docCount, docStoreOffset,
                             docStoreSegment, docStoreIsCompoundFile, normGen, isCompoundFile,
                             delCount, null, diagnostics);
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(input);
      } else {
        input.close();
      }
    }
  }
}

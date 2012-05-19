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
import java.util.Map.Entry;
import java.util.Map;

import org.apache.lucene.codecs.SegmentInfosWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Lucene 4.0 implementation of {@link SegmentInfosWriter}.
 * 
 * @see Lucene40SegmentInfosFormat
 * @lucene.experimental
 */
public class Lucene40SegmentInfosWriter extends SegmentInfosWriter {

  /** Save a single segment's info. */
  @Override
  public void write(SegmentInfo si, FieldInfos fis) throws IOException {
    assert si.getDelCount() <= si.docCount: "delCount=" + si.getDelCount() + " docCount=" + si.docCount + " segment=" + si.name;
    final String fileName = IndexFileNames.segmentFileName(si.name, "", Lucene40SegmentInfosFormat.SI_EXTENSION);
    // nocommit what ioctxt to pass?  cannot call .sizeInBytes()!
    final IndexOutput output = si.dir.createOutput(fileName, new IOContext(new FlushInfo(si.docCount, 0)));

    boolean success = false;
    try {
      // Write the Lucene version that created this segment, since 3.1
      output.writeString(si.getVersion());
      output.writeInt(si.docCount);
      // we still need to write this in 4.0 since we can open a 3.x with shared docStores
      output.writeInt(si.getDocStoreOffset());
      if (si.getDocStoreOffset() != -1) {
        output.writeString(si.getDocStoreSegment());
        output.writeByte((byte) (si.getDocStoreIsCompoundFile() ? 1:0));
      }

      // nocommit remove (4.0 doesn't write normGen)...
      Map<Integer,Long> normGen = si.getNormGen();
      if (normGen == null) {
        output.writeInt(SegmentInfo.NO);
      } else {
        output.writeInt(normGen.size());
        for (Entry<Integer,Long> entry : normGen.entrySet()) {
          output.writeInt(entry.getKey());
          output.writeLong(entry.getValue());
        }
      }

      output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
      output.writeInt(si.getDelCount());
      output.writeStringStringMap(si.getDiagnostics());

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);
        si.dir.deleteFile(fileName);
      } else {
        output.close();
      }
    }
  }
}

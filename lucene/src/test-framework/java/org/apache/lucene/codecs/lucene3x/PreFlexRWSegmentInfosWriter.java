package org.apache.lucene.codecs.lucene3x;

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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.codecs.SegmentInfosWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * PreFlex implementation of {@link SegmentInfosWriter}.
 * @lucene.experimental
 */
class PreFlexRWSegmentInfosWriter extends SegmentInfosWriter {

  @Override
  public IndexOutput writeInfos(Directory dir, String segmentFileName, String codecID, SegmentInfos infos, IOContext context)
          throws IOException {
    IndexOutput out = createOutput(dir, segmentFileName, new IOContext(new FlushInfo(infos.size(), infos.totalDocCount())));
    boolean success = false;
    try {
      out.writeInt(SegmentInfos.FORMAT_3_1); // write FORMAT
      // we don't write a codec - this is 3.x
      out.writeLong(infos.version);
      out.writeInt(infos.counter); // write counter
      out.writeInt(infos.size()); // write infos
      for (SegmentInfo si : infos) {
        writeInfo(out, si);
      }
      out.writeStringStringMap(infos.getUserData());
      success = true;
      return out;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  
  /** Save a single segment's info. */
  private void writeInfo(IndexOutput output, SegmentInfo si) throws IOException {
    // we are about to write this SI in 3.x format, dropping all codec information, etc.
    // so it had better be a 3.x segment or you will get very confusing errors later.
    assert si.getCodec() instanceof Lucene3xCodec : "broken test, trying to mix preflex with other codecs";
    assert si.getDelCount() <= si.docCount: "delCount=" + si.getDelCount() + " docCount=" + si.docCount + " segment=" + si.name;
    // Write the Lucene version that created this segment, since 3.1
    output.writeString(si.getVersion());
    output.writeString(si.name);
    output.writeInt(si.docCount);
    output.writeLong(si.getDelGen());

    output.writeInt(si.getDocStoreOffset());
    if (si.getDocStoreOffset() != -1) {
      output.writeString(si.getDocStoreSegment());
      output.writeByte((byte) (si.getDocStoreIsCompoundFile() ? 1:0));
    }
    // pre-4.0 indexes write a byte if there is a single norms file
    output.writeByte((byte) 1);

    Map<Integer,Long> normGen = si.getNormGen();
    if (normGen == null) {
      output.writeInt(SegmentInfo.NO);
    } else {
      output.writeInt(normGen.size());
      for (Entry<Integer,Long> entry : normGen.entrySet()) {
        output.writeLong(entry.getValue());
      }
    }

    output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
    output.writeInt(si.getDelCount());
    output.writeByte((byte) (si.getHasProxInternal()));
    output.writeStringStringMap(si.getDiagnostics());
    output.writeByte((byte) (si.getHasVectorsInternal()));
  }
  
  protected IndexOutput createOutput(Directory dir, String segmentFileName, IOContext context)
      throws IOException {
    IndexOutput plainOut = dir.createOutput(segmentFileName, context);
    ChecksumIndexOutput out = new ChecksumIndexOutput(plainOut);
    return out;
  }

  @Override
  public void prepareCommit(IndexOutput segmentOutput) throws IOException {
    ((ChecksumIndexOutput)segmentOutput).prepareCommit();
  }

  @Override
  public void finishCommit(IndexOutput out) throws IOException {
    ((ChecksumIndexOutput)out).finishCommit();
    out.close();
  }
}

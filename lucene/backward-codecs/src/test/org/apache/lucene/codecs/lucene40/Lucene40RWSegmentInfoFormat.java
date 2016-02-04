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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Read-write version of 4.0 segmentinfo format for testing
 * @deprecated for test purposes only
 */
@Deprecated
public final class Lucene40RWSegmentInfoFormat extends Lucene40SegmentInfoFormat {

  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(si.name, "", Lucene40SegmentInfoFormat.SI_EXTENSION);

    final IndexOutput output = dir.createOutput(fileName, ioContext);

    boolean success = false;
    try {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      si.addFile(fileName);

      CodecUtil.writeHeader(output, Lucene40SegmentInfoFormat.CODEC_NAME, Lucene40SegmentInfoFormat.VERSION_CURRENT);
      // Write the Lucene version that created this segment, since 3.1
      output.writeString(si.getVersion().toString());
      output.writeInt(si.maxDoc());

      output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
      output.writeStringStringMap(si.getDiagnostics());
      output.writeStringStringMap(si.getAttributes());
      output.writeStringSet(si.files());

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);
      } else {
        output.close();
      }
    }
  }
}

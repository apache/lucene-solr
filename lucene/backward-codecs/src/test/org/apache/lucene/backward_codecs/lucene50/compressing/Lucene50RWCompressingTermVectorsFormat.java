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
package org.apache.lucene.backward_codecs.lucene50.compressing;

import java.io.IOException;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** RW impersonation of Lucene50CompressingTermVectorsFormat. */
public class Lucene50RWCompressingTermVectorsFormat extends Lucene50CompressingTermVectorsFormat {

  /** Sole constructor. */
  public Lucene50RWCompressingTermVectorsFormat(
      String formatName,
      String segmentSuffix,
      CompressionMode compressionMode,
      int chunkSize,
      int blockSize) {
    super(formatName, segmentSuffix, compressionMode, chunkSize, blockSize);
  }

  @Override
  public final TermVectorsWriter vectorsWriter(
      Directory directory, SegmentInfo segmentInfo, IOContext context) throws IOException {
    return new Lucene50CompressingTermVectorsWriter(
        directory,
        segmentInfo,
        segmentSuffix,
        context,
        formatName,
        compressionMode,
        chunkSize,
        blockSize);
  }
}

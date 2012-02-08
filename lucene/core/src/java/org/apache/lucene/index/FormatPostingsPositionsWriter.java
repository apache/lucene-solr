package org.apache.lucene.index;

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

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;


import java.io.Closeable;
import java.io.IOException;

final class FormatPostingsPositionsWriter extends FormatPostingsPositionsConsumer implements Closeable {

  final FormatPostingsDocsWriter parent;
  final IndexOutput out;

  boolean omitTermFreqAndPositions;
  boolean storePayloads;
  int lastPayloadLength = -1;

  FormatPostingsPositionsWriter(SegmentWriteState state, FormatPostingsDocsWriter parent) throws IOException {
    this.parent = parent;
    omitTermFreqAndPositions = parent.omitTermFreqAndPositions;
    if (parent.parent.parent.fieldInfos.hasProx()) {
      // At least one field does not omit TF, so create the
      // prox file
      out = parent.parent.parent.dir.createOutput(IndexFileNames.segmentFileName(parent.parent.parent.segment, IndexFileNames.PROX_EXTENSION));
      parent.skipListWriter.setProxOutput(out);
    } else
      // Every field omits TF so we will write no prox file
      out = null;
  }

  int lastPosition;

  /** Add a new position & payload */
  @Override
  void addPosition(int position, byte[] payload, int payloadOffset, int payloadLength) throws IOException {
    assert !omitTermFreqAndPositions: "omitTermFreqAndPositions is true";
    assert out != null;

    final int delta = position - lastPosition;
    lastPosition = position;

    if (storePayloads) {
      if (payloadLength != lastPayloadLength) {
        lastPayloadLength = payloadLength;
        out.writeVInt((delta<<1)|1);
        out.writeVInt(payloadLength);
      } else
        out.writeVInt(delta << 1);
      if (payloadLength > 0)
        out.writeBytes(payload, payloadLength);
    } else
      out.writeVInt(delta);
  }

  void setField(FieldInfo fieldInfo) {
    omitTermFreqAndPositions = fieldInfo.indexOptions == IndexOptions.DOCS_ONLY;
    storePayloads = omitTermFreqAndPositions ? false : fieldInfo.storePayloads;
  }

  /** Called when we are done adding positions & payloads */
  @Override
  void finish() {       
    lastPosition = 0;
    lastPayloadLength = -1;
  }

  public void close() throws IOException {
    IOUtils.close(out);
  }
}

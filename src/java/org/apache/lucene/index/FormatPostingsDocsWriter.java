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

/** Consumes doc & freq, writing them using the current
 *  index file format */

import java.io.IOException;

import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.store.IndexOutput;

final class FormatPostingsDocsWriter extends FormatPostingsDocsConsumer {

  final IndexOutput out;
  final FormatPostingsTermsWriter parent;
  final FormatPostingsPositionsWriter posWriter;
  final DefaultSkipListWriter skipListWriter;
  final int skipInterval;
  final int totalNumDocs;

  boolean omitTermFreqAndPositions;
  boolean storePayloads;
  long freqStart;
  FieldInfo fieldInfo;

  FormatPostingsDocsWriter(SegmentWriteState state, FormatPostingsTermsWriter parent) throws IOException {
    super();
    this.parent = parent;
    final String fileName = IndexFileNames.segmentFileName(parent.parent.segment, IndexFileNames.FREQ_EXTENSION);
    state.flushedFiles.add(fileName);
    out = parent.parent.dir.createOutput(fileName);
    totalNumDocs = parent.parent.totalNumDocs;

    // TODO: abstraction violation
    skipInterval = parent.parent.termsOut.skipInterval;
    skipListWriter = parent.parent.skipListWriter;
    skipListWriter.setFreqOutput(out);

    posWriter = new FormatPostingsPositionsWriter(state, this);
  }

  void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
    storePayloads = fieldInfo.storePayloads;
    posWriter.setField(fieldInfo);
  }

  int lastDocID;
  int df;

  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  FormatPostingsPositionsConsumer addDoc(int docID, int termDocFreq) throws IOException {

    final int delta = docID - lastDocID;

    if (docID < 0 || (df > 0 && delta <= 0))
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");

    if ((++df % skipInterval) == 0) {
      // TODO: abstraction violation
      skipListWriter.setSkipData(lastDocID, storePayloads, posWriter.lastPayloadLength);
      skipListWriter.bufferSkip(df);
    }

    assert docID < totalNumDocs: "docID=" + docID + " totalNumDocs=" + totalNumDocs;

    lastDocID = docID;
    if (omitTermFreqAndPositions)
      out.writeVInt(delta);
    else if (1 == termDocFreq)
      out.writeVInt((delta<<1) | 1);
    else {
      out.writeVInt(delta<<1);
      out.writeVInt(termDocFreq);
    }

    return posWriter;
  }

  private final TermInfo termInfo = new TermInfo();  // minimize consing
  final UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();

  /** Called when we are done adding docs to this term */
  void finish() throws IOException {
    long skipPointer = skipListWriter.writeSkip(out);

    // TODO: this is abstraction violation -- we should not
    // peek up into parents terms encoding format
    termInfo.set(df, parent.freqStart, parent.proxStart, (int) (skipPointer - parent.freqStart));

    // TODO: we could do this incrementally
    UnicodeUtil.UTF16toUTF8(parent.currentTerm, parent.currentTermStart, utf8);

    if (df > 0) {
      parent.termsOut.add(fieldInfo.number,
                          utf8.result,
                          utf8.length,
                          termInfo);
    }

    lastDocID = 0;
    df = 0;
  }

  void close() throws IOException {
    out.close();
    posWriter.close();
  }
}

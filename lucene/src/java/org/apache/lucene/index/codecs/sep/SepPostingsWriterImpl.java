package org.apache.lucene.index.codecs.sep;

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
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

/** Writes frq to .frq, docs to .doc, pos to .pos, payloads
 *  to .pyl, skip data to .skp
 *
 * @lucene.experimental */
public final class SepPostingsWriterImpl extends PostingsWriterBase {
  final static String CODEC = "SepDocFreqSkip";

  final static String DOC_EXTENSION = "doc";
  final static String SKIP_EXTENSION = "skp";
  final static String FREQ_EXTENSION = "frq";
  final static String POS_EXTENSION = "pos";
  final static String PAYLOAD_EXTENSION = "pyl";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final IntIndexOutput freqOut;
  final IntIndexOutput.Index freqIndex;

  final IntIndexOutput posOut;
  final IntIndexOutput.Index posIndex;

  final IntIndexOutput docOut;
  final IntIndexOutput.Index docIndex;

  final IndexOutput payloadOut;

  final IndexOutput skipOut;
  IndexOutput termsOut;

  final SepSkipListWriter skipListWriter;
  final int skipInterval;
  final int maxSkipLevels;
  final int totalNumDocs;

  boolean storePayloads;
  boolean omitTF;

  // Starts a new term
  long lastSkipStart;

  FieldInfo fieldInfo;

  int lastPayloadLength;
  int lastPosition;
  long payloadStart;
  long lastPayloadStart;
  int lastDocID;
  int df;
  private boolean firstDoc;

  public SepPostingsWriterImpl(SegmentWriteState state, IntStreamFactory factory) throws IOException {
    super();

    final String docFileName = IndexFileNames.segmentFileName(state.segmentName, "", DOC_EXTENSION);
    state.flushedFiles.add(docFileName);
    docOut = factory.createOutput(state.directory, docFileName);
    docIndex = docOut.index();

    if (state.fieldInfos.hasProx()) {
      final String frqFileName = IndexFileNames.segmentFileName(state.segmentName, "", FREQ_EXTENSION);
      state.flushedFiles.add(frqFileName);
      freqOut = factory.createOutput(state.directory, frqFileName);
      freqIndex = freqOut.index();

      final String posFileName = IndexFileNames.segmentFileName(state.segmentName, "", POS_EXTENSION);
      posOut = factory.createOutput(state.directory, posFileName);
      state.flushedFiles.add(posFileName);
      posIndex = posOut.index();

      // TODO: -- only if at least one field stores payloads?
      final String payloadFileName = IndexFileNames.segmentFileName(state.segmentName, "", PAYLOAD_EXTENSION);
      state.flushedFiles.add(payloadFileName);
      payloadOut = state.directory.createOutput(payloadFileName);

    } else {
      freqOut = null;
      freqIndex = null;
      posOut = null;
      posIndex = null;
      payloadOut = null;
    }

    final String skipFileName = IndexFileNames.segmentFileName(state.segmentName, "", SKIP_EXTENSION);
    state.flushedFiles.add(skipFileName);
    skipOut = state.directory.createOutput(skipFileName);

    totalNumDocs = state.numDocs;

    // TODO: -- abstraction violation
    skipListWriter = new SepSkipListWriter(state.skipInterval,
                                           state.maxSkipLevels,
                                           state.numDocs,
                                           freqOut, docOut,
                                           posOut, payloadOut);

    skipInterval = state.skipInterval;
    maxSkipLevels = state.maxSkipLevels;
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    // TODO: -- just ask skipper to "start" here
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
  }

  @Override
  public void startTerm() throws IOException {
    docIndex.mark();
    if (!omitTF) {
      freqIndex.mark();
      posIndex.mark();
      payloadStart = payloadOut.getFilePointer();
      lastPayloadLength = -1;
    }
    firstDoc = true;
    skipListWriter.resetSkip(docIndex, freqIndex, posIndex);
  }

  // TODO: -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    omitTF = fieldInfo.omitTermFreqAndPositions;
    skipListWriter.setOmitTF(omitTF);
    storePayloads = !omitTF && fieldInfo.storePayloads;
  }


  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {

    if (firstDoc) {
      // TODO: we are writing absolute file pointers below,
      // which is wasteful.  It'd be better compression to
      // write the "baseline" into each indexed term, then
      // write only the delta here.
      if (!omitTF) {
        freqIndex.write(docOut, true);
        posIndex.write(docOut, true);
        docOut.writeVLong(payloadStart);
      }
      docOut.writeVLong(skipOut.getFilePointer());
      firstDoc = false;
    }

    final int delta = docID - lastDocID;

    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
    }

    if ((++df % skipInterval) == 0) {
      // TODO: -- awkward we have to make these two
      // separate calls to skipper
      skipListWriter.setSkipData(lastDocID, storePayloads, lastPayloadLength);
      skipListWriter.bufferSkip(df);
    }

    lastDocID = docID;
    docOut.write(delta);
    if (!omitTF) {
      freqOut.write(termDocFreq);
    }
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload) throws IOException {
    assert !omitTF;

    final int delta = position - lastPosition;
    lastPosition = position;

    if (storePayloads) {
      final int payloadLength = payload == null ? 0 : payload.length;
      if (payloadLength != lastPayloadLength) {
        lastPayloadLength = payloadLength;
        // TODO: explore whether we get better compression
        // by not storing payloadLength into prox stream?
        posOut.write((delta<<1)|1);
        posOut.write(payloadLength);
      } else {
        posOut.write(delta << 1);
      }

      if (payloadLength > 0) {
        payloadOut.writeBytes(payload.bytes, payload.offset, payloadLength);
      }
    } else {
      posOut.write(delta);
    }

    lastPosition = position;
  }

  /** Called when we are done adding positions & payloads */
  @Override
  public void finishDoc() {       
    lastPosition = 0;
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(int docCount, boolean isIndexTerm) throws IOException {

    // TODO: -- wasteful we are counting this in two places?
    assert docCount > 0;
    assert docCount == df;

    docIndex.write(termsOut, isIndexTerm);

    if (df >= skipInterval) {
      skipListWriter.writeSkip(skipOut);
    }

    lastDocID = 0;
    df = 0;
  }

  @Override
  public void close() throws IOException {
    try {
      docOut.close();
    } finally {
      try {
        skipOut.close();
      } finally {
        if (freqOut != null) {
          try {
            freqOut.close();
          } finally {
            try {
              posOut.close();
            } finally {
              payloadOut.close();
            }
          }
        }
      }
    }
  }

  public static void getExtensions(Set<String> extensions) {
    extensions.add(DOC_EXTENSION);
    extensions.add(FREQ_EXTENSION);
    extensions.add(SKIP_EXTENSION);
    extensions.add(POS_EXTENSION);
    extensions.add(PAYLOAD_EXTENSION);
  }
}

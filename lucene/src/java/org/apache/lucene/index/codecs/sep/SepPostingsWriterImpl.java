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
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.TermStats;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
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
  /** Expert: The fraction of TermDocs entries stored in skip tables,
   * used to accelerate {@link DocsEnum#advance(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  final int skipInterval = 16;
  
  /**
   * Expert: minimum docFreq to write any skip data at all
   */
  final int skipMinimum = skipInterval;

  /** Expert: The maximum number of skip levels. Smaller values result in 
   * slightly smaller indexes, but slower skipping in big posting lists.
   */
  final int maxSkipLevels = 10;

  final int totalNumDocs;

  boolean storePayloads;
  boolean omitTF;

  long lastSkipFP;

  FieldInfo fieldInfo;

  int lastPayloadLength;
  int lastPosition;
  long payloadStart;
  long lastPayloadStart;
  int lastDocID;
  int df;
  private int pendingTermCount;

  // Holds pending byte[] blob for the current terms block
  private final RAMOutputStream indexBytesWriter = new RAMOutputStream();

  public SepPostingsWriterImpl(SegmentWriteState state, IntStreamFactory factory) throws IOException {
    super();

    final String docFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, DOC_EXTENSION);
    docOut = factory.createOutput(state.directory, docFileName);
    docIndex = docOut.index();

    if (state.fieldInfos.hasProx()) {
      final String frqFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, FREQ_EXTENSION);
      freqOut = factory.createOutput(state.directory, frqFileName);
      freqIndex = freqOut.index();

      final String posFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, POS_EXTENSION);
      posOut = factory.createOutput(state.directory, posFileName);
      posIndex = posOut.index();

      // TODO: -- only if at least one field stores payloads?
      final String payloadFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, PAYLOAD_EXTENSION);
      payloadOut = state.directory.createOutput(payloadFileName);

    } else {
      freqOut = null;
      freqIndex = null;
      posOut = null;
      posIndex = null;
      payloadOut = null;
    }

    final String skipFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, SKIP_EXTENSION);
    skipOut = state.directory.createOutput(skipFileName);

    totalNumDocs = state.numDocs;

    skipListWriter = new SepSkipListWriter(skipInterval,
                                           maxSkipLevels,
                                           state.numDocs,
                                           freqOut, docOut,
                                           posOut, payloadOut);
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    // TODO: -- just ask skipper to "start" here
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    termsOut.writeInt(skipMinimum);                 // write skipMinimum
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
    skipListWriter.resetSkip(docIndex, freqIndex, posIndex);
  }

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

    final int delta = docID - lastDocID;
    //System.out.println("SepW startDoc: write doc=" + docID + " delta=" + delta);

    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
    }

    if ((++df % skipInterval) == 0) {
      // TODO: -- awkward we have to make these two
      // separate calls to skipper
      //System.out.println("    buffer skip lastDocID=" + lastDocID);
      skipListWriter.setSkipData(lastDocID, storePayloads, lastPayloadLength);
      skipListWriter.bufferSkip(df);
    }

    lastDocID = docID;
    docOut.write(delta);
    if (!omitTF) {
      //System.out.println("    sepw startDoc: write freq=" + termDocFreq);
      freqOut.write(termDocFreq);
    }
  }

  @Override
  public void flushTermsBlock() throws IOException {
    //System.out.println("SepW.flushTermsBlock: pendingTermCount=" + pendingTermCount + " bytesUsed=" + indexBytesWriter.getFilePointer());
    termsOut.writeVLong((int) indexBytesWriter.getFilePointer());
    indexBytesWriter.writeTo(termsOut);
    indexBytesWriter.reset();
    pendingTermCount = 0;
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload) throws IOException {
    assert !omitTF;

    final int delta = position - lastPosition;
    assert delta > 0 || position == 0: "position=" + position + " lastPosition=" + lastPosition;            // not quite right (if pos=0 is repeated twice we don't catch it)
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
  public void finishTerm(TermStats stats) throws IOException {
    // TODO: -- wasteful we are counting this in two places?
    assert stats.docFreq > 0;
    assert stats.docFreq == df;

    final boolean isFirstTerm = pendingTermCount == 0;  
    //System.out.println("SepW.finishTerm: isFirstTerm=" + isFirstTerm);

    docIndex.write(indexBytesWriter, isFirstTerm);
    //System.out.println("  docIndex=" + docIndex);

    if (!omitTF) {
      freqIndex.write(indexBytesWriter, isFirstTerm);
      //System.out.println("  freqIndex=" + freqIndex);

      posIndex.write(indexBytesWriter, isFirstTerm);
      //System.out.println("  posIndex=" + posIndex);
      if (storePayloads) {
        if (isFirstTerm) {
          indexBytesWriter.writeVLong(payloadStart);
        } else {
          indexBytesWriter.writeVLong(payloadStart - lastPayloadStart);
        }
        lastPayloadStart = payloadStart;
        //System.out.println("  payloadFP=" + payloadStart);
      }
    }

    if (df >= skipMinimum) {
      //System.out.println("  skipFP=" + skipStart);
      final long skipFP = skipOut.getFilePointer();
      skipListWriter.writeSkip(skipOut);
      //System.out.println("   writeSkip @ " + indexBytesWriter.getFilePointer());
      if (isFirstTerm) {
        indexBytesWriter.writeVLong(skipFP);
      } else {
        indexBytesWriter.writeVLong(skipFP - lastSkipFP);
      }
      lastSkipFP = skipFP;
    } else if (isFirstTerm) {
      // lazily write an absolute delta if a term in this block requires skip data.
      lastSkipFP = 0;
    }

    lastDocID = 0;
    df = 0;
    pendingTermCount++;
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

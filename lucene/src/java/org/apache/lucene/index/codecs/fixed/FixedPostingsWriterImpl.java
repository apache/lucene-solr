package org.apache.lucene.index.codecs.fixed;

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
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexOutput;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

/** 
 * Writes frq and doc blocks in interleaved fashion to .doc, pos to .pos, payloads
 *  to .pyl, skip data to .skp
 *  
 * NOTE: when omitTF is enabled, only pure doc blocks are written.
 * 
 * The doc, freq, and positions codecs must have the same fixed blocksize to use
 * this layout: however they can be different compression algorithms.
 *
 * @lucene.experimental */
public final class FixedPostingsWriterImpl extends PostingsWriterBase {
  final static String CODEC = "FixedDocFreqSkip";

  final static String DOC_EXTENSION = "doc";
  final static String SKIP_EXTENSION = "skp";
  final static String POS_EXTENSION = "pos";
  final static String PAYLOAD_EXTENSION = "pyl";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final FixedIntBlockIndexOutput posOut;
  final IntIndexOutput.Index posIndex;

  IntIndexOutput docOut; // pointer to either docio or docfreqio
  final InterleavedIntBlockIndexOutput docfreqio; // for !omitTF fields, doc+freq stream
  final FixedIntBlockIndexOutput docio; // for omitTF fields, the underlying doc-only stream
  final IntIndexOutput.Index docIndex;

  final IndexOutput payloadOut;

  final IndexOutput skipOut;
  IndexOutput termsOut;

  final FixedSkipListWriter skipListWriter;
  /** Expert: The fraction of TermDocs entries stored in skip tables,
   * used to accelerate {@link DocsEnum#advance(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  final int skipInterval;
  
  /**
   * Expert: minimum docFreq to write any skip data at all
   */
  final int skipMinimum;

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

  public FixedPostingsWriterImpl(SegmentWriteState state, FixedIntStreamFactory factory) throws IOException {
    super();

    final String docFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, DOC_EXTENSION);
    IndexOutput stream = state.directory.createOutput(docFileName);
    docio = factory.createOutput(stream, docFileName, false);
    // nocommit: hardcode to more reasonable like 64?
    skipMinimum = skipInterval = docio.blockSize;
    if (state.fieldInfos.hasProx()) {
      FixedIntBlockIndexOutput freqio = factory.createOutput(stream, docFileName, true); 
      docOut = docfreqio = new InterleavedIntBlockIndexOutput(docio, freqio);

      final String posFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, POS_EXTENSION);
      posOut = factory.createOutput(state.directory, posFileName);
      posIndex = posOut.index();
      
      // nocommit: clean up?
      if (posOut.blockSize != docio.blockSize) {
        throw new IllegalArgumentException("positions blocksize must be equal to docs and freqs blocksize");
      }

      // TODO: -- only if at least one field stores payloads?
      final String payloadFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, PAYLOAD_EXTENSION);
      payloadOut = state.directory.createOutput(payloadFileName);

    } else {
      docOut = docio; // docOut is just a pure doc stream only
      docfreqio = null;
      posOut = null;
      posIndex = null;
      payloadOut = null;
    }

    docIndex = docOut.index();
    final String skipFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, SKIP_EXTENSION);
    skipOut = state.directory.createOutput(skipFileName);

    totalNumDocs = state.numDocs;

    skipListWriter = new FixedSkipListWriter(skipInterval,
                                           maxSkipLevels,
                                           state.numDocs,
                                           docOut,
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
      posIndex.mark();
      payloadStart = payloadOut.getFilePointer();
      lastPayloadLength = -1;
    }
    skipListWriter.resetSkip(docIndex, posIndex);
  }

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    
    // omitTF's differ, we must flush any buffered docs/freqs
    // nocommit: ugly!
    if (omitTF != fieldInfo.omitTermFreqAndPositions) {
      try {
        if (docOut instanceof InterleavedIntBlockIndexOutput) {
          ((InterleavedIntBlockIndexOutput) docOut).flush();
        } else {
          ((FixedIntBlockIndexOutput) docOut).flush();
        }
      } catch (IOException e) { throw new RuntimeException(e); }
    }
    
    omitTF = fieldInfo.omitTermFreqAndPositions;
    docOut = omitTF ? docio : docfreqio;
    skipListWriter.setOmitTF(omitTF);
    storePayloads = !omitTF && fieldInfo.storePayloads;
  }

  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {

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
      docOut.write(termDocFreq);
    }
  }

  @Override
  public void flushTermsBlock() throws IOException {
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

    docIndex.write(indexBytesWriter, isFirstTerm);

    if (!omitTF) {
      posIndex.write(indexBytesWriter, isFirstTerm);
      if (storePayloads) {
        if (isFirstTerm) {
          indexBytesWriter.writeVLong(payloadStart);
        } else {
          indexBytesWriter.writeVLong(payloadStart - lastPayloadStart);
        }
        lastPayloadStart = payloadStart;
      }
    }

    if (df >= skipMinimum) {
      final long skipFP = skipOut.getFilePointer();
      skipListWriter.writeSkip(skipOut);
      if (isFirstTerm) {
        indexBytesWriter.writeVLong(skipFP);
      } else {
        indexBytesWriter.writeVLong(skipFP - lastSkipFP);
      }
      lastSkipFP = skipFP;
    } else if (isFirstTerm) {
      // TODO: this is somewhat wasteful; eg if no terms in
      // this block will use skip data, we don't need to
      // write this:
      final long skipFP = skipOut.getFilePointer();
      indexBytesWriter.writeVLong(skipFP);
      lastSkipFP = skipFP;
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
        if (posOut != null) {
          try {
            posOut.close();
          } finally {
            payloadOut.close();
          }
        }
      }
    }
  }

  public static void getExtensions(Set<String> extensions) {
    extensions.add(DOC_EXTENSION);
    extensions.add(SKIP_EXTENSION);
    extensions.add(POS_EXTENSION);
    extensions.add(PAYLOAD_EXTENSION);
  }
}

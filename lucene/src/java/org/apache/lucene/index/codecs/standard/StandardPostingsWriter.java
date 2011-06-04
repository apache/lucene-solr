package org.apache.lucene.index.codecs.standard;

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
import org.apache.lucene.util.IOUtils;

/** @lucene.experimental */
public final class StandardPostingsWriter extends PostingsWriterBase {
  final static String CODEC = "StandardPostingsWriterImpl";
  
  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  IndexOutput freqOut;
  IndexOutput proxOut;
  final DefaultSkipListWriter skipListWriter;
  /** Expert: The fraction of TermDocs entries stored in skip tables,
   * used to accelerate {@link DocsEnum#advance(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  static final int DEFAULT_SKIP_INTERVAL = 16;
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
  IndexOutput termsOut;

  boolean omitTermFreqAndPositions;
  boolean storePayloads;
  // Starts a new term
  long lastFreqStart;
  long freqStart;
  long lastProxStart;
  long proxStart;
  FieldInfo fieldInfo;
  int lastPayloadLength;
  int lastPosition;

  private int pendingCount;

  //private String segment;

  private RAMOutputStream bytesWriter = new RAMOutputStream();

  public StandardPostingsWriter(SegmentWriteState state) throws IOException {
    this(state, DEFAULT_SKIP_INTERVAL);
  }
  
  public StandardPostingsWriter(SegmentWriteState state, int skipInterval) throws IOException {
    this.skipInterval = skipInterval;
    this.skipMinimum = skipInterval; /* set to the same for now */
    //this.segment = state.segmentName;
    String fileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, StandardCodec.FREQ_EXTENSION);
    freqOut = state.directory.createOutput(fileName);
    boolean success = false;
    try {
      if (state.fieldInfos.hasProx()) {
        // At least one field does not omit TF, so create the
        // prox file
        fileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, StandardCodec.PROX_EXTENSION);
        proxOut = state.directory.createOutput(fileName);
      } else {
        // Every field omits TF so we will write no prox file
        proxOut = null;
      }
      
      totalNumDocs = state.numDocs;
      
      skipListWriter = new DefaultSkipListWriter(skipInterval, maxSkipLevels,
          state.numDocs, freqOut, proxOut);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeSafely(true, freqOut, proxOut);
      }
    }
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    termsOut.writeInt(skipMinimum);                 // write skipMinimum
  }

  @Override
  public void startTerm() {
    //System.out.println("StandardW: startTerm seg=" + segment + " pendingCount=" + pendingCount);
    freqStart = freqOut.getFilePointer();
    if (proxOut != null) {
      proxStart = proxOut.getFilePointer();
      // force first payload to write its length
      lastPayloadLength = -1;
    }
    skipListWriter.resetSkip();
  }

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    //System.out.println("SPW: setField");
    this.fieldInfo = fieldInfo;
    omitTermFreqAndPositions = fieldInfo.omitTermFreqAndPositions;
    storePayloads = fieldInfo.storePayloads;
    //System.out.println("  set init blockFreqStart=" + freqStart);
    //System.out.println("  set init blockProxStart=" + proxStart);
  }

  int lastDocID;
  int df;
  
  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    //System.out.println("StandardW:   startDoc seg=" + segment + " docID=" + docID + " tf=" + termDocFreq);

    final int delta = docID - lastDocID;
    
    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )");
    }

    if ((++df % skipInterval) == 0) {
      skipListWriter.setSkipData(lastDocID, storePayloads, lastPayloadLength);
      skipListWriter.bufferSkip(df);
    }

    assert docID < totalNumDocs: "docID=" + docID + " totalNumDocs=" + totalNumDocs;

    lastDocID = docID;
    if (omitTermFreqAndPositions) {
      freqOut.writeVInt(delta);
    } else if (1 == termDocFreq) {
      freqOut.writeVInt((delta<<1) | 1);
    } else {
      freqOut.writeVInt(delta<<1);
      freqOut.writeVInt(termDocFreq);
    }

    lastPosition = 0;
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload) throws IOException {
    //System.out.println("StandardW:     addPos pos=" + position + " payload=" + (payload == null ? "null" : (payload.length + " bytes")) + " proxFP=" + proxOut.getFilePointer());
    assert !omitTermFreqAndPositions: "omitTermFreqAndPositions is true";
    assert proxOut != null;

    final int delta = position - lastPosition;

    assert delta >= 0: "position=" + position + " lastPosition=" + lastPosition;

    lastPosition = position;

    if (storePayloads) {
      final int payloadLength = payload == null ? 0 : payload.length;

      if (payloadLength != lastPayloadLength) {
        lastPayloadLength = payloadLength;
        proxOut.writeVInt((delta<<1)|1);
        proxOut.writeVInt(payloadLength);
      } else {
        proxOut.writeVInt(delta << 1);
      }

      if (payloadLength > 0) {
        proxOut.writeBytes(payload.bytes, payload.offset, payloadLength);
      }
    } else {
      proxOut.writeVInt(delta);
    }
  }

  @Override
  public void finishDoc() {
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(TermStats stats) throws IOException {
    //System.out.println("StandardW.finishTerm seg=" + segment);
    assert stats.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert stats.docFreq == df;

    final boolean isFirstTerm = pendingCount == 0;
    //System.out.println("  isFirstTerm=" + isFirstTerm);

    //System.out.println("  freqFP=" + freqStart);
    if (isFirstTerm) {
      bytesWriter.writeVLong(freqStart);
    } else {
      bytesWriter.writeVLong(freqStart-lastFreqStart);
    }
    lastFreqStart = freqStart;

    if (df >= skipMinimum) {
      bytesWriter.writeVInt((int) (skipListWriter.writeSkip(freqOut)-freqStart));
    }

    if (!omitTermFreqAndPositions) {
      //System.out.println("  proxFP=" + proxStart);
      if (isFirstTerm) {
        bytesWriter.writeVLong(proxStart);
      } else {
        bytesWriter.writeVLong(proxStart - lastProxStart);
      }
      lastProxStart = proxStart;
    }
     
    lastDocID = 0;
    df = 0;
    pendingCount++;
  }

  @Override
  public void flushTermsBlock() throws IOException {
    //System.out.println("SPW.flushBlock pendingCount=" + pendingCount);
    termsOut.writeVInt((int) bytesWriter.getFilePointer());
    bytesWriter.writeTo(termsOut);
    bytesWriter.reset();
    pendingCount = 0;
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeSafely(false, freqOut, proxOut);
  }
}

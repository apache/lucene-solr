package org.apache.lucene.codecs.sep;

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

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** Writes frq to .frq, docs to .doc, pos to .pos, payloads
 *  to .pyl, skip data to .skp
 *
 * @lucene.experimental */
public final class SepPostingsWriter extends PostingsWriterBase {
  final static String CODEC = "SepPostingsWriter";

  final static String DOC_EXTENSION = "doc";
  final static String SKIP_EXTENSION = "skp";
  final static String FREQ_EXTENSION = "frq";
  final static String POS_EXTENSION = "pos";
  final static String PAYLOAD_EXTENSION = "pyl";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  IntIndexOutput freqOut;
  IntIndexOutput.Index freqIndex;

  IntIndexOutput posOut;
  IntIndexOutput.Index posIndex;

  IntIndexOutput docOut;
  IntIndexOutput.Index docIndex;

  IndexOutput payloadOut;

  IndexOutput skipOut;

  final SepSkipListWriter skipListWriter;
  /** Expert: The fraction of TermDocs entries stored in skip tables,
   * used to accelerate {@link DocsEnum#advance(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases. More detailed experiments would be useful here. */
  final int skipInterval;
  static final int DEFAULT_SKIP_INTERVAL = 16;
  
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
  IndexOptions indexOptions;

  FieldInfo fieldInfo;

  int lastPayloadLength;
  int lastPosition;
  long payloadStart;
  int lastDocID;
  int df;

  SepTermState lastState;
  long lastPayloadFP;
  long lastSkipFP;

  public SepPostingsWriter(SegmentWriteState state, IntStreamFactory factory) throws IOException {
    this(state, factory, DEFAULT_SKIP_INTERVAL);
  }

  public SepPostingsWriter(SegmentWriteState state, IntStreamFactory factory, int skipInterval) throws IOException {
    freqOut = null;
    freqIndex = null;
    posOut = null;
    posIndex = null;
    payloadOut = null;
    boolean success = false;
    try {
      this.skipInterval = skipInterval;
      this.skipMinimum = skipInterval; /* set to the same for now */
      final String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, DOC_EXTENSION);

      docOut = factory.createOutput(state.directory, docFileName, state.context);
      docIndex = docOut.index();

      if (state.fieldInfos.hasFreq()) {
        final String frqFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, FREQ_EXTENSION);
        freqOut = factory.createOutput(state.directory, frqFileName, state.context);
        freqIndex = freqOut.index();
      }

      if (state.fieldInfos.hasProx()) {      
        final String posFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, POS_EXTENSION);
        posOut = factory.createOutput(state.directory, posFileName, state.context);
        posIndex = posOut.index();
        
        // TODO: -- only if at least one field stores payloads?
        final String payloadFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, PAYLOAD_EXTENSION);
        payloadOut = state.directory.createOutput(payloadFileName, state.context);
      }

      final String skipFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, SKIP_EXTENSION);
      skipOut = state.directory.createOutput(skipFileName, state.context);
      
      totalNumDocs = state.segmentInfo.getDocCount();
      
      skipListWriter = new SepSkipListWriter(skipInterval,
          maxSkipLevels,
          totalNumDocs,
          freqOut, docOut,
          posOut, payloadOut);
      
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, skipOut, freqOut, posOut, payloadOut);
      }
    }
  }

  @Override
  public void init(IndexOutput termsOut) throws IOException {
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    // TODO: -- just ask skipper to "start" here
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    termsOut.writeInt(skipMinimum);                 // write skipMinimum
  }

  @Override
  public BlockTermState newTermState() {
    return new SepTermState();
  }

  @Override
  public void startTerm() throws IOException {
    docIndex.mark();
    //System.out.println("SEPW: startTerm docIndex=" + docIndex);

    if (indexOptions != IndexOptions.DOCS_ONLY) {
      freqIndex.mark();
    }
    
    if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
      posIndex.mark();
      payloadStart = payloadOut.getFilePointer();
      lastPayloadLength = -1;
    }

    skipListWriter.resetSkip(docIndex, freqIndex, posIndex);
  }

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public int setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.indexOptions = fieldInfo.getIndexOptions();
    if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
      throw new UnsupportedOperationException("this codec cannot index offsets");
    }
    skipListWriter.setIndexOptions(indexOptions);
    storePayloads = indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS && fieldInfo.hasPayloads();
    lastPayloadFP = 0;
    lastSkipFP = 0;
    lastState = setEmptyState();
    return 0;
  }

  private SepTermState setEmptyState() {
    SepTermState emptyState = new SepTermState();
    emptyState.docIndex = docOut.index();
    if (indexOptions != IndexOptions.DOCS_ONLY) {
      emptyState.freqIndex = freqOut.index();
      if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        emptyState.posIndex = posOut.index();
      }
    }
    emptyState.payloadFP = 0;
    emptyState.skipFP = 0;
    return emptyState;
  }

  /** Adds a new doc in this term.  If this returns null
   *  then we just skip consuming positions/payloads. */
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {

    final int delta = docID - lastDocID;
    //System.out.println("SEPW: startDoc: write doc=" + docID + " delta=" + delta + " out.fp=" + docOut);

    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " ) (docOut: " + docOut + ")");
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
    if (indexOptions != IndexOptions.DOCS_ONLY) {
      //System.out.println("    sepw startDoc: write freq=" + termDocFreq);
      freqOut.write(termDocFreq);
    }
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    assert indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;

    final int delta = position - lastPosition;
    assert delta >= 0: "position=" + position + " lastPosition=" + lastPosition;            // not quite right (if pos=0 is repeated twice we don't catch it)
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

  private static class SepTermState extends BlockTermState {
    public IntIndexOutput.Index docIndex;
    public IntIndexOutput.Index freqIndex;
    public IntIndexOutput.Index posIndex;
    public long payloadFP;
    public long skipFP;
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    SepTermState state = (SepTermState)_state;
    // TODO: -- wasteful we are counting this in two places?
    assert state.docFreq > 0;
    assert state.docFreq == df;

    state.docIndex = docOut.index();
    state.docIndex.copyFrom(docIndex, false);
    if (indexOptions != IndexOptions.DOCS_ONLY) {
      state.freqIndex = freqOut.index();
      state.freqIndex.copyFrom(freqIndex, false);
      if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        state.posIndex = posOut.index();
        state.posIndex.copyFrom(posIndex, false);
      } else {
        state.posIndex = null;
      }
    } else {
      state.freqIndex = null;
      state.posIndex = null;
    }

    if (df >= skipMinimum) {
      state.skipFP = skipOut.getFilePointer();
      //System.out.println("  skipFP=" + skipFP);
      skipListWriter.writeSkip(skipOut);
      //System.out.println("    numBytes=" + (skipOut.getFilePointer()-skipFP));
    } else {
      state.skipFP = -1;
    }
    state.payloadFP = payloadStart;

    lastDocID = 0;
    df = 0;
  }

  @Override
  public void encodeTerm(long[] longs, DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    SepTermState state = (SepTermState)_state;
    if (absolute) {
      lastSkipFP = 0;
      lastPayloadFP = 0;
      lastState = state;
    }
    lastState.docIndex.copyFrom(state.docIndex, false);
    lastState.docIndex.write(out, absolute);
    if (indexOptions != IndexOptions.DOCS_ONLY) {
      lastState.freqIndex.copyFrom(state.freqIndex, false);
      lastState.freqIndex.write(out, absolute);
      if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        lastState.posIndex.copyFrom(state.posIndex, false);
        lastState.posIndex.write(out, absolute);
        if (storePayloads) {
          if (absolute) {
            out.writeVLong(state.payloadFP);
          } else {
            out.writeVLong(state.payloadFP - lastPayloadFP);
          }
          lastPayloadFP = state.payloadFP;
        }
      }
    }
    if (state.skipFP != -1) {
      if (absolute) {
        out.writeVLong(state.skipFP);
      } else {
        out.writeVLong(state.skipFP - lastSkipFP);
      }
      lastSkipFP = state.skipFP;
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docOut, skipOut, freqOut, posOut, payloadOut);
  }
}

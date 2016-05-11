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

/** Consumes doc & freq, writing them using the current
 *  index file format */

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum; // javadocs
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Writer for 4.0 postings format
 * @deprecated for test purposes only
 */
@Deprecated
final class Lucene40PostingsWriter extends PushPostingsWriterBase {

  final IndexOutput freqOut;
  final IndexOutput proxOut;
  final Lucene40SkipListWriter skipListWriter;
  /** Expert: The fraction of TermDocs entries stored in skip tables,
   * used to accelerate {@link PostingsEnum#advance(int)}.  Larger values result in
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

  // Starts a new term
  long freqStart;
  long proxStart;
  int lastPayloadLength;
  int lastOffsetLength;
  int lastPosition;
  int lastOffset;

  final static StandardTermState emptyState = new StandardTermState();
  StandardTermState lastState;

  // private String segment;

  /** Creates a {@link Lucene40PostingsWriter}, with the
   *  {@link #DEFAULT_SKIP_INTERVAL}. */
  public Lucene40PostingsWriter(SegmentWriteState state) throws IOException {
    this(state, DEFAULT_SKIP_INTERVAL);
  }
  
  /** Creates a {@link Lucene40PostingsWriter}, with the
   *  specified {@code skipInterval}. */
  public Lucene40PostingsWriter(SegmentWriteState state, int skipInterval) throws IOException {
    super();
    this.skipInterval = skipInterval;
    this.skipMinimum = skipInterval; /* set to the same for now */
    // this.segment = state.segmentName;
    String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene40PostingsFormat.FREQ_EXTENSION);
    freqOut = state.directory.createOutput(fileName, state.context);
    boolean success = false;
    IndexOutput proxOut = null;
    try {
      CodecUtil.writeHeader(freqOut, Lucene40PostingsReader.FRQ_CODEC, Lucene40PostingsReader.VERSION_CURRENT);
      // TODO: this is a best effort, if one of these fields has no postings
      // then we make an empty prx file, same as if we are wrapped in 
      // per-field postingsformat. maybe... we shouldn't
      // bother w/ this opto?  just create empty prx file...?
      if (state.fieldInfos.hasProx()) {
        // At least one field does not omit TF, so create the
        // prox file
        fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene40PostingsFormat.PROX_EXTENSION);
        proxOut = state.directory.createOutput(fileName, state.context);
        CodecUtil.writeHeader(proxOut, Lucene40PostingsReader.PRX_CODEC, Lucene40PostingsReader.VERSION_CURRENT);
      } else {
        // Every field omits TF so we will write no prox file
        proxOut = null;
      }
      this.proxOut = proxOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(freqOut, proxOut);
      }
    }

    totalNumDocs = state.segmentInfo.maxDoc();

    skipListWriter = new Lucene40SkipListWriter(skipInterval,
                                               maxSkipLevels,
                                               totalNumDocs,
                                               freqOut,
                                               proxOut);
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeHeader(termsOut, Lucene40PostingsReader.TERMS_CODEC, Lucene40PostingsReader.VERSION_CURRENT);
    termsOut.writeInt(skipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    termsOut.writeInt(skipMinimum);                 // write skipMinimum
  }

  @Override
  public BlockTermState newTermState() {
    return new StandardTermState();
  }

  @Override
  public void startTerm() {
    freqStart = freqOut.getFilePointer();
    //if (DEBUG) System.out.println("SPW: startTerm freqOut.fp=" + freqStart);
    if (proxOut != null) {
      proxStart = proxOut.getFilePointer();
    }
    // force first payload to write its length
    lastPayloadLength = -1;
    // force first offset to write its length
    lastOffsetLength = -1;
    skipListWriter.resetSkip();
  }

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public int setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    //System.out.println("SPW: setField");
    /*
    if (BlockTreeTermsWriter.DEBUG && fieldInfo.name.equals("id")) {
      DEBUG = true;
    } else {
      DEBUG = false;
    }
    */

    lastState = emptyState;
    //System.out.println("  set init blockFreqStart=" + freqStart);
    //System.out.println("  set init blockProxStart=" + proxStart);
    return 0;
  }

  int lastDocID;
  int df;

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // if (DEBUG) System.out.println("SPW:   startDoc seg=" + segment + " docID=" + docID + " tf=" + termDocFreq + " freqOut.fp=" + freqOut.getFilePointer());

    final int delta = docID - lastDocID;
    
    if (docID < 0 || (df > 0 && delta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )", freqOut.toString());
    }

    if ((++df % skipInterval) == 0) {
      skipListWriter.setSkipData(lastDocID, writePayloads, lastPayloadLength, writeOffsets, lastOffsetLength);
      skipListWriter.bufferSkip(df);
    }

    assert docID < totalNumDocs: "docID=" + docID + " totalNumDocs=" + totalNumDocs;

    lastDocID = docID;
    if (indexOptions == IndexOptions.DOCS) {
      freqOut.writeVInt(delta);
    } else if (1 == termDocFreq) {
      freqOut.writeVInt((delta<<1) | 1);
    } else {
      freqOut.writeVInt(delta<<1);
      freqOut.writeVInt(termDocFreq);
    }

    lastPosition = 0;
    lastOffset = 0;
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    //if (DEBUG) System.out.println("SPW:     addPos pos=" + position + " payload=" + (payload == null ? "null" : (payload.length + " bytes")) + " proxFP=" + proxOut.getFilePointer());
    assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 : "invalid indexOptions: " + indexOptions;
    assert proxOut != null;

    final int delta = position - lastPosition;
    
    assert delta >= 0: "position=" + position + " lastPosition=" + lastPosition;            // not quite right (if pos=0 is repeated twice we don't catch it)

    lastPosition = position;

    int payloadLength = 0;

    if (writePayloads) {
      payloadLength = payload == null ? 0 : payload.length;

      if (payloadLength != lastPayloadLength) {
        lastPayloadLength = payloadLength;
        proxOut.writeVInt((delta<<1)|1);
        proxOut.writeVInt(payloadLength);
      } else {
        proxOut.writeVInt(delta << 1);
      }
    } else {
      proxOut.writeVInt(delta);
    }
    
    if (writeOffsets) {
      // don't use startOffset - lastEndOffset, because this creates lots of negative vints for synonyms,
      // and the numbers aren't that much smaller anyways.
      int offsetDelta = startOffset - lastOffset;
      int offsetLength = endOffset - startOffset;
      assert offsetDelta >= 0 && offsetLength >= 0 : "startOffset=" + startOffset + ",lastOffset=" + lastOffset + ",endOffset=" + endOffset;
      if (offsetLength != lastOffsetLength) {
        proxOut.writeVInt(offsetDelta << 1 | 1);
        proxOut.writeVInt(offsetLength);
      } else {
        proxOut.writeVInt(offsetDelta << 1);
      }
      lastOffset = startOffset;
      lastOffsetLength = offsetLength;
    }
    
    if (payloadLength > 0) {
      proxOut.writeBytes(payload.bytes, payload.offset, payloadLength);
    }
  }

  @Override
  public void finishDoc() {
  }

  private static class StandardTermState extends BlockTermState {
    public long freqStart;
    public long proxStart;
    public long skipOffset;
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    StandardTermState state = (StandardTermState) _state;
    // if (DEBUG) System.out.println("SPW: finishTerm seg=" + segment + " freqStart=" + freqStart);
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == df;
    state.freqStart = freqStart;
    state.proxStart = proxStart;
    if (df >= skipMinimum) {
      state.skipOffset = skipListWriter.writeSkip(freqOut)-freqStart;
    } else {
      state.skipOffset = -1;
    }
    lastDocID = 0;
    df = 0;
  }

  @Override
  public void encodeTerm(long[] empty, DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    StandardTermState state = (StandardTermState)_state;
    if (absolute) {
      lastState = emptyState;
    }
    out.writeVLong(state.freqStart - lastState.freqStart);
    if (state.skipOffset != -1) {
      assert state.skipOffset > 0;
      out.writeVLong(state.skipOffset);
    }
    if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
      out.writeVLong(state.proxStart - lastState.proxStart);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    try {
      freqOut.close();
    } finally {
      if (proxOut != null) {
        proxOut.close();
      }
    }
  }
}

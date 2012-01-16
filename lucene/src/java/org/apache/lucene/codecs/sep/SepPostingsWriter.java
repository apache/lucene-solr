package org.apache.lucene.codecs.sep;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
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
  IndexOutput termsOut;

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

  // Holds pending byte[] blob for the current terms block
  private final RAMOutputStream indexBytesWriter = new RAMOutputStream();

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
      final String docFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, DOC_EXTENSION);
      docOut = factory.createOutput(state.directory, docFileName, state.context);
      docIndex = docOut.index();
      
      if (state.fieldInfos.hasFreq()) {
        final String frqFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, FREQ_EXTENSION);
        freqOut = factory.createOutput(state.directory, frqFileName, state.context);
        freqIndex = freqOut.index();
      }

      if (state.fieldInfos.hasProx()) {      
        final String posFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, POS_EXTENSION);
        posOut = factory.createOutput(state.directory, posFileName, state.context);
        posIndex = posOut.index();
        
        // TODO: -- only if at least one field stores payloads?
        final String payloadFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, PAYLOAD_EXTENSION);
        payloadOut = state.directory.createOutput(payloadFileName, state.context);
      }
      
      final String skipFileName = IndexFileNames.segmentFileName(state.segmentName, state.segmentSuffix, SKIP_EXTENSION);
      skipOut = state.directory.createOutput(skipFileName, state.context);
      
      totalNumDocs = state.numDocs;
      
      skipListWriter = new SepSkipListWriter(skipInterval,
          maxSkipLevels,
          state.numDocs,
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
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.indexOptions = fieldInfo.indexOptions;
    if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
      throw new UnsupportedOperationException("this codec cannot index offsets");
    }
    skipListWriter.setIndexOptions(indexOptions);
    storePayloads = indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS && fieldInfo.storePayloads;
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

  private static class PendingTerm {
    public final IntIndexOutput.Index docIndex;
    public final IntIndexOutput.Index freqIndex;
    public final IntIndexOutput.Index posIndex;
    public final long payloadFP;
    public final long skipFP;

    public PendingTerm(IntIndexOutput.Index docIndex, IntIndexOutput.Index freqIndex, IntIndexOutput.Index posIndex, long payloadFP, long skipFP) {
      this.docIndex = docIndex;
      this.freqIndex = freqIndex;
      this.posIndex = posIndex;
      this.payloadFP = payloadFP;
      this.skipFP = skipFP;
    }
  }

  private final List<PendingTerm> pendingTerms = new ArrayList<PendingTerm>();

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(TermStats stats) throws IOException {
    // TODO: -- wasteful we are counting this in two places?
    assert stats.docFreq > 0;
    assert stats.docFreq == df;

    final IntIndexOutput.Index docIndexCopy = docOut.index();
    docIndexCopy.copyFrom(docIndex, false);

    final IntIndexOutput.Index freqIndexCopy;
    final IntIndexOutput.Index posIndexCopy;
    if (indexOptions != IndexOptions.DOCS_ONLY) {
      freqIndexCopy = freqOut.index();
      freqIndexCopy.copyFrom(freqIndex, false);
      if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        posIndexCopy = posOut.index();
        posIndexCopy.copyFrom(posIndex, false);
      } else {
        posIndexCopy = null;
      }
    } else {
      freqIndexCopy = null;
      posIndexCopy = null;
    }

    final long skipFP;
    if (df >= skipMinimum) {
      skipFP = skipOut.getFilePointer();
      //System.out.println("  skipFP=" + skipFP);
      skipListWriter.writeSkip(skipOut);
      //System.out.println("    numBytes=" + (skipOut.getFilePointer()-skipFP));
    } else {
      skipFP = -1;
    }

    lastDocID = 0;
    df = 0;

    pendingTerms.add(new PendingTerm(docIndexCopy,
                                     freqIndexCopy,
                                     posIndexCopy,
                                     payloadStart,
                                     skipFP));
  }

  @Override
  public void flushTermsBlock(int start, int count) throws IOException {
    //System.out.println("SEPW: flushTermsBlock: start=" + start + " count=" + count + " pendingTerms.size()=" + pendingTerms.size() + " termsOut.fp=" + termsOut.getFilePointer());
    assert indexBytesWriter.getFilePointer() == 0;
    final int absStart = pendingTerms.size() - start;
    final List<PendingTerm> slice = pendingTerms.subList(absStart, absStart+count);

    long lastPayloadFP = 0;
    long lastSkipFP = 0;

    if (count == 0) {
      termsOut.writeByte((byte) 0);
      return;
    }

    final PendingTerm firstTerm = slice.get(0);
    final IntIndexOutput.Index docIndexFlush = firstTerm.docIndex;
    final IntIndexOutput.Index freqIndexFlush = firstTerm.freqIndex;
    final IntIndexOutput.Index posIndexFlush = firstTerm.posIndex;

    for(int idx=0;idx<slice.size();idx++) {
      final boolean isFirstTerm = idx == 0;
      final PendingTerm t = slice.get(idx);
      //System.out.println("  write idx=" + idx + " docIndex=" + t.docIndex);
      docIndexFlush.copyFrom(t.docIndex, false);
      docIndexFlush.write(indexBytesWriter, isFirstTerm);
      if (indexOptions != IndexOptions.DOCS_ONLY) {
        freqIndexFlush.copyFrom(t.freqIndex, false);
        freqIndexFlush.write(indexBytesWriter, isFirstTerm);
        //System.out.println("    freqIndex=" + t.freqIndex);
        if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
          posIndexFlush.copyFrom(t.posIndex, false);
          posIndexFlush.write(indexBytesWriter, isFirstTerm);
          //System.out.println("    posIndex=" + t.posIndex);
          if (storePayloads) {
            //System.out.println("    payloadFP=" + t.payloadFP);
            if (isFirstTerm) {
              indexBytesWriter.writeVLong(t.payloadFP);
            } else {
              indexBytesWriter.writeVLong(t.payloadFP - lastPayloadFP);
            }
            lastPayloadFP = t.payloadFP;
          }
        }
      }

      if (t.skipFP != -1) {
        if (isFirstTerm) {
          indexBytesWriter.writeVLong(t.skipFP);
        } else {
          indexBytesWriter.writeVLong(t.skipFP - lastSkipFP);
        }
        lastSkipFP = t.skipFP;
        //System.out.println("    skipFP=" + t.skipFP);
      }
    }

    //System.out.println("  numBytes=" + indexBytesWriter.getFilePointer());
    termsOut.writeVLong((int) indexBytesWriter.getFilePointer());
    indexBytesWriter.writeTo(termsOut);
    indexBytesWriter.reset();
    slice.clear();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docOut, skipOut, freqOut, posOut, payloadOut);
  }
}

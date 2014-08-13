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
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/** Concrete class that reads the current doc/freq/skip
 *  postings format.    
 *
 * @lucene.experimental
 */

// TODO: -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class SepPostingsReader extends PostingsReaderBase {

  private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SepPostingsReader.class);

  final IntIndexInput freqIn;
  final IntIndexInput docIn;
  final IntIndexInput posIn;
  final IndexInput payloadIn;
  final IndexInput skipIn;

  int skipInterval;
  int maxSkipLevels;
  int skipMinimum;

  public SepPostingsReader(Directory dir, FieldInfos fieldInfos, SegmentInfo segmentInfo, IOContext context, IntStreamFactory intFactory, String segmentSuffix) throws IOException {
    boolean success = false;
    try {

      final String docFileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, SepPostingsWriter.DOC_EXTENSION);
      docIn = intFactory.openInput(dir, docFileName, context);

      skipIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, SepPostingsWriter.SKIP_EXTENSION), context);

      if (fieldInfos.hasFreq()) {
        freqIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, SepPostingsWriter.FREQ_EXTENSION), context);        
      } else {
        freqIn = null;
      }
      if (fieldInfos.hasProx()) {
        posIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, SepPostingsWriter.POS_EXTENSION), context);
        payloadIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, SepPostingsWriter.PAYLOAD_EXTENSION), context);
      } else {
        posIn = null;
        payloadIn = null;
      }
      success = true;
    } finally {
      if (!success) {
        close();
      }
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn, SepPostingsWriter.CODEC,
      SepPostingsWriter.VERSION_START, SepPostingsWriter.VERSION_START);
    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
    skipMinimum = termsIn.readInt();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(freqIn, docIn, skipIn, posIn, payloadIn);
  }

  private static final class SepTermState extends BlockTermState {
    // We store only the seek point to the docs file because
    // the rest of the info (freqIndex, posIndex, etc.) is
    // stored in the docs file:
    IntIndexInput.Index docIndex;
    IntIndexInput.Index posIndex;
    IntIndexInput.Index freqIndex;
    long payloadFP;
    long skipFP;

    @Override
    public SepTermState clone() {
      SepTermState other = new SepTermState();
      other.copyFrom(this);
      return other;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      SepTermState other = (SepTermState) _other;
      if (docIndex == null) {
        docIndex = other.docIndex.clone();
      } else {
        docIndex.copyFrom(other.docIndex);
      }
      if (other.freqIndex != null) {
        if (freqIndex == null) {
          freqIndex = other.freqIndex.clone();
        } else {
          freqIndex.copyFrom(other.freqIndex);
        }
      } else {
        freqIndex = null;
      }
      if (other.posIndex != null) {
        if (posIndex == null) {
          posIndex = other.posIndex.clone();
        } else {
          posIndex.copyFrom(other.posIndex);
        }
      } else {
        posIndex = null;
      }
      payloadFP = other.payloadFP;
      skipFP = other.skipFP;
    }

    @Override
    public String toString() {
      return super.toString() + " docIndex=" + docIndex + " freqIndex=" + freqIndex + " posIndex=" + posIndex + " payloadFP=" + payloadFP + " skipFP=" + skipFP;
    }
  }

  @Override
  public BlockTermState newTermState() throws IOException {
    final SepTermState state = new SepTermState();
    state.docIndex = docIn.index();
    if (freqIn != null) {
      state.freqIndex = freqIn.index();
    }
    if (posIn != null) {
      state.posIndex = posIn.index();
    }
    return state;
  }

  @Override
  public void decodeTerm(long[] empty, DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute) 
    throws IOException {
    final SepTermState termState = (SepTermState) _termState;
    termState.docIndex.read(in, absolute);
    if (fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
      termState.freqIndex.read(in, absolute);
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        //System.out.println("  freqIndex=" + termState.freqIndex);
        termState.posIndex.read(in, absolute);
        //System.out.println("  posIndex=" + termState.posIndex);
        if (fieldInfo.hasPayloads()) {
          if (absolute) {
            termState.payloadFP = in.readVLong();
          } else {
            termState.payloadFP += in.readVLong();
          }
          //System.out.println("  payloadFP=" + termState.payloadFP);
        }
      }
    }

    if (termState.docFreq >= skipMinimum) {
      //System.out.println("   readSkip @ " + in.getPosition());
      if (absolute) {
        termState.skipFP = in.readVLong();
      } else {
        termState.skipFP += in.readVLong();
      }
      //System.out.println("  skipFP=" + termState.skipFP);
    } else if (absolute) {
      termState.skipFP = 0;
    }
  }

  @Override
  public DocsEnum docs(FieldInfo fieldInfo, BlockTermState _termState, Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    final SepTermState termState = (SepTermState) _termState;
    SepDocsEnum docsEnum;
    if (reuse == null || !(reuse instanceof SepDocsEnum)) {
      docsEnum = new SepDocsEnum();
    } else {
      docsEnum = (SepDocsEnum) reuse;
      if (docsEnum.startDocIn != docIn) {
        // If you are using ParellelReader, and pass in a
        // reused DocsAndPositionsEnum, it could have come
        // from another reader also using sep codec
        docsEnum = new SepDocsEnum();        
      }
    }

    return docsEnum.init(fieldInfo, termState, liveDocs);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState _termState, Bits liveDocs,
                                               DocsAndPositionsEnum reuse, int flags)
    throws IOException {

    assert fieldInfo.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    final SepTermState termState = (SepTermState) _termState;
    SepDocsAndPositionsEnum postingsEnum;
    if (reuse == null || !(reuse instanceof SepDocsAndPositionsEnum)) {
      postingsEnum = new SepDocsAndPositionsEnum();
    } else {
      postingsEnum = (SepDocsAndPositionsEnum) reuse;
      if (postingsEnum.startDocIn != docIn) {
        // If you are using ParellelReader, and pass in a
        // reused DocsAndPositionsEnum, it could have come
        // from another reader also using sep codec
        postingsEnum = new SepDocsAndPositionsEnum();        
      }
    }

    return postingsEnum.init(fieldInfo, termState, liveDocs);
  }

  class SepDocsEnum extends DocsEnum {
    int docFreq;
    int doc = -1;
    int accum;
    int count;
    int freq;
    long freqStart;

    // TODO: -- should we do omitTF with 2 different enum classes?
    private boolean omitTF;
    private IndexOptions indexOptions;
    private boolean storePayloads;
    private Bits liveDocs;
    private final IntIndexInput.Reader docReader;
    private final IntIndexInput.Reader freqReader;
    private long skipFP;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index freqIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;

    // TODO: -- should we do hasProx with 2 different enum classes?

    boolean skipped;
    SepSkipListReader skipper;

    SepDocsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docIndex = docIn.index();
      if (freqIn != null) {
        freqReader = freqIn.reader();
        freqIndex = freqIn.index();
      } else {
        freqReader = null;
        freqIndex = null;
      }
      if (posIn != null) {
        posIndex = posIn.index();                 // only init this so skipper can read it
      } else {
        posIndex = null;
      }
    }

    SepDocsEnum init(FieldInfo fieldInfo, SepTermState termState, Bits liveDocs) throws IOException {
      this.liveDocs = liveDocs;
      this.indexOptions = fieldInfo.getIndexOptions();
      omitTF = indexOptions == IndexOptions.DOCS_ONLY;
      storePayloads = fieldInfo.hasPayloads();

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.copyFrom(termState.docIndex);
      docIndex.seek(docReader);

      if (!omitTF) {
        freqIndex.copyFrom(termState.freqIndex);
        freqIndex.seek(freqReader);
      }

      docFreq = termState.docFreq;
      // NOTE: unused if docFreq < skipMinimum:
      skipFP = termState.skipFP;
      count = 0;
      doc = -1;
      accum = 0;
      freq = 1;
      skipped = false;

      return this;
    }

    @Override
    public int nextDoc() throws IOException {

      while(true) {
        if (count == docFreq) {
          return doc = NO_MORE_DOCS;
        }

        count++;

        // Decode next doc
        //System.out.println("decode docDelta:");
        accum += docReader.next();
          
        if (!omitTF) {
          //System.out.println("decode freq:");
          freq = freqReader.next();
        }

        if (liveDocs == null || liveDocs.get(accum)) {
          break;
        }
      }
      return (doc = accum);
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {

      if ((target - skipInterval) >= doc && docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new SepSkipListReader(skipIn.clone(),
                                          freqIn,
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);

        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipFP,
                       docIndex,
                       freqIndex,
                       posIndex,
                       0,
                       docFreq,
                       storePayloads);
          skipper.setIndexOptions(indexOptions);

          skipped = true;
        }

        final int newCount = skipper.skipTo(target); 

        if (newCount > count) {

          // Skipper did move
          if (!omitTF) {
            skipper.getFreqIndex().seek(freqReader);
          }
          skipper.getDocIndex().seek(docReader);
          count = newCount;
          doc = accum = skipper.getDoc();
        }
      }
        
      // Now, linear scan for the rest:
      do {
        if (nextDoc() == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        }
      } while (target > doc);

      return doc;
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }

  class SepDocsAndPositionsEnum extends DocsAndPositionsEnum {
    int docFreq;
    int doc = -1;
    int accum;
    int count;
    int freq;
    long freqStart;

    private boolean storePayloads;
    private Bits liveDocs;
    private final IntIndexInput.Reader docReader;
    private final IntIndexInput.Reader freqReader;
    private final IntIndexInput.Reader posReader;
    private final IndexInput payloadIn;
    private long skipFP;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index freqIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;

    private long payloadFP;

    private int pendingPosCount;
    private int position;
    private int payloadLength;
    private long pendingPayloadBytes;

    private boolean skipped;
    private SepSkipListReader skipper;
    private boolean payloadPending;
    private boolean posSeekPending;

    SepDocsAndPositionsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docIndex = docIn.index();
      freqReader = freqIn.reader();
      freqIndex = freqIn.index();
      posReader = posIn.reader();
      posIndex = posIn.index();
      payloadIn = SepPostingsReader.this.payloadIn.clone();
    }

    SepDocsAndPositionsEnum init(FieldInfo fieldInfo, SepTermState termState, Bits liveDocs) throws IOException {
      this.liveDocs = liveDocs;
      storePayloads = fieldInfo.hasPayloads();
      //System.out.println("Sep D&P init");

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.copyFrom(termState.docIndex);
      docIndex.seek(docReader);
      //System.out.println("  docIndex=" + docIndex);

      freqIndex.copyFrom(termState.freqIndex);
      freqIndex.seek(freqReader);
      //System.out.println("  freqIndex=" + freqIndex);

      posIndex.copyFrom(termState.posIndex);
      //System.out.println("  posIndex=" + posIndex);
      posSeekPending = true;
      payloadPending = false;

      payloadFP = termState.payloadFP;
      skipFP = termState.skipFP;
      //System.out.println("  skipFP=" + skipFP);

      docFreq = termState.docFreq;
      count = 0;
      doc = -1;
      accum = 0;
      pendingPosCount = 0;
      pendingPayloadBytes = 0;
      skipped = false;

      return this;
    }

    @Override
    public int nextDoc() throws IOException {

      while(true) {
        if (count == docFreq) {
          return doc = NO_MORE_DOCS;
        }

        count++;

        // TODO: maybe we should do the 1-bit trick for encoding
        // freq=1 case?

        // Decode next doc
        //System.out.println("  sep d&p read doc");
        accum += docReader.next();

        //System.out.println("  sep d&p read freq");
        freq = freqReader.next();

        pendingPosCount += freq;

        if (liveDocs == null || liveDocs.get(accum)) {
          break;
        }
      }

      position = 0;
      return (doc = accum);
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      //System.out.println("SepD&P advance target=" + target + " vs current=" + doc + " this=" + this);

      if ((target - skipInterval) >= doc && docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          //System.out.println("  create skipper");
          // This DocsEnum has never done any skipping
          skipper = new SepSkipListReader(skipIn.clone(),
                                          freqIn,
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);
        }

        if (!skipped) {
          //System.out.println("  init skip data skipFP=" + skipFP);
          // We haven't yet skipped for this posting
          skipper.init(skipFP,
                       docIndex,
                       freqIndex,
                       posIndex,
                       payloadFP,
                       docFreq,
                       storePayloads);
          skipper.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
          skipped = true;
        }
        final int newCount = skipper.skipTo(target); 
        //System.out.println("  skip newCount=" + newCount + " vs " + count);

        if (newCount > count) {

          // Skipper did move
          skipper.getFreqIndex().seek(freqReader);
          skipper.getDocIndex().seek(docReader);
          //System.out.println("  doc seek'd to " + skipper.getDocIndex());
          // NOTE: don't seek pos here; do it lazily
          // instead.  Eg a PhraseQuery may skip to many
          // docs before finally asking for positions...
          posIndex.copyFrom(skipper.getPosIndex());
          posSeekPending = true;
          count = newCount;
          doc = accum = skipper.getDoc();
          //System.out.println("    moved to doc=" + doc);
          //payloadIn.seek(skipper.getPayloadPointer());
          payloadFP = skipper.getPayloadPointer();
          pendingPosCount = 0;
          pendingPayloadBytes = 0;
          payloadPending = false;
          payloadLength = skipper.getPayloadLength();
          //System.out.println("    move payloadLen=" + payloadLength);
        }
      }
        
      // Now, linear scan for the rest:
      do {
        if (nextDoc() == NO_MORE_DOCS) {
          //System.out.println("  advance nextDoc=END");
          return NO_MORE_DOCS;
        }
        //System.out.println("  advance nextDoc=" + doc);
      } while (target > doc);

      //System.out.println("  return doc=" + doc);
      return doc;
    }

    @Override
    public int nextPosition() throws IOException {
      if (posSeekPending) {
        posIndex.seek(posReader);
        payloadIn.seek(payloadFP);
        posSeekPending = false;
      }

      // scan over any docs that were iterated without their
      // positions
      while (pendingPosCount > freq) {
        final int code = posReader.next();
        if (storePayloads && (code & 1) != 0) {
          // Payload length has changed
          payloadLength = posReader.next();
          assert payloadLength >= 0;
        }
        pendingPosCount--;
        position = 0;
        pendingPayloadBytes += payloadLength;
      }

      final int code = posReader.next();

      if (storePayloads) {
        if ((code & 1) != 0) {
          // Payload length has changed
          payloadLength = posReader.next();
          assert payloadLength >= 0;
        }
        position += code >>> 1;
        pendingPayloadBytes += payloadLength;
        payloadPending = payloadLength > 0;
      } else {
        position += code;
      }
    
      pendingPosCount--;
      assert pendingPosCount >= 0;
      return position;
    }

    @Override
    public int startOffset() {
      return -1;
    }

    @Override
    public int endOffset() {
      return -1;
    }

    private BytesRefBuilder payload;

    @Override
    public BytesRef getPayload() throws IOException {
      if (!payloadPending) {
        return null;
      }
      
      if (pendingPayloadBytes == 0) {
        return payload.get();
      }

      assert pendingPayloadBytes >= payloadLength;

      if (pendingPayloadBytes > payloadLength) {
        payloadIn.seek(payloadIn.getFilePointer() + (pendingPayloadBytes - payloadLength));
      }

      if (payload == null) {
        payload = new BytesRefBuilder();
      }
      payload.grow(payloadLength);

      payloadIn.readBytes(payload.bytes(), 0, payloadLength);
      payload.setLength(payloadLength);
      pendingPayloadBytes = 0;
      return payload.get();
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }

  @Override
  public long ramBytesUsed() {
    return RAM_BYTES_USED;
  }

  @Override
  public void checkIntegrity() throws IOException {
    // TODO: remove sep layout, its fallen behind on features...
  }
}

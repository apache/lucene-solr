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
import java.util.Collection;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.BulkPostingsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.codecs.BlockTermState;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

/** Concrete class that reads the current doc/freq/skip
 *  postings format.    
 *
 * @lucene.experimental
 */

// TODO: -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class SepPostingsReaderImpl extends PostingsReaderBase {

  final IntIndexInput freqIn;
  final IntIndexInput docIn;
  final IntIndexInput posIn;
  final IndexInput payloadIn;
  final IndexInput skipIn;

  int skipInterval;
  int maxSkipLevels;
  int skipMinimum;

  public SepPostingsReaderImpl(Directory dir, SegmentInfo segmentInfo, int readBufferSize, IntStreamFactory intFactory, String codecId) throws IOException {

    boolean success = false;
    try {

      final String docFileName = IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.DOC_EXTENSION);
      docIn = intFactory.openInput(dir, docFileName);

      skipIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.SKIP_EXTENSION), readBufferSize);

      if (segmentInfo.getHasProx()) {
        freqIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.FREQ_EXTENSION), readBufferSize);
        posIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.POS_EXTENSION), readBufferSize);
        payloadIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.PAYLOAD_EXTENSION), readBufferSize);
      } else {
        posIn = null;
        payloadIn = null;
        freqIn = null;
      }
      success = true;
    } finally {
      if (!success) {
        close();
      }
    }
  }

  public static void files(SegmentInfo segmentInfo, String codecId, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.DOC_EXTENSION));
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.SKIP_EXTENSION));

    if (segmentInfo.getHasProx()) {
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.FREQ_EXTENSION));
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.POS_EXTENSION));
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, SepPostingsWriterImpl.PAYLOAD_EXTENSION));
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn, SepPostingsWriterImpl.CODEC,
      SepPostingsWriterImpl.VERSION_START, SepPostingsWriterImpl.VERSION_START);
    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
    skipMinimum = termsIn.readInt();
  }

  @Override
  public void close() throws IOException {
    try {
      if (freqIn != null)
        freqIn.close();
    } finally {
      try {
        if (docIn != null)
          docIn.close();
      } finally {
        try {
          if (skipIn != null)
            skipIn.close();
        } finally {
          try {
            if (posIn != null) {
              posIn.close();
            }
          } finally {
            if (payloadIn != null) {
              payloadIn.close();
            }
          }
        }
      }
    }
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

    // Only used for "primary" term state; these are never
    // copied on clone:
    byte[] bytes;
    ByteArrayDataInput bytesReader;

    @Override
    public Object clone() {
      SepTermState other = (SepTermState) super.clone();
      other.docIndex = (IntIndexInput.Index) docIndex.clone();
      if (freqIndex != null) {
        other.freqIndex = (IntIndexInput.Index) freqIndex.clone();
      }
      if (posIndex != null) {
        other.posIndex = (IntIndexInput.Index) posIndex.clone();
      }
      return other;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      SepTermState other = (SepTermState) _other;
      docIndex.set(other.docIndex);
      if (freqIndex != null && other.freqIndex != null) {
        freqIndex.set(other.freqIndex);
      }
      if (posIndex != null && other.posIndex != null) {
        posIndex.set(other.posIndex);
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
  public void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo, BlockTermState _termState) throws IOException {
    final SepTermState termState = (SepTermState) _termState;
    final int len = termsIn.readVInt();
    //System.out.println("SepR.readTermsBlock len=" + len);
    if (termState.bytes == null) {
      termState.bytes = new byte[ArrayUtil.oversize(len, 1)];
      termState.bytesReader = new ByteArrayDataInput(termState.bytes);
    } else if (termState.bytes.length < len) {
      termState.bytes = new byte[ArrayUtil.oversize(len, 1)];
    }
    termState.bytesReader.reset(termState.bytes, 0, len);
    termsIn.readBytes(termState.bytes, 0, len);
  }

  @Override
  public void nextTerm(FieldInfo fieldInfo, BlockTermState _termState) throws IOException {
    final SepTermState termState = (SepTermState) _termState;
    //System.out.println("SepR.nextTerm termCount=" + termState.termCount);
    //System.out.println("  docFreq=" + termState.docFreq);
    final boolean isFirstTerm = termState.termCount == 0;
    termState.docIndex.read(termState.bytesReader, isFirstTerm);
    //System.out.println("  docIndex=" + termState.docIndex);
    if (!fieldInfo.omitTermFreqAndPositions) {
      termState.freqIndex.read(termState.bytesReader, isFirstTerm);
      //System.out.println("  freqIndex=" + termState.freqIndex);
      termState.posIndex.read(termState.bytesReader, isFirstTerm);
      //System.out.println("  posIndex=" + termState.posIndex);
      if (fieldInfo.storePayloads) {
        if (isFirstTerm) {
          termState.payloadFP = termState.bytesReader.readVLong();
        } else {
          termState.payloadFP += termState.bytesReader.readVLong();
        }
        //System.out.println("  payloadFP=" + termState.payloadFP);
      }
    }
    if (termState.docFreq >= skipMinimum) {
      //System.out.println("   readSkip @ " + termState.bytesReader.pos);
      if (isFirstTerm) {
        termState.skipFP = termState.bytesReader.readVLong();
      } else {
        termState.skipFP += termState.bytesReader.readVLong();
      }
      //System.out.println("  skipFP=" + termState.skipFP);
    } else if (isFirstTerm) {
      termState.skipFP = termState.bytesReader.readVLong();
    }
  }

  @Override
  public DocsEnum docs(FieldInfo fieldInfo, BlockTermState _termState, Bits skipDocs, DocsEnum reuse) throws IOException {
    final SepTermState termState = (SepTermState) _termState;
    SepDocsEnum docsEnum;
    if (reuse == null || !(reuse instanceof SepDocsEnum) || !((SepDocsEnum) reuse).canReuse(docIn)) {
      docsEnum = new SepDocsEnum();
    } else {
      docsEnum = (SepDocsEnum) reuse;
    }

    return docsEnum.init(fieldInfo, termState, skipDocs);
  }

  private SepBulkPostingsEnum lastBulkEnum;

  @Override
  public BulkPostingsEnum bulkPostings(FieldInfo fieldInfo, BlockTermState _termState, BulkPostingsEnum reuse, boolean doFreqs, boolean doPositions) throws IOException {
    final SepTermState termState = (SepTermState) _termState;
    final SepBulkPostingsEnum lastBulkEnum = this.lastBulkEnum;
    if (lastBulkEnum != null && reuse == lastBulkEnum) {
      // fastpath
      return lastBulkEnum.init(termState);
    } else {
      SepBulkPostingsEnum postingsEnum;
      if (reuse == null || !(reuse instanceof SepBulkPostingsEnum) || !((SepBulkPostingsEnum) reuse).canReuse(fieldInfo, docIn, doFreqs, doPositions)) {
        postingsEnum = new SepBulkPostingsEnum(fieldInfo, doFreqs, doPositions);
      } else {
        postingsEnum = (SepBulkPostingsEnum) reuse;
      }
      this.lastBulkEnum = postingsEnum;
      return postingsEnum.init(termState);
    }
  }

  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState _termState, Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
    assert !fieldInfo.omitTermFreqAndPositions;
    final SepTermState termState = (SepTermState) _termState;
    SepDocsAndPositionsEnum postingsEnum;
    if (reuse == null || !(reuse instanceof SepDocsAndPositionsEnum) || !((SepDocsAndPositionsEnum) reuse).canReuse(docIn)) {
      postingsEnum = new SepDocsAndPositionsEnum();
    } else {
      postingsEnum = (SepDocsAndPositionsEnum) reuse;
    }

    return postingsEnum.init(fieldInfo, termState, skipDocs);
  }

  class SepDocsEnum extends DocsEnum {
    int docFreq;
    int doc;
    int count;
    int freq;

    // TODO: -- should we do omitTF with 2 different enum classes?
    private boolean omitTF;
    private boolean storePayloads;
    private Bits skipDocs;
    private final BulkPostingsEnum.BlockReader docReader;
    private final int[] docDeltaBuffer;
    private int docDeltaUpto;
    private int docDeltaLimit;
    private final BulkPostingsEnum.BlockReader freqReader;
    private final int[] freqBuffer;
    private int freqUpto;
    private int freqLimit;
    private long skipFP;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index freqIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;

    boolean skipped;
    SepSkipListReader skipper;

    public SepDocsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docDeltaBuffer = docReader.getBuffer();
      docIndex = docIn.index();
      if (freqIn != null) {
        freqReader = freqIn.reader();
        freqBuffer = freqReader.getBuffer();
        freqIndex = freqIn.index();
      } else {
        freqReader = null;
        freqIndex = null;
        freqBuffer = null;
      }
      if (posIn != null) {
        posIndex = posIn.index();                 // only init this so skipper can read it
      } else {
        posIndex = null;
      }
    }

    // nocommit -- somehow we have to prevent re-decode of
    // the same block if we have just .next()'d to next term
    // in the terms dict -- this is an O(N^2) cost to eg
    // TermRangeQuery when it steps through low freq terms!!
    SepDocsEnum init(FieldInfo fieldInfo, SepTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;
      omitTF = fieldInfo.omitTermFreqAndPositions;
      storePayloads = fieldInfo.storePayloads;

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);
      docIndex.seek(docReader);
      docDeltaLimit = docReader.end();
      docDeltaUpto = docReader.offset();

      if (!omitTF) {
        freqIndex.set(termState.freqIndex);
        freqIndex.seek(freqReader);
        freqUpto = freqReader.offset();
        freqLimit = freqReader.end();
        //System.out.println("  freqIndex=" + freqIndex + " posIndex=" + posIndex);
      } else {
        freq = 1;
      }

      docDeltaUpto = docReader.offset();
      docDeltaLimit = docReader.end();

      docFreq = termState.docFreq;
      assert docFreq > 0;
      // NOTE: unused if docFreq < skipMinimum:
      skipFP = termState.skipFP;
      count = 0;
      doc = 0;
      skipped = false;
      //System.out.println("  docFreq=" + docFreq);

      return this;
    }

    public boolean canReuse(IntIndexInput docsIn) {
      return startDocIn == docsIn;
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("  sep.nextDoc");

      while(true) {
        if (count == docFreq) {
          return doc = NO_MORE_DOCS;
        }

        assert docDeltaUpto <= docDeltaLimit: "docDeltaUpto=" + docDeltaUpto + " docDeltaLimit=" + docDeltaLimit;

        if (docDeltaUpto == docDeltaLimit) {
          // refill
          //System.out.println("    fill docs");
          docDeltaLimit = docReader.fill();
          docDeltaUpto = 0;
        }

        count++;

        // Decode next doc
        doc += docDeltaBuffer[docDeltaUpto++];
        //System.out.println("    doc="+ doc + " docDeltaUpto=" + (docDeltaUpto-1) + " skipDocs=" + skipDocs + " deleted?=" + (skipDocs != null && skipDocs.get(doc)));
          
        if (!omitTF) {
          if (freqUpto == freqLimit) {
            // refill
            //System.out.println("    fill freqs");
            freqLimit = freqReader.fill();
            freqUpto = 0;
          }

          freq = freqBuffer[freqUpto++];
        }

        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        }
      }
      return doc;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      //System.out.println("SepDocsEnum.advance target=" + target);

      if ((target - skipInterval) >= doc && docFreq >= skipMinimum) {
      //System.out.println("SepDocsEnum.advance target=" + target);

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          //System.out.println("  init skipper");
          skipper = new SepSkipListReader((IndexInput) skipIn.clone(),
                                          freqIn,
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);

        }

        if (!skipped) {
          //System.out.println("  init skipper2");
          // We haven't yet skipped for this posting
          skipper.init(skipFP,
                       docIndex,
                       freqIndex,
                       posIndex,
                       0,
                       docFreq,
                       storePayloads);
          skipper.setOmitTF(omitTF);

          skipped = true;
        }

        final int newCount = skipper.skipTo(target); 

        if (newCount > count) {
          // Skipper did move
          if (!omitTF) {
            skipper.getFreqIndex().seek(freqReader);
            freqUpto = freqReader.offset();
            freqLimit = freqReader.end();
            if (freqUpto >= freqLimit) {
              freqLimit = freqReader.fill();
            }
          }
          skipper.getDocIndex().seek(docReader);
          docDeltaUpto = docReader.offset();
          docDeltaLimit = docReader.end();

          count = newCount;
          doc = skipper.getDoc();
          //System.out.println("  did move count=" + newCount + " doc=" + doc);
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
  }

  class SepDocsAndPositionsEnum extends DocsAndPositionsEnum {
    int docFreq;
    int doc;
    int count;
    int freq;

    private boolean storePayloads;
    private Bits skipDocs;
    private final BulkPostingsEnum.BlockReader docReader;
    private final int[] docDeltaBuffer;
    private int docDeltaUpto;
    private int docDeltaLimit;
    private final BulkPostingsEnum.BlockReader freqReader;
    private final int[] freqBuffer;
    private int freqUpto;
    private int freqLimit;
    private final BulkPostingsEnum.BlockReader posReader;
    private final int[] posBuffer;
    private int posUpto;
    private int posLimit;
    private long skipFP;

    private final IndexInput payloadIn;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index freqIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;

    private long payloadFP;

    private int pendingPosCount;
    private int position;
    private int payloadLength;
    private long pendingPayloadBytes;
    private boolean payloadPending;
    private boolean posSeekPending;

    boolean skipped;
    SepSkipListReader skipper;

    public SepDocsAndPositionsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docDeltaBuffer = docReader.getBuffer();
      docIndex = docIn.index();
      freqReader = freqIn.reader();
      freqBuffer = freqReader.getBuffer();
      freqIndex = freqIn.index();
      posReader = posIn.reader();
      posBuffer = posReader.getBuffer();
      posIndex = posIn.index();
      payloadIn = (IndexInput) SepPostingsReaderImpl.this.payloadIn.clone();
    }

    // nocommit -- somehow we have to prevent re-decode of
    // the same block if we have just .next()'d to next term
    // in the terms dict -- this is an O(N^2) cost to eg
    // TermRangeQuery when it steps through low freq terms!!
    SepDocsAndPositionsEnum init(FieldInfo fieldInfo, SepTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;
      //System.out.println("sep d&p init");
      assert !fieldInfo.omitTermFreqAndPositions;
      storePayloads = fieldInfo.storePayloads;
      //System.out.println("Sep D&P init");

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);
      // nocommit -- verify, during merge, this seek is
      // sometimes w/in block:
      docIndex.seek(docReader);
      docDeltaLimit = docReader.end();
      docDeltaUpto = docReader.offset();
      //System.out.println("  docIndex=" + docIndex);

      freqIndex.set(termState.freqIndex);
      freqIndex.seek(freqReader);
      freqLimit = freqReader.end();
      freqUpto = freqReader.offset();
      //System.out.println("  freqIndex=" + freqIndex);

      posIndex.set(termState.posIndex);
      //System.out.println("  posIndex=" + posIndex);
      posSeekPending = true;
      payloadPending = false;

      payloadFP = termState.payloadFP;
      skipFP = termState.skipFP;
      //System.out.println("  skipFP=" + skipFP);

      docDeltaLimit = docReader.end();
      docDeltaUpto = docReader.offset();
      /*
      if (docDeltaUpto >= docDeltaLimit) {
        // nocommit -- needed anymore?
        docDeltaLimit = docReader.fill();
        docDeltaUpto = 0;
      }
      */

      docFreq = termState.docFreq;
      assert docFreq > 0;
      count = 0;
      doc = 0;
      pendingPosCount = 0;
      pendingPayloadBytes = 0;
      skipped = false;

      //System.out.println("  docUpto=" + docDeltaUpto + " docMax=" + docDeltaLimit + " freqUpto=" + freqUpto + " freqMax=" + freqLimit);

      return this;
    }

    public boolean canReuse(IntIndexInput docsIn) {
      return startDocIn == docsIn;
    }

    @Override
    public int nextDoc() throws IOException {
      while(true) {
        if (count == docFreq) {
          return doc = NO_MORE_DOCS;
        }

        if (docDeltaUpto == docDeltaLimit) {
          // refill
          docDeltaLimit = docReader.fill();
          docDeltaUpto = 0;
        }

        count++;

        // Decode next doc
        doc += docDeltaBuffer[docDeltaUpto++];

        //System.out.println("  sep d&p read freq");
        if (freqUpto == freqLimit) {
          // refill
          freqLimit = freqReader.fill();
          freqUpto = 0;
        }

        freq = freqBuffer[freqUpto++];
        pendingPosCount += freq;

        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        }
      }

      position = 0;
      return doc;
    }

    @Override
    public int freq() {
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
          skipper = new SepSkipListReader((IndexInput) skipIn.clone(),
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
          skipped = true;
        }
        final int newCount = skipper.skipTo(target); 
        //System.out.println("  skip newCount=" + newCount + " vs " + count);

        if (newCount > count) {

          // Skipper did move
          skipper.getFreqIndex().seek(freqReader);
          freqUpto = freqReader.offset();
          freqLimit = freqReader.end();

          skipper.getDocIndex().seek(docReader);
          docDeltaUpto = docReader.offset();
          docDeltaLimit = docReader.end();

          // NOTE: don't seek pos here; do it lazily
          // instead.  Eg a PhraseQuery may skip to many
          // docs before finally asking for positions...
          posIndex.set(skipper.getPosIndex());
          posSeekPending = true;
          count = newCount;
          doc = skipper.getDoc();

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
        posLimit = posReader.end();
        posUpto = posReader.offset();
        payloadIn.seek(payloadFP);
        posSeekPending = false;
      }

      // scan over any docs that were iterated without their
      // positions
      while (pendingPosCount > freq) {

        final int code = nextPosInt();

        if (storePayloads && (code & 1) != 0) {
          // Payload length has changed
          payloadLength = nextPosInt();
          assert payloadLength >= 0;
        }
        pendingPosCount--;
        position = 0;
        pendingPayloadBytes += payloadLength;
      }

      final int code = nextPosInt();

      assert code >= 0;
      if (storePayloads) {
        if ((code & 1) != 0) {
          // Payload length has changed
          payloadLength = nextPosInt();
          assert payloadLength >= 0;
        }
        position += code >> 1;
        pendingPayloadBytes += payloadLength;
        payloadPending = payloadLength > 0;
      } else {
        position += code;
      }
    
      pendingPosCount--;
      assert pendingPosCount >= 0;
      return position;
    }

    private int nextPosInt() throws IOException {
      if (posUpto == posLimit) {
        posLimit = posReader.fill();
        posUpto = 0;
      }
      return posBuffer[posUpto++];
    }

    private BytesRef payload;

    @Override
    public BytesRef getPayload() throws IOException {
      if (!payloadPending) {
        throw new IOException("Either no payload exists at this term position or an attempt was made to load it more than once.");
      }

      assert pendingPayloadBytes >= payloadLength;

      if (pendingPayloadBytes > payloadLength) {
        payloadIn.seek(payloadIn.getFilePointer() + (pendingPayloadBytes - payloadLength));
      }

      if (payload == null) {
        payload = new BytesRef();
        payload.bytes = new byte[payloadLength];
      } else if (payload.bytes.length < payloadLength) {
        payload.grow(payloadLength);
      }

      payloadIn.readBytes(payload.bytes, 0, payloadLength);
      payloadPending = false;
      payload.length = payloadLength;
      pendingPayloadBytes = 0;
      return payload;
    }

    @Override
    public boolean hasPayload() {
      return payloadPending && payloadLength > 0;
    }
  }

  class SepBulkPostingsEnum extends BulkPostingsEnum {
    private int docFreq;

    private final BulkPostingsEnum.BlockReader docReader;
    private final IntIndexInput.Index docIndex;

    private final BulkPostingsEnum.BlockReader freqReader;
    private final IntIndexInput.Index freqIndex;

    private final BulkPostingsEnum.BlockReader posReader;
    private final IntIndexInput.Index posIndex;

    private final boolean storePayloads;
    private final boolean omitTF;
    private long skipFP;

    private final IntIndexInput startDocIn;

    private boolean skipped;
    private SepSkipListReader skipper;

    public SepBulkPostingsEnum(FieldInfo fieldInfo, boolean doFreq, boolean doPos) throws IOException {
      this.storePayloads = fieldInfo.storePayloads;
      this.omitTF = fieldInfo.omitTermFreqAndPositions;
      startDocIn = docIn;
      docReader = docIn.reader();
      docIndex = docIn.index();

      if (doFreq && !omitTF) {
        freqReader = freqIn.reader();
      } else {
        freqReader = null;
      }

      if (doPos && !omitTF) {
        if (storePayloads) {
          // Must rewrite each posDelta:
          posReader = new PosPayloadReader(posIn.reader());
        } else {
          // Pass through
          posReader = posIn.reader();
        }
      } else {
        posReader = null;
      }

      if (!omitTF) {
        // we have to pull these even if doFreq is false
        // just so we can decode the index from the docs
        // file
        freqIndex = freqIn.index();
        posIndex = posIn.index();
      } else {
        posIndex = null;
        freqIndex = null;
      }
    }

    public boolean canReuse(FieldInfo fieldInfo, IntIndexInput docIn, boolean doFreq, boolean doPos) {
      return fieldInfo.storePayloads == storePayloads &&
        startDocIn == docIn &&
        doFreq == (freqReader != null) &&
        doPos == (posReader != null);
    }

    // nocommit -- make sure this is tested!!

    // Only used when payloads were stored -- we cannot do
    // pass-through read for this since the payload lengths
    // are also encoded into the position deltas
    private final class PosPayloadReader extends BulkPostingsEnum.BlockReader {
      final BulkPostingsEnum.BlockReader other;
      private boolean fillPending;
      private int pendingOffset;
      private int limit;
      private boolean skipNext;

      public PosPayloadReader(BulkPostingsEnum.BlockReader other) {
        this.other = other;
      }

      void doAfterSeek() {
        limit = 0;
        skipNext = false;
        fillPending = false;
      }

      @Override
      public int[] getBuffer() {
        return other.getBuffer();
      }

      // nocommit -- make sure this works correctly in the
      // "reuse"/seek case
      @Override
      public int offset() {
        pendingOffset = other.offset();
        return 0;
      }

      @Override
      public int fill() throws IOException {
        // Translate code back to pos deltas, and filter out
        // any changes in payload length.  NOTE: this is a
        // perf hit on indices that encode payloads, even if
        // they use "normal" positional queries
        limit = 0;
        boolean skippedLast = false;
        do { 
          final int otherLimit = fillPending ? other.fill() : other.end();
          fillPending = true;
          assert otherLimit > pendingOffset;
          final int[] buffer = other.getBuffer();
          for(int i=pendingOffset;i<otherLimit;i++) {
            if (skipNext) {
              skipNext = false;
              skippedLast = true;
            } else {
              skippedLast = false;
              final int code = buffer[i];
              buffer[limit++] = code >>> 1;
              if ((code & 1) != 0) {
                // skip the payload length
                skipNext = true;
              }
            }
          }
          pendingOffset = 0;
          /*
           *  some readers will only fill a single element of the buffer
           *  if that single element is skipped we need to do another round.
           */
        }while(limit == 0 && skippedLast);
        return limit;
      }

      @Override
      public int end() {
        return limit;
      }
    }

    /** Position readers to the specified term */
    SepBulkPostingsEnum init(SepTermState termState) throws IOException {

      // To reduce cost of scanning the terms dict, sep
      // codecs store only the docDelta index in the terms
      // dict, and then stuff the other term metadata (freq
      // index, pos index, skip offset) into the front of
      // the docDeltas.  So here we seek the docReader and
      // decode this metadata:

      // nocommit -- make sure seek w/in buffer is efficient
      // here:

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);
      docIndex.seek(docReader);
      //System.out.println("sep init offset=" + docReader.offset() + " limit=" + docReader.end() + " omitTF=" + omitTF);
      //System.out.println("  v[0]=" + docReader.getBuffer()[0]);

      if (!omitTF) {
        // nocommit -- would be better (fewer bytes used) to
        // make this a relative index read (pass false not
        // true), eg relative to first term in the terms
        // index block
        freqIndex.set(termState.freqIndex);
        if (freqReader != null) {
          freqIndex.seek(freqReader);
        }

        posIndex.set(termState.posIndex);
      }

      skipFP = termState.skipFP;
      //System.out.println("skipOffset=" + skipOffset);

      if (posReader != null) {
        if (storePayloads) {
          PosPayloadReader posPayloadReader = (PosPayloadReader) posReader;
          posIndex.seek(posPayloadReader.other);
          posPayloadReader.doAfterSeek();
        } else {
          posIndex.seek(posReader);
        }
      }

      docFreq = termState.docFreq;
      skipped = false;

      return this;
    }

    @Override
    public BulkPostingsEnum.BlockReader getDocDeltasReader() {
      // Maximize perf -- just pass through the underlying
      // intblock reader:
      return docReader;
    }

    @Override
    public BulkPostingsEnum.BlockReader getFreqsReader() {
      // Maximize perf -- just pass through the underlying
      // intblock reader:
      return freqReader;
    }

    @Override
    public BulkPostingsEnum.BlockReader getPositionDeltasReader() {
      // Maximize perf -- just pass through the underlying
      // intblock reader (if payloads were not indexed):
      return posReader;
    }

    private final JumpResult jumpResult = new JumpResult();

    @Override
    public JumpResult jump(int target, int curCount) throws IOException {

      // TODO: require jump to take current docid and prevent skipping for close jumps?
      if (docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data

        if (skipper == null) {
          // This enum has never done any skipping
          skipper = new SepSkipListReader((IndexInput) skipIn.clone(),
                                          freqIn,
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);
        }

        if (!skipped) {
          // We haven't yet skipped for this particular posting
          skipper.init(skipFP,
                       docIndex,
                       freqIndex,
                       posIndex,
                       0,
                       docFreq,
                       storePayloads);
          skipper.setOmitTF(omitTF);
          skipped = true;
        }

        final int newCount = skipper.skipTo(target); 
        //System.out.println("  sep skip newCount=" + newCount + " vs count=" + curCount);

        if (newCount > curCount) {

          // Skipper did move -- seek all readers:
          skipper.getDocIndex().seek(docReader);

          if (freqReader != null) {
            skipper.getFreqIndex().seek(freqReader);
          }
          if (posReader != null) {
            if (storePayloads) {
              PosPayloadReader posPayloadReader = (PosPayloadReader) posReader;
              skipper.getPosIndex().seek(posPayloadReader.other);
              posPayloadReader.doAfterSeek();
            } else {
              skipper.getPosIndex().seek(posReader);
            }
          }
          jumpResult.count = newCount;
          jumpResult.docID = skipper.getDoc();
          return jumpResult;
        }
      }
      return null;
    }        
  }
}

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
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

/** Concrete class that reads the current doc+freq,skp,pos,pyl
 *  postings format.    
 *
 * @lucene.experimental
 */
public class FixedPostingsReaderImpl extends PostingsReaderBase {

  final FixedIntBlockIndexInput freqIn;
  final FixedIntBlockIndexInput docIn;
  final FixedIntBlockIndexInput posIn;
  final IndexInput payloadIn;
  final IndexInput skipIn;
  final int blocksize;

  int skipInterval;
  int maxSkipLevels;
  int skipMinimum;

  public FixedPostingsReaderImpl(Directory dir, SegmentInfo segmentInfo, int readBufferSize, FixedIntStreamFactory intFactory, String codecId) throws IOException {

    boolean success = false;
    try {

      final String docFileName = IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.DOC_EXTENSION);
      final IndexInput docsAndFreqsIn = dir.openInput(docFileName, readBufferSize);
      
      docIn = intFactory.openInput(docsAndFreqsIn, docFileName, false);
      blocksize = docIn.blockSize;
      skipIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.SKIP_EXTENSION), readBufferSize);

      if (segmentInfo.getHasProx()) {
        freqIn = intFactory.openInput(docsAndFreqsIn, docFileName, true);
        posIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.POS_EXTENSION), readBufferSize);
        payloadIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.PAYLOAD_EXTENSION), readBufferSize);
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
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.DOC_EXTENSION));
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.SKIP_EXTENSION));

    if (segmentInfo.getHasProx()) {
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.POS_EXTENSION));
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, codecId, FixedPostingsWriterImpl.PAYLOAD_EXTENSION));
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn, FixedPostingsWriterImpl.CODEC,
      FixedPostingsWriterImpl.VERSION_START, FixedPostingsWriterImpl.VERSION_START);
    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
    skipMinimum = termsIn.readInt();
  }

  @Override
  public void close() throws IOException {
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

  private static final class FixedTermState extends BlockTermState {
    // We store no seek point to freqs because
    // any freqs are interleaved blocks in the doc file.
    IntIndexInput.Index docIndex;
    IntIndexInput.Index posIndex;
    long payloadFP;
    long skipFP;

    // Only used for "primary" term state; these are never
    // copied on clone:
    byte[] bytes;
    ByteArrayDataInput bytesReader;

    @Override
    public Object clone() {
      FixedTermState other = (FixedTermState) super.clone();
      other.docIndex = (IntIndexInput.Index) docIndex.clone();
      if (posIndex != null) {
        other.posIndex = (IntIndexInput.Index) posIndex.clone();
      }
      return other;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      FixedTermState other = (FixedTermState) _other;
      docIndex.set(other.docIndex);
      if (posIndex != null && other.posIndex != null) {
        posIndex.set(other.posIndex);
      }
      payloadFP = other.payloadFP;
      skipFP = other.skipFP;
    }

    @Override
    public String toString() {
      return super.toString() + " docIndex=" + docIndex + " posIndex=" + posIndex + " payloadFP=" + payloadFP + " skipFP=" + skipFP;
    }
  }

  @Override
  public BlockTermState newTermState() throws IOException {
    final FixedTermState state = new FixedTermState();
    state.docIndex = docIn.index();
    if (posIn != null) {
      state.posIndex = posIn.index();
    }
    return state;
  }

  @Override
  public void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo, BlockTermState _termState) throws IOException {
    final FixedTermState termState = (FixedTermState) _termState;
    final int len = termsIn.readVInt();
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
    final FixedTermState termState = (FixedTermState) _termState;

    final boolean isFirstTerm = termState.termCount == 0;
    termState.docIndex.read(termState.bytesReader, isFirstTerm);

    if (!fieldInfo.omitTermFreqAndPositions) {
      termState.posIndex.read(termState.bytesReader, isFirstTerm);

      if (fieldInfo.storePayloads) {
        if (isFirstTerm) {
          termState.payloadFP = termState.bytesReader.readVLong();
        } else {
          termState.payloadFP += termState.bytesReader.readVLong();
        }
      }
    }
    
    if (termState.docFreq >= skipMinimum) {
      if (isFirstTerm) {
        termState.skipFP = termState.bytesReader.readVLong();
      } else {
        termState.skipFP += termState.bytesReader.readVLong();
      }
    } else if (isFirstTerm) {
      termState.skipFP = termState.bytesReader.readVLong();
    }
  }

  @Override
  public DocsEnum docs(FieldInfo fieldInfo, BlockTermState _termState, Bits skipDocs, DocsEnum reuse) throws IOException {
    final FixedTermState termState = (FixedTermState) _termState;

    // we separate the omitTF case (docs only) versus the docs+freqs case.
    // when omitTF is enabled, the blocks are structured differently (D, D, D, D, ...)
    // versus when frequencies are available (D, F, D, F, ...)

    if (fieldInfo.omitTermFreqAndPositions) {
      FixedDocsEnum docsEnum;
      if (reuse == null || !(reuse instanceof FixedDocsEnum) || !((FixedDocsEnum) reuse).canReuse(docIn)) {
        docsEnum = new FixedDocsEnum();
      } else {
        docsEnum = (FixedDocsEnum) reuse;
      }

      return docsEnum.init(fieldInfo, termState, skipDocs);
    } else {
      FixedDocsAndFreqsEnum docsEnum;
      if (reuse == null || !(reuse instanceof FixedDocsAndFreqsEnum) || !((FixedDocsAndFreqsEnum) reuse).canReuse(docIn)) {
        docsEnum = new FixedDocsAndFreqsEnum();
      } else {
        docsEnum = (FixedDocsAndFreqsEnum) reuse;
      }
      
      return docsEnum.init(fieldInfo, termState, skipDocs);
    }
  }

  private FixedBulkPostingsEnum lastBulkEnum;

  @Override
  public BulkPostingsEnum bulkPostings(FieldInfo fieldInfo, BlockTermState _termState, BulkPostingsEnum reuse, boolean doFreqs, boolean doPositions) throws IOException {
    final FixedTermState termState = (FixedTermState) _termState;
    final FixedBulkPostingsEnum lastBulkEnum = this.lastBulkEnum;
    if (lastBulkEnum != null && reuse == lastBulkEnum) {
      // fastpath
      return lastBulkEnum.init(termState);
    } else {
      FixedBulkPostingsEnum postingsEnum;
      if (reuse == null || !(reuse instanceof FixedBulkPostingsEnum) || !((FixedBulkPostingsEnum) reuse).canReuse(fieldInfo, docIn, doFreqs, doPositions, fieldInfo.omitTermFreqAndPositions)) {
        postingsEnum = new FixedBulkPostingsEnum(fieldInfo, doFreqs, doPositions);
      } else {
        postingsEnum = (FixedBulkPostingsEnum) reuse;
      }
      this.lastBulkEnum = postingsEnum;
      return postingsEnum.init(termState);
    }
  }

  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState _termState, Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
    assert !fieldInfo.omitTermFreqAndPositions;
    final FixedTermState termState = (FixedTermState) _termState;
    
    // we separate the no-payloads case (positions only) versus the case where payload
    // lengths are encoded into the position deltas.
    // when positions are pure, we can skip entire blocks of pending positions efficiently
    // without decode, and some operations are more efficient.
    
    if (!fieldInfo.storePayloads) {
      FixedDocsAndPositionsEnum postingsEnum;
      if (reuse == null || !(reuse instanceof FixedDocsAndPositionsEnum) || !((FixedDocsAndPositionsEnum) reuse).canReuse(docIn)) {
        postingsEnum = new FixedDocsAndPositionsEnum();
      } else {
        postingsEnum = (FixedDocsAndPositionsEnum) reuse;
      }

    return postingsEnum.init(fieldInfo, termState, skipDocs);
    } else {
      FixedDocsAndPositionsAndPayloadsEnum postingsEnum;
      if (reuse == null || !(reuse instanceof FixedDocsAndPositionsAndPayloadsEnum) || !((FixedDocsAndPositionsAndPayloadsEnum) reuse).canReuse(docIn)) {
        postingsEnum = new FixedDocsAndPositionsAndPayloadsEnum();
      } else {
        postingsEnum = (FixedDocsAndPositionsAndPayloadsEnum) reuse;
      }

    return postingsEnum.init(fieldInfo, termState, skipDocs);
    }
  }

  /** This docsenum class is used when the field omits frequencies (omitTFAP)
   *  In this case, the underlying file format is different, as it is just
   *  a pure stream of doc deltas.
   */
  final class FixedDocsEnum extends DocsEnum {
    int docFreq;
    int doc;
    int count;

    private boolean storePayloads;
    private Bits skipDocs;
    private final FixedIntBlockIndexInput.Reader docReader;
    private final int[] docDeltaBuffer;
    private int upto;
    private long skipFP;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;
    
    boolean skipped;
    FixedSkipListReader skipper;

    public FixedDocsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docDeltaBuffer = docReader.getBuffer();
      docIndex = docIn.index();
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
    FixedDocsEnum init(FieldInfo fieldInfo, FixedTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;
      assert fieldInfo.omitTermFreqAndPositions == true;
      storePayloads = fieldInfo.storePayloads;

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);

      docIndex.seek(docReader);

      upto = docReader.offset();
      
      docFreq = termState.docFreq;
      assert docFreq > 0;
      // NOTE: unused if docFreq < skipMinimum:
      skipFP = termState.skipFP;
      count = 0;
      doc = 0;
      skipped = false;

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

        assert upto <= blocksize: "docDeltaUpto=" + upto + " docDeltaLimit=" + blocksize;

        if (upto == blocksize) {
          // refill
          docReader.fill();
          upto = 0;
        }

        count++;

        // Decode next doc
        doc += docDeltaBuffer[upto++];
          
        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        }
      }
      return doc;
    }

    @Override
    public int freq() {
      return 1;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      if ((target - blocksize) >= doc && docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new FixedSkipListReader((IndexInput) skipIn.clone(),
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);

        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipFP,
                       docIndex,
                       posIndex,
                       0,
                       docFreq,
                       storePayloads);
          skipper.setOmitTF(true);

          skipped = true;
        }

        final int newCount = skipper.skipTo(target); 

        if (newCount > count) {
          // Skipper did move
          skipper.getDocIndex().seek(docReader);
          
          upto = docReader.offset();
          
          count = newCount;
          doc = skipper.getDoc();
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
  
  /** This docsenum class is used when the field has frequencies
   *  In this case, the underlying file format is interleaved
   *  blocks of doc deltas and freqs.
   */
  // nocommit: keep freqs buffer "one step behind" docs buffer
  // and lazily decode it when freq() is called?
  // this could help something like conjunctions that next()/advance()
  // often, but evaluate freq() less often, but this
  // api is not used by anything serious anymore?!
  final class FixedDocsAndFreqsEnum extends DocsEnum {
    int docFreq;
    int doc;
    int count;
    int freq;

    private boolean storePayloads;
    private Bits skipDocs;
    private final FixedIntBlockIndexInput.Reader docReader;
    private final int[] docDeltaBuffer;
    private final FixedIntBlockIndexInput.Reader freqReader;
    private final int[] freqBuffer;
    private int upto;
    private long skipFP;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;
    
    boolean skipped;
    FixedSkipListReader skipper;

    public FixedDocsAndFreqsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docDeltaBuffer = docReader.getBuffer();
      docIndex = docIn.index();
      if (freqIn != null) {
        freqReader = freqIn.reader(docReader);
        freqBuffer = freqReader.getBuffer();
      } else {
        freqReader = null;
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
    FixedDocsAndFreqsEnum init(FieldInfo fieldInfo, FixedTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;
      assert fieldInfo.omitTermFreqAndPositions == false;
      storePayloads = fieldInfo.storePayloads;

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);

      docReader.seek(docIndex, freqReader, true);

      upto = docReader.offset();
      
      docFreq = termState.docFreq;
      assert docFreq > 0;
      // NOTE: unused if docFreq < skipMinimum:
      skipFP = termState.skipFP;
      count = 0;
      doc = 0;
      skipped = false;

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

        assert upto <= blocksize: "docDeltaUpto=" + upto + " docDeltaLimit=" + blocksize;

        if (upto == blocksize) {
          // refill
          docReader.fill();
          upto = 0;
          freqReader.fill();
        }

        count++;

        freq = freqBuffer[upto];
        doc += docDeltaBuffer[upto++];
          
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

      // nocommit: with the current skip settings we probably
      // are skipping to much. instead it would be better to only
      // actually try to skip on higher differences, since we have
      // an optimized block-skipping scan(). a successful skip,
      // as currently implemented, is going to populate both doc and freq
      // blocks, but scan() can scan just docs... so maybe 8*blocksize
      // or similar would work better... need to benchmark

      if ((target - blocksize) >= doc && docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new FixedSkipListReader((IndexInput) skipIn.clone(),
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);

        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipFP,
                       docIndex,
                       posIndex,
                       0,
                       docFreq,
                       storePayloads);
          skipper.setOmitTF(false);

          skipped = true;
        }

        final int newCount = skipper.skipTo(target); 

        if (newCount > count) {
          // Skipper did move
          final IntIndexInput.Index idx = skipper.getDocIndex();

          docReader.seek(idx, freqReader, true);
          
          upto = docReader.offset();
          
          count = newCount;
          doc = skipper.getDoc();
        }
      }
        
      // Now, linear scan for the rest:
      return scan(target);
    }
    
    /** 
     * optimized scan, it reads docbuffer one step ahead of freqs,
     * so that it can skipBlock() whenever possible over freqs
     */
    int scan(int target) throws IOException {
      boolean freqsPending = false;
      
      while (true) {  
        if (count == docFreq) {
          // nocommit: can we skipBlock / must we do this?
          if (freqsPending) {
            freqReader.fill();
          }
          return (doc = NO_MORE_DOCS);
        }
        
        if (upto == blocksize) {
          // leapfrog
          if (freqsPending) {
            freqReader.skipBlock();
          }
          // refill
          docReader.fill();
          upto = 0;
          freqsPending = true;
        }
        
        count++;

        doc += docDeltaBuffer[upto++];
        
        if (doc >= target && (skipDocs == null || !skipDocs.get(doc))) {
          if (freqsPending) {
            freqReader.fill();
          }
          freq = freqBuffer[upto-1];
          return doc;
        }
      }
    }
  }

  /** This dp enum class is used when the field has no payloads:
   * positions are just pure deltas and no payloads ever exist.
   * 
   * Because our blocksizes are in sync, we can skip over pending
   * positions very efficiently: skipping over N positions means
   * calling skipBlock N / blocksize times and positioning the offset
   * to N % blocksize.
   */
  final class FixedDocsAndPositionsEnum extends DocsAndPositionsEnum {
    int docFreq;
    int doc;
    int count;
    int freq;

    private Bits skipDocs;
    private final FixedIntBlockIndexInput.Reader docReader;
    private final int[] docDeltaBuffer;
    private final FixedIntBlockIndexInput.Reader freqReader;
    private final int[] freqBuffer;
    private int upto;
    private final FixedIntBlockIndexInput.Reader posReader;
    private final int[] posBuffer;
    private int posUpto;
    private long skipFP;

    private final IndexInput payloadIn;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;

    private long payloadFP;

    private int pendingPosCount;
    private int position;
    private boolean posSeekPending;

    boolean skipped;
    FixedSkipListReader skipper;

    public FixedDocsAndPositionsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docDeltaBuffer = docReader.getBuffer();
      docIndex = docIn.index();
      freqReader = freqIn.reader(docReader);
      freqBuffer = freqReader.getBuffer();
      posReader = posIn.reader();
      posBuffer = posReader.getBuffer();
      posIndex = posIn.index();
      payloadIn = (IndexInput) FixedPostingsReaderImpl.this.payloadIn.clone();
    }

    // nocommit -- somehow we have to prevent re-decode of
    // the same block if we have just .next()'d to next term
    // in the terms dict -- this is an O(N^2) cost to eg
    // TermRangeQuery when it steps through low freq terms!!
    FixedDocsAndPositionsEnum init(FieldInfo fieldInfo, FixedTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;
      assert !fieldInfo.omitTermFreqAndPositions;
      assert !fieldInfo.storePayloads;

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);
      // nocommit -- verify, during merge, this seek is
      // sometimes w/in block:
      docReader.seek(docIndex, freqReader, true);
      upto = docReader.offset();

      posIndex.set(termState.posIndex);
      posSeekPending = true;

      payloadFP = termState.payloadFP;
      skipFP = termState.skipFP;

      docFreq = termState.docFreq;
      assert docFreq > 0;
      count = 0;
      doc = 0;
      pendingPosCount = 0;
      skipped = false;

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

        if (upto == blocksize) {
          // refill
          docReader.fill();
          freqReader.fill();
          upto = 0;
        }

        count++;

        // Decode next doc
        freq = freqBuffer[upto];
        doc += docDeltaBuffer[upto++];

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

      // nocommit: again we should likely use a higher distance,
      // and avoid true skipping since we can block-skip pending positions.
      // need to benchmark.
      
      if ((target - blocksize) >= doc && docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new FixedSkipListReader((IndexInput) skipIn.clone(),
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);

        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipFP,
                       docIndex,
                       posIndex,
                       payloadFP,
                       docFreq,
                       false);
          skipped = true;
        }
        final int newCount = skipper.skipTo(target); 

        if (newCount > count) {

          // Skipper did move
          final IntIndexInput.Index idx = skipper.getDocIndex();
          docReader.seek(idx, freqReader, true);
          upto = docReader.offset();

          // NOTE: don't seek pos here; do it lazily
          // instead.  Eg a PhraseQuery may skip to many
          // docs before finally asking for positions...
          posIndex.set(skipper.getPosIndex());
          posSeekPending = true;
          count = newCount;
          doc = skipper.getDoc();

          payloadFP = skipper.getPayloadPointer();
          pendingPosCount = 0;
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
    public int nextPosition() throws IOException {
      if (posSeekPending) {
        posIndex.seek(posReader);
        posUpto = posReader.offset();
        payloadIn.seek(payloadFP);
        posSeekPending = false;
      }

      // scan over any docs that were iterated without their
      // positions. if the pending positions is within-block,
      // we just increase the block offset. otherwise we skip
      // over positions blocks and set the block offset to
      // the remainder.

      if (pendingPosCount > freq) {
        int remaining = pendingPosCount - freq;
        final int bufferedRemaining = blocksize - posUpto;
        if (remaining <= bufferedRemaining) {
          posUpto += remaining; // just advance upto
        } else {
          remaining -= bufferedRemaining;
          final int blocksToSkip = remaining / blocksize;
          for (int i = 0; i < blocksToSkip; i++)
            posReader.skipBlock();
          posReader.fill();
          posUpto = remaining % blocksize;
        }
        pendingPosCount = freq;
      }

      if (posUpto == blocksize) {
        posReader.fill();
        posUpto = 0;
      }

      position += posBuffer[posUpto++];
    
      pendingPosCount--;
      assert pendingPosCount >= 0;
      return position;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      throw new IOException("no payload exists for this field.");
    }

    @Override
    public boolean hasPayload() {
      return false;
    }
  }

  /** 
   * This class is only used when the field has payloads 
   * it would be better for payload-users to separate the payload lengths
   * somehow from the positions.
   */
  final class FixedDocsAndPositionsAndPayloadsEnum extends DocsAndPositionsEnum {
    int docFreq;
    int doc;
    int count;
    int freq;

    private Bits skipDocs;
    private final FixedIntBlockIndexInput.Reader docReader;
    private final int[] docDeltaBuffer;
    private final FixedIntBlockIndexInput.Reader freqReader;
    private final int[] freqBuffer;
    private int upto;
    private final BulkPostingsEnum.BlockReader posReader;
    private final int[] posBuffer;
    private int posUpto;
    private long skipFP;

    private final IndexInput payloadIn;

    private final IntIndexInput.Index docIndex;
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
    FixedSkipListReader skipper;

    public FixedDocsAndPositionsAndPayloadsEnum() throws IOException {
      startDocIn = docIn;
      docReader = docIn.reader();
      docDeltaBuffer = docReader.getBuffer();
      docIndex = docIn.index();
      freqReader = freqIn.reader(docReader);
      freqBuffer = freqReader.getBuffer();
      posReader = posIn.reader();
      posBuffer = posReader.getBuffer();
      posIndex = posIn.index();
      payloadIn = (IndexInput) FixedPostingsReaderImpl.this.payloadIn.clone();
    }

    // nocommit -- somehow we have to prevent re-decode of
    // the same block if we have just .next()'d to next term
    // in the terms dict -- this is an O(N^2) cost to eg
    // TermRangeQuery when it steps through low freq terms!!
    FixedDocsAndPositionsAndPayloadsEnum init(FieldInfo fieldInfo, FixedTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;

      assert !fieldInfo.omitTermFreqAndPositions;
      assert fieldInfo.storePayloads == true;

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);
      // nocommit -- verify, during merge, this seek is
      // sometimes w/in block:
      docReader.seek(docIndex, freqReader, true);
      upto = docReader.offset();

      posIndex.set(termState.posIndex);
      posSeekPending = true;
      payloadPending = false;

      payloadFP = termState.payloadFP;
      skipFP = termState.skipFP;

      docFreq = termState.docFreq;
      assert docFreq > 0;
      count = 0;
      doc = 0;
      pendingPosCount = 0;
      pendingPayloadBytes = 0;
      skipped = false;

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

        if (upto == blocksize) {
          // refill
          docReader.fill();
          freqReader.fill();
          upto = 0;
        }

        count++;

        // Decode next doc
        freq = freqBuffer[upto];
        doc += docDeltaBuffer[upto++];

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
      if ((target - blocksize) >= doc && docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new FixedSkipListReader((IndexInput) skipIn.clone(),
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);

        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipFP,
                       docIndex,
                       posIndex,
                       payloadFP,
                       docFreq,
                       true);
          skipped = true;
        }
        final int newCount = skipper.skipTo(target); 

        if (newCount > count) {

          // Skipper did move
          final IntIndexInput.Index idx = skipper.getDocIndex();
          docReader.seek(idx, freqReader, true);
          upto = docReader.offset();

          // NOTE: don't seek pos here; do it lazily
          // instead.  Eg a PhraseQuery may skip to many
          // docs before finally asking for positions...
          posIndex.set(skipper.getPosIndex());
          posSeekPending = true;
          count = newCount;
          doc = skipper.getDoc();

          payloadFP = skipper.getPayloadPointer();
          pendingPosCount = 0;
          pendingPayloadBytes = 0;
          payloadPending = false;
          payloadLength = skipper.getPayloadLength();
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
    public int nextPosition() throws IOException {
      if (posSeekPending) {
        posIndex.seek(posReader);
        posUpto = posReader.offset();
        payloadIn.seek(payloadFP);
        posSeekPending = false;
      }

      // scan over any docs that were iterated without their
      // positions
      while (pendingPosCount > freq) {

        final int code = nextPosInt();

        if ((code & 1) != 0) {
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
      if ((code & 1) != 0) {
        // Payload length has changed
        payloadLength = nextPosInt();
        assert payloadLength >= 0;
      }
      position += code >> 1;
      pendingPayloadBytes += payloadLength;
      payloadPending = payloadLength > 0;
    
      pendingPosCount--;
      assert pendingPosCount >= 0;
      return position;
    }

    private int nextPosInt() throws IOException {
      if (posUpto == blocksize) {
        posReader.fill();
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

  /** 
   * This class is returned to the caller as a docs reader when they dont want freqs,
   * but freqs are actually in the file. we skipBlock() in tandem with doc delta fills.
   */
  static final class FreqSkippingDocReader extends BulkPostingsEnum.BlockReader {
    final BulkPostingsEnum.BlockReader docs;
    final FixedIntBlockIndexInput.Reader freqs;
    
    FreqSkippingDocReader(BulkPostingsEnum.BlockReader docs, FixedIntBlockIndexInput.Reader freqs) {
      this.docs = docs;
      this.freqs = freqs;
    }
    
    @Override
    public int[] getBuffer() {
      return docs.getBuffer();
    }

    @Override
    public int fill() throws IOException {
      final int ret = docs.fill();
      freqs.skipBlock();
      return ret;
    }

    @Override
    public int end() {
      return docs.end();
    }

    @Override
    public int offset() {
      return docs.offset();
    }
  }
  
  final class FixedBulkPostingsEnum extends BulkPostingsEnum {
    private int docFreq;

    private final BulkPostingsEnum.BlockReader docReader;
    // nocommit: confusing. just a pointer to the above, when doFreq = true
    // when doFreq = false and omitTF is false, freqReader is null,
    // but this one automatically fills() the child.
    private final FixedIntBlockIndexInput.Reader parentDocReader;

    private final IntIndexInput.Index docIndex;

    private final FixedIntBlockIndexInput.Reader freqReader;
    // nocommit: confusing. just a pointer to the above, when doFreq = true
    // when doFreq = false and omitTF is false, freqReader is null,
    // but this one is still valid so it can skipBlocks
    private final FixedIntBlockIndexInput.Reader childFreqReader;

    private final BulkPostingsEnum.BlockReader posReader;
    private final IntIndexInput.Index posIndex;

    private final boolean storePayloads;
    private final boolean omitTF;
    private long skipFP;

    private final IntIndexInput startDocIn;

    private boolean skipped;
    private FixedSkipListReader skipper;
    
    public FixedBulkPostingsEnum(FieldInfo fieldInfo, boolean doFreq, boolean doPos) throws IOException {
      this.storePayloads = fieldInfo.storePayloads;
      this.omitTF = fieldInfo.omitTermFreqAndPositions;
      startDocIn = docIn;

      parentDocReader = docIn.reader();
      
      if (doFreq && !omitTF) {
        childFreqReader = freqReader = freqIn.reader(parentDocReader);
      } else if (!doFreq && !omitTF) {
        childFreqReader = freqIn.reader(parentDocReader); // for skipping blocks
        freqReader = null;
      } else {
        childFreqReader = null;
        freqReader = null;
      }

      if (!doFreq && !omitTF) {
        docReader = new FreqSkippingDocReader(parentDocReader, childFreqReader);
      } else {
        docReader = parentDocReader;
      }
      
      docIndex = docIn.index();

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
        posIndex = posIn.index();
      } else {
        posIndex = null;
      }
    }

    public boolean canReuse(FieldInfo fieldInfo, IntIndexInput docIn, boolean doFreq, boolean doPos, boolean omitTF) {
      return fieldInfo.storePayloads == storePayloads &&
        startDocIn == docIn &&
        doFreq == (freqReader != null) &&
        omitTF == (this.omitTF) &&
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
    FixedBulkPostingsEnum init(FixedTermState termState) throws IOException {

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

      if (!omitTF) {
        // nocommit -- would be better (fewer bytes used) to
        // make this a relative index read (pass false not
        // true), eg relative to first term in the terms
        // index block
        parentDocReader.seek(docIndex, childFreqReader, freqReader != null);

        posIndex.set(termState.posIndex);
      } else {
        docIndex.seek(parentDocReader);
      }

      skipFP = termState.skipFP;

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
      // we can do all kinds of cool stuff here.

      if (docFreq >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data

        if (skipper == null) {
          // This enum has never done any skipping
          skipper = new FixedSkipListReader((IndexInput) skipIn.clone(),
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);
        }

        if (!skipped) {
          // We haven't yet skipped for this particular posting
          skipper.init(skipFP,
                       docIndex,
                       posIndex,
                       0,
                       docFreq,
                       storePayloads);
          skipper.setOmitTF(omitTF);
          skipped = true;
        }

        final int newCount = skipper.skipTo(target); 

        if (newCount > curCount) {

          // Skipper did move -- seek all readers:
          final IntIndexInput.Index idx = skipper.getDocIndex();
          
          if (!omitTF) {
            parentDocReader.seek(idx, childFreqReader, freqReader != null);
          } else {
            idx.seek(parentDocReader);
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

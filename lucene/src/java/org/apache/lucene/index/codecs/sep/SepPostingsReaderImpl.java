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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.TermState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
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

  public SepPostingsReaderImpl(Directory dir, SegmentInfo segmentInfo, int readBufferSize, IntStreamFactory intFactory) throws IOException {

    boolean success = false;
    try {

      final String docFileName = IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.DOC_EXTENSION);
      docIn = intFactory.openInput(dir, docFileName);

      skipIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.SKIP_EXTENSION), readBufferSize);

      if (segmentInfo.getHasProx()) {
        freqIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.FREQ_EXTENSION));
        posIn = intFactory.openInput(dir, IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.POS_EXTENSION), readBufferSize);
        payloadIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.PAYLOAD_EXTENSION), readBufferSize);
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

  public static void files(SegmentInfo segmentInfo, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.DOC_EXTENSION));
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.SKIP_EXTENSION));

    if (segmentInfo.getHasProx()) {
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.FREQ_EXTENSION));
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.POS_EXTENSION));
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, "", SepPostingsWriterImpl.PAYLOAD_EXTENSION));
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn, SepPostingsWriterImpl.CODEC,
      SepPostingsWriterImpl.VERSION_START, SepPostingsWriterImpl.VERSION_START);
    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
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

  private static class SepTermState extends TermState {
    IntIndexInput.Index docIndex;
    IntIndexInput.Index freqIndex;
    IntIndexInput.Index posIndex;
    long skipOffset;
    long payloadOffset;

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

    public void copy(TermState _other) {
      super.copy(_other);
      SepTermState other = (SepTermState) _other;
      docIndex.set(other.docIndex);
      if (other.posIndex != null) {
        if (posIndex == null) {
          posIndex = (IntIndexInput.Index) other.posIndex.clone();
        } else {
          posIndex.set(other.posIndex);
        }
      }
      if (other.freqIndex != null) {
        if (freqIndex == null) {
          freqIndex = (IntIndexInput.Index) other.freqIndex.clone();
        } else {
          freqIndex.set(other.freqIndex);
        }
      }
      skipOffset = other.skipOffset;
      payloadOffset = other.payloadOffset;
    }

    @Override
    public String toString() {
      return "tis.fp=" + filePointer + " docFreq=" + docFreq + " ord=" + ord + " docIndex=" + docIndex;
    }
  }

  @Override
  public TermState newTermState() throws IOException {
    final SepTermState state =  new SepTermState();
    state.docIndex = docIn.index();
    return state;
  }

  @Override
  public void readTerm(IndexInput termsIn, FieldInfo fieldInfo, TermState _termState, boolean isIndexTerm) throws IOException {
    final SepTermState termState = (SepTermState) _termState;

    // read freq index
    if (!fieldInfo.omitTermFreqAndPositions) {
      if (termState.freqIndex == null) {
        assert isIndexTerm;
        termState.freqIndex = freqIn.index();
        termState.posIndex = posIn.index();
      }
      termState.freqIndex.read(termsIn, isIndexTerm);
    }

    // read doc index
    termState.docIndex.read(termsIn, isIndexTerm);

    // read skip index
    if (isIndexTerm) {    
      termState.skipOffset = termsIn.readVLong();
    } else if (termState.docFreq >= skipInterval) {
      termState.skipOffset += termsIn.readVLong();
    }

    // read pos, payload index
    if (!fieldInfo.omitTermFreqAndPositions) {
      termState.posIndex.read(termsIn, isIndexTerm);
      final long v = termsIn.readVLong();
      if (isIndexTerm) {
        termState.payloadOffset = v;
      } else {
        termState.payloadOffset += v;
      }
    }
  }

  @Override
  public DocsEnum docs(FieldInfo fieldInfo, TermState _termState, Bits skipDocs, DocsEnum reuse) throws IOException {
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

    return docsEnum.init(fieldInfo, termState, skipDocs);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, TermState _termState, Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
    assert !fieldInfo.omitTermFreqAndPositions;
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

    return postingsEnum.init(fieldInfo, termState, skipDocs);
  }

  class SepDocsEnum extends DocsEnum {
    int docFreq;
    int doc;
    int count;
    int freq;
    long freqStart;

    // TODO: -- should we do omitTF with 2 different enum classes?
    private boolean omitTF;
    private boolean storePayloads;
    private Bits skipDocs;
    private final IntIndexInput.Reader docReader;
    private final IntIndexInput.Reader freqReader;
    private long skipOffset;

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

    SepDocsEnum init(FieldInfo fieldInfo, SepTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;
      omitTF = fieldInfo.omitTermFreqAndPositions;
      storePayloads = fieldInfo.storePayloads;

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);
      docIndex.seek(docReader);

      skipOffset = termState.skipOffset;

      if (!omitTF) {
        freqIndex.set(termState.freqIndex);
        freqIndex.seek(freqReader);
      } else {
        freq = 1;
      }
      docFreq = termState.docFreq;
      count = 0;
      doc = 0;
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
        doc += docReader.next();
          
        if (!omitTF) {
          freq = freqReader.next();
        }

        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        }
      }

      return doc;
    }

    @Override
    public int read() throws IOException {
      // TODO: -- switch to bulk read api in IntIndexInput
      final int[] docs = bulkResult.docs.ints;
      final int[] freqs = bulkResult.freqs.ints;
      int i = 0;
      final int length = docs.length;
      while (i < length && count < docFreq) {
        count++;
        // manually inlined call to next() for speed
        doc += docReader.next();
        if (!omitTF) {
          freq = freqReader.next();
        }

        if (skipDocs == null || !skipDocs.get(doc)) {
          docs[i] = doc;
          freqs[i] = freq;
          i++;
        }
      }
      return i;
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

      // TODO: jump right to next() if target is < X away
      // from where we are now?

      if (docFreq >= skipInterval) {

        // There are enough docs in the posting to have
        // skip data

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new SepSkipListReader((IndexInput) skipIn.clone(),
                                          freqIn,
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);

        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipOffset,
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
          }
          skipper.getDocIndex().seek(docReader);
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

  class SepDocsAndPositionsEnum extends DocsAndPositionsEnum {
    int docFreq;
    int doc;
    int count;
    int freq;
    long freqStart;

    private boolean storePayloads;
    private Bits skipDocs;
    private final IntIndexInput.Reader docReader;
    private final IntIndexInput.Reader freqReader;
    private final IntIndexInput.Reader posReader;
    private final IndexInput payloadIn;
    private long skipOffset;

    private final IntIndexInput.Index docIndex;
    private final IntIndexInput.Index freqIndex;
    private final IntIndexInput.Index posIndex;
    private final IntIndexInput startDocIn;

    private long payloadOffset;

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
      payloadIn = (IndexInput) SepPostingsReaderImpl.this.payloadIn.clone();
    }

    SepDocsAndPositionsEnum init(FieldInfo fieldInfo, SepTermState termState, Bits skipDocs) throws IOException {
      this.skipDocs = skipDocs;
      storePayloads = fieldInfo.storePayloads;

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      docIndex.set(termState.docIndex);
      docIndex.seek(docReader);

      freqIndex.set(termState.freqIndex);
      freqIndex.seek(freqReader);

      posIndex.set(termState.posIndex);
      posSeekPending = true;
      //posIndex.seek(posReader);

      skipOffset = termState.skipOffset;
      payloadOffset = termState.payloadOffset;
      //payloadIn.seek(payloadOffset);

      docFreq = termState.docFreq;
      count = 0;
      doc = 0;
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
        doc += docReader.next();
          
        freq = freqReader.next();

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

      // TODO: jump right to next() if target is < X away
      // from where we are now?

      if (docFreq >= skipInterval) {

        // There are enough docs in the posting to have
        // skip data

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new SepSkipListReader((IndexInput) skipIn.clone(),
                                          freqIn,
                                          docIn,
                                          posIn,
                                          maxSkipLevels, skipInterval);
        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipOffset,
                       docIndex,
                       freqIndex,
                       posIndex,
                       payloadOffset,
                       docFreq,
                       storePayloads);

          skipped = true;
        }

        final int newCount = skipper.skipTo(target); 

        if (newCount > count) {

          // Skipper did move
          skipper.getFreqIndex().seek(freqReader);
          skipper.getDocIndex().seek(docReader);
          //skipper.getPosIndex().seek(posReader);
          posIndex.set(skipper.getPosIndex());
          posSeekPending = true;
          count = newCount;
          doc = skipper.getDoc();
          //payloadIn.seek(skipper.getPayloadPointer());
          payloadOffset = skipper.getPayloadPointer();
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
        payloadIn.seek(payloadOffset);
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
        payloadPending = true;
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
        position += code >> 1;
      } else {
        position += code;
      }
    
      pendingPayloadBytes += payloadLength;
      payloadPending = payloadLength > 0;
      pendingPosCount--;
      payloadPending = true;
      assert pendingPosCount >= 0;
      return position;
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
}

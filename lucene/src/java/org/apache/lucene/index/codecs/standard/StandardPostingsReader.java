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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.TermState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

/** Concrete class that reads the current doc/freq/skip
 *  postings format. 
 *  @lucene.experimental */

public class StandardPostingsReader extends PostingsReaderBase {

  private final IndexInput freqIn;
  private final IndexInput proxIn;

  int skipInterval;
  int maxSkipLevels;

  public StandardPostingsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize) throws IOException {
    freqIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, "", StandardCodec.FREQ_EXTENSION),
                           readBufferSize);
    if (segmentInfo.getHasProx()) {
      boolean success = false;
      try {
        proxIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, "", StandardCodec.PROX_EXTENSION),
                               readBufferSize);
        success = true;
      } finally {
        if (!success) {
          freqIn.close();
        }
      }
    } else {
      proxIn = null;
    }
  }

  public static void files(Directory dir, SegmentInfo segmentInfo, Collection<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, "", StandardCodec.FREQ_EXTENSION));
    if (segmentInfo.getHasProx()) {
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, "", StandardCodec.PROX_EXTENSION));
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {

    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn, StandardPostingsWriter.CODEC,
      StandardPostingsWriter.VERSION_START, StandardPostingsWriter.VERSION_START);

    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
  }

  private static class DocTermState extends TermState {
    long freqOffset;
    long proxOffset;
    int skipOffset;

    public Object clone() {
      DocTermState other = (DocTermState) super.clone();
      other.freqOffset = freqOffset;
      other.proxOffset = proxOffset;
      other.skipOffset = skipOffset;
      return other;
    }

    public void copy(TermState _other) {
      super.copy(_other);
      DocTermState other = (DocTermState) _other;
      freqOffset = other.freqOffset;
      proxOffset = other.proxOffset;
      skipOffset = other.skipOffset;
    }

    public String toString() {
      return super.toString() + " freqFP=" + freqOffset + " proxFP=" + proxOffset + " skipOffset=" + skipOffset;
    }
  }

  @Override
  public TermState newTermState() {
    return new DocTermState();
  }

  @Override
  public void close() throws IOException {
    try {
      if (freqIn != null) {
        freqIn.close();
      }
    } finally {
      if (proxIn != null) {
        proxIn.close();
      }
    }
  }

  @Override
  public void readTerm(IndexInput termsIn, FieldInfo fieldInfo, TermState termState, boolean isIndexTerm)
    throws IOException {

    final DocTermState docTermState = (DocTermState) termState;

    if (isIndexTerm) {
      docTermState.freqOffset = termsIn.readVLong();
    } else {
      docTermState.freqOffset += termsIn.readVLong();
    }

    if (docTermState.docFreq >= skipInterval) {
      docTermState.skipOffset = termsIn.readVInt();
    } else {
      docTermState.skipOffset = 0;
    }

    if (!fieldInfo.omitTermFreqAndPositions) {
      if (isIndexTerm) {
        docTermState.proxOffset = termsIn.readVLong();
      } else {
        docTermState.proxOffset += termsIn.readVLong();
      }
    }
  }
    
  @Override
  public DocsEnum docs(FieldInfo fieldInfo, TermState termState, Bits skipDocs, DocsEnum reuse) throws IOException {
    SegmentDocsEnum docsEnum;
    if (reuse == null || !(reuse instanceof SegmentDocsEnum)) {
      docsEnum = new SegmentDocsEnum(freqIn);
    } else {
      docsEnum = (SegmentDocsEnum) reuse;
      if (docsEnum.startFreqIn != freqIn) {
        // If you are using ParellelReader, and pass in a
        // reused DocsEnum, it could have come from another
        // reader also using standard codec
        docsEnum = new SegmentDocsEnum(freqIn);
      }
    }
    return docsEnum.reset(fieldInfo, (DocTermState) termState, skipDocs);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, TermState termState, Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
    if (fieldInfo.omitTermFreqAndPositions) {
      return null;
    }
    SegmentDocsAndPositionsEnum docsEnum;
    if (reuse == null || !(reuse instanceof SegmentDocsAndPositionsEnum)) {
      docsEnum = new SegmentDocsAndPositionsEnum(freqIn, proxIn);
    } else {
      docsEnum = (SegmentDocsAndPositionsEnum) reuse;
      if (docsEnum.startFreqIn != freqIn) {
        // If you are using ParellelReader, and pass in a
        // reused DocsEnum, it could have come from another
        // reader also using standard codec
        docsEnum = new SegmentDocsAndPositionsEnum(freqIn, proxIn);
      }
    }
    return docsEnum.reset(fieldInfo, (DocTermState) termState, skipDocs);
  }

  // Decodes only docs
  private class SegmentDocsEnum extends DocsEnum {
    final IndexInput freqIn;
    final IndexInput startFreqIn;

    boolean omitTF;                               // does current field omit term freq?
    boolean storePayloads;                        // does current field store payloads?

    int limit;                                    // number of docs in this posting
    int ord;                                      // how many docs we've read
    int doc;                                      // doc we last read
    int freq;                                     // freq we last read

    Bits skipDocs;

    long freqOffset;
    int skipOffset;

    boolean skipped;
    DefaultSkipListReader skipper;

    public SegmentDocsEnum(IndexInput freqIn) throws IOException {
      startFreqIn = freqIn;
      this.freqIn = (IndexInput) freqIn.clone();
    }

    public SegmentDocsEnum reset(FieldInfo fieldInfo, DocTermState termState, Bits skipDocs) throws IOException {
      omitTF = fieldInfo.omitTermFreqAndPositions;
      if (omitTF) {
        freq = 1;
      }
      storePayloads = fieldInfo.storePayloads;
      this.skipDocs = skipDocs;
      freqOffset = termState.freqOffset;
      skipOffset = termState.skipOffset;

      // TODO: for full enum case (eg segment merging) this
      // seek is unnecessary; maybe we can avoid in such
      // cases
      freqIn.seek(termState.freqOffset);
      limit = termState.docFreq;
      ord = 0;
      doc = 0;

      skipped = false;

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      while(true) {
        if (ord == limit) {
          return doc = NO_MORE_DOCS;
        }

        ord++;

        // Decode next doc/freq pair
        final int code = freqIn.readVInt();
        if (omitTF) {
          doc += code;
        } else {
          doc += code >>> 1;              // shift off low bit
          if ((code & 1) != 0) {          // if low bit is set
            freq = 1;                     // freq is one
          } else {
            freq = freqIn.readVInt();     // else read freq
          }
        }

        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        }
      }

      return doc;
    }

    @Override
    public int read() throws IOException {

      final int[] docs = bulkResult.docs.ints;
      final int[] freqs = bulkResult.freqs.ints;
      int i = 0;
      final int length = docs.length;
      while (i < length && ord < limit) {
        ord++;
        // manually inlined call to next() for speed
        final int code = freqIn.readVInt();
        if (omitTF) {
          doc += code;
        } else {
          doc += code >>> 1;              // shift off low bit
          if ((code & 1) != 0) {          // if low bit is set
            freq = 1;                     // freq is one
          } else {
            freq = freqIn.readVInt();     // else read freq
          }
        }

        if (skipDocs == null || !skipDocs.get(doc)) {
          docs[i] = doc;
          freqs[i] = freq;
          ++i;
        }
      }
      
      return i;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int advance(int target) throws IOException {

      // TODO: jump right to next() if target is < X away
      // from where we are now?

      if (skipOffset > 0) {

        // There are enough docs in the posting to have
        // skip data

        if (skipper == null) {
          // This is the first time this enum has ever been used for skipping -- do lazy init
          skipper = new DefaultSkipListReader((IndexInput) freqIn.clone(), maxSkipLevels, skipInterval);
        }

        if (!skipped) {

          // This is the first time this posting has
          // skipped since reset() was called, so now we
          // load the skip data for this posting

          skipper.init(freqOffset + skipOffset,
                       freqOffset, 0,
                       limit, storePayloads);

          skipped = true;
        }

        final int newOrd = skipper.skipTo(target); 

        if (newOrd > ord) {
          // Skipper moved

          ord = newOrd;
          doc = skipper.getDoc();
          freqIn.seek(skipper.getFreqPointer());
        }
      }
        
      // scan for the rest:
      do {
        nextDoc();
      } while (target > doc);

      return doc;
    }
  }

  // Decodes docs & positions
  private class SegmentDocsAndPositionsEnum extends DocsAndPositionsEnum {
    final IndexInput startFreqIn;
    private final IndexInput freqIn;
    private final IndexInput proxIn;

    boolean storePayloads;                        // does current field store payloads?

    int limit;                                    // number of docs in this posting
    int ord;                                      // how many docs we've read
    int doc;                                      // doc we last read
    int freq;                                     // freq we last read
    int position;

    Bits skipDocs;

    long freqOffset;
    int skipOffset;
    long proxOffset;

    int posPendingCount;
    int payloadLength;
    boolean payloadPending;

    boolean skipped;
    DefaultSkipListReader skipper;
    private BytesRef payload;
    private long lazyProxPointer;

    public SegmentDocsAndPositionsEnum(IndexInput freqIn, IndexInput proxIn) throws IOException {
      startFreqIn = freqIn;
      this.freqIn = (IndexInput) freqIn.clone();
      this.proxIn = (IndexInput) proxIn.clone();
    }

    public SegmentDocsAndPositionsEnum reset(FieldInfo fieldInfo, DocTermState termState, Bits skipDocs) throws IOException {
      assert !fieldInfo.omitTermFreqAndPositions;
      storePayloads = fieldInfo.storePayloads;
      if (storePayloads && payload == null) {
        payload = new BytesRef();
        payload.bytes = new byte[1];
      }

      this.skipDocs = skipDocs;

      // TODO: for full enum case (eg segment merging) this
      // seek is unnecessary; maybe we can avoid in such
      // cases
      freqIn.seek(termState.freqOffset);
      lazyProxPointer = termState.proxOffset;

      limit = termState.docFreq;
      ord = 0;
      doc = 0;
      position = 0;

      skipped = false;
      posPendingCount = 0;
      payloadPending = false;

      freqOffset = termState.freqOffset;
      proxOffset = termState.proxOffset;
      skipOffset = termState.skipOffset;

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      while(true) {
        if (ord == limit) {
          return doc = NO_MORE_DOCS;
        }

        ord++;

        // Decode next doc/freq pair
        final int code = freqIn.readVInt();

        doc += code >>> 1;              // shift off low bit
        if ((code & 1) != 0) {          // if low bit is set
          freq = 1;                     // freq is one
        } else {
          freq = freqIn.readVInt();     // else read freq
        }
        posPendingCount += freq;

        if (skipDocs == null || !skipDocs.get(doc)) {
          break;
        }
      }

      position = 0;

      return doc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int advance(int target) throws IOException {

      // TODO: jump right to next() if target is < X away
      // from where we are now?

      if (skipOffset > 0) {

        // There are enough docs in the posting to have
        // skip data

        if (skipper == null) {
          // This is the first time this enum has ever been used for skipping -- do lazy init
          skipper = new DefaultSkipListReader((IndexInput) freqIn.clone(), maxSkipLevels, skipInterval);
        }

        if (!skipped) {

          // This is the first time this posting has
          // skipped, since reset() was called, so now we
          // load the skip data for this posting

          skipper.init(freqOffset+skipOffset,
                       freqOffset, proxOffset,
                       limit, storePayloads);

          skipped = true;
        }

        final int newOrd = skipper.skipTo(target); 

        if (newOrd > ord) {
          // Skipper moved
          ord = newOrd;
          doc = skipper.getDoc();
          freqIn.seek(skipper.getFreqPointer());
          lazyProxPointer = skipper.getProxPointer();
          posPendingCount = 0;
          position = 0;
          payloadPending = false;
          payloadLength = skipper.getPayloadLength();
        }
      }
        
      // Now, linear scan for the rest:
      do {
        nextDoc();
      } while (target > doc);

      return doc;
    }

    public int nextPosition() throws IOException {

      if (lazyProxPointer != -1) {
        proxIn.seek(lazyProxPointer);
        lazyProxPointer = -1;
      }
      
      if (payloadPending && payloadLength > 0) {
        // payload of last position as never retrieved -- skip it
        proxIn.seek(proxIn.getFilePointer() + payloadLength);
        payloadPending = false;
      }

      // scan over any docs that were iterated without their positions
      while(posPendingCount > freq) {

        final int code = proxIn.readVInt();

        if (storePayloads) {
          if ((code & 1) != 0) {
            // new payload length
            payloadLength = proxIn.readVInt();
            assert payloadLength >= 0;
          }
          assert payloadLength != -1;
          proxIn.seek(proxIn.getFilePointer() + payloadLength);
        }

        posPendingCount--;
        position = 0;
        payloadPending = false;
      }

      // read next position
      if (storePayloads) {

        if (payloadPending && payloadLength > 0) {
          // payload wasn't retrieved for last position
          proxIn.seek(proxIn.getFilePointer()+payloadLength);
        }

        final int code = proxIn.readVInt();
        if ((code & 1) != 0) {
          // new payload length
          payloadLength = proxIn.readVInt();
          assert payloadLength >= 0;
        }
        assert payloadLength != -1;
          
        payloadPending = true;
        position += code >>> 1;
      } else {
        position += proxIn.readVInt();
      }

      posPendingCount--;

      assert posPendingCount >= 0: "nextPosition() was called too many times (more than freq() times) posPendingCount=" + posPendingCount;

      return position;
    }

    /** Returns the payload at this position, or null if no
     *  payload was indexed. */
    public BytesRef getPayload() throws IOException {
      assert lazyProxPointer == -1;
      assert posPendingCount < freq;
      if (!payloadPending) {
        throw new IOException("Either no payload exists at this term position or an attempt was made to load it more than once.");
      }
      if (payloadLength > payload.bytes.length) {
        payload.grow(payloadLength);
      }
      proxIn.readBytes(payload.bytes, 0, payloadLength);
      payload.length = payloadLength;
      payloadPending = false;

      return payload;
    }

    public boolean hasPayload() {
      return payloadPending && payloadLength > 0;
    }
  }
}

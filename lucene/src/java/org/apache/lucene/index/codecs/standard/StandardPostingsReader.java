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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
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
 *  @lucene.experimental */

public class StandardPostingsReader extends PostingsReaderBase {

  private final IndexInput freqIn;
  private final IndexInput proxIn;

  int skipInterval;
  int maxSkipLevels;
  int skipMinimum;

  //private String segment;

  public StandardPostingsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize, int codecId) throws IOException {
    freqIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, codecId, StandardCodec.FREQ_EXTENSION),
                           readBufferSize);
    //this.segment = segmentInfo.name;
    if (segmentInfo.getHasProx()) {
      boolean success = false;
      try {
        proxIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, codecId, StandardCodec.PROX_EXTENSION),
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

  public static void files(Directory dir, SegmentInfo segmentInfo, int id, Collection<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, id, StandardCodec.FREQ_EXTENSION));
    if (segmentInfo.getHasProx()) {
      files.add(IndexFileNames.segmentFileName(segmentInfo.name, id, StandardCodec.PROX_EXTENSION));
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {

    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn, StandardPostingsWriter.CODEC,
      StandardPostingsWriter.VERSION_START, StandardPostingsWriter.VERSION_START);

    skipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
    skipMinimum = termsIn.readInt();
  }

  // Must keep final because we do non-standard clone
  private final static class StandardTermState extends BlockTermState {
    long freqOffset;
    long proxOffset;
    int skipOffset;

    // Only used by the "primary" TermState -- clones don't
    // copy this (basically they are "transient"):
    ByteArrayDataInput bytesReader;
    byte[] bytes;

    @Override
    public Object clone() {
      StandardTermState other = new StandardTermState();
      other.copyFrom(this);
      return other;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      StandardTermState other = (StandardTermState) _other;
      freqOffset = other.freqOffset;
      proxOffset = other.proxOffset;
      skipOffset = other.skipOffset;

      // Do not copy bytes, bytesReader (else TermState is
      // very heavy, ie drags around the entire block's
      // byte[]).  On seek back, if next() is in fact used
      // (rare!), they will be re-read from disk.
    }

    @Override
    public String toString() {
      return super.toString() + " freqFP=" + freqOffset + " proxFP=" + proxOffset + " skipOffset=" + skipOffset;
    }
  }

  @Override
  public BlockTermState newTermState() {
    return new StandardTermState();
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

  /* Reads but does not decode the byte[] blob holding
     metadata for the current terms block */
  @Override
  public void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo, BlockTermState _termState) throws IOException {
    final StandardTermState termState = (StandardTermState) _termState;

    final int len = termsIn.readVInt();
    //System.out.println("SPR.readTermsBlock termsIn.fp=" + termsIn.getFilePointer());
    if (termState.bytes == null) {
      termState.bytes = new byte[ArrayUtil.oversize(len, 1)];
      termState.bytesReader = new ByteArrayDataInput();
    } else if (termState.bytes.length < len) {
      termState.bytes = new byte[ArrayUtil.oversize(len, 1)];
    }

    termsIn.readBytes(termState.bytes, 0, len);
    termState.bytesReader.reset(termState.bytes, 0, len);
  }

  @Override
  public void nextTerm(FieldInfo fieldInfo, BlockTermState _termState)
    throws IOException {
    final StandardTermState termState = (StandardTermState) _termState;
    //System.out.println("StandardR.nextTerm seg=" + segment);
    final boolean isFirstTerm = termState.termCount == 0;

    if (isFirstTerm) {
      termState.freqOffset = termState.bytesReader.readVLong();
    } else {
      termState.freqOffset += termState.bytesReader.readVLong();
    }
    //System.out.println("  dF=" + termState.docFreq);
    //System.out.println("  freqFP=" + termState.freqOffset);
    assert termState.freqOffset < freqIn.length();

    if (termState.docFreq >= skipMinimum) {
      termState.skipOffset = termState.bytesReader.readVInt();
      //System.out.println("  skipOffset=" + termState.skipOffset + " vs freqIn.length=" + freqIn.length());
      assert termState.freqOffset + termState.skipOffset < freqIn.length();
    } else {
      // undefined
    }

    if (!fieldInfo.omitTermFreqAndPositions) {
      if (isFirstTerm) {
        termState.proxOffset = termState.bytesReader.readVLong();
      } else {
        termState.proxOffset += termState.bytesReader.readVLong();
      }
      //System.out.println("  proxFP=" + termState.proxOffset);
    }
  }
    
  @Override
  public DocsEnum docs(FieldInfo fieldInfo, BlockTermState termState, Bits skipDocs, DocsEnum reuse) throws IOException {
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
    return docsEnum.reset(fieldInfo, (StandardTermState) termState, skipDocs);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState termState, Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
    if (fieldInfo.omitTermFreqAndPositions) {
      return null;
    }
    
    // TODO: refactor
    if (fieldInfo.storePayloads) {
      SegmentDocsAndPositionsAndPayloadsEnum docsEnum;
      if (reuse == null || !(reuse instanceof SegmentDocsAndPositionsAndPayloadsEnum)) {
        docsEnum = new SegmentDocsAndPositionsAndPayloadsEnum(freqIn, proxIn);
      } else {
        docsEnum = (SegmentDocsAndPositionsAndPayloadsEnum) reuse;
        if (docsEnum.startFreqIn != freqIn) {
          // If you are using ParellelReader, and pass in a
          // reused DocsEnum, it could have come from another
          // reader also using standard codec
          docsEnum = new SegmentDocsAndPositionsAndPayloadsEnum(freqIn, proxIn);
        }
      }
      return docsEnum.reset(fieldInfo, (StandardTermState) termState, skipDocs);
    } else {
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
      return docsEnum.reset(fieldInfo, (StandardTermState) termState, skipDocs);
    }
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

    public SegmentDocsEnum reset(FieldInfo fieldInfo, StandardTermState termState, Bits skipDocs) throws IOException {
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
      assert limit > 0;
      ord = 0;
      doc = 0;
      //System.out.println("  sde limit=" + limit + " freqFP=" + freqOffset);

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

      if ((target - skipInterval) >= doc && limit >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and it isn't too close.

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

  // Decodes docs & positions. payloads are not present.
  private class SegmentDocsAndPositionsEnum extends DocsAndPositionsEnum {
    final IndexInput startFreqIn;
    private final IndexInput freqIn;
    private final IndexInput proxIn;

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

    boolean skipped;
    DefaultSkipListReader skipper;
    private long lazyProxPointer;

    public SegmentDocsAndPositionsEnum(IndexInput freqIn, IndexInput proxIn) throws IOException {
      startFreqIn = freqIn;
      this.freqIn = (IndexInput) freqIn.clone();
      this.proxIn = (IndexInput) proxIn.clone();
    }

    public SegmentDocsAndPositionsEnum reset(FieldInfo fieldInfo, StandardTermState termState, Bits skipDocs) throws IOException {
      assert !fieldInfo.omitTermFreqAndPositions;
      assert !fieldInfo.storePayloads;

      this.skipDocs = skipDocs;

      // TODO: for full enum case (eg segment merging) this
      // seek is unnecessary; maybe we can avoid in such
      // cases
      freqIn.seek(termState.freqOffset);
      lazyProxPointer = termState.proxOffset;

      limit = termState.docFreq;
      assert limit > 0;

      ord = 0;
      doc = 0;
      position = 0;

      skipped = false;
      posPendingCount = 0;

      freqOffset = termState.freqOffset;
      proxOffset = termState.proxOffset;
      skipOffset = termState.skipOffset;
      //System.out.println("StandardR.D&PE reset seg=" + segment + " limit=" + limit + " freqFP=" + freqOffset + " proxFP=" + proxOffset);

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      while(true) {
        if (ord == limit) {
          //System.out.println("StandardR.D&PE seg=" + segment + " nextDoc return doc=END");
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

      //System.out.println("StandardR.D&PE nextDoc seg=" + segment + " return doc=" + doc);
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

      //System.out.println("StandardR.D&PE advance target=" + target);

      if ((target - skipInterval) >= doc && limit >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and it isn't too close

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
                       limit, false);

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
        }
      }
        
      // Now, linear scan for the rest:
      do {
        nextDoc();
      } while (target > doc);

      return doc;
    }

    @Override
    public int nextPosition() throws IOException {

      if (lazyProxPointer != -1) {
        proxIn.seek(lazyProxPointer);
        lazyProxPointer = -1;
      }

      // scan over any docs that were iterated without their positions
      if (posPendingCount > freq) {
        position = 0;
        while(posPendingCount != freq) {
          if ((proxIn.readByte() & 0x80) == 0) {
            posPendingCount--;
          }
        }
      }

      position += proxIn.readVInt();

      posPendingCount--;

      assert posPendingCount >= 0: "nextPosition() was called too many times (more than freq() times) posPendingCount=" + posPendingCount;

      return position;
    }

    /** Returns the payload at this position, or null if no
     *  payload was indexed. */
    @Override
    public BytesRef getPayload() throws IOException {
      throw new IOException("No payloads exist for this field!");
    }

    @Override
    public boolean hasPayload() {
      return false;
    }
  }
  
  // Decodes docs & positions & payloads
  private class SegmentDocsAndPositionsAndPayloadsEnum extends DocsAndPositionsEnum {
    final IndexInput startFreqIn;
    private final IndexInput freqIn;
    private final IndexInput proxIn;

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

    public SegmentDocsAndPositionsAndPayloadsEnum(IndexInput freqIn, IndexInput proxIn) throws IOException {
      startFreqIn = freqIn;
      this.freqIn = (IndexInput) freqIn.clone();
      this.proxIn = (IndexInput) proxIn.clone();
    }

    public SegmentDocsAndPositionsAndPayloadsEnum reset(FieldInfo fieldInfo, StandardTermState termState, Bits skipDocs) throws IOException {
      assert !fieldInfo.omitTermFreqAndPositions;
      assert fieldInfo.storePayloads;
      if (payload == null) {
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
      //System.out.println("StandardR.D&PE reset seg=" + segment + " limit=" + limit + " freqFP=" + freqOffset + " proxFP=" + proxOffset + " this=" + this);

      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      while(true) {
        if (ord == limit) {
          //System.out.println("StandardR.D&PE seg=" + segment + " nextDoc return doc=END");
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

      //System.out.println("StandardR.D&PE nextDoc seg=" + segment + " return doc=" + doc);
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

      //System.out.println("StandardR.D&PE advance seg=" + segment + " target=" + target + " this=" + this);

      if ((target - skipInterval) >= doc && limit >= skipMinimum) {

        // There are enough docs in the posting to have
        // skip data, and it isn't too close

        if (skipper == null) {
          // This is the first time this enum has ever been used for skipping -- do lazy init
          skipper = new DefaultSkipListReader((IndexInput) freqIn.clone(), maxSkipLevels, skipInterval);
        }

        if (!skipped) {

          // This is the first time this posting has
          // skipped, since reset() was called, so now we
          // load the skip data for this posting
          //System.out.println("  init skipper freqOffset=" + freqOffset + " skipOffset=" + skipOffset + " vs len=" + freqIn.length());
          skipper.init(freqOffset+skipOffset,
                       freqOffset, proxOffset,
                       limit, true);

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

    @Override
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

        if ((code & 1) != 0) {
          // new payload length
          payloadLength = proxIn.readVInt();
          assert payloadLength >= 0;
        }
        
        assert payloadLength != -1;
        proxIn.seek(proxIn.getFilePointer() + payloadLength);

        posPendingCount--;
        position = 0;
        payloadPending = false;
        //System.out.println("StandardR.D&PE skipPos");
      }

      // read next position
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

      posPendingCount--;

      assert posPendingCount >= 0: "nextPosition() was called too many times (more than freq() times) posPendingCount=" + posPendingCount;

      //System.out.println("StandardR.D&PE nextPos   return pos=" + position);
      return position;
    }

    /** Returns the payload at this position, or null if no
     *  payload was indexed. */
    @Override
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

    @Override
    public boolean hasPayload() {
      return payloadPending && payloadLength > 0;
    }
  }
}

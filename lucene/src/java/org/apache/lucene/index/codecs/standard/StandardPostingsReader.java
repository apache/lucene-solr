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
import org.apache.lucene.index.BulkPostingsEnum;
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

  public StandardPostingsReader(Directory dir, SegmentInfo segmentInfo, int readBufferSize, String codecId) throws IOException {
    freqIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, codecId, StandardCodec.FREQ_EXTENSION),
                           readBufferSize);
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

  public static void files(Directory dir, SegmentInfo segmentInfo, String id, Collection<String> files) throws IOException {
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
  }

  // Must keep final because we do non-standard clone
  private final static class DocTermState extends TermState {
    long freqOffset;
    long proxOffset;
    int skipOffset;

    public Object clone() {
      DocTermState other = new DocTermState();
      other.copyFrom(this);
      return other;
    }

    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
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

  private SegmentBulkPostingsEnum lastBulkEnum;

  @Override
  public BulkPostingsEnum bulkPostings(FieldInfo fieldInfo, TermState termState, BulkPostingsEnum reuse, boolean doFreqs, boolean doPositions) throws IOException {
    // must assign to local, first, for thread safety:
    final SegmentBulkPostingsEnum lastBulkEnum = this.lastBulkEnum;

    if (lastBulkEnum != null && reuse == lastBulkEnum) {
      // fastpath
      return lastBulkEnum.reset((DocTermState) termState);
    } else {
      final SegmentBulkPostingsEnum postingsEnum;
      if (reuse == null || !(reuse instanceof SegmentBulkPostingsEnum) || !((SegmentBulkPostingsEnum) reuse).canReuse(fieldInfo, freqIn, doFreqs, doPositions)) {
        postingsEnum = new SegmentBulkPostingsEnum(fieldInfo, doFreqs, doPositions);
      } else {
        postingsEnum = (SegmentBulkPostingsEnum) reuse;
      }
      this.lastBulkEnum = postingsEnum;
      return postingsEnum.reset((DocTermState) termState);
    }
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, TermState termState, Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
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
      return docsEnum.reset(fieldInfo, (DocTermState) termState, skipDocs);
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
      return docsEnum.reset(fieldInfo, (DocTermState) termState, skipDocs);
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
      assert limit > 0;
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

    public SegmentDocsAndPositionsEnum reset(FieldInfo fieldInfo, DocTermState termState, Bits skipDocs) throws IOException {
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
    public BytesRef getPayload() throws IOException {
      throw new IOException("No payloads exist for this field!");
    }

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

    public SegmentDocsAndPositionsAndPayloadsEnum reset(FieldInfo fieldInfo, DocTermState termState, Bits skipDocs) throws IOException {
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
  
  static final int BULK_BUFFER_SIZE = 64;
  
  // Bulk postings API
  private final class SegmentBulkPostingsEnum extends BulkPostingsEnum {
    final IndexInput freqIn;
    final IndexInput proxIn;
  
    final IndexInput startFreqIn;
    final boolean omitTF;
  
    final boolean storePayloads;                        // does current field store payloads?
  
    int ord;                                      // how many docs we've read
    int docFreq;
  
    long freqOffset;
    long proxOffset;
    int skipOffset;
  
    boolean skipped;
    DefaultSkipListReader skipper;
    int payloadLength;
  
    final DocDeltasReader docDeltasReader;
    final FreqsReader freqsReader;
    final PositionsReader positionDeltasReader;
  
    boolean docsPending;
    boolean freqsPending;
  
    public SegmentBulkPostingsEnum(FieldInfo fieldInfo, boolean doFreqs, boolean doPositions) throws IOException {
      startFreqIn = StandardPostingsReader.this.freqIn;
      this.freqIn = (IndexInput) StandardPostingsReader.this.freqIn.clone();
      omitTF = fieldInfo.omitTermFreqAndPositions;
      storePayloads = fieldInfo.storePayloads;
  
      docDeltasReader = new DocDeltasReader();
      if (doFreqs && !omitTF) {
        freqsReader = new FreqsReader();
        if (doPositions) {
          proxIn = (IndexInput) StandardPostingsReader.this.proxIn.clone();
          positionDeltasReader = new PositionsReader();
        } else {
          proxIn = null;
          positionDeltasReader = null;
        }
      } else {
        proxIn = null;
        freqsReader = null;
        positionDeltasReader = null;
      }
    }
  
    public boolean canReuse(FieldInfo fieldInfo, IndexInput freqIn, boolean doFreqs, boolean doPositions) {
      return freqIn == startFreqIn &&
        doFreqs == (freqsReader != null) &&
        doPositions == (positionDeltasReader != null) && 
        omitTF == fieldInfo.omitTermFreqAndPositions &&
        storePayloads == fieldInfo.storePayloads;
    }
  
    void read() throws IOException {
      final int left = docFreq - ord;
      final int limit = left > BULK_BUFFER_SIZE ? BULK_BUFFER_SIZE : left;
      if (freqsReader == null) {
        // Consumer only wants doc deltas
        assert !docsPending;
        if (omitTF) {
          // Index already only stores doc deltas
          for(int i=0;i<limit;i++) {
            docDeltasReader.buffer[i] = freqIn.readVInt();
          }
        } else {
          // Index stores doc deltas & freq
          for(int i=0;i<limit;i++) {
            final int code = freqIn.readVInt();
            docDeltasReader.buffer[i] = code >>> 1;
            if ((code & 1) == 0) {
              // nocommit -- skipVInt?
              freqIn.readVInt();
            }
          }
        }
      } else if (positionDeltasReader == null) {
        // Consumer wants both, but no positions
        assert !docsPending;
        assert !freqsPending;
        for(int i=0;i<limit;i++) {
          final int code = freqIn.readVInt();
          docDeltasReader.buffer[i] = code >>> 1;
          if ((code & 1) == 0) {
            freqsReader.buffer[i] = freqIn.readVInt();
          } else {
            freqsReader.buffer[i] = 1;
          }
        }

        freqsPending = true;
        freqsReader.limit = limit;
      } else {
        // Consumer wants all three
        assert !docsPending;
        assert !freqsPending;
        int sumFreq = 0;
        for(int i=0;i<limit;i++) {
          final int code = freqIn.readVInt();
          docDeltasReader.buffer[i] = code >>> 1;
          final int freq;
          if ((code & 1) == 0) {
            freq = freqIn.readVInt();
          } else {
            freq = 1;
          }
          sumFreq += freq;
          freqsReader.buffer[i] = freq;
        }
        freqsPending = true;
        freqsReader.limit = limit;
        positionDeltasReader.pending += sumFreq;
      }
      docDeltasReader.limit = limit;
      ord += limit;
      docsPending = true;
    }
  
    class DocDeltasReader extends BulkPostingsEnum.BlockReader {
      final int[] buffer = new int[BULK_BUFFER_SIZE];
      int limit;
  
      @Override
      public int[] getBuffer() {
        return buffer;
      }
  
      @Override
      public int end() {
        return limit;
      }

      @Override
      public int fill() throws IOException {
        if (!docsPending) {
          read();
        }
        docsPending = false;
        return limit;
      }
  
      @Override
      public int offset() {
        return 0;
      }
  
      @Override
      public void setOffset(int offset) {
        throw new UnsupportedOperationException();
      }
    }
  
    class FreqsReader extends BulkPostingsEnum.BlockReader {
      final int[] buffer = new int[BULK_BUFFER_SIZE];
      int limit;
  
      @Override
      public int[] getBuffer() {
        return buffer;
      }
  
      @Override
      public int end() {
        return limit;
      }
  
      @Override
      public int fill() throws IOException {
        if (!freqsPending) {
          read();
        }
        freqsPending = false;
        return limit;
      }
  
      @Override
      public int offset() {
        return 0;
      }
  
      @Override
      public void setOffset(int offset) {
        throw new UnsupportedOperationException();
      }
    }
  
    class PositionsReader extends BulkPostingsEnum.BlockReader {
      final int[] buffer = new int[BULK_BUFFER_SIZE];
      int limit;
      int pending;
  
      @Override
      public int[] getBuffer() {
        return buffer;
      }
  
      @Override
      public int end() {
        return limit;
      }
  
      @Override
      public int fill() throws IOException {
        limit = pending > BULK_BUFFER_SIZE ? BULK_BUFFER_SIZE : pending;
        if (storePayloads) {
          for(int i=0;i<limit;i++) {
            final int code = proxIn.readVInt();
            buffer[i] = code >>> 1;
            if ((code & 1) != 0) {
              payloadLength = proxIn.readVInt();
            }
            if (payloadLength != 0) {
              // skip payload
              proxIn.seek(proxIn.getFilePointer()+payloadLength);
            }
          }
        } else {
          for(int i=0;i<limit;i++) {
            buffer[i] = proxIn.readVInt();
          }
        }
        pending -= limit;
        return limit;
      }
  
      @Override
      public int offset() {
        return 0;
      }
  
      @Override
      public void setOffset(int offset) {
        throw new UnsupportedOperationException();
      }
    }
    
    @Override
    public BlockReader getDocDeltasReader() {
      return docDeltasReader;
    }
      
    @Override
    public BlockReader getFreqsReader() {
      return freqsReader;
    }
  
    @Override
    public BlockReader getPositionDeltasReader() {
      return positionDeltasReader;
    }

    public SegmentBulkPostingsEnum reset(DocTermState termState) throws IOException {
      freqOffset = termState.freqOffset;
      freqIn.seek(freqOffset);
  
      docFreq = termState.docFreq;
      assert docFreq > 0;

      docDeltasReader.limit = 0;
      if (freqsReader != null) {
        freqsReader.limit = 0;
        if (positionDeltasReader != null) {
          positionDeltasReader.limit = 0;
          positionDeltasReader.pending = 0;
          // TODO: for full enum case (eg MTQ that .next()'s
          // across terms) this seek is unnecessary; maybe we
          // can avoid in such cases
          if (positionDeltasReader != null) {
            proxOffset = termState.proxOffset;
            proxIn.seek(proxOffset);
          }
        }
      }
      skipOffset = termState.skipOffset;
  
      ord = 0;
      skipped = false;
  
      return this;
    }
  
    private final JumpResult jumpResult = new JumpResult();
  
    @Override
    public JumpResult jump(int target, int curCount) throws IOException {
  
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
                       freqOffset, proxOffset,
                       docFreq, storePayloads);
  
          skipped = true;
        }
  
        final int newOrd = skipper.skipTo(target); 
  
        // nocommit rename ord -> count
        assert curCount == ord: "curCount=" + curCount + " ord=" + ord;
  
        if (newOrd > ord) {
          // Skipper moved
          //System.out.println("newOrd=" + newOrd + " vs ord=" + ord + " doc=" + skipper.getDoc());
  
          freqIn.seek(skipper.getFreqPointer());
          docDeltasReader.limit = 0;
  
          if (freqsReader != null) {
            freqsReader.limit = 0;
            if (positionDeltasReader != null) {
              positionDeltasReader.limit = 0;
              positionDeltasReader.pending = 0;
              proxIn.seek(skipper.getProxPointer());
            }
          }
  
          jumpResult.count = ord = newOrd;
          jumpResult.docID = skipper.getDoc();
  
          return jumpResult;
        }
      }
  
      // no jump occurred
      return null;
    }
  }
}

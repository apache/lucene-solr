package org.apache.lucene.codecs.lucene41;

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

import static org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene41.ForUtil.MAX_DATA_SIZE;
import static org.apache.lucene.codecs.lucene41.ForUtil.MAX_ENCODED_SIZE;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that reads docId(maybe frq,pos,offset,payloads) list
 * with postings format.
 *
 * @see Lucene41SkipReader for details
 * @lucene.experimental
 */
public final class Lucene41PostingsReader extends PostingsReaderBase {

  private final IndexInput docIn;
  private final IndexInput posIn;
  private final IndexInput payIn;

  private final ForUtil forUtil;

  // public static boolean DEBUG = false;

  /** Sole constructor. */
  public Lucene41PostingsReader(Directory dir, FieldInfos fieldInfos, SegmentInfo segmentInfo, IOContext ioContext, String segmentSuffix) throws IOException {
    boolean success = false;
    IndexInput docIn = null;
    IndexInput posIn = null;
    IndexInput payIn = null;
    try {
      docIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, Lucene41PostingsFormat.DOC_EXTENSION),
                            ioContext);
      CodecUtil.checkHeader(docIn,
                            Lucene41PostingsWriter.DOC_CODEC,
                            Lucene41PostingsWriter.VERSION_CURRENT,
                            Lucene41PostingsWriter.VERSION_CURRENT);
      forUtil = new ForUtil(docIn);

      if (fieldInfos.hasProx()) {
        posIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, Lucene41PostingsFormat.POS_EXTENSION),
                              ioContext);
        CodecUtil.checkHeader(posIn,
                              Lucene41PostingsWriter.POS_CODEC,
                              Lucene41PostingsWriter.VERSION_CURRENT,
                              Lucene41PostingsWriter.VERSION_CURRENT);

        if (fieldInfos.hasPayloads() || fieldInfos.hasOffsets()) {
          payIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, Lucene41PostingsFormat.PAY_EXTENSION),
                                ioContext);
          CodecUtil.checkHeader(payIn,
                                Lucene41PostingsWriter.PAY_CODEC,
                                Lucene41PostingsWriter.VERSION_CURRENT,
                                Lucene41PostingsWriter.VERSION_CURRENT);
        }
      }

      this.docIn = docIn;
      this.posIn = posIn;
      this.payIn = payIn;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docIn, posIn, payIn);
      }
    }
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching postings writer
    CodecUtil.checkHeader(termsIn,
                          Lucene41PostingsWriter.TERMS_CODEC,
                          Lucene41PostingsWriter.VERSION_CURRENT,
                          Lucene41PostingsWriter.VERSION_CURRENT);
    final int indexBlockSize = termsIn.readVInt();
    if (indexBlockSize != BLOCK_SIZE) {
      throw new IllegalStateException("index-time BLOCK_SIZE (" + indexBlockSize + ") != read-time BLOCK_SIZE (" + BLOCK_SIZE + ")");
    }
  }

  /**
   * Read values that have been written using variable-length encoding instead of bit-packing.
   */
  static void readVIntBlock(IndexInput docIn, int[] docBuffer,
      int[] freqBuffer, int num, boolean indexHasFreq) throws IOException {
    if (indexHasFreq) {
      for(int i=0;i<num;i++) {
        final int code = docIn.readVInt();
        docBuffer[i] = code >>> 1;
        if ((code & 1) != 0) {
          freqBuffer[i] = 1;
        } else {
          freqBuffer[i] = docIn.readVInt();
        }
      }
    } else {
      for(int i=0;i<num;i++) {
        docBuffer[i] = docIn.readVInt();
      }
    }
  }

  // Must keep final because we do non-standard clone
  private final static class IntBlockTermState extends BlockTermState {
    long docStartFP;
    long posStartFP;
    long payStartFP;
    long skipOffset;
    long lastPosBlockOffset;
    // docid when there is a single pulsed posting, otherwise -1
    // freq is always implicitly totalTermFreq in this case.
    int singletonDocID;

    // Only used by the "primary" TermState -- clones don't
    // copy this (basically they are "transient"):
    ByteArrayDataInput bytesReader;  // TODO: should this NOT be in the TermState...?
    byte[] bytes;

    @Override
    public IntBlockTermState clone() {
      IntBlockTermState other = new IntBlockTermState();
      other.copyFrom(this);
      return other;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      IntBlockTermState other = (IntBlockTermState) _other;
      docStartFP = other.docStartFP;
      posStartFP = other.posStartFP;
      payStartFP = other.payStartFP;
      lastPosBlockOffset = other.lastPosBlockOffset;
      skipOffset = other.skipOffset;
      singletonDocID = other.singletonDocID;

      // Do not copy bytes, bytesReader (else TermState is
      // very heavy, ie drags around the entire block's
      // byte[]).  On seek back, if next() is in fact used
      // (rare!), they will be re-read from disk.
    }

    @Override
    public String toString() {
      return super.toString() + " docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset + " singletonDocID=" + singletonDocID;
    }
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docIn, posIn, payIn);
  }

  /* Reads but does not decode the byte[] blob holding
     metadata for the current terms block */
  @Override
  public void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo, BlockTermState _termState) throws IOException {
    final IntBlockTermState termState = (IntBlockTermState) _termState;

    final int numBytes = termsIn.readVInt();

    if (termState.bytes == null) {
      termState.bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
      termState.bytesReader = new ByteArrayDataInput();
    } else if (termState.bytes.length < numBytes) {
      termState.bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }

    termsIn.readBytes(termState.bytes, 0, numBytes);
    termState.bytesReader.reset(termState.bytes, 0, numBytes);
  }

  @Override
  public void nextTerm(FieldInfo fieldInfo, BlockTermState _termState)
    throws IOException {
    final IntBlockTermState termState = (IntBlockTermState) _termState;
    final boolean isFirstTerm = termState.termBlockOrd == 0;
    final boolean fieldHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    final boolean fieldHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    final boolean fieldHasPayloads = fieldInfo.hasPayloads();

    final DataInput in = termState.bytesReader;
    if (isFirstTerm) {
      if (termState.docFreq == 1) {
        termState.singletonDocID = in.readVInt();
        termState.docStartFP = 0;
      } else {
        termState.singletonDocID = -1;
        termState.docStartFP = in.readVLong();
      }
      if (fieldHasPositions) {
        termState.posStartFP = in.readVLong();
        if (termState.totalTermFreq > BLOCK_SIZE) {
          termState.lastPosBlockOffset = in.readVLong();
        } else {
          termState.lastPosBlockOffset = -1;
        }
        if ((fieldHasPayloads || fieldHasOffsets) && termState.totalTermFreq >= BLOCK_SIZE) {
          termState.payStartFP = in.readVLong();
        } else {
          termState.payStartFP = -1;
        }
      }
    } else {
      if (termState.docFreq == 1) {
        termState.singletonDocID = in.readVInt();
      } else {
        termState.singletonDocID = -1;
        termState.docStartFP += in.readVLong();
      }
      if (fieldHasPositions) {
        termState.posStartFP += in.readVLong();
        if (termState.totalTermFreq > BLOCK_SIZE) {
          termState.lastPosBlockOffset = in.readVLong();
        } else {
          termState.lastPosBlockOffset = -1;
        }
        if ((fieldHasPayloads || fieldHasOffsets) && termState.totalTermFreq >= BLOCK_SIZE) {
          long delta = in.readVLong();
          if (termState.payStartFP == -1) {
            termState.payStartFP = delta;
          } else {
            termState.payStartFP += delta;
          }
        }
      }
    }

    if (termState.docFreq > BLOCK_SIZE) {
      termState.skipOffset = in.readVLong();
    } else {
      termState.skipOffset = -1;
    }
  }
    
  @Override
  public DocsEnum docs(FieldInfo fieldInfo, BlockTermState termState, Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    BlockDocsEnum docsEnum;
    if (reuse instanceof BlockDocsEnum) {
      docsEnum = (BlockDocsEnum) reuse;
      if (!docsEnum.canReuse(docIn, fieldInfo)) {
        docsEnum = new BlockDocsEnum(fieldInfo);
      }
    } else {
      docsEnum = new BlockDocsEnum(fieldInfo);
    }
    return docsEnum.reset(liveDocs, (IntBlockTermState) termState, flags);
  }

  // TODO: specialize to liveDocs vs not
  
  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState termState, Bits liveDocs,
                                               DocsAndPositionsEnum reuse, int flags)
    throws IOException {

    boolean indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    boolean indexHasPayloads = fieldInfo.hasPayloads();

    if ((!indexHasOffsets || (flags & DocsAndPositionsEnum.FLAG_OFFSETS) == 0) &&
        (!indexHasPayloads || (flags & DocsAndPositionsEnum.FLAG_PAYLOADS) == 0)) {
      BlockDocsAndPositionsEnum docsAndPositionsEnum;
      if (reuse instanceof BlockDocsAndPositionsEnum) {
        docsAndPositionsEnum = (BlockDocsAndPositionsEnum) reuse;
        if (!docsAndPositionsEnum.canReuse(docIn, fieldInfo)) {
          docsAndPositionsEnum = new BlockDocsAndPositionsEnum(fieldInfo);
        }
      } else {
        docsAndPositionsEnum = new BlockDocsAndPositionsEnum(fieldInfo);
      }
      return docsAndPositionsEnum.reset(liveDocs, (IntBlockTermState) termState);
    } else {
      EverythingEnum everythingEnum;
      if (reuse instanceof EverythingEnum) {
        everythingEnum = (EverythingEnum) reuse;
        if (!everythingEnum.canReuse(docIn, fieldInfo)) {
          everythingEnum = new EverythingEnum(fieldInfo);
        }
      } else {
        everythingEnum = new EverythingEnum(fieldInfo);
      }
      return everythingEnum.reset(liveDocs, (IntBlockTermState) termState, flags);
    }
  }

  final class BlockDocsEnum extends DocsEnum {
    private final byte[] encoded;
    
    private final int[] docDeltaBuffer = new int[MAX_DATA_SIZE];
    private final int[] freqBuffer = new int[MAX_DATA_SIZE];

    private int docBufferUpto;

    private Lucene41SkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    IndexInput docIn;
    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
    private long totalTermFreq;                       // sum of freqs in this posting list (or docFreq when omitted)
    private int docUpto;                              // how many docs we've read
    private int doc;                                  // doc we last read
    private int accum;                                // accumulator for doc deltas
    private int freq;                                 // freq we last read

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    private long skipOffset;

    // docID for next skip point, we won't use skipper if 
    // target docID is not larger than this
    private int nextSkipDoc;

    private Bits liveDocs;
    
    private boolean needsFreq; // true if the caller actually needs frequencies
    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1

    public BlockDocsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene41PostingsReader.this.docIn;
      this.docIn = null;
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();
      encoded = new byte[MAX_ENCODED_SIZE];    
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasFreq == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) &&
        indexHasPos == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public DocsEnum reset(Bits liveDocs, IntBlockTermState termState, int flags) throws IOException {
      this.liveDocs = liveDocs;
      // if (DEBUG) {
      //   System.out.println("  FPR.reset: termState=" + termState);
      // }
      docFreq = termState.docFreq;
      totalTermFreq = indexHasFreq ? termState.totalTermFreq : docFreq;
      docTermStartFP = termState.docStartFP;
      skipOffset = termState.skipOffset;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        docIn.seek(docTermStartFP);
      }

      doc = -1;
      this.needsFreq = (flags & DocsEnum.FLAG_FREQS) != 0;
      if (!indexHasFreq) {
        Arrays.fill(freqBuffer, 1);
      }
      accum = 0;
      docUpto = 0;
      nextSkipDoc = BLOCK_SIZE - 1; // we won't skip if target is found in first block
      docBufferUpto = BLOCK_SIZE;
      skipped = false;
      return this;
    }
    
    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }
    
    private void refillDocs() throws IOException {
      final int left = docFreq - docUpto;
      assert left > 0;

      if (left >= BLOCK_SIZE) {
        // if (DEBUG) {
        //   System.out.println("    fill doc block from fp=" + docIn.getFilePointer());
        // }
        forUtil.readBlock(docIn, encoded, docDeltaBuffer);

        if (indexHasFreq) {
          // if (DEBUG) {
          //   System.out.println("    fill freq block from fp=" + docIn.getFilePointer());
          // }
          if (needsFreq) {
            forUtil.readBlock(docIn, encoded, freqBuffer);
          } else {
            forUtil.skipBlock(docIn); // skip over freqs
          }
        }
      } else if (docFreq == 1) {
        docDeltaBuffer[0] = singletonDocID;
        freqBuffer[0] = (int) totalTermFreq;
      } else {
        // Read vInts:
        // if (DEBUG) {
        //   System.out.println("    fill last vInt block from fp=" + docIn.getFilePointer());
        // }
        readVIntBlock(docIn, docDeltaBuffer, freqBuffer, left, indexHasFreq);
      }
      docBufferUpto = 0;
    }

    @Override
    public int nextDoc() throws IOException {
      // if (DEBUG) {
      //   System.out.println("\nFPR.nextDoc");
      // }
      while (true) {
        // if (DEBUG) {
        //   System.out.println("  docUpto=" + docUpto + " (of df=" + docFreq + ") docBufferUpto=" + docBufferUpto);
        // }

        if (docUpto == docFreq) {
          // if (DEBUG) {
          //   System.out.println("  return doc=END");
          // }
          return doc = NO_MORE_DOCS;
        }
        if (docBufferUpto == BLOCK_SIZE) {
          refillDocs();
        }

        // if (DEBUG) {
        //   System.out.println("    accum=" + accum + " docDeltaBuffer[" + docBufferUpto + "]=" + docDeltaBuffer[docBufferUpto]);
        // }
        accum += docDeltaBuffer[docBufferUpto];
        docUpto++;

        if (liveDocs == null || liveDocs.get(accum)) {
          doc = accum;
          freq = freqBuffer[docBufferUpto];
          docBufferUpto++;
          // if (DEBUG) {
          //   System.out.println("  return doc=" + doc + " freq=" + freq);
          // }
          return doc;
        }
        // if (DEBUG) {
        //   System.out.println("  doc=" + accum + " is deleted; try next doc");
        // }
        docBufferUpto++;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      // TODO: make frq block load lazy/skippable
      // if (DEBUG) {
      //   System.out.println("  FPR.advance target=" + target);
      // }

      // current skip docID < docIDs generated from current buffer <= next skip docID
      // we don't need to skip if target is buffered already
      if (docFreq > BLOCK_SIZE && target > nextSkipDoc) {

        // if (DEBUG) {
        //   System.out.println("load skipper");
        // }

        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          skipper = new Lucene41SkipReader(docIn.clone(),
                                        Lucene41PostingsWriter.maxSkipLevels,
                                        BLOCK_SIZE,
                                        indexHasPos,
                                        indexHasOffsets,
                                        indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, 0, 0, docFreq);
          skipped = true;
        }

        // always plus one to fix the result, since skip position in Lucene41SkipReader 
        // is a little different from MultiLevelSkipListReader
        final int newDocUpto = skipper.skipTo(target) + 1; 

        if (newDocUpto > docUpto) {
          // Skipper moved
          // if (DEBUG) {
          //   System.out.println("skipper moved to docUpto=" + newDocUpto + " vs current=" + docUpto + "; docID=" + skipper.getDoc() + " fp=" + skipper.getDocPointer());
          // }
          assert newDocUpto % BLOCK_SIZE == 0 : "got " + newDocUpto;
          docUpto = newDocUpto;

          // Force to read next block
          docBufferUpto = BLOCK_SIZE;
          accum = skipper.getDoc();               // actually, this is just lastSkipEntry
          docIn.seek(skipper.getDocPointer());    // now point to the block we want to search
        }
        // next time we call advance, this is used to 
        // foresee whether skipper is necessary.
        nextSkipDoc = skipper.getNextSkipDoc();
      }
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      // Now scan... this is an inlined/pared down version
      // of nextDoc():
      while (true) {
        // if (DEBUG) {
        //   System.out.println("  scan doc=" + accum + " docBufferUpto=" + docBufferUpto);
        // }
        accum += docDeltaBuffer[docBufferUpto];
        docUpto++;

        if (accum >= target) {
          break;
        }
        docBufferUpto++;
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
      }

      if (liveDocs == null || liveDocs.get(accum)) {
        // if (DEBUG) {
        //   System.out.println("  return doc=" + accum);
        // }
        freq = freqBuffer[docBufferUpto];
        docBufferUpto++;
        return doc = accum;
      } else {
        // if (DEBUG) {
        //   System.out.println("  now do nextDoc()");
        // }
        docBufferUpto++;
        return nextDoc();
      }
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }


  final class BlockDocsAndPositionsEnum extends DocsAndPositionsEnum {
    
    private final byte[] encoded;

    private final int[] docDeltaBuffer = new int[MAX_DATA_SIZE];
    private final int[] freqBuffer = new int[MAX_DATA_SIZE];
    private final int[] posDeltaBuffer = new int[MAX_DATA_SIZE];

    private int docBufferUpto;
    private int posBufferUpto;

    private Lucene41SkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    IndexInput docIn;
    final IndexInput posIn;

    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
    private long totalTermFreq;                       // number of positions in this posting list
    private int docUpto;                              // how many docs we've read
    private int doc;                                  // doc we last read
    private int accum;                                // accumulator for doc deltas
    private int freq;                                 // freq we last read
    private int position;                             // current position

    // how many positions "behind" we are; nextPosition must
    // skip these to "catch up":
    private int posPendingCount;

    // Lazy pos seek: if != -1 then we must seek to this FP
    // before reading positions:
    private long posPendingFP;

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's postings start in the .pos file:
    private long posTermStartFP;

    // Where this term's payloads/offsets start in the .pay
    // file:
    private long payTermStartFP;

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    private long lastPosBlockFP;

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    private long skipOffset;

    private int nextSkipDoc;

    private Bits liveDocs;
    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1
    
    public BlockDocsAndPositionsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene41PostingsReader.this.docIn;
      this.docIn = null;
      this.posIn = Lucene41PostingsReader.this.posIn.clone();
      encoded = new byte[MAX_ENCODED_SIZE];
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasOffsets == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public DocsAndPositionsEnum reset(Bits liveDocs, IntBlockTermState termState) throws IOException {
      this.liveDocs = liveDocs;
      // if (DEBUG) {
      //   System.out.println("  FPR.reset: termState=" + termState);
      // }
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      skipOffset = termState.skipOffset;
      totalTermFreq = termState.totalTermFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        docIn.seek(docTermStartFP);
      }
      posPendingFP = posTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < BLOCK_SIZE) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == BLOCK_SIZE) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      doc = -1;
      accum = 0;
      docUpto = 0;
      nextSkipDoc = BLOCK_SIZE - 1;
      docBufferUpto = BLOCK_SIZE;
      skipped = false;
      return this;
    }
    
    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    private void refillDocs() throws IOException {
      final int left = docFreq - docUpto;
      assert left > 0;

      if (left >= BLOCK_SIZE) {
        // if (DEBUG) {
        //   System.out.println("    fill doc block from fp=" + docIn.getFilePointer());
        // }
        forUtil.readBlock(docIn, encoded, docDeltaBuffer);
        // if (DEBUG) {
        //   System.out.println("    fill freq block from fp=" + docIn.getFilePointer());
        // }
        forUtil.readBlock(docIn, encoded, freqBuffer);
      } else if (docFreq == 1) {
        docDeltaBuffer[0] = singletonDocID;
        freqBuffer[0] = (int) totalTermFreq;
      } else {
        // Read vInts:
        // if (DEBUG) {
        //   System.out.println("    fill last vInt doc block from fp=" + docIn.getFilePointer());
        // }
        readVIntBlock(docIn, docDeltaBuffer, freqBuffer, left, true);
      }
      docBufferUpto = 0;
    }
    
    private void refillPositions() throws IOException {
      // if (DEBUG) {
      //   System.out.println("      refillPositions");
      // }
      if (posIn.getFilePointer() == lastPosBlockFP) {
        // if (DEBUG) {
        //   System.out.println("        vInt pos block @ fp=" + posIn.getFilePointer() + " hasPayloads=" + indexHasPayloads + " hasOffsets=" + indexHasOffsets);
        // }
        final int count = (int) (totalTermFreq % BLOCK_SIZE);
        int payloadLength = 0;
        for(int i=0;i<count;i++) {
          int code = posIn.readVInt();
          if (indexHasPayloads) {
            if ((code & 1) != 0) {
              payloadLength = posIn.readVInt();
            }
            posDeltaBuffer[i] = code >>> 1;
            if (payloadLength != 0) {
              posIn.seek(posIn.getFilePointer() + payloadLength);
            }
          } else {
            posDeltaBuffer[i] = code;
          }
          if (indexHasOffsets) {
            if ((posIn.readVInt() & 1) != 0) {
              // offset length changed
              posIn.readVInt();
            }
          }
        }
      } else {
        // if (DEBUG) {
        //   System.out.println("        bulk pos block @ fp=" + posIn.getFilePointer());
        // }
        forUtil.readBlock(posIn, encoded, posDeltaBuffer);
      }
    }

    @Override
    public int nextDoc() throws IOException {
      // if (DEBUG) {
      //   System.out.println("  FPR.nextDoc");
      // }
      while (true) {
        // if (DEBUG) {
        //   System.out.println("    docUpto=" + docUpto + " (of df=" + docFreq + ") docBufferUpto=" + docBufferUpto);
        // }
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
        if (docBufferUpto == BLOCK_SIZE) {
          refillDocs();
        }
        // if (DEBUG) {
        //   System.out.println("    accum=" + accum + " docDeltaBuffer[" + docBufferUpto + "]=" + docDeltaBuffer[docBufferUpto]);
        // }
        accum += docDeltaBuffer[docBufferUpto];
        freq = freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (liveDocs == null || liveDocs.get(accum)) {
          doc = accum;
          position = 0;
          // if (DEBUG) {
          //   System.out.println("    return doc=" + doc + " freq=" + freq + " posPendingCount=" + posPendingCount);
          // }
          return doc;
        }
        // if (DEBUG) {
        //   System.out.println("    doc=" + accum + " is deleted; try next doc");
        // }
      }
    }
    
    @Override
    public int advance(int target) throws IOException {
      // TODO: make frq block load lazy/skippable
      // if (DEBUG) {
      //   System.out.println("  FPR.advance target=" + target);
      // }

      if (docFreq > BLOCK_SIZE && target > nextSkipDoc) {
        // if (DEBUG) {
        //   System.out.println("    try skipper");
        // }
        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          // if (DEBUG) {
          //   System.out.println("    create skipper");
          // }
          skipper = new Lucene41SkipReader(docIn.clone(),
                                        Lucene41PostingsWriter.maxSkipLevels,
                                        BLOCK_SIZE,
                                        true,
                                        indexHasOffsets,
                                        indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          // if (DEBUG) {
          //   System.out.println("    init skipper");
          // }
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, posTermStartFP, payTermStartFP, docFreq);
          skipped = true;
        }

        final int newDocUpto = skipper.skipTo(target) + 1; 

        if (newDocUpto > docUpto) {
          // Skipper moved
          // if (DEBUG) {
          //   System.out.println("    skipper moved to docUpto=" + newDocUpto + " vs current=" + docUpto + "; docID=" + skipper.getDoc() + " fp=" + skipper.getDocPointer() + " pos.fp=" + skipper.getPosPointer() + " pos.bufferUpto=" + skipper.getPosBufferUpto());
          // }

          assert newDocUpto % BLOCK_SIZE == 0 : "got " + newDocUpto;
          docUpto = newDocUpto;

          // Force to read next block
          docBufferUpto = BLOCK_SIZE;
          accum = skipper.getDoc();
          docIn.seek(skipper.getDocPointer());
          posPendingFP = skipper.getPosPointer();
          posPendingCount = skipper.getPosBufferUpto();
        }
        nextSkipDoc = skipper.getNextSkipDoc();
      }
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      // Now scan... this is an inlined/pared down version
      // of nextDoc():
      while (true) {
        // if (DEBUG) {
        //   System.out.println("  scan doc=" + accum + " docBufferUpto=" + docBufferUpto);
        // }
        accum += docDeltaBuffer[docBufferUpto];
        freq = freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (accum >= target) {
          break;
        }
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
      }

      if (liveDocs == null || liveDocs.get(accum)) {
        // if (DEBUG) {
        //   System.out.println("  return doc=" + accum);
        // }
        position = 0;
        return doc = accum;
      } else {
        // if (DEBUG) {
        //   System.out.println("  now do nextDoc()");
        // }
        return nextDoc();
      }
    }

    // TODO: in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointer ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    private void skipPositions() throws IOException {
      // Skip positions now:
      int toSkip = posPendingCount - freq;
      // if (DEBUG) {
      //   System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      // }

      final int leftInBlock = BLOCK_SIZE - posBufferUpto;
      if (toSkip < leftInBlock) {
        posBufferUpto += toSkip;
        // if (DEBUG) {
        //   System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        // }
      } else {
        toSkip -= leftInBlock;
        while(toSkip >= BLOCK_SIZE) {
          // if (DEBUG) {
          //   System.out.println("        skip whole block @ fp=" + posIn.getFilePointer());
          // }
          assert posIn.getFilePointer() != lastPosBlockFP;
          forUtil.skipBlock(posIn);
          toSkip -= BLOCK_SIZE;
        }
        refillPositions();
        posBufferUpto = toSkip;
        // if (DEBUG) {
        //   System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        // }
      }

      position = 0;
    }

    @Override
    public int nextPosition() throws IOException {
      // if (DEBUG) {
      //   System.out.println("    FPR.nextPosition posPendingCount=" + posPendingCount + " posBufferUpto=" + posBufferUpto);
      // }
      if (posPendingFP != -1) {
        // if (DEBUG) {
        //   System.out.println("      seek to pendingFP=" + posPendingFP);
        // }
        posIn.seek(posPendingFP);
        posPendingFP = -1;

        // Force buffer refill:
        posBufferUpto = BLOCK_SIZE;
      }

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == BLOCK_SIZE) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += posDeltaBuffer[posBufferUpto++];
      posPendingCount--;
      // if (DEBUG) {
      //   System.out.println("      return pos=" + position);
      // }
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
  
    @Override
    public BytesRef getPayload() {
      return null;
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }

  // Also handles payloads + offsets
  final class EverythingEnum extends DocsAndPositionsEnum {
    
    private final byte[] encoded;

    private final int[] docDeltaBuffer = new int[MAX_DATA_SIZE];
    private final int[] freqBuffer = new int[MAX_DATA_SIZE];
    private final int[] posDeltaBuffer = new int[MAX_DATA_SIZE];

    private final int[] payloadLengthBuffer;
    private final int[] offsetStartDeltaBuffer;
    private final int[] offsetLengthBuffer;

    private byte[] payloadBytes;
    private int payloadByteUpto;
    private int payloadLength;

    private int lastStartOffset;
    private int startOffset;
    private int endOffset;

    private int docBufferUpto;
    private int posBufferUpto;

    private Lucene41SkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    IndexInput docIn;
    final IndexInput posIn;
    final IndexInput payIn;
    final BytesRef payload;

    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
    private long totalTermFreq;                       // number of positions in this posting list
    private int docUpto;                              // how many docs we've read
    private int doc;                                  // doc we last read
    private int accum;                                // accumulator for doc deltas
    private int freq;                                 // freq we last read
    private int position;                             // current position

    // how many positions "behind" we are; nextPosition must
    // skip these to "catch up":
    private int posPendingCount;

    // Lazy pos seek: if != -1 then we must seek to this FP
    // before reading positions:
    private long posPendingFP;

    // Lazy pay seek: if != -1 then we must seek to this FP
    // before reading payloads/offsets:
    private long payPendingFP;

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's postings start in the .pos file:
    private long posTermStartFP;

    // Where this term's payloads/offsets start in the .pay
    // file:
    private long payTermStartFP;

    // File pointer where the last (vInt encoded) pos delta
    // block is.  We need this to know whether to bulk
    // decode vs vInt decode the block:
    private long lastPosBlockFP;

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    private long skipOffset;

    private int nextSkipDoc;

    private Bits liveDocs;
    
    private boolean needsOffsets; // true if we actually need offsets
    private boolean needsPayloads; // true if we actually need payloads
    private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1
    
    public EverythingEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = Lucene41PostingsReader.this.docIn;
      this.docIn = null;
      this.posIn = Lucene41PostingsReader.this.posIn.clone();
      this.payIn = Lucene41PostingsReader.this.payIn.clone();
      encoded = new byte[MAX_ENCODED_SIZE];
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      if (indexHasOffsets) {
        offsetStartDeltaBuffer = new int[MAX_DATA_SIZE];
        offsetLengthBuffer = new int[MAX_DATA_SIZE];
      } else {
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        startOffset = -1;
        endOffset = -1;
      }

      indexHasPayloads = fieldInfo.hasPayloads();
      if (indexHasPayloads) {
        payloadLengthBuffer = new int[MAX_DATA_SIZE];
        payloadBytes = new byte[128];
        payload = new BytesRef();
      } else {
        payloadLengthBuffer = null;
        payloadBytes = null;
        payload = null;
      }
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasOffsets == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public EverythingEnum reset(Bits liveDocs, IntBlockTermState termState, int flags) throws IOException {
      this.liveDocs = liveDocs;
      // if (DEBUG) {
      //   System.out.println("  FPR.reset: termState=" + termState);
      // }
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      skipOffset = termState.skipOffset;
      totalTermFreq = termState.totalTermFreq;
      singletonDocID = termState.singletonDocID;
      if (docFreq > 1) {
        if (docIn == null) {
          // lazy init
          docIn = startDocIn.clone();
        }
        docIn.seek(docTermStartFP);
      }
      posPendingFP = posTermStartFP;
      payPendingFP = payTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < BLOCK_SIZE) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == BLOCK_SIZE) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      this.needsOffsets = (flags & DocsAndPositionsEnum.FLAG_OFFSETS) != 0;
      this.needsPayloads = (flags & DocsAndPositionsEnum.FLAG_PAYLOADS) != 0;

      doc = -1;
      accum = 0;
      docUpto = 0;
      nextSkipDoc = BLOCK_SIZE - 1;
      docBufferUpto = BLOCK_SIZE;
      skipped = false;
      return this;
    }
    
    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    private void refillDocs() throws IOException {
      final int left = docFreq - docUpto;
      assert left > 0;

      if (left >= BLOCK_SIZE) {
        // if (DEBUG) {
        //   System.out.println("    fill doc block from fp=" + docIn.getFilePointer());
        // }
        forUtil.readBlock(docIn, encoded, docDeltaBuffer);
        // if (DEBUG) {
        //   System.out.println("    fill freq block from fp=" + docIn.getFilePointer());
        // }
        forUtil.readBlock(docIn, encoded, freqBuffer);
      } else if (docFreq == 1) {
        docDeltaBuffer[0] = singletonDocID;
        freqBuffer[0] = (int) totalTermFreq;
      } else {
        // if (DEBUG) {
        //   System.out.println("    fill last vInt doc block from fp=" + docIn.getFilePointer());
        // }
        readVIntBlock(docIn, docDeltaBuffer, freqBuffer, left, true);
      }
      docBufferUpto = 0;
    }
    
    private void refillPositions() throws IOException {
      // if (DEBUG) {
      //   System.out.println("      refillPositions");
      // }
      if (posIn.getFilePointer() == lastPosBlockFP) {
        // if (DEBUG) {
        //   System.out.println("        vInt pos block @ fp=" + posIn.getFilePointer() + " hasPayloads=" + indexHasPayloads + " hasOffsets=" + indexHasOffsets);
        // }
        final int count = (int) (totalTermFreq % BLOCK_SIZE);
        int payloadLength = 0;
        int offsetLength = 0;
        payloadByteUpto = 0;
        for(int i=0;i<count;i++) {
          int code = posIn.readVInt();
          if (indexHasPayloads) {
            if ((code & 1) != 0) {
              payloadLength = posIn.readVInt();
            }
            // if (DEBUG) {
            //   System.out.println("        i=" + i + " payloadLen=" + payloadLength);
            // }
            payloadLengthBuffer[i] = payloadLength;
            posDeltaBuffer[i] = code >>> 1;
            if (payloadLength != 0) {
              if (payloadByteUpto + payloadLength > payloadBytes.length) {
                payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payloadLength);
              }
              //System.out.println("          read payload @ pos.fp=" + posIn.getFilePointer());
              posIn.readBytes(payloadBytes, payloadByteUpto, payloadLength);
              payloadByteUpto += payloadLength;
            }
          } else {
            posDeltaBuffer[i] = code;
          }

          if (indexHasOffsets) {
            // if (DEBUG) {
            //   System.out.println("        i=" + i + " read offsets from posIn.fp=" + posIn.getFilePointer());
            // }
            int deltaCode = posIn.readVInt();
            if ((deltaCode & 1) != 0) {
              offsetLength = posIn.readVInt();
            }
            offsetStartDeltaBuffer[i] = deltaCode >>> 1;
            offsetLengthBuffer[i] = offsetLength;
            // if (DEBUG) {
            //   System.out.println("          startOffDelta=" + offsetStartDeltaBuffer[i] + " offsetLen=" + offsetLengthBuffer[i]);
            // }
          }
        }
        payloadByteUpto = 0;
      } else {
        // if (DEBUG) {
        //   System.out.println("        bulk pos block @ fp=" + posIn.getFilePointer());
        // }
        forUtil.readBlock(posIn, encoded, posDeltaBuffer);

        if (indexHasPayloads) {
          // if (DEBUG) {
          //   System.out.println("        bulk payload block @ pay.fp=" + payIn.getFilePointer());
          // }
          if (needsPayloads) {
            forUtil.readBlock(payIn, encoded, payloadLengthBuffer);
            int numBytes = payIn.readVInt();
            // if (DEBUG) {
            //   System.out.println("        " + numBytes + " payload bytes @ pay.fp=" + payIn.getFilePointer());
            // }
            if (numBytes > payloadBytes.length) {
              payloadBytes = ArrayUtil.grow(payloadBytes, numBytes);
            }
            payIn.readBytes(payloadBytes, 0, numBytes);
          } else {
            // this works, because when writing a vint block we always force the first length to be written
            forUtil.skipBlock(payIn); // skip over lengths
            int numBytes = payIn.readVInt(); // read length of payloadBytes
            payIn.seek(payIn.getFilePointer() + numBytes); // skip over payloadBytes
          }
          payloadByteUpto = 0;
        }

        if (indexHasOffsets) {
          // if (DEBUG) {
          //   System.out.println("        bulk offset block @ pay.fp=" + payIn.getFilePointer());
          // }
          if (needsOffsets) {
            forUtil.readBlock(payIn, encoded, offsetStartDeltaBuffer);
            forUtil.readBlock(payIn, encoded, offsetLengthBuffer);
          } else {
            // this works, because when writing a vint block we always force the first length to be written
            forUtil.skipBlock(payIn); // skip over starts
            forUtil.skipBlock(payIn); // skip over lengths
          }
        }
      }
    }

    @Override
    public int nextDoc() throws IOException {
      // if (DEBUG) {
      //   System.out.println("  FPR.nextDoc");
      // }
      while (true) {
        // if (DEBUG) {
        //   System.out.println("    docUpto=" + docUpto + " (of df=" + docFreq + ") docBufferUpto=" + docBufferUpto);
        // }
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
        if (docBufferUpto == BLOCK_SIZE) {
          refillDocs();
        }
        // if (DEBUG) {
        //   System.out.println("    accum=" + accum + " docDeltaBuffer[" + docBufferUpto + "]=" + docDeltaBuffer[docBufferUpto]);
        // }
        accum += docDeltaBuffer[docBufferUpto];
        freq = freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (liveDocs == null || liveDocs.get(accum)) {
          doc = accum;
          // if (DEBUG) {
          //   System.out.println("    return doc=" + doc + " freq=" + freq + " posPendingCount=" + posPendingCount);
          // }
          position = 0;
          lastStartOffset = 0;
          return doc;
        }

        // if (DEBUG) {
        //   System.out.println("    doc=" + accum + " is deleted; try next doc");
        // }
      }
    }
    
    @Override
    public int advance(int target) throws IOException {
      // TODO: make frq block load lazy/skippable
      // if (DEBUG) {
      //   System.out.println("  FPR.advance target=" + target);
      // }

      if (docFreq > BLOCK_SIZE && target > nextSkipDoc) {

        // if (DEBUG) {
        //   System.out.println("    try skipper");
        // }

        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          // if (DEBUG) {
          //   System.out.println("    create skipper");
          // }
          skipper = new Lucene41SkipReader(docIn.clone(),
                                        Lucene41PostingsWriter.maxSkipLevels,
                                        BLOCK_SIZE,
                                        true,
                                        indexHasOffsets,
                                        indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          // if (DEBUG) {
          //   System.out.println("    init skipper");
          // }
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, posTermStartFP, payTermStartFP, docFreq);
          skipped = true;
        }

        final int newDocUpto = skipper.skipTo(target) + 1; 

        if (newDocUpto > docUpto) {
          // Skipper moved
          // if (DEBUG) {
          //   System.out.println("    skipper moved to docUpto=" + newDocUpto + " vs current=" + docUpto + "; docID=" + skipper.getDoc() + " fp=" + skipper.getDocPointer() + " pos.fp=" + skipper.getPosPointer() + " pos.bufferUpto=" + skipper.getPosBufferUpto() + " pay.fp=" + skipper.getPayPointer() + " lastStartOffset=" + lastStartOffset);
          // }
          assert newDocUpto % BLOCK_SIZE == 0 : "got " + newDocUpto;
          docUpto = newDocUpto;

          // Force to read next block
          docBufferUpto = BLOCK_SIZE;
          accum = skipper.getDoc();
          docIn.seek(skipper.getDocPointer());
          posPendingFP = skipper.getPosPointer();
          payPendingFP = skipper.getPayPointer();
          posPendingCount = skipper.getPosBufferUpto();
          lastStartOffset = 0; // new document
          payloadByteUpto = skipper.getPayloadByteUpto();
        }
        nextSkipDoc = skipper.getNextSkipDoc();
      }
      if (docUpto == docFreq) {
        return doc = NO_MORE_DOCS;
      }
      if (docBufferUpto == BLOCK_SIZE) {
        refillDocs();
      }

      // Now scan:
      while (true) {
        // if (DEBUG) {
        //   System.out.println("  scan doc=" + accum + " docBufferUpto=" + docBufferUpto);
        // }
        accum += docDeltaBuffer[docBufferUpto];
        freq = freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (accum >= target) {
          break;
        }
        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }
      }

      if (liveDocs == null || liveDocs.get(accum)) {
        // if (DEBUG) {
        //   System.out.println("  return doc=" + accum);
        // }
        position = 0;
        lastStartOffset = 0;
        return doc = accum;
      } else {
        // if (DEBUG) {
        //   System.out.println("  now do nextDoc()");
        // }
        return nextDoc();
      }
    }

    // TODO: in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointer ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    private void skipPositions() throws IOException {
      // Skip positions now:
      int toSkip = posPendingCount - freq;
      // if (DEBUG) {
      //   System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      // }

      final int leftInBlock = BLOCK_SIZE - posBufferUpto;
      if (toSkip < leftInBlock) {
        int end = posBufferUpto + toSkip;
        while(posBufferUpto < end) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
        // if (DEBUG) {
        //   System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        // }
      } else {
        toSkip -= leftInBlock;
        while(toSkip >= BLOCK_SIZE) {
          // if (DEBUG) {
          //   System.out.println("        skip whole block @ fp=" + posIn.getFilePointer());
          // }
          assert posIn.getFilePointer() != lastPosBlockFP;
          forUtil.skipBlock(posIn);

          if (indexHasPayloads) {
            // Skip payloadLength block:
            forUtil.skipBlock(payIn);

            // Skip payloadBytes block:
            int numBytes = payIn.readVInt();
            payIn.seek(payIn.getFilePointer() + numBytes);
          }

          if (indexHasOffsets) {
            forUtil.skipBlock(payIn);
            forUtil.skipBlock(payIn);
          }
          toSkip -= BLOCK_SIZE;
        }
        refillPositions();
        payloadByteUpto = 0;
        posBufferUpto = 0;
        while(posBufferUpto < toSkip) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
        // if (DEBUG) {
        //   System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        // }
      }

      position = 0;
      lastStartOffset = 0;
    }

    @Override
    public int nextPosition() throws IOException {
      // if (DEBUG) {
      //   System.out.println("    FPR.nextPosition posPendingCount=" + posPendingCount + " posBufferUpto=" + posBufferUpto + " payloadByteUpto=" + payloadByteUpto)// ;
      // }
      if (posPendingFP != -1) {
        // if (DEBUG) {
        //   System.out.println("      seek pos to pendingFP=" + posPendingFP);
        // }
        posIn.seek(posPendingFP);
        posPendingFP = -1;

        if (payPendingFP != -1) {
          // if (DEBUG) {
          //   System.out.println("      seek pay to pendingFP=" + payPendingFP);
          // }
          payIn.seek(payPendingFP);
          payPendingFP = -1;
        }

        // Force buffer refill:
        posBufferUpto = BLOCK_SIZE;
      }

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == BLOCK_SIZE) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += posDeltaBuffer[posBufferUpto];

      if (indexHasPayloads) {
        payloadLength = payloadLengthBuffer[posBufferUpto];
        payload.bytes = payloadBytes;
        payload.offset = payloadByteUpto;
        payload.length = payloadLength;
        payloadByteUpto += payloadLength;
      }

      if (indexHasOffsets) {
        startOffset = lastStartOffset + offsetStartDeltaBuffer[posBufferUpto];
        endOffset = startOffset + offsetLengthBuffer[posBufferUpto];
        lastStartOffset = startOffset;
      }

      posBufferUpto++;
      posPendingCount--;
      // if (DEBUG) {
      //   System.out.println("      return pos=" + position);
      // }
      return position;
    }

    @Override
    public int startOffset() {
      return startOffset;
    }
  
    @Override
    public int endOffset() {
      return endOffset;
    }
  
    @Override
    public BytesRef getPayload() {
      // if (DEBUG) {
      //   System.out.println("    FPR.getPayload payloadLength=" + payloadLength + " payloadByteUpto=" + payloadByteUpto);
      // }
      if (payloadLength == 0) {
        return null;
      } else {
        return payload;
      }
    }
    
    @Override
    public long cost() {
      return docFreq;
    }
  }
}

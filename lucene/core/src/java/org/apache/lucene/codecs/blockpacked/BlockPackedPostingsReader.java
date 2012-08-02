package org.apache.lucene.codecs.blockpacked;

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
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

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
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

// nocommit can we share more w/ BlockPF?  like make the
// ForUtil class pluggable then pass it in...

// nocommit javadocs
public final class BlockPackedPostingsReader extends PostingsReaderBase {

  private final IndexInput docIn;
  private final IndexInput posIn;
  private final IndexInput payIn;

  public static boolean DEBUG = false;

  // nocommit
  final String segment;

  // NOTE: not private to avoid access$NNN methods:
  final int blockSize;

  public BlockPackedPostingsReader(Directory dir, FieldInfos fieldInfos, SegmentInfo segmentInfo, IOContext ioContext, String segmentSuffix, int blockSize) throws IOException {
    boolean success = false;
    segment = segmentInfo.name;
    IndexInput docIn = null;
    IndexInput posIn = null;
    IndexInput payIn = null;
    try {
      docIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, BlockPackedPostingsFormat.DOC_EXTENSION),
                            ioContext);
      CodecUtil.checkHeader(docIn,
                            BlockPackedPostingsWriter.DOC_CODEC,
                            BlockPackedPostingsWriter.VERSION_START,
                            BlockPackedPostingsWriter.VERSION_START);

      if (fieldInfos.hasProx()) {
        posIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, BlockPackedPostingsFormat.POS_EXTENSION),
                              ioContext);
        CodecUtil.checkHeader(posIn,
                              BlockPackedPostingsWriter.POS_CODEC,
                              BlockPackedPostingsWriter.VERSION_START,
                              BlockPackedPostingsWriter.VERSION_START);

        if (fieldInfos.hasPayloads() || fieldInfos.hasOffsets()) {
          payIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, BlockPackedPostingsFormat.PAY_EXTENSION),
                                ioContext);
          CodecUtil.checkHeader(payIn,
                                BlockPackedPostingsWriter.PAY_CODEC,
                                BlockPackedPostingsWriter.VERSION_START,
                                BlockPackedPostingsWriter.VERSION_START);
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

    this.blockSize = blockSize;
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn,
                          BlockPackedPostingsWriter.TERMS_CODEC,
                          BlockPackedPostingsWriter.VERSION_START,
                          BlockPackedPostingsWriter.VERSION_START);
    final int indexBlockSize = termsIn.readVInt();
    if (indexBlockSize != blockSize) {
      throw new IllegalStateException("index-time blockSize (" + indexBlockSize + ") != read-time blockSize (" + blockSize + ")");
    }
  }

  static void readBlock(IndexInput in, byte[] encoded, LongBuffer encodedBuffer, LongBuffer buffer) throws IOException {
    int header = in.readVInt();
    in.readBytes(encoded, 0, ForUtil.getEncodedSize(header));
    ForUtil.decompress(buffer, encodedBuffer, header);
  }

  static void skipBlock(IndexInput in) throws IOException {
    int header = in.readVInt();
    in.seek(in.getFilePointer() + ForUtil.getEncodedSize(header));
  }

  // Must keep final because we do non-standard clone
  private final static class IntBlockTermState extends BlockTermState {
    long docStartFP;
    long posStartFP;
    long payStartFP;
    int skipOffset;
    int lastPosBlockOffset;

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

      // Do not copy bytes, bytesReader (else TermState is
      // very heavy, ie drags around the entire block's
      // byte[]).  On seek back, if next() is in fact used
      // (rare!), they will be re-read from disk.
    }

    @Override
    public String toString() {
      return super.toString() + " docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset;
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
      termState.docStartFP = in.readVLong();
      if (fieldHasPositions) {
        termState.posStartFP = in.readVLong();
        if (termState.totalTermFreq > blockSize) {
          termState.lastPosBlockOffset = in.readVInt();
        } else {
          termState.lastPosBlockOffset = -1;
        }
        if ((fieldHasPayloads || fieldHasOffsets) && termState.totalTermFreq >= blockSize) {
          termState.payStartFP = in.readVLong();
        } else {
          termState.payStartFP = -1;
        }
      }
    } else {
      termState.docStartFP += in.readVLong();
      if (fieldHasPositions) {
        termState.posStartFP += in.readVLong();
        if (termState.totalTermFreq > blockSize) {
          termState.lastPosBlockOffset = in.readVInt();
        } else {
          termState.lastPosBlockOffset = -1;
        }
        if ((fieldHasPayloads || fieldHasOffsets) && termState.totalTermFreq >= blockSize) {
          long delta = in.readVLong();
          if (termState.payStartFP == -1) {
            termState.payStartFP = delta;
          } else {
            termState.payStartFP += delta;
          }
        }
      }
    }

    if (termState.docFreq > blockSize) {
      termState.skipOffset = in.readVInt();
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
    return docsEnum.reset(liveDocs, (IntBlockTermState) termState);
  }

  // TODO: specialize to liveDocs vs not, and freqs vs not
  
  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, BlockTermState termState, Bits liveDocs,
                                               DocsAndPositionsEnum reuse, int flags)
    throws IOException {

    boolean indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    boolean indexHasPayloasd = fieldInfo.hasPayloads();

    if ((!indexHasOffsets || (flags & DocsAndPositionsEnum.FLAG_OFFSETS) == 0) &&
        (!fieldInfo.hasPayloads() || (flags & DocsAndPositionsEnum.FLAG_PAYLOADS) == 0)) {
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
      return everythingEnum.reset(liveDocs, (IntBlockTermState) termState);
    }
  }

  final class BlockDocsEnum extends DocsEnum {
    private final byte[] encoded;
    private final LongBuffer encodedBuffer;
    
    private final long[] docDeltaBuffer = new long[blockSize];
    private final long[] freqBuffer = new long[blockSize];
    private final LongBuffer docDeltaLBuffer = LongBuffer.wrap(docDeltaBuffer);
    private final LongBuffer freqLBuffer = LongBuffer.wrap(freqBuffer);

    private int docBufferUpto;

    private BlockPackedSkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    final IndexInput docIn;
    final boolean indexHasFreq;
    final boolean indexHasPos;
    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
    private int docUpto;                              // how many docs we've read
    private int doc;                                  // doc we last read
    private int accum;                                // accumulator for doc deltas
    private int freq;                                 // freq we last read

    // Where this term's postings start in the .doc file:
    private long docTermStartFP;

    // Where this term's skip data starts (after
    // docTermStartFP) in the .doc file (or -1 if there is
    // no skip data for this term):
    private int skipOffset;

    private Bits liveDocs;

    public BlockDocsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = BlockPackedPostingsReader.this.docIn;
      this.docIn = (IndexInput) startDocIn.clone();
      indexHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
      indexHasPos = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      indexHasPayloads = fieldInfo.hasPayloads();
      encoded = new byte[blockSize*4];
      encodedBuffer = ByteBuffer.wrap(encoded).asLongBuffer();      
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasFreq == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) &&
        indexHasPos == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public DocsEnum reset(Bits liveDocs, IntBlockTermState termState) throws IOException {
      this.liveDocs = liveDocs;
      if (DEBUG) {
        System.out.println("  FPR.reset: seg=" + segment + " termState=" + termState);
      }
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      docIn.seek(docTermStartFP);
      skipOffset = termState.skipOffset;
      if (!indexHasFreq) {
        Arrays.fill(freqBuffer, 1);
      }

      doc = -1;
      accum = 0;
      docUpto = 0;
      docBufferUpto = blockSize;
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

      if (left >= blockSize) {
        if (DEBUG) {
          System.out.println("    fill doc block from fp=" + docIn.getFilePointer());
        }
        readBlock(docIn, encoded, encodedBuffer, docDeltaLBuffer);

        if (indexHasFreq) {
          if (DEBUG) {
            System.out.println("    fill freq block from fp=" + docIn.getFilePointer());
          }
          readBlock(docIn, encoded, encodedBuffer, freqLBuffer);
        }
      } else {
        // Read vInts:
        if (DEBUG) {
          System.out.println("    fill last vInt block from fp=" + docIn.getFilePointer());
        }
        for(int i=0;i<left;i++) {
          final int code = docIn.readVInt();
          if (indexHasFreq) {
            docDeltaBuffer[i] = code >>> 1;
            if ((code & 1) != 0) {
              freqBuffer[i] = 1;
            } else {
              freqBuffer[i] = docIn.readVInt();
            }
          } else {
            docDeltaBuffer[i] = code;
          }
        }
      }
      docBufferUpto = 0;
    }

    @Override
    public int nextDoc() throws IOException {

      if (DEBUG) {
        System.out.println("\nFPR.nextDoc");
      }

      while (true) {
        if (DEBUG) {
          System.out.println("  docUpto=" + docUpto + " (of df=" + docFreq + ") docBufferUpto=" + docBufferUpto);
        }

        if (docUpto == docFreq) {
          if (DEBUG) {
            System.out.println("  return doc=END");
          }
          return doc = NO_MORE_DOCS;
        }

        if (docBufferUpto == blockSize) {
          refillDocs();
        }

        if (DEBUG) {
          System.out.println("    accum=" + accum + " docDeltaBuffer[" + docBufferUpto + "]=" + docDeltaBuffer[docBufferUpto]);
        }
        accum += docDeltaBuffer[docBufferUpto];
        docUpto++;

        if (liveDocs == null || liveDocs.get(accum)) {
          doc = accum;
          freq = (int)freqBuffer[docBufferUpto];
          docBufferUpto++;
          if (DEBUG) {
            System.out.println("  return doc=" + doc + " freq=" + freq);
          }
          return doc;
        }

        if (DEBUG) {
          System.out.println("  doc=" + accum + " is deleted; try next doc");
        }

        docBufferUpto++;
      }
    }
    
    @Override
    public int advance(int target) throws IOException {
      // nocommit make frq block load lazy/skippable

      // nocommit 2 is heuristic guess!!
      // nocommit put cheating back!  does it help?
      // nocommit use skipper!!!  it has next last doc id!!
      //if (docFreq > blockSize && target - (blockSize - docBufferUpto) - 2*blockSize > accum) {
      if (docFreq > blockSize && target - accum > blockSize) {

        if (DEBUG) {
          System.out.println("load skipper");
        }

        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          skipper = new BlockPackedSkipReader((IndexInput) docIn.clone(),
                                        BlockPackedPostingsWriter.maxSkipLevels,
                                        blockSize,
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

        final int newDocUpto = skipper.skipTo(target); 

        if (newDocUpto > docUpto) {
          // Skipper moved

          if (DEBUG) {
            System.out.println("skipper moved to docUpto=" + newDocUpto + " vs current=" + docUpto + "; docID=" + skipper.getDoc() + " fp=" + skipper.getDocPointer());
          }

          assert newDocUpto % blockSize == (blockSize-1): "got " + newDocUpto;
          docUpto = newDocUpto+1;

          // Force block read next:
          docBufferUpto = blockSize;
          accum = skipper.getDoc();
          docIn.seek(skipper.getDocPointer());
        }
      }

      // Now scan:
      while (nextDoc() != NO_MORE_DOCS) {
        if (doc >= target) {
          if (DEBUG) {
            System.out.println("  advance return doc=" + doc);
          }
          return doc;
        }
      }

      if (DEBUG) {
        System.out.println("  advance return doc=END");
      }

      return NO_MORE_DOCS;
    }
  }


  final class BlockDocsAndPositionsEnum extends DocsAndPositionsEnum {
    
    private final byte[] encoded;
    private final LongBuffer encodedBuffer;

    private final long[] docDeltaBuffer = new long[blockSize];
    private final long[] freqBuffer = new long[blockSize];
    private final long[] posDeltaBuffer = new long[blockSize];


    private final LongBuffer docDeltaLBuffer = LongBuffer.wrap(docDeltaBuffer);
    private final LongBuffer freqLBuffer = LongBuffer.wrap(freqBuffer);
    private final LongBuffer posDeltaLBuffer = LongBuffer.wrap(posDeltaBuffer);

    private int docBufferUpto;
    private int posBufferUpto;

    private BlockPackedSkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    final IndexInput docIn;
    final IndexInput posIn;

    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
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
    private int skipOffset;

    private Bits liveDocs;
    
    public BlockDocsAndPositionsEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = BlockPackedPostingsReader.this.docIn;
      this.docIn = (IndexInput) startDocIn.clone();
      this.posIn = (IndexInput) BlockPackedPostingsReader.this.posIn.clone();
      encoded = new byte[blockSize*4];
      encodedBuffer = ByteBuffer.wrap(encoded).asLongBuffer();
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
      if (DEBUG) {
        System.out.println("  FPR.reset: termState=" + termState);
      }
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      docIn.seek(docTermStartFP);
      skipOffset = termState.skipOffset;
      posPendingFP = posTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < blockSize) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == blockSize) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      doc = -1;
      accum = 0;
      docUpto = 0;
      docBufferUpto = blockSize;
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

      if (left >= blockSize) {
        if (DEBUG) {
          System.out.println("    fill doc block from fp=" + docIn.getFilePointer());
        }

        readBlock(docIn, encoded, encodedBuffer, docDeltaLBuffer);

        if (DEBUG) {
          System.out.println("    fill freq block from fp=" + docIn.getFilePointer());
        }

        readBlock(docIn, encoded, encodedBuffer, freqLBuffer);
      } else {
        // Read vInts:
        if (DEBUG) {
          System.out.println("    fill last vInt doc block from fp=" + docIn.getFilePointer());
        }
        for(int i=0;i<left;i++) {
          final int code = docIn.readVInt();
          docDeltaBuffer[i] = code >>> 1;
          if ((code & 1) != 0) {
            freqBuffer[i] = 1;
          } else {
            freqBuffer[i] = docIn.readVInt();
          }
        }
      }
      docBufferUpto = 0;
    }
    
    private void refillPositions() throws IOException {
      if (DEBUG) {
        System.out.println("      refillPositions");
      }
      if (posIn.getFilePointer() == lastPosBlockFP) {
        if (DEBUG) {
          System.out.println("        vInt pos block @ fp=" + posIn.getFilePointer() + " hasPayloads=" + indexHasPayloads + " hasOffsets=" + indexHasOffsets);
        }
        final int count = posIn.readVInt();
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
            posIn.readVInt();
            posIn.readVInt();
          }
        }
      } else {
        if (DEBUG) {
          System.out.println("        bulk pos block @ fp=" + posIn.getFilePointer());
        }
        readBlock(posIn, encoded, encodedBuffer, posDeltaLBuffer);
      }
    }

    @Override
    public int nextDoc() throws IOException {

      if (DEBUG) {
        System.out.println("  FPR.nextDoc");
      }

      while (true) {
        if (DEBUG) {
          System.out.println("    docUpto=" + docUpto + " (of df=" + docFreq + ") docBufferUpto=" + docBufferUpto);
        }

        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }

        if (docBufferUpto == blockSize) {
          refillDocs();
        }

        if (DEBUG) {
          System.out.println("    accum=" + accum + " docDeltaBuffer[" + docBufferUpto + "]=" + docDeltaBuffer[docBufferUpto]);
        }
        accum += (int)docDeltaBuffer[docBufferUpto];
        freq = (int)freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (liveDocs == null || liveDocs.get(accum)) {
          doc = accum;
          if (DEBUG) {
            System.out.println("    return doc=" + doc + " freq=" + freq + " posPendingCount=" + posPendingCount);
          }
          position = 0;
          return doc;
        }

        if (DEBUG) {
          System.out.println("    doc=" + accum + " is deleted; try next doc");
        }
      }
    }
    
    @Override
    public int advance(int target) throws IOException {
      // nocommit make frq block load lazy/skippable
      if (DEBUG) {
        System.out.println("  FPR.advance target=" + target);
      }

      // nocommit 2 is heuristic guess!!
      // nocommit put cheating back!  does it help?
      // nocommit use skipper!!!  it has next last doc id!!
      //if (docFreq > blockSize && target - (blockSize - docBufferUpto) - 2*blockSize > accum) {
      if (docFreq > blockSize && target - accum > blockSize) {

        if (DEBUG) {
          System.out.println("    try skipper");
        }

        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          if (DEBUG) {
            System.out.println("    create skipper");
          }
          skipper = new BlockPackedSkipReader((IndexInput) docIn.clone(),
                                        BlockPackedPostingsWriter.maxSkipLevels,
                                        blockSize,
                                        true,
                                        indexHasOffsets,
                                        indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          if (DEBUG) {
            System.out.println("    init skipper");
          }
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, posTermStartFP, payTermStartFP, docFreq);
          skipped = true;
        }

        final int newDocUpto = skipper.skipTo(target); 

        if (newDocUpto > docUpto) {
          // Skipper moved

          if (DEBUG) {
            System.out.println("    skipper moved to docUpto=" + newDocUpto + " vs current=" + docUpto + "; docID=" + skipper.getDoc() + " fp=" + skipper.getDocPointer() + " pos.fp=" + skipper.getPosPointer() + " pos.bufferUpto=" + skipper.getPosBufferUpto());
          }

          assert newDocUpto % blockSize == (blockSize-1): "got " + newDocUpto;
          docUpto = newDocUpto+1;

          // Force block read next:
          docBufferUpto = blockSize;
          accum = skipper.getDoc();
          docIn.seek(skipper.getDocPointer());
          posPendingFP = skipper.getPosPointer();
          posPendingCount = skipper.getPosBufferUpto();
        }
      }

      // Now scan:
      while (nextDoc() != NO_MORE_DOCS) {
        if (doc >= target) {
          if (DEBUG) {
            System.out.println("  advance return doc=" + doc);
          }
          return doc;
        }
      }

      if (DEBUG) {
        System.out.println("  advance return doc=END");
      }

      return NO_MORE_DOCS;
    }

    // nocommit in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointe ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    private void skipPositions() throws IOException {
      // Skip positions now:
      int toSkip = posPendingCount - freq;
      if (DEBUG) {
        System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      }

      final int leftInBlock = blockSize - posBufferUpto;
      if (toSkip < leftInBlock) {
        posBufferUpto += toSkip;
        if (DEBUG) {
          System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        }
      } else {
        toSkip -= leftInBlock;
        while(toSkip >= blockSize) {
          if (DEBUG) {
            System.out.println("        skip whole block @ fp=" + posIn.getFilePointer());
          }
          assert posIn.getFilePointer() != lastPosBlockFP;
          skipBlock(posIn);
          toSkip -= blockSize;
        }
        refillPositions();
        posBufferUpto = toSkip;
        if (DEBUG) {
          System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        }
      }

      position = 0;
    }

    @Override
    public int nextPosition() throws IOException {
      if (DEBUG) {
        System.out.println("    FPR.nextPosition posPendingCount=" + posPendingCount + " posBufferUpto=" + posBufferUpto);
      }
      if (posPendingFP != -1) {
        if (DEBUG) {
          System.out.println("      seek to pendingFP=" + posPendingFP);
        }
        posIn.seek(posPendingFP);
        posPendingFP = -1;

        // Force buffer refill:
        posBufferUpto = blockSize;
      }

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == blockSize) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += (int)posDeltaBuffer[posBufferUpto++];
      posPendingCount--;
      if (DEBUG) {
        System.out.println("      return pos=" + position);
      }
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
    public boolean hasPayload() {
      return false;
    }

    @Override
    public BytesRef getPayload() {
      return null;
    }
  }

  // Also handles payloads + offsets
  final class EverythingEnum extends DocsAndPositionsEnum {
    
    private final byte[] encoded;
    private final LongBuffer encodedBuffer;

    private final long[] docDeltaBuffer = new long[blockSize];
    private final long[] freqBuffer = new long[blockSize];
    private final long[] posDeltaBuffer = new long[blockSize];

    private final long[] payloadLengthBuffer;
    private final long[] offsetStartDeltaBuffer;
    private final long[] offsetLengthBuffer;


    private final LongBuffer docDeltaLBuffer = LongBuffer.wrap(docDeltaBuffer);
    private final LongBuffer freqLBuffer = LongBuffer.wrap(freqBuffer);
    private final LongBuffer posDeltaLBuffer = LongBuffer.wrap(posDeltaBuffer);

    private final LongBuffer payloadLengthLBuffer;
    private final LongBuffer offsetStartDeltaLBuffer;
    private final LongBuffer offsetLengthLBuffer;

    private byte[] payloadBytes;
    private int payloadByteUpto;
    private int payloadLength;

    private int lastEndOffset;
    private int startOffset;
    private int endOffset;

    private int docBufferUpto;
    private int posBufferUpto;

    private BlockPackedSkipReader skipper;
    private boolean skipped;

    final IndexInput startDocIn;

    final IndexInput docIn;
    final IndexInput posIn;
    final IndexInput payIn;
    final BytesRef payload;

    final boolean indexHasOffsets;
    final boolean indexHasPayloads;

    private int docFreq;                              // number of docs in this posting list
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
    private int skipOffset;

    private Bits liveDocs;
    
    public EverythingEnum(FieldInfo fieldInfo) throws IOException {
      this.startDocIn = BlockPackedPostingsReader.this.docIn;
      this.docIn = (IndexInput) startDocIn.clone();
      this.posIn = (IndexInput) BlockPackedPostingsReader.this.posIn.clone();
      this.payIn = (IndexInput) BlockPackedPostingsReader.this.payIn.clone();
      encoded = new byte[blockSize*4];
      encodedBuffer = ByteBuffer.wrap(encoded).asLongBuffer();
      indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      if (indexHasOffsets) {
        offsetStartDeltaBuffer = new long[blockSize];
        offsetLengthBuffer = new long[blockSize];
        offsetStartDeltaLBuffer = LongBuffer.wrap(offsetStartDeltaBuffer); 
        offsetLengthLBuffer = LongBuffer.wrap(offsetLengthBuffer); 
      } else {
        offsetStartDeltaBuffer = null;
        offsetStartDeltaLBuffer = null;
        offsetLengthBuffer = null;
        offsetLengthLBuffer = null;
        startOffset = -1;
        endOffset = -1;
      }

      indexHasPayloads = fieldInfo.hasPayloads();
      if (indexHasPayloads) {
        payloadLengthBuffer = new long[blockSize];
        payloadLengthLBuffer = LongBuffer.wrap(payloadLengthBuffer); 
        payloadBytes = new byte[128];
        payload = new BytesRef();
      } else {
        payloadLengthBuffer = null;
        payloadLengthLBuffer = null;
        payloadBytes = null;
        payload = null;
      }
    }

    public boolean canReuse(IndexInput docIn, FieldInfo fieldInfo) {
      return docIn == startDocIn &&
        indexHasOffsets == (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) &&
        indexHasPayloads == fieldInfo.hasPayloads();
    }
    
    public EverythingEnum reset(Bits liveDocs, IntBlockTermState termState) throws IOException {
      this.liveDocs = liveDocs;
      if (DEBUG) {
        System.out.println("  FPR.reset: termState=" + termState);
      }
      docFreq = termState.docFreq;
      docTermStartFP = termState.docStartFP;
      posTermStartFP = termState.posStartFP;
      payTermStartFP = termState.payStartFP;
      docIn.seek(docTermStartFP);
      skipOffset = termState.skipOffset;
      posPendingFP = posTermStartFP;
      payPendingFP = payTermStartFP;
      posPendingCount = 0;
      if (termState.totalTermFreq < blockSize) {
        lastPosBlockFP = posTermStartFP;
      } else if (termState.totalTermFreq == blockSize) {
        lastPosBlockFP = -1;
      } else {
        lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
      }

      doc = -1;
      accum = 0;
      docUpto = 0;
      docBufferUpto = blockSize;
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

      if (left >= blockSize) {
        if (DEBUG) {
          System.out.println("    fill doc block from fp=" + docIn.getFilePointer());
        }

        readBlock(docIn, encoded, encodedBuffer, docDeltaLBuffer);

        if (DEBUG) {
          System.out.println("    fill freq block from fp=" + docIn.getFilePointer());
        }

        readBlock(docIn, encoded, encodedBuffer, freqLBuffer);
      } else {
        // Read vInts:
        if (DEBUG) {
          System.out.println("    fill last vInt doc block from fp=" + docIn.getFilePointer());
        }
        for(int i=0;i<left;i++) {
          final int code = docIn.readVInt();
          docDeltaBuffer[i] = code >>> 1;
          if ((code & 1) != 0) {
            freqBuffer[i] = 1;
          } else {
            freqBuffer[i] = docIn.readVInt();
          }
        }
      }
      docBufferUpto = 0;
    }
    
    private void refillPositions() throws IOException {
      if (DEBUG) {
        System.out.println("      refillPositions");
      }
      if (posIn.getFilePointer() == lastPosBlockFP) {
        if (DEBUG) {
          System.out.println("        vInt pos block @ fp=" + posIn.getFilePointer() + " hasPayloads=" + indexHasPayloads + " hasOffsets=" + indexHasOffsets);
        }
        final int count = posIn.readVInt();
        int payloadLength = 0;
        payloadByteUpto = 0;
        for(int i=0;i<count;i++) {
          int code = posIn.readVInt();
          if (indexHasPayloads) {
            if ((code & 1) != 0) {
              payloadLength = posIn.readVInt();
            }
            if (DEBUG) {
              System.out.println("        i=" + i + " payloadLen=" + payloadLength);
            }
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
            if (DEBUG) {
              System.out.println("        i=" + i + " read offsets from posIn.fp=" + posIn.getFilePointer());
            }
            offsetStartDeltaBuffer[i] = posIn.readVInt();
            offsetLengthBuffer[i] = posIn.readVInt();
            if (DEBUG) {
              System.out.println("          startOffDelta=" + offsetStartDeltaBuffer[i] + " offsetLen=" + offsetLengthBuffer[i]);
            }
          }
        }
        payloadByteUpto = 0;
      } else {
        if (DEBUG) {
          System.out.println("        bulk pos block @ fp=" + posIn.getFilePointer());
        }
        readBlock(posIn, encoded, encodedBuffer, posDeltaLBuffer);

        if (indexHasPayloads) {
          if (DEBUG) {
            System.out.println("        bulk payload block @ pay.fp=" + payIn.getFilePointer());
          }
          readBlock(payIn, encoded, encodedBuffer, payloadLengthLBuffer);
          int numBytes = payIn.readVInt();
          if (DEBUG) {
            System.out.println("        " + numBytes + " payload bytes @ pay.fp=" + payIn.getFilePointer());
          }
          if (numBytes > payloadBytes.length) {
            payloadBytes = ArrayUtil.grow(payloadBytes, numBytes);
          }
          payIn.readBytes(payloadBytes, 0, numBytes);
          payloadByteUpto = 0;
        }

        if (indexHasOffsets) {
          if (DEBUG) {
            System.out.println("        bulk offset block @ pay.fp=" + payIn.getFilePointer());
          }
          readBlock(payIn, encoded, encodedBuffer, offsetStartDeltaLBuffer);
          readBlock(payIn, encoded, encodedBuffer, offsetLengthLBuffer);
        }
      }
    }

    @Override
    public int nextDoc() throws IOException {

      if (DEBUG) {
        System.out.println("  FPR.nextDoc");
      }

      if (indexHasPayloads) {
        payloadByteUpto += payloadLength;
        payloadLength = 0;
      }

      while (true) {
        if (DEBUG) {
          System.out.println("    docUpto=" + docUpto + " (of df=" + docFreq + ") docBufferUpto=" + docBufferUpto);
        }

        if (docUpto == docFreq) {
          return doc = NO_MORE_DOCS;
        }

        if (docBufferUpto == blockSize) {
          refillDocs();
        }

        if (DEBUG) {
          System.out.println("    accum=" + accum + " docDeltaBuffer[" + docBufferUpto + "]=" + docDeltaBuffer[docBufferUpto]);
        }
        accum += (int)docDeltaBuffer[docBufferUpto];
        freq = (int)freqBuffer[docBufferUpto];
        posPendingCount += freq;
        docBufferUpto++;
        docUpto++;

        if (liveDocs == null || liveDocs.get(accum)) {
          doc = accum;
          if (DEBUG) {
            System.out.println("    return doc=" + doc + " freq=" + freq + " posPendingCount=" + posPendingCount);
          }
          position = 0;
          payloadLength = 0;
          lastEndOffset = 0;
          return doc;
        }

        if (DEBUG) {
          System.out.println("    doc=" + accum + " is deleted; try next doc");
        }
      }
    }
    
    @Override
    public int advance(int target) throws IOException {
      // nocommit make frq block load lazy/skippable
      if (DEBUG) {
        System.out.println("  FPR.advance target=" + target);
      }

      // nocommit 2 is heuristic guess!!
      // nocommit put cheating back!  does it help?
      // nocommit use skipper!!!  it has next last doc id!!
      //if (docFreq > blockSize && target - (blockSize - docBufferUpto) - 2*blockSize > accum) {
      if (docFreq > blockSize && target - accum > blockSize) {

        if (DEBUG) {
          System.out.println("    try skipper");
        }

        if (skipper == null) {
          // Lazy init: first time this enum has ever been used for skipping
          if (DEBUG) {
            System.out.println("    create skipper");
          }
          skipper = new BlockPackedSkipReader((IndexInput) docIn.clone(),
                                        BlockPackedPostingsWriter.maxSkipLevels,
                                        blockSize,
                                        true,
                                        indexHasOffsets,
                                        indexHasPayloads);
        }

        if (!skipped) {
          assert skipOffset != -1;
          // This is the first time this enum has skipped
          // since reset() was called; load the skip data:
          if (DEBUG) {
            System.out.println("    init skipper");
          }
          skipper.init(docTermStartFP+skipOffset, docTermStartFP, posTermStartFP, payTermStartFP, docFreq);
          skipped = true;
        }

        final int newDocUpto = skipper.skipTo(target); 

        if (newDocUpto > docUpto) {
          // Skipper moved

          if (DEBUG) {
            System.out.println("    skipper moved to docUpto=" + newDocUpto + " vs current=" + docUpto + "; docID=" + skipper.getDoc() + " fp=" + skipper.getDocPointer() + " pos.fp=" + skipper.getPosPointer() + " pos.bufferUpto=" + skipper.getPosBufferUpto() + " pay.fp=" + skipper.getPayPointer() + " lastEndOffset=" + lastEndOffset);
          }

          assert newDocUpto % blockSize == (blockSize-1): "got " + newDocUpto;
          docUpto = newDocUpto+1;

          // Force block read next:
          docBufferUpto = blockSize;
          accum = skipper.getDoc();
          docIn.seek(skipper.getDocPointer());
          posPendingFP = skipper.getPosPointer();
          payPendingFP = skipper.getPayPointer();
          posPendingCount = skipper.getPosBufferUpto();
          lastEndOffset = skipper.getEndOffset();
          payloadByteUpto = skipper.getPayloadByteUpto();
        }
      }

      // Now scan:
      while (nextDoc() != NO_MORE_DOCS) {
        if (doc >= target) {
          if (DEBUG) {
            System.out.println("  advance return doc=" + doc);
          }
          return doc;
        }
      }

      if (DEBUG) {
        System.out.println("  advance return doc=END");
      }

      return NO_MORE_DOCS;
    }

    // nocommit in theory we could avoid loading frq block
    // when not needed, ie, use skip data to load how far to
    // seek the pos pointe ... instead of having to load frq
    // blocks only to sum up how many positions to skip
    private void skipPositions() throws IOException {
      // Skip positions now:
      int toSkip = posPendingCount - freq;
      if (DEBUG) {
        System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
      }

      final int leftInBlock = blockSize - posBufferUpto;
      if (toSkip < leftInBlock) {
        int end = posBufferUpto + toSkip;
        while(posBufferUpto < end) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          if (indexHasOffsets) {
            lastEndOffset += offsetStartDeltaBuffer[posBufferUpto] + offsetLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
        if (DEBUG) {
          System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        }
      } else {
        toSkip -= leftInBlock;
        while(toSkip >= blockSize) {
          if (DEBUG) {
            System.out.println("        skip whole block @ fp=" + posIn.getFilePointer());
          }
          assert posIn.getFilePointer() != lastPosBlockFP;
          skipBlock(posIn);

          if (indexHasPayloads) {
            // Skip payloadLength block:
            skipBlock(payIn);

            // Skip payloadBytes block:
            int numBytes = payIn.readVInt();
            payIn.seek(payIn.getFilePointer() + numBytes);
          }

          if (indexHasOffsets) {
            // Must load offset blocks merely to sum
            // up into lastEndOffset:
            readBlock(payIn, encoded, encodedBuffer, offsetStartDeltaLBuffer);
            readBlock(payIn, encoded, encodedBuffer, offsetLengthLBuffer);
            for(int i=0;i<blockSize;i++) {
              lastEndOffset += offsetStartDeltaBuffer[i] + offsetLengthBuffer[i];
            }
          }
          toSkip -= blockSize;
        }
        refillPositions();
        payloadByteUpto = 0;
        posBufferUpto = 0;
        while(posBufferUpto < toSkip) {
          if (indexHasPayloads) {
            payloadByteUpto += payloadLengthBuffer[posBufferUpto];
          }
          if (indexHasOffsets) {
            lastEndOffset += offsetStartDeltaBuffer[posBufferUpto] + offsetLengthBuffer[posBufferUpto];
          }
          posBufferUpto++;
        }
        if (DEBUG) {
          System.out.println("        skip w/in block to posBufferUpto=" + posBufferUpto);
        }
      }

      position = 0;
      payloadLength = 0;
      lastEndOffset = 0;
    }

    @Override
    public int nextPosition() throws IOException {
      if (DEBUG) {
        System.out.println("    FPR.nextPosition posPendingCount=" + posPendingCount + " posBufferUpto=" + posBufferUpto + " payloadByteUpto=" + payloadByteUpto);
      }
      if (posPendingFP != -1) {
        if (DEBUG) {
          System.out.println("      seek pos to pendingFP=" + posPendingFP);
        }
        posIn.seek(posPendingFP);
        posPendingFP = -1;

        if (payPendingFP != -1) {
          if (DEBUG) {
            System.out.println("      seek pay to pendingFP=" + payPendingFP);
          }
          payIn.seek(payPendingFP);
          payPendingFP = -1;
        }

        // Force buffer refill:
        posBufferUpto = blockSize;
      }

      if (indexHasPayloads) {
        if (DEBUG) {
          if (payloadLength != 0) {
            System.out.println("      skip unread payload length=" + payloadLength);
          }
        }
        payloadByteUpto += payloadLength;
        payloadLength = 0;
      }

      if (posPendingCount > freq) {
        skipPositions();
        posPendingCount = freq;
      }

      if (posBufferUpto == blockSize) {
        refillPositions();
        posBufferUpto = 0;
      }
      position += (int)posDeltaBuffer[posBufferUpto];

      if (indexHasPayloads) {
        payloadLength = (int)payloadLengthBuffer[posBufferUpto];
      }

      if (indexHasOffsets) {
        startOffset = lastEndOffset + (int)offsetStartDeltaBuffer[posBufferUpto];
        endOffset = startOffset + (int)offsetLengthBuffer[posBufferUpto];
        lastEndOffset = endOffset;
      }

      posBufferUpto++;
      posPendingCount--;
      if (DEBUG) {
        System.out.println("      return pos=" + position);
      }
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
    public boolean hasPayload() {
      return payloadLength != 0;
    }

    @Override
    public BytesRef getPayload() {
      if (DEBUG) {
        System.out.println("    FPR.getPayload payloadLength=" + payloadLength + " payloadByteUpto=" + payloadByteUpto);
      }
      payload.bytes = payloadBytes;
      payload.offset = payloadByteUpto;
      payload.length = payloadLength;
      payloadByteUpto += payloadLength;
      payloadLength = 0;
      return payload;
    }
  }
}

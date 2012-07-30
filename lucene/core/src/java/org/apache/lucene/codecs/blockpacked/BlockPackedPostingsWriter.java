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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

// nocommit javadocs

public final class BlockPackedPostingsWriter extends PostingsWriterBase {

  private boolean DEBUG = BlockPackedPostingsReader.DEBUG;

  // nocommit move these constants to the PF:

  static final int maxSkipLevels = 10;

  final static String TERMS_CODEC = "BlockPackedPostingsWriterTerms";
  final static String DOC_CODEC = "BlockPackedPostingsWriterDoc";
  final static String POS_CODEC = "BlockPackedPostingsWriterPos";
  final static String PAY_CODEC = "BlockPackedPostingsWriterPay";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final IndexOutput docOut;
  final IndexOutput posOut;
  final IndexOutput payOut;

  static final int DEFAULT_BLOCK_SIZE = 128;

  final int blockSize;

  private IndexOutput termsOut;

  // How current field indexes postings:
  private boolean fieldHasFreqs;
  private boolean fieldHasPositions;
  private boolean fieldHasOffsets;
  private boolean fieldHasPayloads;

  // Holds starting file pointers for each term:
  private long docTermStartFP;
  private long posTermStartFP;
  private long payTermStartFP;

  final long[] docDeltaBuffer;
  final long[] freqBuffer;
  final LongBuffer docDeltaLBuffer;
  final LongBuffer freqLBuffer;
  private int docBufferUpto;

  final long[] posDeltaBuffer;
  final long[] payloadLengthBuffer;
  final long[] offsetStartDeltaBuffer;
  final long[] offsetLengthBuffer;
  final LongBuffer posDeltaLBuffer;
  final LongBuffer payloadLengthLBuffer;
  final LongBuffer offsetStartDeltaLBuffer;
  final LongBuffer offsetLengthLBuffer;
  private int posBufferUpto;

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int lastBlockDocID;
  private boolean saveNextPosBlock;
  private long lastBlockPosFP;
  private long lastBlockPayFP;
  private int lastBlockPosBufferUpto;
  private int lastBlockEndOffset;
  private int lastBlockPayloadByteUpto;
  private int lastDocID;
  private int lastPosition;
  private int lastEndOffset;
  private int docCount;

  final byte[] encoded;
  final LongBuffer encodedBuffer;

  private final BlockPackedSkipWriter skipWriter;
  
  public BlockPackedPostingsWriter(SegmentWriteState state, int blockSize) throws IOException {
    super();
    this.blockSize = blockSize;

    docOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockPackedPostingsFormat.DOC_EXTENSION),
                                          state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeHeader(docOut, DOC_CODEC, VERSION_CURRENT);
      if (state.fieldInfos.hasProx()) {
        posDeltaBuffer = new long[blockSize];
        posDeltaLBuffer = LongBuffer.wrap(posDeltaBuffer);
        posOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockPackedPostingsFormat.POS_EXTENSION),
                                              state.context);
        CodecUtil.writeHeader(posOut, POS_CODEC, VERSION_CURRENT);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new long[blockSize];
          payloadLengthLBuffer = LongBuffer.wrap(payloadLengthBuffer);
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
          payloadLengthLBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new long[blockSize];
          offsetLengthBuffer = new long[blockSize];
          offsetStartDeltaLBuffer = LongBuffer.wrap(offsetStartDeltaBuffer);
          offsetLengthLBuffer = LongBuffer.wrap(offsetLengthBuffer);
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
          offsetStartDeltaLBuffer = null;
          offsetLengthLBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          payOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockPackedPostingsFormat.PAY_EXTENSION),
                                                state.context);
          CodecUtil.writeHeader(payOut, PAY_CODEC, VERSION_CURRENT);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
        posDeltaLBuffer = null;
        payloadLengthLBuffer = null;
        offsetStartDeltaLBuffer = null;
        offsetLengthLBuffer = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new long[blockSize];
    freqBuffer = new long[blockSize];
    docDeltaLBuffer = LongBuffer.wrap(docDeltaBuffer);
    freqLBuffer = LongBuffer.wrap(freqBuffer);

    skipWriter = new BlockPackedSkipWriter(blockSize,
                                     maxSkipLevels, 
                                     state.segmentInfo.getDocCount(),
                                     docOut,
                                     posOut,
                                     payOut);

    encoded = new byte[blockSize*4];
    encodedBuffer = ByteBuffer.wrap(encoded).asLongBuffer();
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, TERMS_CODEC, VERSION_CURRENT);
    termsOut.writeVInt(blockSize);
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    IndexOptions indexOptions = fieldInfo.getIndexOptions();
    fieldHasFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    fieldHasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    fieldHasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    fieldHasPayloads = fieldInfo.hasPayloads();
    skipWriter.setField(fieldHasPositions, fieldHasOffsets, fieldHasPayloads);
  }

  @Override
  public void startTerm() {
    docTermStartFP = docOut.getFilePointer();
    if (fieldHasPositions) {
      posTermStartFP = posOut.getFilePointer();
      if (fieldHasPayloads || fieldHasOffsets) {
        payTermStartFP = payOut.getFilePointer();
      }
    }
    lastBlockDocID = -1;
    lastDocID = 0;
    if (DEBUG) {
      System.out.println("FPW.startTerm startFP=" + docTermStartFP);
    }
    skipWriter.resetSkip();
  }

  private void writeBlock(LongBuffer buffer, IndexOutput out) throws IOException {
    final int header = ForUtil.compress(buffer, encodedBuffer);
    out.writeVInt(header);
    out.writeBytes(encoded, ForUtil.getEncodedSize(header));
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    if (DEBUG) {
      System.out.println("FPW.startDoc docID=" + docID);
    }

    // nocommit do this in finishDoc... but does it fail...?
    // is it not always called...?
    if (posOut != null && saveNextPosBlock) {
      lastBlockPosFP = posOut.getFilePointer();
      if (payOut != null) {
        lastBlockPayFP = payOut.getFilePointer();
      }
      lastBlockPosBufferUpto = posBufferUpto;
      lastBlockEndOffset = lastEndOffset;
      lastBlockPayloadByteUpto = payloadByteUpto;
      saveNextPosBlock = false;
      if (DEBUG) {
        System.out.println("  now save lastBlockPosFP=" + lastBlockPosFP + " lastBlockPosBufferUpto=" + lastBlockPosBufferUpto + " lastBlockPayloadByteUpto=" + lastBlockPayloadByteUpto);
      }
    }

    final int docDelta = docID - lastDocID;
    if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " ) (docOut: " + docOut + ")");
    }
    lastDocID = docID;

    docDeltaBuffer[docBufferUpto] = docDelta;
    if (DEBUG) {
      System.out.println("  docDeltaBuffer[" + docBufferUpto + "]=" + docDelta);
    }
    if (fieldHasFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }

    docBufferUpto++;
    docCount++;

    if (docBufferUpto == blockSize) {
      // nocommit maybe instead of buffering skip before
      // writing a block based on last block's end data
      // ... we could buffer after writing the block?  only
      // iffiness with that approach is it could be a
      // pointlness skip?  like we may stop adding docs
      // right after that, then we have skip point AFTER
      // last doc.  the thing is, in finishTerm we are
      // already sometimes adding a skip point AFTER the
      // last doc?
      if (lastBlockDocID != -1) {
        if (DEBUG) {
          System.out.println("  bufferSkip at writeBlock: lastDocID=" + lastBlockDocID + " docCount=" + (docCount-blockSize));
        }
        skipWriter.bufferSkip(lastBlockDocID, docCount-blockSize, lastBlockPosFP, lastBlockPayFP, lastBlockPosBufferUpto, lastBlockEndOffset, lastBlockPayloadByteUpto);
      }
      lastBlockDocID = docID;
      saveNextPosBlock = true;

      if (DEBUG) {
        System.out.println("  write docDelta block @ fp=" + docOut.getFilePointer());
      }
      writeBlock(docDeltaLBuffer, docOut);
      if (fieldHasFreqs) {
        if (DEBUG) {
          System.out.println("  write freq block @ fp=" + docOut.getFilePointer());
        }
        writeBlock(freqLBuffer, docOut);
      }
      docBufferUpto = 0;
    }

    lastPosition = 0;
    lastEndOffset = 0;
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    if (DEBUG) {
      System.out.println("FPW.addPosition pos=" + position + " posBufferUpto=" + posBufferUpto + (fieldHasPayloads ? " payloadByteUpto=" + payloadByteUpto: ""));
    }
    posDeltaBuffer[posBufferUpto] = position - lastPosition;
    if (fieldHasPayloads) {
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        System.arraycopy(payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    if (fieldHasOffsets) {
      assert startOffset >= lastEndOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastEndOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastEndOffset = endOffset;
    }
    
    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == blockSize) {
      if (DEBUG) {
        System.out.println("  write pos bulk block @ fp=" + posOut.getFilePointer());
      }
      writeBlock(posDeltaLBuffer, posOut);

      if (fieldHasPayloads) {
        writeBlock(payloadLengthLBuffer, payOut);
        payOut.writeVInt(payloadByteUpto);
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (fieldHasOffsets) {
        writeBlock(offsetStartDeltaLBuffer, payOut);
        writeBlock(offsetLengthLBuffer, payOut);
      }
      posBufferUpto = 0;
    }
  }

  @Override
  public void finishDoc() {
  }

  private static class PendingTerm {
    public final long docStartFP;
    public final long posStartFP;
    public final long payStartFP;
    public final int skipOffset;
    public final int lastPosBlockOffset;

    public PendingTerm(long docStartFP, long posStartFP, long payStartFP, int skipOffset, int lastPosBlockOffset) {
      this.docStartFP = docStartFP;
      this.posStartFP = posStartFP;
      this.payStartFP = payStartFP;
      this.skipOffset = skipOffset;
      this.lastPosBlockOffset = lastPosBlockOffset;
    }
  }

  private final List<PendingTerm> pendingTerms = new ArrayList<PendingTerm>();

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(TermStats stats) throws IOException {

    assert stats.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert stats.docFreq == docCount: stats.docFreq + " vs " + docCount;

    if (DEBUG) {
      System.out.println("FPW.finishTerm docFreq=" + stats.docFreq);
    }

    // nocommit silly that skipper must write skip when we no
    // postings come after it, but if we don't do this, skip
    // reader incorrectly thinks it can read another level 0
    // skip entry here!:
    //if (docCount > blockSize && docBufferUpto > 0) {
    if (docCount > blockSize) {
      final int lastDocCount = blockSize*(docCount/blockSize);
      if (DEBUG) {
        System.out.println("  bufferSkip at finishTerm: lastDocID=" + lastBlockDocID + " docCount=" + lastDocCount);
      }
      skipWriter.bufferSkip(lastBlockDocID, lastDocCount, lastBlockPosFP, lastBlockPayFP, lastBlockPosBufferUpto, lastBlockEndOffset, lastBlockPayloadByteUpto);
    }

    if (DEBUG) {
      if (docBufferUpto > 0) {
        System.out.println("  write doc/freq vInt block (count=" + docBufferUpto + ") at fp=" + docOut.getFilePointer() + " docTermStartFP=" + docTermStartFP);
      }
    }

    // vInt encode the remaining doc deltas and freqs:
    for(int i=0;i<docBufferUpto;i++) {
      final int docDelta = (int)docDeltaBuffer[i];
      final int freq = (int)freqBuffer[i];
      if (!fieldHasFreqs) {
        docOut.writeVInt(docDelta);
      } else if (freqBuffer[i] == 1) {
        docOut.writeVInt((docDelta<<1)|1);
      } else {
        docOut.writeVInt(docDelta<<1);
        docOut.writeVInt(freq);
      }
    }

    final int lastPosBlockOffset;

    if (fieldHasPositions) {
      if (DEBUG) {
        if (posBufferUpto > 0) {
          System.out.println("  write pos vInt block (count=" + posBufferUpto + ") at fp=" + posOut.getFilePointer() + " posTermStartFP=" + posTermStartFP + " hasPayloads=" + fieldHasPayloads + " hasOffsets=" + fieldHasOffsets);
        }
      }

      assert stats.totalTermFreq != -1;
      if (stats.totalTermFreq > blockSize) {
        lastPosBlockOffset = (int) (posOut.getFilePointer() - posTermStartFP);
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {
        posOut.writeVInt(posBufferUpto);
        
        // nocommit should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< blockSize) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1;
        int payloadBytesReadUpto = 0;
        for(int i=0;i<posBufferUpto;i++) {
          final int posDelta = (int)posDeltaBuffer[i];
          if (fieldHasPayloads) {
            final int payloadLength = (int)payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta<<1)|1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta<<1);
            }

            if (DEBUG) {
              System.out.println("        i=" + i + " payloadLen=" + payloadLength);
            }

            if (payloadLength != 0) {
              if (DEBUG) {
                System.out.println("          write payload @ pos.fp=" + posOut.getFilePointer());
              }
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }

          if (fieldHasOffsets) {
            if (DEBUG) {
              System.out.println("          write offset @ pos.fp=" + posOut.getFilePointer());
            }
            posOut.writeVInt((int)offsetStartDeltaBuffer[i]);
            posOut.writeVInt((int)offsetLengthBuffer[i]);
          }
        }

        if (fieldHasPayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
      if (DEBUG) {
        System.out.println("  totalTermFreq=" + stats.totalTermFreq + " lastPosBlockOffset=" + lastPosBlockOffset);
      }
    } else {
      lastPosBlockOffset = -1;
    }

    int skipOffset;
    if (docCount > blockSize) {
      skipOffset = (int) (skipWriter.writeSkip(docOut)-docTermStartFP);
      
      if (DEBUG) {
        System.out.println("skip packet " + (docOut.getFilePointer() - (docTermStartFP + skipOffset)) + " bytes");
      }
    } else {
      skipOffset = -1;
      if (DEBUG) {
        System.out.println("  no skip: docCount=" + docCount);
      }
    }

    long payStartFP;
    if (stats.totalTermFreq >= blockSize) {
      payStartFP = payTermStartFP;
    } else {
      payStartFP = -1;
    }

    if (DEBUG) {
      System.out.println("  payStartFP=" + payStartFP);
    }

    pendingTerms.add(new PendingTerm(docTermStartFP, posTermStartFP, payStartFP, skipOffset, lastPosBlockOffset));
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = 0;
    docCount = 0;
  }

  private final RAMOutputStream bytesWriter = new RAMOutputStream();

  @Override
  public void flushTermsBlock(int start, int count) throws IOException {

    if (count == 0) {
      termsOut.writeByte((byte) 0);
      return;
    }

    assert start <= pendingTerms.size();
    assert count <= start;

    final int limit = pendingTerms.size() - start + count;

    long lastDocStartFP = 0;
    long lastPosStartFP = 0;
    long lastPayStartFP = 0;
    for(int idx=limit-count; idx<limit; idx++) {
      PendingTerm term = pendingTerms.get(idx);

      bytesWriter.writeVLong(term.docStartFP - lastDocStartFP);
      lastDocStartFP = term.docStartFP;

      if (fieldHasPositions) {
        bytesWriter.writeVLong(term.posStartFP - lastPosStartFP);
        lastPosStartFP = term.posStartFP;
        if (term.lastPosBlockOffset != -1) {
          bytesWriter.writeVInt(term.lastPosBlockOffset);
        }
        if ((fieldHasPayloads || fieldHasOffsets) && term.payStartFP != -1) {
          bytesWriter.writeVLong(term.payStartFP - lastPayStartFP);
          lastPayStartFP = term.payStartFP;
        }
      }

      if (term.skipOffset != -1) {
        bytesWriter.writeVInt(term.skipOffset);
      }
    }

    termsOut.writeVInt((int) bytesWriter.getFilePointer());
    bytesWriter.writeTo(termsOut);
    bytesWriter.reset();

    // Remove the terms we just wrote:
    pendingTerms.subList(limit-count, limit).clear();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docOut, posOut, payOut);
  }
}

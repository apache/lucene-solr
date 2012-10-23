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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;


/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list
 * with postings format.
 *
 * Postings list for each term will be stored separately. 
 *
 * @see Lucene41SkipWriter for details about skipping setting and postings layout.
 * @lucene.experimental
 */
public final class Lucene41PostingsWriter extends PostingsWriterBase {

  /** 
   * Expert: The maximum number of skip levels. Smaller values result in 
   * slightly smaller indexes, but slower skipping in big posting lists.
   */
  static final int maxSkipLevels = 10;

  final static String TERMS_CODEC = "Lucene41PostingsWriterTerms";
  final static String DOC_CODEC = "Lucene41PostingsWriterDoc";
  final static String POS_CODEC = "Lucene41PostingsWriterPos";
  final static String PAY_CODEC = "Lucene41PostingsWriterPay";

  // Increment version to change it
  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;

  final IndexOutput docOut;
  final IndexOutput posOut;
  final IndexOutput payOut;

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

  final int[] docDeltaBuffer;
  final int[] freqBuffer;
  private int docBufferUpto;

  final int[] posDeltaBuffer;
  final int[] payloadLengthBuffer;
  final int[] offsetStartDeltaBuffer;
  final int[] offsetLengthBuffer;
  private int posBufferUpto;

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int lastBlockDocID;
  private long lastBlockPosFP;
  private long lastBlockPayFP;
  private int lastBlockPosBufferUpto;
  private int lastBlockPayloadByteUpto;

  private int lastDocID;
  private int lastPosition;
  private int lastStartOffset;
  private int docCount;

  final byte[] encoded;

  private final ForUtil forUtil;
  private final Lucene41SkipWriter skipWriter;
  
  /** Creates a postings writer with the specified PackedInts overhead ratio */
  // TODO: does this ctor even make sense?
  public Lucene41PostingsWriter(SegmentWriteState state, float acceptableOverheadRatio) throws IOException {
    super();

    docOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene41PostingsFormat.DOC_EXTENSION),
                                          state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeHeader(docOut, DOC_CODEC, VERSION_CURRENT);
      forUtil = new ForUtil(acceptableOverheadRatio, docOut);
      if (state.fieldInfos.hasProx()) {
        posDeltaBuffer = new int[MAX_DATA_SIZE];
        posOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene41PostingsFormat.POS_EXTENSION),
                                              state.context);
        CodecUtil.writeHeader(posOut, POS_CODEC, VERSION_CURRENT);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new int[MAX_DATA_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new int[MAX_DATA_SIZE];
          offsetLengthBuffer = new int[MAX_DATA_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          payOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene41PostingsFormat.PAY_EXTENSION),
                                                state.context);
          CodecUtil.writeHeader(payOut, PAY_CODEC, VERSION_CURRENT);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new int[MAX_DATA_SIZE];
    freqBuffer = new int[MAX_DATA_SIZE];

    // TODO: should we try skipping every 2/4 blocks...?
    skipWriter = new Lucene41SkipWriter(maxSkipLevels,
                                     BLOCK_SIZE, 
                                     state.segmentInfo.getDocCount(),
                                     docOut,
                                     posOut,
                                     payOut);

    encoded = new byte[MAX_ENCODED_SIZE];
  }

  /** Creates a postings writer with <code>PackedInts.COMPACT</code> */
  public Lucene41PostingsWriter(SegmentWriteState state) throws IOException {
    this(state, PackedInts.COMPACT);
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, TERMS_CODEC, VERSION_CURRENT);
    termsOut.writeVInt(BLOCK_SIZE);
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
    lastDocID = 0;
    lastBlockDocID = -1;
    // if (DEBUG) {
    //   System.out.println("FPW.startTerm startFP=" + docTermStartFP);
    // }
    skipWriter.resetSkip();
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // if (DEBUG) {
    //   System.out.println("FPW.startDoc docID["+docBufferUpto+"]=" + docID);
    // }
    // Have collected a block of docs, and get a new doc. 
    // Should write skip data as well as postings list for
    // current block.
    if (lastBlockDocID != -1 && docBufferUpto == 0) {
      // if (DEBUG) {
      //   System.out.println("  bufferSkip at writeBlock: lastDocID=" + lastBlockDocID + " docCount=" + (docCount-1));
      // }
      skipWriter.bufferSkip(lastBlockDocID, docCount, lastBlockPosFP, lastBlockPayFP, lastBlockPosBufferUpto, lastBlockPayloadByteUpto);
    }

    final int docDelta = docID - lastDocID;

    if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " ) (docOut: " + docOut + ")");
    }

    docDeltaBuffer[docBufferUpto] = docDelta;
    // if (DEBUG) {
    //   System.out.println("  docDeltaBuffer[" + docBufferUpto + "]=" + docDelta);
    // }
    if (fieldHasFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }
    docBufferUpto++;
    docCount++;

    if (docBufferUpto == BLOCK_SIZE) {
      // if (DEBUG) {
      //   System.out.println("  write docDelta block @ fp=" + docOut.getFilePointer());
      // }
      forUtil.writeBlock(docDeltaBuffer, encoded, docOut);
      if (fieldHasFreqs) {
        // if (DEBUG) {
        //   System.out.println("  write freq block @ fp=" + docOut.getFilePointer());
        // }
        forUtil.writeBlock(freqBuffer, encoded, docOut);
      }
      // NOTE: don't set docBufferUpto back to 0 here;
      // finishDoc will do so (because it needs to see that
      // the block was filled so it can save skip data)
    }


    lastDocID = docID;
    lastPosition = 0;
    lastStartOffset = 0;
  }

  /** Add a new position & payload */
  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    // if (DEBUG) {
    //   System.out.println("FPW.addPosition pos=" + position + " posBufferUpto=" + posBufferUpto + (fieldHasPayloads ? " payloadByteUpto=" + payloadByteUpto: ""));
    // }
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
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }
    
    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {
      // if (DEBUG) {
      //   System.out.println("  write pos bulk block @ fp=" + posOut.getFilePointer());
      // }
      forUtil.writeBlock(posDeltaBuffer, encoded, posOut);

      if (fieldHasPayloads) {
        forUtil.writeBlock(payloadLengthBuffer, encoded, payOut);
        payOut.writeVInt(payloadByteUpto);
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (fieldHasOffsets) {
        forUtil.writeBlock(offsetStartDeltaBuffer, encoded, payOut);
        forUtil.writeBlock(offsetLengthBuffer, encoded, payOut);
      }
      posBufferUpto = 0;
    }
  }

  @Override
  public void finishDoc() throws IOException {
    // Since we don't know df for current term, we had to buffer
    // those skip data for each block, and when a new doc comes, 
    // write them to skip file.
    if (docBufferUpto == BLOCK_SIZE) {
      lastBlockDocID = lastDocID;
      if (posOut != null) {
        if (payOut != null) {
          lastBlockPayFP = payOut.getFilePointer();
        }
        lastBlockPosFP = posOut.getFilePointer();
        lastBlockPosBufferUpto = posBufferUpto;
        lastBlockPayloadByteUpto = payloadByteUpto;
      }
      // if (DEBUG) {
      //   System.out.println("  docBufferUpto="+docBufferUpto+" now get lastBlockDocID="+lastBlockDocID+" lastBlockPosFP=" + lastBlockPosFP + " lastBlockPosBufferUpto=" +  lastBlockPosBufferUpto + " lastBlockPayloadByteUpto=" + lastBlockPayloadByteUpto);
      // }
      docBufferUpto = 0;
    }
  }

  private static class PendingTerm {
    public final long docStartFP;
    public final long posStartFP;
    public final long payStartFP;
    public final long skipOffset;
    public final long lastPosBlockOffset;
    public final int singletonDocID;

    public PendingTerm(long docStartFP, long posStartFP, long payStartFP, long skipOffset, long lastPosBlockOffset, int singletonDocID) {
      this.docStartFP = docStartFP;
      this.posStartFP = posStartFP;
      this.payStartFP = payStartFP;
      this.skipOffset = skipOffset;
      this.lastPosBlockOffset = lastPosBlockOffset;
      this.singletonDocID = singletonDocID;
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

    // if (DEBUG) {
    //   System.out.println("FPW.finishTerm docFreq=" + stats.docFreq);
    // }

    // if (DEBUG) {
    //   if (docBufferUpto > 0) {
    //     System.out.println("  write doc/freq vInt block (count=" + docBufferUpto + ") at fp=" + docOut.getFilePointer() + " docTermStartFP=" + docTermStartFP);
    //   }
    // }
    
    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to it.
    final int singletonDocID;
    if (stats.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = docDeltaBuffer[0];
    } else {
      singletonDocID = -1;
      // vInt encode the remaining doc deltas and freqs:
      for(int i=0;i<docBufferUpto;i++) {
        final int docDelta = docDeltaBuffer[i];
        final int freq = freqBuffer[i];
        if (!fieldHasFreqs) {
          docOut.writeVInt(docDelta);
        } else if (freqBuffer[i] == 1) {
          docOut.writeVInt((docDelta<<1)|1);
        } else {
          docOut.writeVInt(docDelta<<1);
          docOut.writeVInt(freq);
        }
      }
    }

    final long lastPosBlockOffset;

    if (fieldHasPositions) {
      // if (DEBUG) {
      //   if (posBufferUpto > 0) {
      //     System.out.println("  write pos vInt block (count=" + posBufferUpto + ") at fp=" + posOut.getFilePointer() + " posTermStartFP=" + posTermStartFP + " hasPayloads=" + fieldHasPayloads + " hasOffsets=" + fieldHasOffsets);
      //   }
      // }

      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert stats.totalTermFreq != -1;
      if (stats.totalTermFreq > BLOCK_SIZE) {
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posTermStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {       
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1;  // force first payload length to be written
        int lastOffsetLength = -1;   // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for(int i=0;i<posBufferUpto;i++) {
          final int posDelta = posDeltaBuffer[i];
          if (fieldHasPayloads) {
            final int payloadLength = payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta<<1)|1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta<<1);
            }

            // if (DEBUG) {
            //   System.out.println("        i=" + i + " payloadLen=" + payloadLength);
            // }

            if (payloadLength != 0) {
              // if (DEBUG) {
              //   System.out.println("          write payload @ pos.fp=" + posOut.getFilePointer());
              // }
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }

          if (fieldHasOffsets) {
            // if (DEBUG) {
            //   System.out.println("          write offset @ pos.fp=" + posOut.getFilePointer());
            // }
            int delta = offsetStartDeltaBuffer[i];
            int length = offsetLengthBuffer[i];
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1);
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
              lastOffsetLength = length;
            }
          }
        }

        if (fieldHasPayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
      // if (DEBUG) {
      //   System.out.println("  totalTermFreq=" + stats.totalTermFreq + " lastPosBlockOffset=" + lastPosBlockOffset);
      // }
    } else {
      lastPosBlockOffset = -1;
    }

    long skipOffset;
    if (docCount > BLOCK_SIZE) {
      skipOffset = skipWriter.writeSkip(docOut) - docTermStartFP;
      
      // if (DEBUG) {
      //   System.out.println("skip packet " + (docOut.getFilePointer() - (docTermStartFP + skipOffset)) + " bytes");
      // }
    } else {
      skipOffset = -1;
      // if (DEBUG) {
      //   System.out.println("  no skip: docCount=" + docCount);
      // }
    }

    long payStartFP;
    if (stats.totalTermFreq >= BLOCK_SIZE) {
      payStartFP = payTermStartFP;
    } else {
      payStartFP = -1;
    }

    // if (DEBUG) {
    //   System.out.println("  payStartFP=" + payStartFP);
    // }

    pendingTerms.add(new PendingTerm(docTermStartFP, posTermStartFP, payStartFP, skipOffset, lastPosBlockOffset, singletonDocID));
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

      if (term.singletonDocID == -1) {
        bytesWriter.writeVLong(term.docStartFP - lastDocStartFP);
        lastDocStartFP = term.docStartFP;
      } else {
        bytesWriter.writeVInt(term.singletonDocID);
      }

      if (fieldHasPositions) {
        bytesWriter.writeVLong(term.posStartFP - lastPosStartFP);
        lastPosStartFP = term.posStartFP;
        if (term.lastPosBlockOffset != -1) {
          bytesWriter.writeVLong(term.lastPosBlockOffset);
        }
        if ((fieldHasPayloads || fieldHasOffsets) && term.payStartFP != -1) {
          bytesWriter.writeVLong(term.payStartFP - lastPayStartFP);
          lastPayStartFP = term.payStartFP;
        }
      }

      if (term.skipOffset != -1) {
        bytesWriter.writeVLong(term.skipOffset);
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

package org.apache.lucene.codecs.pulsing;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

// TODO: we now inline based on total TF of the term,
// but it might be better to inline by "net bytes used"
// so that a term that has only 1 posting but a huge
// payload would not be inlined.  Though this is
// presumably rare in practice...

/** 
 * Writer for the pulsing format. 
 * <p>
 * Wraps another postings implementation and decides 
 * (based on total number of occurrences), whether a terms 
 * postings should be inlined into the term dictionary,
 * or passed through to the wrapped writer.
 *
 * @lucene.experimental */
public final class PulsingPostingsWriter extends PostingsWriterBase {

  final static String CODEC = "PulsedPostingsWriter";

  // recording field summary
  final static String SUMMARY_EXTENSION = "smy";

  // To add a new version, increment from the last one, and
  // change VERSION_CURRENT to point to your new version:
  final static int VERSION_START = 0;

  final static int VERSION_META_ARRAY = 1;

  final static int VERSION_CURRENT = VERSION_META_ARRAY;

  private SegmentWriteState segmentState;

  private List<FieldMetaData> fields;

  // Reused by writeTerm:
  private DocsEnum docsEnum;
  private DocsAndPositionsEnum posEnum;
  private int enumFlags;

  private final RAMOutputStream buffer = new RAMOutputStream();

  private IndexOptions indexOptions;

  // information for wrapped PF, in current field
  private int longsSize;
  private long[] longs;
  private boolean fieldHasFreqs;
  private boolean fieldHasPositions;
  private boolean fieldHasOffsets;
  private boolean fieldHasPayloads;
  boolean absolute;

  private static class PulsingTermState extends BlockTermState {
    private byte[] bytes;
    private BlockTermState wrappedState;

    @Override
    public String toString() {
      if (bytes != null) {
        return "inlined";
      } else {
        return "not inlined wrapped=" + wrappedState;
      }
    }
  }

  private static final class FieldMetaData {
    int fieldNumber;
    int longsSize;
    FieldMetaData(int number, int size) {
      fieldNumber = number;
      longsSize = size;
    }
  }

  // TODO: -- lazy init this?  ie, if every single term
  // was inlined (eg for a "primary key" field) then we
  // never need to use this fallback?  Fallback writer for
  // non-inlined terms:
  final PostingsWriterBase wrappedPostingsWriter;

  final int maxPositions;

  /** If the total number of positions (summed across all docs
   *  for this term) is <= maxPositions, then the postings are
   *  inlined into terms dict */
  public PulsingPostingsWriter(SegmentWriteState state, int maxPositions, PostingsWriterBase wrappedPostingsWriter) {
    fields = new ArrayList<FieldMetaData>();
    this.maxPositions = maxPositions;
    // We simply wrap another postings writer, but only call
    // on it when tot positions is >= the cutoff:
    this.wrappedPostingsWriter = wrappedPostingsWriter;
    this.segmentState = state;
  }

  @Override
  public void init(IndexOutput termsOut) throws IOException {
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeVInt(maxPositions); // encode maxPositions in header
    wrappedPostingsWriter.init(termsOut);
  }

  @Override
  public BlockTermState writeTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen) throws IOException {

    // First pass: figure out whether we should pulse this term
    long posCount = 0;

    if (fieldHasPositions == false) {
      // No positions:
      docsEnum = termsEnum.docs(null, docsEnum, enumFlags);
      assert docsEnum != null;
      while (posCount <= maxPositions) {
        if (docsEnum.nextDoc() == DocsEnum.NO_MORE_DOCS) {
          break;
        }
        posCount++;
      }
    } else {
      posEnum = termsEnum.docsAndPositions(null, posEnum, enumFlags);
      assert posEnum != null;
      while (posCount <= maxPositions) {
        if (posEnum.nextDoc() == DocsEnum.NO_MORE_DOCS) {
          break;
        }
        posCount += posEnum.freq();
      }
    }

    if (posCount == 0) {
      // All docs were deleted
      return null;
    }

    // Second pass: write postings
    if (posCount > maxPositions) {
      // Too many positions; do not pulse.  Just lset
      // wrapped postingsWriter encode the postings:

      PulsingTermState state = new PulsingTermState();
      state.wrappedState = wrappedPostingsWriter.writeTerm(term, termsEnum, docsSeen);
      state.docFreq = state.wrappedState.docFreq;
      state.totalTermFreq = state.wrappedState.totalTermFreq;
      return state;
    } else {
      // Pulsed:
      if (fieldHasPositions == false) {
        docsEnum = termsEnum.docs(null, docsEnum, enumFlags);
      } else {
        posEnum = termsEnum.docsAndPositions(null, posEnum, enumFlags);
        docsEnum = posEnum;
      }
      assert docsEnum != null;

      // There were few enough total occurrences for this
      // term, so we fully inline our postings data into
      // terms dict, now:

      // TODO: it'd be better to share this encoding logic
      // in some inner codec that knows how to write a
      // single doc / single position, etc.  This way if a
      // given codec wants to store other interesting
      // stuff, it could use this pulsing codec to do so

      int lastDocID = 0;
      int lastPayloadLength = -1;
      int lastOffsetLength = -1;

      int docFreq = 0;
      long totalTermFreq = 0;
      while (true) {
        int docID = docsEnum.nextDoc();
        if (docID == DocsEnum.NO_MORE_DOCS) {
          break;
        }
        docsSeen.set(docID);

        int delta = docID - lastDocID;
        lastDocID = docID;

        docFreq++;

        if (fieldHasFreqs) {
          int freq = docsEnum.freq();
          totalTermFreq += freq;

          if (freq == 1) {
            buffer.writeVInt((delta << 1) | 1);
          } else {
            buffer.writeVInt(delta << 1);
            buffer.writeVInt(freq);
          }

          if (fieldHasPositions) {
            int lastPos = 0;
            int lastOffset = 0;
            for(int posIDX=0;posIDX<freq;posIDX++) {
              int pos = posEnum.nextPosition();
              int posDelta = pos - lastPos;
              lastPos = pos;
              int payloadLength;
              BytesRef payload;
              if (fieldHasPayloads) {
                payload = posEnum.getPayload();
                payloadLength = payload == null ? 0 : payload.length;
                if (payloadLength != lastPayloadLength) {
                  buffer.writeVInt((posDelta << 1)|1);
                  buffer.writeVInt(payloadLength);
                  lastPayloadLength = payloadLength;
                } else {
                  buffer.writeVInt(posDelta << 1);
                }
              } else {
                payloadLength = 0;
                payload = null;
                buffer.writeVInt(posDelta);
              }

              if (fieldHasOffsets) {
                int startOffset = posEnum.startOffset();
                int endOffset = posEnum.endOffset();
                int offsetDelta = startOffset - lastOffset;
                int offsetLength = endOffset - startOffset;
                if (offsetLength != lastOffsetLength) {
                  buffer.writeVInt(offsetDelta << 1 | 1);
                  buffer.writeVInt(offsetLength);
                } else {
                  buffer.writeVInt(offsetDelta << 1);
                }
                lastOffset = startOffset;
                lastOffsetLength = offsetLength;             
              }
            
              if (payloadLength > 0) {
                assert fieldHasPayloads;
                assert payload != null;
                buffer.writeBytes(payload.bytes, payload.offset, payload.length);
              }
            }
          }
        } else {
          buffer.writeVInt(delta);
        }
      }
      
      PulsingTermState state = new PulsingTermState();
      state.bytes = new byte[(int) buffer.getFilePointer()];
      state.docFreq = docFreq;
      state.totalTermFreq = fieldHasFreqs ? totalTermFreq : -1;
      buffer.writeTo(state.bytes, 0);
      buffer.reset();
      return state;
    }
  }

  // TODO: -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public int setField(FieldInfo fieldInfo) {
    this.indexOptions = fieldInfo.getIndexOptions();
    //if (DEBUG) System.out.println("PW field=" + fieldInfo.name + " indexOptions=" + indexOptions);
    fieldHasPayloads = fieldInfo.hasPayloads();
    absolute = false;
    longsSize = wrappedPostingsWriter.setField(fieldInfo);
    longs = new long[longsSize];
    fields.add(new FieldMetaData(fieldInfo.number, longsSize));

    fieldHasFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    fieldHasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    fieldHasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;

    if (fieldHasFreqs == false) {
      enumFlags = 0;
    } else if (fieldHasPositions == false) {
      enumFlags = DocsEnum.FLAG_FREQS;
    } else if (fieldHasOffsets == false) {
      if (fieldHasPayloads) {
        enumFlags = DocsAndPositionsEnum.FLAG_PAYLOADS;
      } else {
        enumFlags = 0;
      }
    } else {
      if (fieldHasPayloads) {
        enumFlags = DocsAndPositionsEnum.FLAG_PAYLOADS | DocsAndPositionsEnum.FLAG_OFFSETS;
      } else {
        enumFlags = DocsAndPositionsEnum.FLAG_OFFSETS;
      }
    }
    return 0;
    //DEBUG = BlockTreeTermsWriter.DEBUG;
  }

  @Override
  public void encodeTerm(long[] empty, DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    PulsingTermState state = (PulsingTermState)_state;
    assert empty.length == 0;
    this.absolute = this.absolute || absolute;
    if (state.bytes == null) {
      wrappedPostingsWriter.encodeTerm(longs, buffer, fieldInfo, state.wrappedState, this.absolute);
      for (int i = 0; i < longsSize; i++) {
        out.writeVLong(longs[i]);
      }
      buffer.writeTo(out);
      buffer.reset();
      this.absolute = false;
    } else {
      out.writeVInt(state.bytes.length);
      out.writeBytes(state.bytes, 0, state.bytes.length);
      this.absolute = this.absolute || absolute;
    }
  }

  @Override
  public void close() throws IOException {
    wrappedPostingsWriter.close();
    if (wrappedPostingsWriter instanceof PulsingPostingsWriter ||
        VERSION_CURRENT < VERSION_META_ARRAY) {
      return;
    }
    String summaryFileName = IndexFileNames.segmentFileName(segmentState.segmentInfo.name, segmentState.segmentSuffix, SUMMARY_EXTENSION);
    IndexOutput out = null;
    try {
      out = segmentState.directory.createOutput(summaryFileName, segmentState.context);
      CodecUtil.writeHeader(out, CODEC, VERSION_CURRENT);
      out.writeVInt(fields.size());
      for (FieldMetaData field : fields) {
        out.writeVInt(field.fieldNumber);
        out.writeVInt(field.longsSize);
      }
      out.close();
    } finally {
      IOUtils.closeWhileHandlingException(out);
    }
  }
}

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
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
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
  private IndexOutput termsOut;

  private List<FieldMetaData> fields;

  private IndexOptions indexOptions;
  private boolean storePayloads;

  // information for wrapped PF, in current field
  private int longsSize;
  private long[] longs;
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

  // one entry per position
  private final Position[] pending;
  private int pendingCount = 0;                           // -1 once we've hit too many positions
  private Position currentDoc;                    // first Position entry of current doc

  private static final class Position {
    BytesRefBuilder payload;
    int termFreq;                                 // only incremented on first position for a given doc
    int pos;
    int docID;
    int startOffset;
    int endOffset;
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

  /** If the total number of positions (summed across all docs
   *  for this term) is <= maxPositions, then the postings are
   *  inlined into terms dict */
  public PulsingPostingsWriter(SegmentWriteState state, int maxPositions, PostingsWriterBase wrappedPostingsWriter) {

    pending = new Position[maxPositions];
    for(int i=0;i<maxPositions;i++) {
      pending[i] = new Position();
    }
    fields = new ArrayList<>();

    // We simply wrap another postings writer, but only call
    // on it when tot positions is >= the cutoff:
    this.wrappedPostingsWriter = wrappedPostingsWriter;
    this.segmentState = state;
  }

  @Override
  public void init(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeVInt(pending.length); // encode maxPositions in header
    wrappedPostingsWriter.init(termsOut);
  }

  @Override
  public BlockTermState newTermState() throws IOException {
    PulsingTermState state = new PulsingTermState();
    state.wrappedState = wrappedPostingsWriter.newTermState();
    return state;
  }

  @Override
  public void startTerm() {
    //if (DEBUG) System.out.println("PW   startTerm");
    assert pendingCount == 0;
  }

  // TODO: -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public int setField(FieldInfo fieldInfo) {
    this.indexOptions = fieldInfo.getIndexOptions();
    //if (DEBUG) System.out.println("PW field=" + fieldInfo.name + " indexOptions=" + indexOptions);
    storePayloads = fieldInfo.hasPayloads();
    absolute = false;
    longsSize = wrappedPostingsWriter.setField(fieldInfo);
    longs = new long[longsSize];
    fields.add(new FieldMetaData(fieldInfo.number, longsSize));
    return 0;
    //DEBUG = BlockTreeTermsWriter.DEBUG;
  }

  private boolean DEBUG;

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    assert docID >= 0: "got docID=" + docID;

    /*
    if (termID != -1) {
      if (docID == 0) {
        baseDocID = termID;
      } else if (baseDocID + docID != termID) {
        throw new RuntimeException("WRITE: baseDocID=" + baseDocID + " docID=" + docID + " termID=" + termID);
      }
    }
    */

    //if (DEBUG) System.out.println("PW     doc=" + docID);

    if (pendingCount == pending.length) {
      push();
      //if (DEBUG) System.out.println("PW: wrapped.finishDoc");
      wrappedPostingsWriter.finishDoc();
    }

    if (pendingCount != -1) {
      assert pendingCount < pending.length;
      currentDoc = pending[pendingCount];
      currentDoc.docID = docID;
      if (indexOptions == IndexOptions.DOCS_ONLY) {
        pendingCount++;
      } else if (indexOptions == IndexOptions.DOCS_AND_FREQS) { 
        pendingCount++;
        currentDoc.termFreq = termDocFreq;
      } else {
        currentDoc.termFreq = termDocFreq;
      }
    } else {
      // We've already seen too many docs for this term --
      // just forward to our fallback writer
      wrappedPostingsWriter.startDoc(docID, termDocFreq);
    }
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {

    //if (DEBUG) System.out.println("PW       pos=" + position + " payload=" + (payload == null ? "null" : payload.length + " bytes"));
    if (pendingCount == pending.length) {
      push();
    }

    if (pendingCount == -1) {
      // We've already seen too many docs for this term --
      // just forward to our fallback writer
      wrappedPostingsWriter.addPosition(position, payload, startOffset, endOffset);
    } else {
      // buffer up
      final Position pos = pending[pendingCount++];
      pos.pos = position;
      pos.startOffset = startOffset;
      pos.endOffset = endOffset;
      pos.docID = currentDoc.docID;
      if (payload != null && payload.length > 0) {
        if (pos.payload == null) {
          pos.payload = new BytesRefBuilder();
        }
        pos.payload.copyBytes(payload);
      } else if (pos.payload != null) {
        pos.payload.clear();
      }
    }
  }

  @Override
  public void finishDoc() throws IOException {
    // if (DEBUG) System.out.println("PW     finishDoc");
    if (pendingCount == -1) {
      wrappedPostingsWriter.finishDoc();
    }
  }

  private final RAMOutputStream buffer = new RAMOutputStream();

  // private int baseDocID;

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    PulsingTermState state = (PulsingTermState) _state;

    // if (DEBUG) System.out.println("PW   finishTerm docCount=" + stats.docFreq + " pendingCount=" + pendingCount + " pendingTerms.size()=" + pendingTerms.size());

    assert pendingCount > 0 || pendingCount == -1;

    if (pendingCount == -1) {
      state.wrappedState.docFreq = state.docFreq;
      state.wrappedState.totalTermFreq = state.totalTermFreq;
      state.bytes = null;
      wrappedPostingsWriter.finishTerm(state.wrappedState);
    } else {
      // There were few enough total occurrences for this
      // term, so we fully inline our postings data into
      // terms dict, now:

      // TODO: it'd be better to share this encoding logic
      // in some inner codec that knows how to write a
      // single doc / single position, etc.  This way if a
      // given codec wants to store other interesting
      // stuff, it could use this pulsing codec to do so

      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
        int lastDocID = 0;
        int pendingIDX = 0;
        int lastPayloadLength = -1;
        int lastOffsetLength = -1;
        while(pendingIDX < pendingCount) {
          final Position doc = pending[pendingIDX];

          final int delta = doc.docID - lastDocID;
          lastDocID = doc.docID;

          // if (DEBUG) System.out.println("  write doc=" + doc.docID + " freq=" + doc.termFreq);

          if (doc.termFreq == 1) {
            buffer.writeVInt((delta<<1)|1);
          } else {
            buffer.writeVInt(delta<<1);
            buffer.writeVInt(doc.termFreq);
          }

          int lastPos = 0;
          int lastOffset = 0;
          for(int posIDX=0;posIDX<doc.termFreq;posIDX++) {
            final Position pos = pending[pendingIDX++];
            assert pos.docID == doc.docID;
            final int posDelta = pos.pos - lastPos;
            lastPos = pos.pos;
            // if (DEBUG) System.out.println("    write pos=" + pos.pos);
            final int payloadLength = pos.payload == null ? 0 : pos.payload.length();
            if (storePayloads) {
              if (payloadLength != lastPayloadLength) {
                buffer.writeVInt((posDelta << 1)|1);
                buffer.writeVInt(payloadLength);
                lastPayloadLength = payloadLength;
              } else {
                buffer.writeVInt(posDelta << 1);
              }
            } else {
              buffer.writeVInt(posDelta);
            }
            
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
              //System.out.println("write=" + pos.startOffset + "," + pos.endOffset);
              int offsetDelta = pos.startOffset - lastOffset;
              int offsetLength = pos.endOffset - pos.startOffset;
              if (offsetLength != lastOffsetLength) {
                buffer.writeVInt(offsetDelta << 1 | 1);
                buffer.writeVInt(offsetLength);
              } else {
                buffer.writeVInt(offsetDelta << 1);
              }
              lastOffset = pos.startOffset;
              lastOffsetLength = offsetLength;             
            }
            
            if (payloadLength > 0) {
              assert storePayloads;
              buffer.writeBytes(pos.payload.bytes(), 0, pos.payload.length());
            }
          }
        }
      } else if (indexOptions == IndexOptions.DOCS_AND_FREQS) {
        int lastDocID = 0;
        for(int posIDX=0;posIDX<pendingCount;posIDX++) {
          final Position doc = pending[posIDX];
          final int delta = doc.docID - lastDocID;
          assert doc.termFreq != 0;
          if (doc.termFreq == 1) {
            buffer.writeVInt((delta<<1)|1);
          } else {
            buffer.writeVInt(delta<<1);
            buffer.writeVInt(doc.termFreq);
          }
          lastDocID = doc.docID;
        }
      } else if (indexOptions == IndexOptions.DOCS_ONLY) {
        int lastDocID = 0;
        for(int posIDX=0;posIDX<pendingCount;posIDX++) {
          final Position doc = pending[posIDX];
          buffer.writeVInt(doc.docID - lastDocID);
          lastDocID = doc.docID;
        }
      }

      state.bytes = new byte[(int) buffer.getFilePointer()];
      buffer.writeTo(state.bytes, 0);
      buffer.reset();
    }
    pendingCount = 0;
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

  // Pushes pending positions to the wrapped codec
  private void push() throws IOException {
    // if (DEBUG) System.out.println("PW now push @ " + pendingCount + " wrapped=" + wrappedPostingsWriter);
    assert pendingCount == pending.length;
      
    wrappedPostingsWriter.startTerm();
      
    // Flush all buffered docs
    if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
      Position doc = null;
      for(Position pos : pending) {
        if (doc == null) {
          doc = pos;
          // if (DEBUG) System.out.println("PW: wrapped.startDoc docID=" + doc.docID + " tf=" + doc.termFreq);
          wrappedPostingsWriter.startDoc(doc.docID, doc.termFreq);
        } else if (doc.docID != pos.docID) {
          assert pos.docID > doc.docID;
          // if (DEBUG) System.out.println("PW: wrapped.finishDoc");
          wrappedPostingsWriter.finishDoc();
          doc = pos;
          // if (DEBUG) System.out.println("PW: wrapped.startDoc docID=" + doc.docID + " tf=" + doc.termFreq);
          wrappedPostingsWriter.startDoc(doc.docID, doc.termFreq);
        }
        // if (DEBUG) System.out.println("PW:   wrapped.addPos pos=" + pos.pos);
        final BytesRef payload = pos.payload == null ? null : pos.payload.get();
        wrappedPostingsWriter.addPosition(pos.pos, payload, pos.startOffset, pos.endOffset);
      }
      //wrappedPostingsWriter.finishDoc();
    } else {
      for(Position doc : pending) {
        wrappedPostingsWriter.startDoc(doc.docID, indexOptions == IndexOptions.DOCS_ONLY ? 0 : doc.termFreq);
      }
    }
    pendingCount = -1;
  }
}

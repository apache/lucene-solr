package org.apache.lucene.codecs.pulsing;

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
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

// TODO: we now inline based on total TF of the term,
// but it might be better to inline by "net bytes used"
// so that a term that has only 1 posting but a huge
// payload would not be inlined.  Though this is
// presumably rare in practice...

/** @lucene.experimental */
public final class PulsingPostingsWriter extends PostingsWriterBase {

  final static String CODEC = "PulsedPostingsWriter";

  // To add a new version, increment from the last one, and
  // change VERSION_CURRENT to point to your new version:
  final static int VERSION_START = 0;

  final static int VERSION_CURRENT = VERSION_START;

  private IndexOutput termsOut;

  private IndexOptions indexOptions;
  private boolean storePayloads;

  private static class PendingTerm {
    private final byte[] bytes;
    public PendingTerm(byte[] bytes) {
      this.bytes = bytes;
    }
  }

  private final List<PendingTerm> pendingTerms = new ArrayList<PendingTerm>();

  // one entry per position
  private final Position[] pending;
  private int pendingCount = 0;                           // -1 once we've hit too many positions
  private Position currentDoc;                    // first Position entry of current doc

  private static final class Position {
    BytesRef payload;
    int termFreq;                                 // only incremented on first position for a given doc
    int pos;
    int docID;
  }

  // TODO: -- lazy init this?  ie, if every single term
  // was inlined (eg for a "primary key" field) then we
  // never need to use this fallback?  Fallback writer for
  // non-inlined terms:
  final PostingsWriterBase wrappedPostingsWriter;

  /** If the total number of positions (summed across all docs
   *  for this term) is <= maxPositions, then the postings are
   *  inlined into terms dict */
  public PulsingPostingsWriter(int maxPositions, PostingsWriterBase wrappedPostingsWriter) throws IOException {
    pending = new Position[maxPositions];
    for(int i=0;i<maxPositions;i++) {
      pending[i] = new Position();
    }

    // We simply wrap another postings writer, but only call
    // on it when tot positions is >= the cutoff:
    this.wrappedPostingsWriter = wrappedPostingsWriter;
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeVInt(pending.length); // encode maxPositions in header
    wrappedPostingsWriter.start(termsOut);
  }

  @Override
  public void startTerm() {
    if (DEBUG) System.out.println("PW   startTerm");
    assert pendingCount == 0;
  }

  // TODO: -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.indexOptions = fieldInfo.indexOptions;
    if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
      throw new UnsupportedOperationException("this codec cannot index offsets: " + indexOptions);
    }
    if (DEBUG) System.out.println("PW field=" + fieldInfo.name + " indexOptions=" + indexOptions);
    storePayloads = fieldInfo.storePayloads;
    wrappedPostingsWriter.setField(fieldInfo);
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

    if (DEBUG) System.out.println("PW     doc=" + docID);

    if (pendingCount == pending.length) {
      push();
      if (DEBUG) System.out.println("PW: wrapped.finishDoc");
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

    if (DEBUG) System.out.println("PW       pos=" + position + " payload=" + (payload == null ? "null" : payload.length + " bytes"));
    if (pendingCount == pending.length) {
      push();
    }

    if (pendingCount == -1) {
      // We've already seen too many docs for this term --
      // just forward to our fallback writer
      wrappedPostingsWriter.addPosition(position, payload, -1, -1);
    } else {
      // buffer up
      final Position pos = pending[pendingCount++];
      pos.pos = position;
      pos.docID = currentDoc.docID;
      if (payload != null && payload.length > 0) {
        if (pos.payload == null) {
          pos.payload = BytesRef.deepCopyOf(payload);
        } else {
          pos.payload.copyBytes(payload);
        }
      } else if (pos.payload != null) {
        pos.payload.length = 0;
      }
    }
  }

  @Override
  public void finishDoc() throws IOException {
    if (DEBUG) System.out.println("PW     finishDoc");
    if (pendingCount == -1) {
      wrappedPostingsWriter.finishDoc();
    }
  }

  private final RAMOutputStream buffer = new RAMOutputStream();

  // private int baseDocID;

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(TermStats stats) throws IOException {
    if (DEBUG) System.out.println("PW   finishTerm docCount=" + stats.docFreq + " pendingCount=" + pendingCount + " pendingTerms.size()=" + pendingTerms.size());

    assert pendingCount > 0 || pendingCount == -1;

    if (pendingCount == -1) {
      wrappedPostingsWriter.finishTerm(stats);
      // Must add null entry to record terms that our
      // wrapped postings impl added
      pendingTerms.add(null);
    } else {

      // There were few enough total occurrences for this
      // term, so we fully inline our postings data into
      // terms dict, now:

      // TODO: it'd be better to share this encoding logic
      // in some inner codec that knows how to write a
      // single doc / single position, etc.  This way if a
      // given codec wants to store other interesting
      // stuff, it could use this pulsing codec to do so

      if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        int lastDocID = 0;
        int pendingIDX = 0;
        int lastPayloadLength = -1;
        while(pendingIDX < pendingCount) {
          final Position doc = pending[pendingIDX];

          final int delta = doc.docID - lastDocID;
          lastDocID = doc.docID;

          if (DEBUG) System.out.println("  write doc=" + doc.docID + " freq=" + doc.termFreq);

          if (doc.termFreq == 1) {
            buffer.writeVInt((delta<<1)|1);
          } else {
            buffer.writeVInt(delta<<1);
            buffer.writeVInt(doc.termFreq);
          }

          int lastPos = 0;
          for(int posIDX=0;posIDX<doc.termFreq;posIDX++) {
            final Position pos = pending[pendingIDX++];
            assert pos.docID == doc.docID;
            final int posDelta = pos.pos - lastPos;
            lastPos = pos.pos;
            if (DEBUG) System.out.println("    write pos=" + pos.pos);
            if (storePayloads) {
              final int payloadLength = pos.payload == null ? 0 : pos.payload.length;
              if (payloadLength != lastPayloadLength) {
                buffer.writeVInt((posDelta << 1)|1);
                buffer.writeVInt(payloadLength);
                lastPayloadLength = payloadLength;
              } else {
                buffer.writeVInt(posDelta << 1);
              }
              if (payloadLength > 0) {
                buffer.writeBytes(pos.payload.bytes, 0, pos.payload.length);
              }
            } else {
              buffer.writeVInt(posDelta);
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

      final byte[] bytes = new byte[(int) buffer.getFilePointer()];
      buffer.writeTo(bytes, 0);
      pendingTerms.add(new PendingTerm(bytes));
      buffer.reset();
    }

    pendingCount = 0;
  }

  @Override
  public void close() throws IOException {
    wrappedPostingsWriter.close();
  }

  @Override
  public void flushTermsBlock(int start, int count) throws IOException {
    if (DEBUG) System.out.println("PW: flushTermsBlock start=" + start + " count=" + count + " pendingTerms.size()=" + pendingTerms.size());
    int wrappedCount = 0;
    assert buffer.getFilePointer() == 0;
    assert start >= count;

    final int limit = pendingTerms.size() - start + count;

    for(int idx=pendingTerms.size()-start; idx<limit; idx++) {
      final PendingTerm term = pendingTerms.get(idx);
      if (term == null) {
        wrappedCount++;
      } else {
        buffer.writeVInt(term.bytes.length);
        buffer.writeBytes(term.bytes, 0, term.bytes.length);
      }
    }

    termsOut.writeVInt((int) buffer.getFilePointer());
    buffer.writeTo(termsOut);
    buffer.reset();

    // TDOO: this could be somewhat costly since
    // pendingTerms.size() could be biggish?
    int futureWrappedCount = 0;
    final int limit2 = pendingTerms.size();
    for(int idx=limit;idx<limit2;idx++) {
      if (pendingTerms.get(idx) == null) {
        futureWrappedCount++;
      }
    }

    // Remove the terms we just wrote:
    pendingTerms.subList(pendingTerms.size()-start, limit).clear();

    if (DEBUG) System.out.println("PW:   len=" + buffer.getFilePointer() + " fp=" + termsOut.getFilePointer() + " futureWrappedCount=" + futureWrappedCount + " wrappedCount=" + wrappedCount);
    // TODO: can we avoid calling this if all terms
    // were inlined...?  Eg for a "primary key" field, the
    // wrapped codec is never invoked...
    wrappedPostingsWriter.flushTermsBlock(futureWrappedCount+wrappedCount, wrappedCount);
  }

  // Pushes pending positions to the wrapped codec
  private void push() throws IOException {
    if (DEBUG) System.out.println("PW now push @ " + pendingCount + " wrapped=" + wrappedPostingsWriter);
    assert pendingCount == pending.length;
      
    wrappedPostingsWriter.startTerm();
      
    // Flush all buffered docs
    if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
      Position doc = null;
      for(Position pos : pending) {
        if (doc == null) {
          doc = pos;
          if (DEBUG) System.out.println("PW: wrapped.startDoc docID=" + doc.docID + " tf=" + doc.termFreq);
          wrappedPostingsWriter.startDoc(doc.docID, doc.termFreq);
        } else if (doc.docID != pos.docID) {
          assert pos.docID > doc.docID;
          if (DEBUG) System.out.println("PW: wrapped.finishDoc");
          wrappedPostingsWriter.finishDoc();
          doc = pos;
          if (DEBUG) System.out.println("PW: wrapped.startDoc docID=" + doc.docID + " tf=" + doc.termFreq);
          wrappedPostingsWriter.startDoc(doc.docID, doc.termFreq);
        }
        if (DEBUG) System.out.println("PW:   wrapped.addPos pos=" + pos.pos);
        wrappedPostingsWriter.addPosition(pos.pos, pos.payload, -1, -1);
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

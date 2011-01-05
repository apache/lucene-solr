package org.apache.lucene.index.codecs.pulsing;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: we now pulse entirely according to docFreq of the
// term; it might be better to eg pulse by "net bytes used"
// so that a term that has only 1 doc but zillions of
// positions would not be inlined.  Though this is
// presumably rare in practice...

/** @lucene.experimental */
public final class PulsingPostingsWriterImpl extends PostingsWriterBase {

  final static String CODEC = "PulsedPostings";

  // To add a new version, increment from the last one, and
  // change VERSION_CURRENT to point to your new version:
  final static int VERSION_START = 0;

  final static int VERSION_CURRENT = VERSION_START;

  IndexOutput termsOut;

  boolean omitTF;
  boolean storePayloads;

  // Starts a new term
  FieldInfo fieldInfo;

  /** @lucene.experimental */
  public static class Document {
    int docID;
    int termDocFreq;
    int numPositions;
    Position[] positions;
    Document() {
      positions = new Position[1];
      positions[0] = new Position();
    }
    
    @Override
    public Object clone() {
      Document doc = new Document();
      doc.docID = docID;
      doc.termDocFreq = termDocFreq;
      doc.numPositions = numPositions;
      doc.positions = new Position[positions.length];
      for(int i = 0; i < positions.length; i++) {
        doc.positions[i] = (Position) positions[i].clone();
      }

      return doc;
    }

    void reallocPositions(int minSize) {
      final Position[] newArray = new Position[ArrayUtil.oversize(minSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(positions, 0, newArray, 0, positions.length);
      for(int i=positions.length;i<newArray.length;i++) {
        newArray[i] = new Position();
      }
      positions = newArray;
    }
  }

  final Document[] pendingDocs;
  int pendingDocCount = 0;
  Document currentDoc;
  boolean pulsed;                                 // false if we've seen > maxPulsingDocFreq docs

  static class Position {
    BytesRef payload;
    int pos;
    
    @Override
    public Object clone() {
      Position position = new Position();
      position.pos = pos;
      if (payload != null) {
        position.payload = new BytesRef(payload);
      }
      return position;
    }
  }

  // TODO: -- lazy init this?  ie, if every single term
  // was pulsed then we never need to use this fallback?
  // Fallback writer for non-pulsed terms:
  final PostingsWriterBase wrappedPostingsWriter;

  /** If docFreq <= maxPulsingDocFreq, its postings are
   *  inlined into terms dict */
  public PulsingPostingsWriterImpl(int maxPulsingDocFreq, PostingsWriterBase wrappedPostingsWriter) throws IOException {
    super();

    pendingDocs = new Document[maxPulsingDocFreq];
    for(int i=0;i<maxPulsingDocFreq;i++) {
      pendingDocs[i] = new Document();
    }

    // We simply wrap another postings writer, but only call
    // on it when doc freq is higher than our cutoff
    this.wrappedPostingsWriter = wrappedPostingsWriter;
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    this.termsOut = termsOut;
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeVInt(pendingDocs.length);
    wrappedPostingsWriter.start(termsOut);
  }

  @Override
  public void startTerm() {
    assert pendingDocCount == 0;
    pulsed = false;
  }

  // TODO: -- should we NOT reuse across fields?  would
  // be cleaner

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    omitTF = fieldInfo.omitTermFreqAndPositions;
    storePayloads = fieldInfo.storePayloads;
    wrappedPostingsWriter.setField(fieldInfo);
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {

    assert docID >= 0: "got docID=" + docID;
        
    if (!pulsed && pendingDocCount == pendingDocs.length) {
      
      // OK we just crossed the threshold, this term should
      // now be written with our wrapped codec:
      wrappedPostingsWriter.startTerm();
      
      // Flush all buffered docs
      for(int i=0;i<pendingDocCount;i++) {
        final Document doc = pendingDocs[i];

        wrappedPostingsWriter.startDoc(doc.docID, doc.termDocFreq);

        if (!omitTF) {
          assert doc.termDocFreq == doc.numPositions;
          for(int j=0;j<doc.termDocFreq;j++) {
            final Position pos = doc.positions[j];
            if (pos.payload != null && pos.payload.length > 0) {
              assert storePayloads;
              wrappedPostingsWriter.addPosition(pos.pos, pos.payload);
            } else {
              wrappedPostingsWriter.addPosition(pos.pos, null);
            }
          }
          wrappedPostingsWriter.finishDoc();
        }
      }

      pendingDocCount = 0;

      pulsed = true;
    }

    if (pulsed) {
      // We've already seen too many docs for this term --
      // just forward to our fallback writer
      wrappedPostingsWriter.startDoc(docID, termDocFreq);
    } else {
      currentDoc = pendingDocs[pendingDocCount++];
      currentDoc.docID = docID;
      // TODO: -- need not store in doc?  only used for alloc & assert
      currentDoc.termDocFreq = termDocFreq;
      if (termDocFreq > currentDoc.positions.length) {
        currentDoc.reallocPositions(termDocFreq);
      }
      currentDoc.numPositions = 0;
    }
  }

  @Override
  public void addPosition(int position, BytesRef payload) throws IOException {
    if (pulsed) {
      wrappedPostingsWriter.addPosition(position, payload);
    } else {
      // just buffer up
      Position pos = currentDoc.positions[currentDoc.numPositions++];
      pos.pos = position;
      if (payload != null && payload.length > 0) {
        if (pos.payload == null) {
          pos.payload = new BytesRef(payload);
        } else {
          pos.payload.copy(payload);
        }
      } else if (pos.payload != null) {
        pos.payload.length = 0;
      }
    }
  }

  @Override
  public void finishDoc() throws IOException {
    assert omitTF || currentDoc.numPositions == currentDoc.termDocFreq;
    if (pulsed) {
      wrappedPostingsWriter.finishDoc();
    }
  }

  boolean pendingIsIndexTerm;

  int pulsedCount;
  int nonPulsedCount;

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(int docCount, boolean isIndexTerm) throws IOException {

    assert docCount > 0;

    pendingIsIndexTerm |= isIndexTerm;

    if (pulsed) {
      wrappedPostingsWriter.finishTerm(docCount, pendingIsIndexTerm);
      pendingIsIndexTerm = false;
      pulsedCount++;
    } else {
      nonPulsedCount++;
      // OK, there were few enough occurrences for this
      // term, so we fully inline our postings data into
      // terms dict, now:
      int lastDocID = 0;
      for(int i=0;i<pendingDocCount;i++) {
        final Document doc = pendingDocs[i];
        final int delta = doc.docID - lastDocID;
        lastDocID = doc.docID;
        if (omitTF) {
          termsOut.writeVInt(delta);
        } else {
          assert doc.numPositions == doc.termDocFreq;
          if (doc.numPositions == 1)
            termsOut.writeVInt((delta<<1)|1);
          else {
            termsOut.writeVInt(delta<<1);
            termsOut.writeVInt(doc.numPositions);
          }

          // TODO: we could do better in encoding
          // payloadLength, eg, if it's always the same
          // across all terms
          int lastPosition = 0;
          int lastPayloadLength = -1;

          for(int j=0;j<doc.numPositions;j++) {
            final Position pos = doc.positions[j];
            final int delta2 = pos.pos - lastPosition;
            lastPosition = pos.pos;
            if (storePayloads) {
              final int payloadLength = pos.payload == null ? 0 : pos.payload.length;
              if (payloadLength != lastPayloadLength) {
                termsOut.writeVInt((delta2 << 1)|1);
                termsOut.writeVInt(payloadLength);
                lastPayloadLength = payloadLength;
              } else {
                termsOut.writeVInt(delta2 << 1);
              }

              if (payloadLength > 0) {
                termsOut.writeBytes(pos.payload.bytes, 0, pos.payload.length);
              }
            } else {
              termsOut.writeVInt(delta2);
            }
          }
        }
      }
    }

    pendingDocCount = 0;
  }

  @Override
  public void close() throws IOException {
    wrappedPostingsWriter.close();
  }
}

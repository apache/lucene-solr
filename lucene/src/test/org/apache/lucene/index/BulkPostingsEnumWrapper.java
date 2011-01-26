package org.apache.lucene.index;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.IOException;

import org.apache.lucene.index.BulkPostingsEnum.BlockReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * This class wraps a {@link BulkPostingsEnum} to use it as a {@link DocsAndPositionsEnum}  
 * <p>
 * Note: Payloads are currently not supported with this wrapper
 */
public class BulkPostingsEnumWrapper extends DocsAndPositionsEnum {
  private final BulkPostingsEnum docsEnum;

  private final BlockReader freqsReader;
  private final BlockReader docDeltasReader;
  private BlockReader positionDeltaReader;

  private final int[] docDeltas;
  private int docPointer;
  private int docPointerMax;
  private boolean first = true;

  private final int[] freqs;
  private int freqPointer;
  private int freqPointerMax;

  private final int[] pos;
  private int posPointer;
  private int posPointerMax;
  private int positionsPending;
  private int currentPos;

  private final Bits skipDocs;

  private int doc;
  private int docFreq;
  private int count;
  
  /**
   * Creates a new {@link BulkPostingsEnumWrapper}
   */
  public BulkPostingsEnumWrapper(BulkPostingsEnum bulkPostingsEnum,
      Bits skipDoc, int docFreq) throws IOException {
    this.docsEnum = bulkPostingsEnum;
    this.docFreq = docFreq;

    this.docDeltasReader = bulkPostingsEnum.getDocDeltasReader();
    this.docDeltas = docDeltasReader.getBuffer();
    this.freqsReader = bulkPostingsEnum.getFreqsReader();
    this.freqs = freqsReader == null ? null : freqsReader.getBuffer();
    this.positionDeltaReader = bulkPostingsEnum.getPositionDeltasReader();
    this.pos = positionDeltaReader == null ? null : positionDeltaReader
        .getBuffer();
    this.skipDocs = skipDoc;
    reset();

  }

  @Override
  public int nextPosition() throws IOException {
    if (positionDeltaReader != null) {
      if (--positionsPending >= 0) {
        if (++posPointer >= posPointerMax) {
          posPointerMax = positionDeltaReader.fill();
          assert posPointerMax != 0;
          posPointer = 0;
        }
        currentPos += pos[posPointer];
        return currentPos;
      }
      currentPos = 0;
      positionsPending = 0;
    }
    return -1;
  }

  @Override
  public BytesRef getPayload() throws IOException {
    // TODO
    return null;
  }

  @Override
  public boolean hasPayload() {
    // TODO
    return false;
  }

  @Override
  public int freq() {
    return freqsReader == null ? 1 : freqs[freqPointer];
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() throws IOException {
    while (count < docFreq) {
      fillDeltas();
      fillFreq();
      count++;
      doc += docDeltas[docPointer];
      first = false;
      assert doc >= 0 && (skipDocs == null || doc < skipDocs.length())
          && doc != NO_MORE_DOCS : "doc=" + doc + " skipDocs=" + skipDocs
          + " skipDocs.length="
          + (skipDocs == null ? "n/a" : skipDocs.length());
      if (skipDocs == null || !skipDocs.get(doc)) {
        return doc;
      }
    }

    return doc = NO_MORE_DOCS;
  }

  @Override
  public int advance(int target) throws IOException {
    // nocommit: should we, here, optimize .advance(target that isn't
    // too far away) into scan? seems like simple win?
    // first scan current doc deltas block
    for (docPointer++; docPointer < docPointerMax && count < docFreq; docPointer++) {
      assert first || docDeltas[docPointer] > 0;
      doc += docDeltas[docPointer];
      first = false;
      count++;
      fillFreq();
      if (doc >= target && (skipDocs == null || !skipDocs.get(doc))) {
        return doc;
      }
    }

    if (count == docFreq) {
      return doc = NO_MORE_DOCS;
    }

    // not found in current block, seek underlying stream
    final BulkPostingsEnum.JumpResult jumpResult;
    if (target - doc > docDeltas.length && // avoid useless jumps
        (jumpResult = docsEnum.jump(target, count)) != null) {
      count = jumpResult.count;
      doc = jumpResult.docID;
      first = false;
      reset();
    } else {
      // seek did not jump -- just fill next buffer
      docPointerMax = docDeltasReader.fill();
      if (docPointerMax != 0) {
        docPointer = 0;
        assert first || docDeltas[0] > 0;
        doc += docDeltas[0];
        count++;
        first = false;
      } else {
        return doc = NO_MORE_DOCS;
      }
      fillFreq();
    }

    // now scan -- let the compiler inline this
    return scan(target);
  }

  private int scan(final int target) throws IOException {
    while (true) {
      assert doc >= 0 && doc != NO_MORE_DOCS;
      if (doc >= target && (skipDocs == null || !skipDocs.get(doc))) {
        return doc;
      }

      if (count >= docFreq) {
        break;
      }

      if (++docPointer >= docPointerMax) {
        docPointerMax = docDeltasReader.fill();
        if (docPointerMax != 0) {
          docPointer = 0;
        } else {
          return doc = NO_MORE_DOCS;
        }
      }

      fillFreq();
      assert first || docDeltas[docPointer] > 0;
      doc += docDeltas[docPointer];
      count++;
    }
    return doc = NO_MORE_DOCS;
  }

  private void fillDeltas() throws IOException {
    if (++docPointer >= docPointerMax) {
      docPointerMax = docDeltasReader.fill();
      assert docPointerMax != 0;
      docPointer = 0;
    }
  }

  private void fillFreq() throws IOException {
    if (freqsReader != null) {
      if (++freqPointer >= freqPointerMax) {
        freqPointerMax = freqsReader.fill();
        assert freqPointerMax != 0;
        freqPointer = 0;
      }

      if (positionDeltaReader != null) {
        if (positionsPending > 0) {
          posPointer += positionsPending;
          while (posPointer >= posPointerMax) { // we need while here if
                                                // numPos
                                                // > buffersize
            posPointer -= posPointerMax; // add the pending positions from
                                         // last
                                         // round
            posPointerMax = positionDeltaReader.fill();
            assert posPointerMax != 0;
          }
        } else if (posPointer + 1 >= posPointerMax) {
          posPointerMax = positionDeltaReader.fill();
          assert posPointerMax != 0;
          posPointer = -1;
        }
        currentPos = 0;
        positionsPending = freqs[freqPointer];
      }
    }
  }

  private final void reset() throws IOException {
    docPointer = docDeltasReader.offset();
    docPointerMax = docDeltasReader.end();
    assert docPointerMax >= docPointer : "dP=" + docPointer + " dPMax="
        + docPointerMax;
    if (freqsReader != null) { // do we have freqs?
      freqPointer = freqsReader.offset();
      freqPointerMax = freqsReader.end();
      assert freqPointerMax >= freqPointer : "fP=" + freqPointer + " fPMax="
          + freqPointerMax;
      --docPointer;
      --freqPointer;

      if (positionDeltaReader != null) { // compiler should optimize this away
        currentPos = 0;
        posPointer = positionDeltaReader.offset();
        posPointerMax = positionDeltaReader.end();
        assert posPointerMax >= posPointer : "pP=" + posPointer + " pPMax="
            + posPointerMax;
        --posPointer;
        positionsPending = 0;
      }
    }
  }
}
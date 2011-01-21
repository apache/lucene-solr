package org.apache.lucene.search;

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

import org.apache.lucene.index.BulkPostingsEnum;
import org.apache.lucene.index.BulkPostingsEnum.BlockReader;
import org.apache.lucene.util.Bits;

// nocommit -- break out aligned & not cases?
// nocommit -- break out bulk vs doc-at-time scorer?

/** Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 */
final class TermScorer extends Scorer {
  private final BulkPostingsEnum docsEnum;
  private final byte[] norms;
  private final float weightValue;
  private int doc;

  private final int[] docDeltas;
  private int docPointer;
  private int docPointerMax;
  private boolean first = true;

  private final int[] freqs;
  private int freqPointer;
  private int freqPointerMax;

  private static final int SCORE_CACHE_SIZE = 32;
  private final float[] scoreCache = new float[SCORE_CACHE_SIZE];
  private final BlockReader freqsReader;
  private final BlockReader docDeltasReader;
  private final Bits skipDocs;
  private final int docFreq;
  private int count;
  private final Similarity similarity;
  
  /**
   * Construct a <code>TermScorer</code>.
   * 
   * @param weight
   *          The weight of the <code>Term</code> in the query.
   * @param td
   *          An iterator over the documents matching the <code>Term</code>.
   * @param similarity
   *          The </code>Similarity</code> implementation to be used for score
   *          computations.
   * @param norms
   *          The field norms of the document fields for the <code>Term</code>.
   */
  TermScorer(Weight weight, BulkPostingsEnum td, BlockReader docDeltaReader, BlockReader freqReader, int docFreq, Bits skipDocs, Similarity similarity, byte[] norms) throws IOException {
    super(weight);
    this.similarity = similarity;
    this.docsEnum = td;
    this.docFreq = docFreq;
    this.docDeltasReader = docDeltaReader;
    docDeltas = docDeltasReader.getBuffer();
    this.freqsReader = freqReader;
    freqs = freqsReader.getBuffer();
    reset();
    this.skipDocs = skipDocs;
    this.norms = norms;
    this.weightValue = weight.getValue();

    for (int i = 0; i < SCORE_CACHE_SIZE; i++)
      scoreCache[i] = similarity.tf(i) * weightValue;
  }

  @Override
  public void score(Collector c) throws IOException {
    score(c, Integer.MAX_VALUE, nextDoc());
  }

  // firstDocID is ignored since nextDoc() sets 'doc'
  @Override
  public boolean score(Collector c, int end, int firstDocID) throws IOException {
    c.setScorer(this);
    // nocommit -- this can leave scorer on a deleted doc...
    while (doc < end) {                           // for docs in window
      if (skipDocs == null || !skipDocs.get(doc)) {
        c.collect(doc);                      // collect
      }
      if (count == docFreq) {
        doc = NO_MORE_DOCS;
        return false;
      }
      count++;
      fillDeltas();
      fillFreq();
      doc += docDeltas[docPointer];
    }
    return true;
  }

  @Override
  public int docID() {
    return first ? -1 : doc;
  }

  @Override
  public float freq() {
    return freqs[freqPointer];
  }

  /**
   * Advances to the next document matching the query. <br>
   * The iterator over the matching documents is buffered using
   * {@link TermDocs#read(int[],int[])}.
   * 
   * @return the document matching the query or NO_MORE_DOCS if there are no more documents.
   */
  @Override
  public int nextDoc() throws IOException {
    while(count < docFreq) {
      fillDeltas();
      fillFreq();
      count++;
      doc += docDeltas[docPointer];
      first = false;
      assert doc >= 0 && (skipDocs == null || doc < skipDocs.length()) && doc != NO_MORE_DOCS: "doc=" + doc + " skipDocs=" + skipDocs + " skipDocs.length=" + (skipDocs==null? "n/a" : skipDocs.length());
      if (skipDocs == null || !skipDocs.get(doc)) {
        return doc;
      }
    }

    return doc = NO_MORE_DOCS;
  }

  @Override
  public float score() {
    assert !first;
    final int freq = freqs[freqPointer];
    assert freq > 0;
    assert doc != NO_MORE_DOCS;
    float raw =                                   // compute tf(f)*weight
      freq < SCORE_CACHE_SIZE                        // check cache
      ? scoreCache[freq]                             // cache hit
      : similarity.tf(freq)*weightValue;        // cache miss

    return norms == null ? raw : raw * similarity.decodeNormValue(norms[doc]); // normalize for field
  }

  /**
   * Advances to the first match beyond the current whose document number is
   * greater than or equal to a given target. <br>
   * The implementation uses {@link DocsEnum#advance(int)}.
   * 
   * @param target
   *          The target document number.
   * @return the matching document or NO_MORE_DOCS if none exist.
   */
  @Override
  public int advance(final int target) throws IOException {

    // nocommit: should we, here, optimize .advance(target that isn't
    // too far away) into scan?  seems like simple win?

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
    while(true) {
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

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() { return "scorer(" + weight + ")"; }
  
  private final void fillFreq() throws IOException {
    if (++freqPointer >= freqPointerMax) {
      freqPointerMax = freqsReader.fill();
      assert freqPointerMax != 0;
      freqPointer = 0;
    }
  }
  
  private void fillDeltas() throws IOException {
    if (++docPointer >= docPointerMax) {
      docPointerMax = docDeltasReader.fill();
      assert docPointerMax != 0;
      docPointer = 0;
    }
  }
  
  private final void reset() throws IOException {
    docPointer = docDeltasReader.offset();
    docPointerMax = docDeltasReader.end();
    assert docPointerMax >= docPointer: "dP=" + docPointer + " dPMax=" + docPointerMax;
    freqPointer = freqsReader.offset();
    freqPointerMax = freqsReader.end();
    assert freqPointerMax >= freqPointer: "fP=" + freqPointer + " fPMax=" + freqPointerMax;
    --docPointer;
    --freqPointer;
  }
}

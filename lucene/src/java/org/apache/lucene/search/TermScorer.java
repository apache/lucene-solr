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

import org.apache.lucene.index.DocsEnum;

/** Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 */
final class TermScorer extends Scorer {
  private DocsEnum docsEnum;
  private int doc = -1;
  private int freq;

  private int pointer;
  private int pointerMax;

  private int[] docs;
  private int[] freqs;
  private final DocsEnum.BulkReadResult bulkResult;
  private final Similarity.ExactDocScorer docScorer;
  
  /**
   * Construct a <code>TermScorer</code>.
   * 
   * @param weight
   *          The weight of the <code>Term</code> in the query.
   * @param td
   *          An iterator over the documents matching the <code>Term</code>.
   * @param docScorer
   *          The </code>Similarity.ExactDocScorer</code> implementation 
   *          to be used for score computations.
   */
  TermScorer(Weight weight, DocsEnum td, Similarity.ExactDocScorer docScorer) throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.docsEnum = td;
    bulkResult = td.getBulkResult();
  }

  @Override
  public void score(Collector c) throws IOException {
    score(c, Integer.MAX_VALUE, nextDoc());
  }

  private final void refillBuffer() throws IOException {
    pointerMax = docsEnum.read();  // refill
    docs = bulkResult.docs.ints;
    freqs = bulkResult.freqs.ints;
  }

  // firstDocID is ignored since nextDoc() sets 'doc'
  @Override
  public boolean score(Collector c, int end, int firstDocID) throws IOException {
    c.setScorer(this);
    while (doc < end) {                           // for docs in window
      //System.out.println("TS: collect doc=" + doc);
      c.collect(doc);                      // collect score
      if (++pointer >= pointerMax) {
        refillBuffer();
        if (pointerMax != 0) {
          pointer = 0;
        } else {
          doc = NO_MORE_DOCS;                // set to sentinel value
          return false;
        }
      } 
      doc = docs[pointer];
      freq = freqs[pointer];
    }
    return true;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public float freq() {
    return freq;
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
    pointer++;
    if (pointer >= pointerMax) {
      refillBuffer();
      if (pointerMax != 0) {
        pointer = 0;
      } else {
        return doc = NO_MORE_DOCS;
      }
    } 
    doc = docs[pointer];
    freq = freqs[pointer];
    assert doc != NO_MORE_DOCS;
    return doc;
  }
  
  @Override
  public float score() {
    assert doc != NO_MORE_DOCS;
    return docScorer.score(doc, freq);  
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
  public int advance(int target) throws IOException {
    // first scan in cache
    for (pointer++; pointer < pointerMax; pointer++) {
      if (docs[pointer] >= target) {
        freq = freqs[pointer];
        return doc = docs[pointer];
      }
    }

    // not found in readahead cache, seek underlying stream
    int newDoc = docsEnum.advance(target);
    //System.out.println("ts.advance docsEnum=" + docsEnum);
    if (newDoc != NO_MORE_DOCS) {
      doc = newDoc;
      freq = docsEnum.freq();
    } else {
      doc = NO_MORE_DOCS;
    }
    return doc;
  }

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() { return "scorer(" + weight + ")"; }

}

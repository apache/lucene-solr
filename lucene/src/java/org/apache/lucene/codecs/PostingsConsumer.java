package org.apache.lucene.codecs;

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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * @lucene.experimental
 */

public abstract class PostingsConsumer {

  /** Adds a new doc in this term. */
  public abstract void startDoc(int docID, int termDocFreq) throws IOException;

  public static class PostingsMergeState {
    DocsEnum docsEnum;
    int[] docMap;
    int docBase;
  }

  /** Add a new position & payload, and start/end offset.  A
   *  null payload means no payload; a non-null payload with
   *  zero length also means no payload.  Caller may reuse
   *  the {@link BytesRef} for the payload between calls
   *  (method must fully consume the payload). */
  public abstract void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException;

  /** Called when we are done adding positions & payloads
   *  for each doc. */
  public abstract void finishDoc() throws IOException;

  /** Default merge impl: append documents, mapping around
   *  deletes */
  public TermStats merge(final MergeState mergeState, final DocsEnum postings, final FixedBitSet visitedDocs) throws IOException {

    int df = 0;
    long totTF = 0;

    if (mergeState.fieldInfo.indexOptions == IndexOptions.DOCS_ONLY) {
      while(true) {
        final int doc = postings.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        visitedDocs.set(doc);
        this.startDoc(doc, 0);
        this.finishDoc();
        df++;
      }
      totTF = -1;
    } else if (mergeState.fieldInfo.indexOptions == IndexOptions.DOCS_AND_FREQS) {
      while(true) {
        final int doc = postings.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        visitedDocs.set(doc);
        final int freq = postings.freq();
        this.startDoc(doc, freq);
        this.finishDoc();
        df++;
        totTF += freq;
      }
    } else if (mergeState.fieldInfo.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
      final DocsAndPositionsEnum postingsEnum = (DocsAndPositionsEnum) postings;
      while(true) {
        final int doc = postingsEnum.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        visitedDocs.set(doc);
        final int freq = postingsEnum.freq();
        this.startDoc(doc, freq);
        totTF += freq;
        for(int i=0;i<freq;i++) {
          final int position = postingsEnum.nextPosition();
          final BytesRef payload;
          if (postingsEnum.hasPayload()) {
            payload = postingsEnum.getPayload();
          } else {
            payload = null;
          }
          this.addPosition(position, payload, -1, -1);
        }
        this.finishDoc();
        df++;
      }
    } else {
      assert mergeState.fieldInfo.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
      final DocsAndPositionsEnum postingsEnum = (DocsAndPositionsEnum) postings;
      while(true) {
        final int doc = postingsEnum.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        visitedDocs.set(doc);
        final int freq = postingsEnum.freq();
        this.startDoc(doc, freq);
        totTF += freq;
        for(int i=0;i<freq;i++) {
          final int position = postingsEnum.nextPosition();
          final BytesRef payload;
          if (postingsEnum.hasPayload()) {
            payload = postingsEnum.getPayload();
          } else {
            payload = null;
          }
          this.addPosition(position, payload, postingsEnum.startOffset(), postingsEnum.endOffset());
        }
        this.finishDoc();
        df++;
      }
    }
    return new TermStats(df, totTF);
  }
}

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
import java.util.Arrays;

import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.Similarity;

final class ExactPhraseScorer extends Scorer {
  private final int endMinus1;

  private final static int CHUNK = 4096;

  private int gen;
  private final int[] counts = new int[CHUNK];
  private final int[] gens = new int[CHUNK];

  boolean noDocs;

  private final static class ChunkState {
    final DocsAndPositionsEnum posEnum;
    final int offset;
    final boolean useAdvance;
    int posUpto;
    int posLimit;
    int pos;
    int lastPos;

    public ChunkState(DocsAndPositionsEnum posEnum, int offset, boolean useAdvance) {
      this.posEnum = posEnum;
      this.offset = offset;
      this.useAdvance = useAdvance;
    }
  }

  private final ChunkState[] chunkStates;

  private int docID = -1;
  private int freq;

  private final Similarity.ExactSimScorer docScorer;
  
  ExactPhraseScorer(Weight weight, PhraseQuery.PostingsAndFreq[] postings,
                    Similarity.ExactSimScorer docScorer) throws IOException {
    super(weight);
    this.docScorer = docScorer;

    chunkStates = new ChunkState[postings.length];

    endMinus1 = postings.length-1;

    for(int i=0;i<postings.length;i++) {

      // Coarse optimization: advance(target) is fairly
      // costly, so, if the relative freq of the 2nd
      // rarest term is not that much (> 1/5th) rarer than
      // the first term, then we just use .nextDoc() when
      // ANDing.  This buys ~15% gain for phrases where
      // freq of rarest 2 terms is close:
      final boolean useAdvance = postings[i].docFreq > 5*postings[0].docFreq;
      chunkStates[i] = new ChunkState(postings[i].postings, -postings[i].position, useAdvance);
      if (i > 0 && postings[i].postings.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
        noDocs = true;
        return;
      }
    }
  }

  @Override
  public int nextDoc() throws IOException {
    while(true) {

      // first (rarest) term
      final int doc = chunkStates[0].posEnum.nextDoc();
      if (doc == DocIdSetIterator.NO_MORE_DOCS) {
        docID = doc;
        return doc;
      }

      // not-first terms
      int i = 1;
      while(i < chunkStates.length) {
        final ChunkState cs = chunkStates[i];
        int doc2 = cs.posEnum.docID();
        if (cs.useAdvance) {
          if (doc2 < doc) {
            doc2 = cs.posEnum.advance(doc);
          }
        } else {
          int iter = 0;
          while(doc2 < doc) {
            // safety net -- fallback to .advance if we've
            // done too many .nextDocs
            if (++iter == 50) {
              doc2 = cs.posEnum.advance(doc);
              break;
            } else {
              doc2 = cs.posEnum.nextDoc();
            }
          }
        }
        if (doc2 > doc) {
          break;
        }
        i++;
      }

      if (i == chunkStates.length) {
        // this doc has all the terms -- now test whether
        // phrase occurs
        docID = doc;

        freq = phraseFreq();
        if (freq != 0) {
          return docID;
        }
      }
    }
  }

  @Override
  public int advance(int target) throws IOException {

    // first term
    int doc = chunkStates[0].posEnum.advance(target);
    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
      docID = DocIdSetIterator.NO_MORE_DOCS;
      return doc;
    }

    while(true) {
      
      // not-first terms
      int i = 1;
      while(i < chunkStates.length) {
        int doc2 = chunkStates[i].posEnum.docID();
        if (doc2 < doc) {
          doc2 = chunkStates[i].posEnum.advance(doc);
        }
        if (doc2 > doc) {
          break;
        }
        i++;
      }

      if (i == chunkStates.length) {
        // this doc has all the terms -- now test whether
        // phrase occurs
        docID = doc;
        freq = phraseFreq();
        if (freq != 0) {
          return docID;
        }
      }

      doc = chunkStates[0].posEnum.nextDoc();
      if (doc == DocIdSetIterator.NO_MORE_DOCS) {
        docID = doc;
        return doc;
      }
    }
  }

  @Override
  public String toString() {
    return "ExactPhraseScorer(" + weight + ")";
  }

  @Override
  public float freq() {
    return freq;
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public float score() throws IOException {
    return docScorer.score(docID, freq);
  }

  private int phraseFreq() throws IOException {

    freq = 0;

    // init chunks
    for(int i=0;i<chunkStates.length;i++) {
      final ChunkState cs = chunkStates[i];
      cs.posLimit = cs.posEnum.freq();
      cs.pos = cs.offset + cs.posEnum.nextPosition();
      cs.posUpto = 1;
      cs.lastPos = -1;
    }

    int chunkStart = 0;
    int chunkEnd = CHUNK;

    // process chunk by chunk
    boolean end = false;

    // TODO: we could fold in chunkStart into offset and
    // save one subtract per pos incr

    while(!end) {

      gen++;

      if (gen == 0) {
        // wraparound
        Arrays.fill(gens, 0);
        gen++;
      }

      // first term
      {
        final ChunkState cs = chunkStates[0];
        while(cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            counts[posIndex] = 1;
            assert gens[posIndex] != gen;
            gens[posIndex] = gen;
          }

          if (cs.posUpto == cs.posLimit) {
            end = true;
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }
      }

      // middle terms
      boolean any = true;
      for(int t=1;t<endMinus1;t++) {
        final ChunkState cs = chunkStates[t];
        any = false;
        while(cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            if (posIndex >= 0 && gens[posIndex] == gen && counts[posIndex] == t) {
              // viable
              counts[posIndex]++;
              any = true;
            }
          }

          if (cs.posUpto == cs.posLimit) {
            end = true;
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }

        if (!any) {
          break;
        }
      }

      if (!any) {
        // petered out for this chunk
        chunkStart += CHUNK;
        chunkEnd += CHUNK;
        continue;
      }

      // last term

      {
        final ChunkState cs = chunkStates[endMinus1];
        while(cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            if (posIndex >= 0 && gens[posIndex] == gen && counts[posIndex] == endMinus1) {
              freq++;
            }
          }

          if (cs.posUpto == cs.posLimit) {
            end = true;
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }
      }

      chunkStart += CHUNK;
      chunkEnd += CHUNK;
    }

    return freq;
  }
}

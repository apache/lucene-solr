package org.apache.lucene.search;

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
import java.util.Arrays;

import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.Similarity;

final class ExactPhraseScorer extends Scorer {
  private final int endMinus1;

  private final static int CHUNK = 4096;

  private int gen;
  private final int[] counts = new int[CHUNK];
  private final int[] gens = new int[CHUNK];

  private final long cost;

  private final static class ChunkState {
    final DocsAndPositionsEnum posEnum;
    final int offset;
    int posUpto;
    int posLimit;
    int pos;
    int lastPos;

    public ChunkState(DocsAndPositionsEnum posEnum, int offset) {
      this.posEnum = posEnum;
      this.offset = offset;
    }
  }

  private final ChunkState[] chunkStates;
  private final DocsAndPositionsEnum lead;

  private int docID = -1;
  private int freq;

  private final Similarity.SimScorer docScorer;
  
  ExactPhraseScorer(Weight weight, PhraseQuery.PostingsAndFreq[] postings,
                    Similarity.SimScorer docScorer) throws IOException {
    super(weight);
    this.docScorer = docScorer;

    chunkStates = new ChunkState[postings.length];

    endMinus1 = postings.length-1;
    
    lead = postings[0].postings;
    // min(cost)
    cost = lead.cost();

    for(int i=0;i<postings.length;i++) {
      chunkStates[i] = new ChunkState(postings[i].postings, -postings[i].position);
    }
  }
  
  private int doNext(int doc) throws IOException {
    for(;;) {
      // TODO: don't dup this logic from conjunctionscorer :)
      advanceHead: for(;;) {
        for (int i = 1; i < chunkStates.length; i++) {
          final DocsAndPositionsEnum de = chunkStates[i].posEnum;
          if (de.docID() < doc) {
            int d = de.advance(doc);

            if (d > doc) {
              // DocsEnum beyond the current doc - break and advance lead to the new highest doc.
              doc = d;
              break advanceHead;
            }
          }
        }
        // all DocsEnums are on the same doc
        if (doc == NO_MORE_DOCS) {
          return doc;
        } else if (phraseFreq() > 0) {
          return doc;            // success: matches phrase
        } else {
          doc = lead.nextDoc();  // doesn't match phrase
        }
      }
      // advance head for next iteration
      doc = lead.advance(doc);
    }
  }

  @Override
  public int nextDoc() throws IOException {
    return docID = doNext(lead.nextDoc());
  }

  @Override
  public int advance(int target) throws IOException {
    return docID = doNext(lead.advance(target));
  }

  @Override
  public String toString() {
    return "ExactPhraseScorer(" + weight + ")";
  }

  @Override
  public int freq() {
    return freq;
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public float score() {
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

  @Override
  public long cost() {
    return cost;
  }
}

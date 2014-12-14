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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.PhraseQuery.TermDocsEnumFactory;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

final class ExactPhraseScorer extends Scorer {
  private final int endMinus1;
  
  private final static int CHUNK = 4096;
  
  private int gen;
  private final int[] counts = new int[CHUNK];
  private final int[] gens = new int[CHUNK];
  private final int[] offsets = new int[CHUNK];

  private final long cost;

  private final static class ChunkState {
    final TermDocsEnumFactory factory;
    final DocsEnum posEnum;
    final int offset;
    int posUpto;
    int posLimit;
    int pos;
    int lastPos;

    public ChunkState(TermDocsEnumFactory factory, DocsEnum posEnum, int offset) {
      this.factory = factory;
      this.posEnum = posEnum;
      this.offset = offset;
    }
  }
  
  private final ChunkState[] chunkStates;
  private final DocsEnum lead;

  private int docID = -1;

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
      chunkStates[i] = new ChunkState(postings[i].factory, postings[i].postings, -postings[i].position);
    }
  }
  
  private int doNext(int doc) throws IOException {
    for(;;) {
      // TODO: don't dup this logic from conjunctionscorer :)
      advanceHead: for(;;) {
        for (int i = 1; i < chunkStates.length; i++) {
          final DocsEnum de = chunkStates[i].posEnum;
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
        } else if (firstPosition() != NO_MORE_POSITIONS) {
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
  public int docID() {
    return docID;
  }
  
  @Override
  public float score() throws IOException {
    return docScorer.score(docID, phraseFreq());
  }

  private int freq = -1;

  protected int phraseFreq() throws IOException {
    if (freq != -1)
      return freq;
    freq = 0;
    while (nextPosition() != NO_MORE_POSITIONS)
      freq++;
    return freq;
  }

  private int chunkStart = 0;
  private int chunkEnd = CHUNK;

  private int posRemaining;
  private int positionsInChunk;
  private boolean cached = false;

  private void resetPositions() throws IOException {
    chunkStart = 0;
    chunkEnd = CHUNK;
    posRemaining = 0;
    cached = false;
    freq = -1;
    for (final ChunkState cs : chunkStates) {
      cs.posLimit = cs.posEnum.freq();
      cs.pos = cs.offset + cs.posEnum.nextPosition();
      cs.posUpto = 1;
      cs.lastPos = -1;
    }
  }

  private int firstPosition() throws IOException {
    resetPositions();
    int pos = nextPosition();
    cached = true;
    return pos;
  }

  @Override
  public int startPosition() throws IOException {
    return posQueue[positionsInChunk - posRemaining - 1];
  }

  @Override
  public int startOffset() throws IOException {
    return offsetQueue[(positionsInChunk - posRemaining - 1) / 2];
  }

  @Override
  public int endOffset() throws IOException {
    return offsetQueue[(positionsInChunk - posRemaining - 1) / 2 + 1];
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return null;  // TODO payloads over intervals
  }

  @Override
  public int endPosition() throws IOException {
    return startPosition() + chunkStates.length - 1;
  }

  @Override
  public int freq() throws IOException {
    return phraseFreq();
  }

  @Override
  public int nextPosition() throws IOException {
    if (cached) {
      cached = false;
      return startPosition();
    }

    if (posRemaining == 0 && !findNextMatches())
      return NO_MORE_POSITIONS;

    if (posRemaining == 0)
      return NO_MORE_POSITIONS;

    posRemaining--;
    return startPosition();
  }

  int[] posQueue = new int[8];
  int[] offsetQueue = new int[16];

  private void addPosition(int pos, int beginOffset, int endOffset) {
    positionsInChunk++;
    if (posQueue.length < positionsInChunk) {
      int[] newQueue = new int[posQueue.length * 2];
      System.arraycopy(posQueue, 0, newQueue, 0, posQueue.length);
      posQueue = newQueue;
      int[] newOffsets = new int[posQueue.length * 2];
      System.arraycopy(offsetQueue, 0, newOffsets, 0, offsetQueue.length);
      offsetQueue = newOffsets;
    }
    posQueue[positionsInChunk - 1] = pos;
    offsetQueue[(positionsInChunk - 1) * 2] = beginOffset;
    offsetQueue[(positionsInChunk - 1) * 2 + 1] = endOffset;
  }

  private boolean findNextMatches() throws IOException {

    // TODO: we could fold in chunkStart into phraseOffset and
    // save one subtract per pos incr

    boolean exhausted = false;

    while (true) {
      positionsInChunk = 0;
      gen++;

      if (gen == 0) {
        // wraparound
        Arrays.fill(gens, 0);
        gen++;
      }

      boolean any = false;

      // first term
      {
        final ChunkState cs = chunkStates[0];
        while (cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            counts[posIndex] = 1;
            any = true;
            assert gens[posIndex] != gen;
            gens[posIndex] = gen;
            offsets[posIndex] = cs.posEnum.startOffset();
          }

          if (cs.posUpto == cs.posLimit) {
            exhausted = true;
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }
      }

      // middle terms
      for (int t = 1; t < endMinus1; t++) {
        final ChunkState cs = chunkStates[t];
        any = false;
        while (cs.pos < chunkEnd) {
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
            exhausted = true;
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
        if (exhausted)
          return false;
        continue;
      }

      // last term

      {
        any = false;
        final ChunkState cs = chunkStates[endMinus1];
        while (cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            if (posIndex >= 0 && gens[posIndex] == gen
                && counts[posIndex] == endMinus1) {
              addPosition(cs.pos, offsets[posIndex], cs.posEnum.endOffset());
              any = true;
            }
          }

          if (cs.posUpto == cs.posLimit) {
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }
      }

      chunkStart += CHUNK;
      chunkEnd += CHUNK;

      if (any) {
        posRemaining = positionsInChunk;
        return true;
      }
    }
  }

  @Override
  public long cost() {
    return cost;
  }

}

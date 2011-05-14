package org.apache.lucene.search.grouping;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Caches all docs, and optionally also scores, coming from
 * a search, and is then able to replay them to another
 * collector.  You specify the max RAM this class may use.
 * Once the collection is done, call {@link #isCached}.  If
 * this returns true, you can use {@link #replay} against a
 * new collector.  If it returns false, this means too much
 * RAM was required and you must instead re-run the original
 * search.
 *
 * <p><b>NOTE</b>: this class consumes 4 (or 8 bytes, if
 * scoring is cached) per collected document.  If the result
 * set is large this can easily be a very substantial amount
 * of RAM!
 * 
 * <p>See {@link org.apache.lucene.search.grouping} for more
 * details including a full code example.</p>
 *
 * @lucene.experimental
 */
public class CachingCollector extends Collector {
  
  private static class SegStart {
    public final IndexReader reader;
    public final int base;
    public final int end;

    public SegStart(IndexReader reader, int base, int end) {
      this.reader = reader;
      this.base = base;
      this.end = end;
    }
  }

  // TODO: would be nice if a collector defined a
  // needsScores() method so we can specialize / do checks
  // up front:
  private final Collector other;
  private final int maxDocsToCache;

  private final Scorer cachedScorer;
  private final List<int[]> cachedDocs;
  private final List<float[]> cachedScores;
  private final List<SegStart> cachedSegs = new ArrayList<SegStart>();

  private Scorer scorer;
  private int[] curDocs;
  private float[] curScores;
  private int upto;
  private IndexReader lastReader;
  private float score;
  private int base;
  private int lastDocBase;
  private int doc;

  public CachingCollector(Collector other, boolean cacheScores, double maxRAMMB) {
    this.other = other;
    if (cacheScores) {
      cachedScorer = new Scorer((Similarity) null) {
          @Override
          public float score() {
            return score;
          }

          @Override
          public int advance(int target) {
            throw new UnsupportedOperationException();
          }

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public float freq() {
            throw new UnsupportedOperationException();
          }

          @Override
          public int nextDoc() {
            throw new UnsupportedOperationException();
          }
        };
      cachedScores = new ArrayList<float[]>();
      curScores = new float[128];
      cachedScores.add(curScores);
    } else {
      cachedScorer = null;
      cachedScores = null;
    }
    cachedDocs = new ArrayList<int[]>();
    curDocs = new int[128];
    cachedDocs.add(curDocs);

    final int bytesPerDoc;
    if (curScores != null) {
      bytesPerDoc = RamUsageEstimator.NUM_BYTES_INT + RamUsageEstimator.NUM_BYTES_FLOAT;
    } else {
      bytesPerDoc = RamUsageEstimator.NUM_BYTES_INT;
    }
    maxDocsToCache = (int) ((maxRAMMB * 1024 * 1024)/bytesPerDoc);
  }
  
  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    other.setScorer(cachedScorer);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return other.acceptsDocsOutOfOrder();
  }

  @Override
  public void collect(int doc) throws IOException {

    if (curDocs == null) {
      // Cache was too large
      if (curScores != null) {
        score = scorer.score();
      }
      this.doc = doc;
      other.collect(doc);
      return;
    }

    if (upto == curDocs.length) {
      base += upto;
      final int nextLength;
      // Max out at 512K arrays:
      if (curDocs.length < 524288) {
        nextLength = 8*curDocs.length;
      } else {
        nextLength = curDocs.length;
      }

      if (base + nextLength > maxDocsToCache) {
        // Too many docs to collect -- clear cache
        curDocs = null;
        if (curScores != null) {
          score = scorer.score();
        }
        this.doc = doc;
        other.collect(doc);
        cachedDocs.clear();
        cachedScores.clear();
        return;
      }
      curDocs = new int[nextLength];
      cachedDocs.add(curDocs);
      if (curScores != null) {
        curScores = new float[nextLength];
        cachedScores.add(curScores);
      }
      upto = 0;
    }
    curDocs[upto] = doc;
    // TODO: maybe specialize private subclass so we don't
    // null check per collect...
    if (curScores != null) {
      score = curScores[upto] = scorer.score();
    }
    upto++;
    this.doc = doc;
    other.collect(doc);
  }

  public boolean isCached() {
    return curDocs != null;
  }

  @Override  
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    other.setNextReader(reader, docBase);
    if (lastReader != null) {
      cachedSegs.add(new SegStart(lastReader, lastDocBase, base+upto));
    }
    lastDocBase = docBase;
    lastReader = reader;
  }

  private final static int[] EMPTY_INT_ARRAY = new int[0];

  @Override
  public String toString() {
    if (isCached()) {
      return "CachingCollector (" + (base+upto) + " docs " + (curScores != null ? " & scores" : "") + " cached)";
    } else {
      return "CachingCollector (cache was cleared)";
    }
  }

  public void replay(Collector other) throws IOException {
    if (!isCached()) {
      throw new IllegalStateException("cannot replay: cache was cleared because too much RAM was required");
    }
    //System.out.println("CC: replay totHits=" + (upto + base));
    if (lastReader != null) {
      cachedSegs.add(new SegStart(lastReader, lastDocBase, base+upto));
      lastReader = null;
    }
    final int uptoSav = upto;
    final int baseSav = base;
    try {
      upto = 0;
      base = 0;
      int chunkUpto = 0;
      other.setScorer(cachedScorer);
      curDocs = EMPTY_INT_ARRAY;
      for(SegStart seg : cachedSegs) {
        other.setNextReader(seg.reader, seg.base);
        while(base+upto < seg.end) {
          if (upto == curDocs.length) {
            base += curDocs.length;
            curDocs = cachedDocs.get(chunkUpto);
            if (curScores != null) {
              curScores = cachedScores.get(chunkUpto);
            }
            chunkUpto++;
            upto = 0;
          }
          if (curScores != null) {
            score = curScores[upto];
          }
          other.collect(curDocs[upto++]);
        }
      }
    } finally {
      upto = uptoSav;
      base = baseSav;
    }
  }
}

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
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
 * <p><b>NOTE</b>: this class caches at least 128 documents
 * before checking RAM limits.
 * 
 * <p>See {@link org.apache.lucene.search.grouping} for more
 * details including a full code example.</p>
 *
 * @lucene.experimental
 */
public class CachingCollector extends Collector {
  
  // Max out at 512K arrays
  private static final int MAX_ARRAY_SIZE = 512 * 1024;
  private static final int INITIAL_ARRAY_SIZE = 128;
  private final static int[] EMPTY_INT_ARRAY = new int[0];

  private static class SegStart {
    public final AtomicReaderContext readerContext;
    public final int end;

    public SegStart(AtomicReaderContext readerContext, int end) {
      this.readerContext = readerContext;
      this.end = end;
    }
  }
  
  private static class CachedScorer extends Scorer {

    // NOTE: these members are package-private b/c that way accessing them from
    // the outer class does not incur access check by the JVM. The same
    // situation would be if they were defined in the outer class as private
    // members.
    int doc;
    float score;
    
    private CachedScorer() { super(null); }

    @Override
    public float score() { return score; }

    @Override
    public int advance(int target) { throw new UnsupportedOperationException(); }

    @Override
    public int docID() { return doc; }

    @Override
    public float freq() { throw new UnsupportedOperationException(); }

    @Override
    public int nextDoc() { throw new UnsupportedOperationException(); }
  }

  // TODO: would be nice if a collector defined a
  // needsScores() method so we can specialize / do checks
  // up front:
  private final Collector other;
  private final int maxDocsToCache;

  private final boolean cacheScores;
  private final CachedScorer cachedScorer;
  private final List<int[]> cachedDocs;
  private final List<float[]> cachedScores;
  private final List<SegStart> cachedSegs = new ArrayList<SegStart>();

  private Scorer scorer;
  private int[] curDocs;
  private float[] curScores;
  private int upto;
  private AtomicReaderContext lastReaderContext;
  private int base;

  public CachingCollector(Collector other, boolean cacheScores, double maxRAMMB) {
    this.other = other;
    this.cacheScores = cacheScores;
    if (cacheScores) {
      cachedScorer = new CachedScorer();
      cachedScores = new ArrayList<float[]>();
      curScores = new float[128];
      cachedScores.add(curScores);
    } else {
      cachedScorer = null;
      cachedScores = null;
    }
    cachedDocs = new ArrayList<int[]>();
    curDocs = new int[INITIAL_ARRAY_SIZE];
    cachedDocs.add(curDocs);

    int bytesPerDoc = RamUsageEstimator.NUM_BYTES_INT;
    if (cacheScores) {
      bytesPerDoc += RamUsageEstimator.NUM_BYTES_FLOAT;
    }
    maxDocsToCache = (int) ((maxRAMMB * 1024 * 1024) / bytesPerDoc);
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
      if (cacheScores) {
        cachedScorer.score = scorer.score();
      }
      cachedScorer.doc = doc;
      other.collect(doc);
      return;
    }

    // Allocate a bigger array or abort caching
    if (upto == curDocs.length) {
      base += upto;
      
      // Compute next array length - don't allocate too big arrays
      int nextLength = 8*curDocs.length;
      if (nextLength > MAX_ARRAY_SIZE) {
        nextLength = MAX_ARRAY_SIZE;
      }

      if (base + nextLength > maxDocsToCache) {
        // try to allocate a smaller array
        nextLength = maxDocsToCache - base;
        if (nextLength <= 0) {
          // Too many docs to collect -- clear cache
          curDocs = null;
          curScores = null;
          cachedSegs.clear();
          cachedDocs.clear();
          cachedScores.clear();
          if (cacheScores) {
            cachedScorer.score = scorer.score();
          }
          cachedScorer.doc = doc;
          other.collect(doc);
          return;
        }
      }
      
      curDocs = new int[nextLength];
      cachedDocs.add(curDocs);
      if (cacheScores) {
        curScores = new float[nextLength];
        cachedScores.add(curScores);
      }
      upto = 0;
    }
    
    curDocs[upto] = doc;
    // TODO: maybe specialize private subclass so we don't
    // null check per collect...
    if (cacheScores) {
      cachedScorer.score = curScores[upto] = scorer.score();
    }
    upto++;
    cachedScorer.doc = doc;
    other.collect(doc);
  }

  public boolean isCached() {
    return curDocs != null;
  }

  @Override  
  public void setNextReader(AtomicReaderContext context) throws IOException {
    other.setNextReader(context);
    if (lastReaderContext != null) {
      cachedSegs.add(new SegStart(lastReaderContext, base+upto));
    }
    lastReaderContext = context;
  }

  @Override
  public String toString() {
    if (isCached()) {
      return "CachingCollector (" + (base+upto) + " docs " + (cacheScores ? " & scores" : "") + " cached)";
    } else {
      return "CachingCollector (cache was cleared)";
    }
  }

  /**
   * Replays the cached doc IDs (and scores) to the given Collector.
   * 
   * @throws IllegalStateException
   *           if this collector is not cached (i.e., if the RAM limits were too
   *           low for the number of documents + scores to cache).
   * @throws IllegalArgumentException
   *           if the given Collect's does not support out-of-order collection,
   *           while the collector passed to the ctor does.
   */
  public void replay(Collector other) throws IOException {
    if (!isCached()) {
      throw new IllegalStateException("cannot replay: cache was cleared because too much RAM was required");
    }
    
    if (!other.acceptsDocsOutOfOrder() && this.other.acceptsDocsOutOfOrder()) {
      throw new IllegalArgumentException(
          "cannot replay: given collector does not support "
              + "out-of-order collection, while the wrapped collector does. "
              + "Therefore cached documents may be out-of-order.");
    }

    //System.out.println("CC: replay totHits=" + (upto + base));
    if (lastReaderContext != null) {
      cachedSegs.add(new SegStart(lastReaderContext, base+upto));
      lastReaderContext = null;
    }
    
    int curupto = 0;
    int curbase = 0;
    int chunkUpto = 0;
    other.setScorer(cachedScorer);
    curDocs = EMPTY_INT_ARRAY;
    for(SegStart seg : cachedSegs) {
      other.setNextReader(seg.readerContext);
      while(curbase+curupto < seg.end) {
        if (curupto == curDocs.length) {
          curbase += curDocs.length;
          curDocs = cachedDocs.get(chunkUpto);
          if (cacheScores) {
            curScores = cachedScores.get(chunkUpto);
          }
          chunkUpto++;
          curupto = 0;
        }
        if (cacheScores) {
          cachedScorer.score = curScores[curupto];
        }
        other.collect(curDocs[curupto++]);
      }
    }
  }
}

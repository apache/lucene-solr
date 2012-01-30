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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Caches all docs, and optionally also scores, coming from
 * a search, and is then able to replay them to another
 * collector.  You specify the max RAM this class may use.
 * Once the collection is done, call {@link #isCached}. If
 * this returns true, you can use {@link #replay(Collector)}
 * against a new collector.  If it returns false, this means
 * too much RAM was required and you must instead re-run the
 * original search.
 *
 * <p><b>NOTE</b>: this class consumes 4 (or 8 bytes, if
 * scoring is cached) per collected document.  If the result
 * set is large this can easily be a very substantial amount
 * of RAM!
 * 
 * <p><b>NOTE</b>: this class caches at least 128 documents
 * before checking RAM limits.
 * 
 * <p>See the Lucene <tt>modules/grouping</tt> module for more
 * details including a full code example.</p>
 *
 * @lucene.experimental
 */
public abstract class CachingCollector extends Collector {
  
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
  
  private static final class CachedScorer extends Scorer {
    
    // NOTE: these members are package-private b/c that way accessing them from
    // the outer class does not incur access check by the JVM. The same
    // situation would be if they were defined in the outer class as private
    // members.
    int doc;
    float score;
    
    private CachedScorer() { super(null); }

    @Override
    public final float score() { return score; }
    
    @Override
    public final int advance(int target) { throw new UnsupportedOperationException(); }
    
    @Override
    public final int docID() { return doc; }
    
    @Override
    public final float freq() { throw new UnsupportedOperationException(); }
    
    @Override
    public final int nextDoc() { throw new UnsupportedOperationException(); }
    }

  // A CachingCollector which caches scores
  private static final class ScoreCachingCollector extends CachingCollector {

    private final CachedScorer cachedScorer;
    private final List<float[]> cachedScores;

    private Scorer scorer;
    private float[] curScores;

    ScoreCachingCollector(Collector other, double maxRAMMB) {
      super(other, maxRAMMB, true);

      cachedScorer = new CachedScorer();
      cachedScores = new ArrayList<float[]>();
      curScores = new float[INITIAL_ARRAY_SIZE];
      cachedScores.add(curScores);
    }

    ScoreCachingCollector(Collector other, int maxDocsToCache) {
      super(other, maxDocsToCache);

      cachedScorer = new CachedScorer();
      cachedScores = new ArrayList<float[]>();
      curScores = new float[INITIAL_ARRAY_SIZE];
      cachedScores.add(curScores);
    }
    
    @Override
    public void collect(int doc) throws IOException {

      if (curDocs == null) {
        // Cache was too large
        cachedScorer.score = scorer.score();
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
            cachedScorer.score = scorer.score();
            cachedScorer.doc = doc;
            other.collect(doc);
            return;
          }
        }
        
        curDocs = new int[nextLength];
        cachedDocs.add(curDocs);
        curScores = new float[nextLength];
        cachedScores.add(curScores);
        upto = 0;
      }
      
      curDocs[upto] = doc;
      cachedScorer.score = curScores[upto] = scorer.score();
      upto++;
      cachedScorer.doc = doc;
      other.collect(doc);
    }

    @Override
    public void replay(Collector other) throws IOException {
      replayInit(other);
      
      int curUpto = 0;
      int curBase = 0;
      int chunkUpto = 0;
      curDocs = EMPTY_INT_ARRAY;
      for (SegStart seg : cachedSegs) {
        other.setNextReader(seg.readerContext);
        other.setScorer(cachedScorer);
        while (curBase + curUpto < seg.end) {
          if (curUpto == curDocs.length) {
            curBase += curDocs.length;
            curDocs = cachedDocs.get(chunkUpto);
            curScores = cachedScores.get(chunkUpto);
            chunkUpto++;
            curUpto = 0;
          }
          cachedScorer.score = curScores[curUpto];
          cachedScorer.doc = curDocs[curUpto];
          other.collect(curDocs[curUpto++]);
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      other.setScorer(cachedScorer);
    }

    @Override
    public String toString() {
      if (isCached()) {
        return "CachingCollector (" + (base+upto) + " docs & scores cached)";
      } else {
        return "CachingCollector (cache was cleared)";
      }
    }

  }

  // A CachingCollector which does not cache scores
  private static final class NoScoreCachingCollector extends CachingCollector {
    
    NoScoreCachingCollector(Collector other, double maxRAMMB) {
     super(other, maxRAMMB, false);
    }

    NoScoreCachingCollector(Collector other, int maxDocsToCache) {
     super(other, maxDocsToCache);
    }

    @Override
    public void collect(int doc) throws IOException {

      if (curDocs == null) {
        // Cache was too large
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
            cachedSegs.clear();
            cachedDocs.clear();
            other.collect(doc);
            return;
          }
        }
        
        curDocs = new int[nextLength];
        cachedDocs.add(curDocs);
        upto = 0;
      }
      
      curDocs[upto] = doc;
      upto++;
      other.collect(doc);
    }

    @Override
    public void replay(Collector other) throws IOException {
      replayInit(other);
      
      int curUpto = 0;
      int curbase = 0;
      int chunkUpto = 0;
      curDocs = EMPTY_INT_ARRAY;
      for (SegStart seg : cachedSegs) {
        other.setNextReader(seg.readerContext);
        while (curbase + curUpto < seg.end) {
          if (curUpto == curDocs.length) {
            curbase += curDocs.length;
            curDocs = cachedDocs.get(chunkUpto);
            chunkUpto++;
            curUpto = 0;
          }
          other.collect(curDocs[curUpto++]);
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      other.setScorer(scorer);
    }

    @Override
    public String toString() {
      if (isCached()) {
        return "CachingCollector (" + (base+upto) + " docs cached)";
      } else {
        return "CachingCollector (cache was cleared)";
      }
    }

  }

  // TODO: would be nice if a collector defined a
  // needsScores() method so we can specialize / do checks
  // up front. This is only relevant for the ScoreCaching
  // version -- if the wrapped Collector does not need
  // scores, it can avoid cachedScorer entirely.
  protected final Collector other;
  
  protected final int maxDocsToCache;
  protected final List<SegStart> cachedSegs = new ArrayList<SegStart>();
  protected final List<int[]> cachedDocs;
  
  private AtomicReaderContext lastReaderContext;
  
  protected int[] curDocs;
  protected int upto;
  protected int base;
  protected int lastDocBase;
  
  /**
   * Creates a {@link CachingCollector} which does not wrap another collector.
   * The cached documents and scores can later be {@link #replay(Collector)
   * replayed}.
   * 
   * @param acceptDocsOutOfOrder
   *          whether documents are allowed to be collected out-of-order
   */
  public static CachingCollector create(final boolean acceptDocsOutOfOrder, boolean cacheScores, double maxRAMMB) {
    Collector other = new Collector() {
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return acceptDocsOutOfOrder;
      }
      
      @Override
      public void setScorer(Scorer scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {}

      @Override
      public void setNextReader(AtomicReaderContext context) throws IOException {}

    };
    return create(other, cacheScores, maxRAMMB);
  }

  /**
   * Create a new {@link CachingCollector} that wraps the given collector and
   * caches documents and scores up to the specified RAM threshold.
   * 
   * @param other
   *          the Collector to wrap and delegate calls to.
   * @param cacheScores
   *          whether to cache scores in addition to document IDs. Note that
   *          this increases the RAM consumed per doc
   * @param maxRAMMB
   *          the maximum RAM in MB to consume for caching the documents and
   *          scores. If the collector exceeds the threshold, no documents and
   *          scores are cached.
   */
  public static CachingCollector create(Collector other, boolean cacheScores, double maxRAMMB) {
    return cacheScores ? new ScoreCachingCollector(other, maxRAMMB) : new NoScoreCachingCollector(other, maxRAMMB);
  }

  /**
   * Create a new {@link CachingCollector} that wraps the given collector and
   * caches documents and scores up to the specified max docs threshold.
   *
   * @param other
   *          the Collector to wrap and delegate calls to.
   * @param cacheScores
   *          whether to cache scores in addition to document IDs. Note that
   *          this increases the RAM consumed per doc
   * @param maxDocsToCache
   *          the maximum number of documents for caching the documents and
   *          possible the scores. If the collector exceeds the threshold,
   *          no documents and scores are cached.
   */
  public static CachingCollector create(Collector other, boolean cacheScores, int maxDocsToCache) {
    return cacheScores ? new ScoreCachingCollector(other, maxDocsToCache) : new NoScoreCachingCollector(other, maxDocsToCache);
  }
  
  // Prevent extension from non-internal classes
  private CachingCollector(Collector other, double maxRAMMB, boolean cacheScores) {
    this.other = other;
    
    cachedDocs = new ArrayList<int[]>();
    curDocs = new int[INITIAL_ARRAY_SIZE];
    cachedDocs.add(curDocs);

    int bytesPerDoc = RamUsageEstimator.NUM_BYTES_INT;
    if (cacheScores) {
      bytesPerDoc += RamUsageEstimator.NUM_BYTES_FLOAT;
    }
    maxDocsToCache = (int) ((maxRAMMB * 1024 * 1024) / bytesPerDoc);
  }

  private CachingCollector(Collector other, int maxDocsToCache) {
    this.other = other;

    cachedDocs = new ArrayList<int[]>();
    curDocs = new int[INITIAL_ARRAY_SIZE];
    cachedDocs.add(curDocs);
    this.maxDocsToCache = maxDocsToCache;
  }
  
  @Override
  public boolean acceptsDocsOutOfOrder() {
    return other.acceptsDocsOutOfOrder();
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

  /** Reused by the specialized inner classes. */
  void replayInit(Collector other) {
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
  }

  /**
   * Replays the cached doc IDs (and scores) to the given Collector. If this
   * instance does not cache scores, then Scorer is not set on
   * {@code other.setScorer} as well as scores are not replayed.
   * 
   * @throws IllegalStateException
   *           if this collector is not cached (i.e., if the RAM limits were too
   *           low for the number of documents + scores to cache).
   * @throws IllegalArgumentException
   *           if the given Collect's does not support out-of-order collection,
   *           while the collector passed to the ctor does.
   */
  public abstract void replay(Collector other) throws IOException;
  
}

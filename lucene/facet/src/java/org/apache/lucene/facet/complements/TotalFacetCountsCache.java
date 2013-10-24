package org.apache.lucene.facet.complements;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;

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

/**
 * Manage an LRU cache for {@link TotalFacetCounts} per index, taxonomy, and
 * facet indexing params.
 * 
 * @lucene.experimental
 */
public final class TotalFacetCountsCache {
  
  /**
   * Default size of in memory cache for computed total facet counts.
   * Set to 2 for the case when an application reopened a reader and 
   * the original one is still in use (Otherwise there will be 
   * switching again and again between the two.) 
   */
  public static final int DEFAULT_CACHE_SIZE = 2; 

  private static final TotalFacetCountsCache singleton = new TotalFacetCountsCache();
  
  /**
   * Get the single instance of this cache
   */
  public static TotalFacetCountsCache getSingleton() {
    return singleton;
  }
  
  /**
   * In-memory cache of TFCs.
   * <ul>  
   * <li>It's size is kept within limits through {@link #trimCache()}.
   * <li>An LRU eviction policy is applied, by maintaining active keys in {@link #lruKeys}. 
   * <li>After each addition to the cache, trimCache is called, to remove entries least recently used.
   * </ul>  
   * @see #markRecentlyUsed(TFCKey)
   */
  private ConcurrentHashMap<TFCKey,TotalFacetCounts> cache = new ConcurrentHashMap<TFCKey,TotalFacetCounts>();
  
  /**
   * A queue of active keys for applying LRU policy on eviction from the {@link #cache}.
   * @see #markRecentlyUsed(TFCKey)
   */
  private ConcurrentLinkedQueue<TFCKey> lruKeys = new ConcurrentLinkedQueue<TFCKey>();
  
  private int maxCacheSize = DEFAULT_CACHE_SIZE; 
  
  /** private constructor for singleton pattern */ 
  private TotalFacetCountsCache() {
  }
  
  /**
   * Get the total facet counts for a reader/taxonomy pair and facet indexing
   * parameters. If not in cache, computed here and added to the cache for later
   * use.
   * 
   * @param indexReader
   *          the documents index
   * @param taxonomy
   *          the taxonomy index
   * @param facetIndexingParams
   *          facet indexing parameters
   * @return the total facet counts.
   */
  public TotalFacetCounts getTotalCounts(IndexReader indexReader, TaxonomyReader taxonomy,
      FacetIndexingParams facetIndexingParams) throws IOException {
    // create the key
    TFCKey key = new TFCKey(indexReader, taxonomy, facetIndexingParams);
    // it is important that this call is not synchronized, so that available TFC 
    // would not wait for one that needs to be computed.  
    TotalFacetCounts tfc = cache.get(key);
    if (tfc != null) {
      markRecentlyUsed(key); 
      return tfc;
    }
    return computeAndCache(key);
  }

  /**
   * Mark key as it as recently used.
   * <p>
   * <b>Implementation notes: Synchronization considerations and the interaction between lruKeys and cache:</b>
   * <ol>
   *  <li>A concurrent {@link LinkedHashMap} would have made this class much simpler.
   *      But unfortunately, Java does not provide one.
   *      Instead, we combine two concurrent objects:
   *  <ul>
   *   <li>{@link ConcurrentHashMap} for the cached TFCs.
   *   <li>{@link ConcurrentLinkedQueue} for active keys
   *  </ul>
   *  <li>Both {@link #lruKeys} and {@link #cache} are concurrently safe.
   *  <li>Checks for a cached item through getTotalCounts() are not synchronized.
   *      Therefore, the case that a needed TFC is in the cache is very fast:
   *      it does not wait for the computation of other TFCs.
   *  <li>computeAndCache() is synchronized, and, has a (double) check of the required
   *       TFC, to avoid computing the same TFC twice. 
   *  <li>A race condition in this method (markRecentlyUsed) might result in two copies 
   *      of the same 'key' in lruKeys, but this is handled by the loop in trimCache(), 
   *      where an attempt to remove the same key twice is a no-op.
   * </ol>
   */
  private void markRecentlyUsed(TFCKey key) {
    lruKeys.remove(key);  
    lruKeys.add(key);
  }

  private synchronized void trimCache() {
    // loop until cache is of desired  size.
    while (cache.size()>maxCacheSize ) { 
      TFCKey key = lruKeys.poll();
      if (key==null) { //defensive
        // it is defensive since lruKeys presumably covers the cache keys 
        key = cache.keys().nextElement(); 
      }
      // remove this element. Note that an attempt to remove with the same key again is a no-op,
      // which gracefully handles the possible race in markRecentlyUsed(). 
      cache.remove(key);
    }
  }
  
  /**
   * compute TFC and cache it, after verifying it was not just added - for this
   * matter this method is synchronized, which is not too bad, because there is
   * lots of work done in the computations.
   */
  private synchronized TotalFacetCounts computeAndCache(TFCKey key) throws IOException {
    TotalFacetCounts tfc = cache.get(key); 
    if (tfc == null) {
      tfc = TotalFacetCounts.compute(key.indexReader, key.taxonomy, key.facetIndexingParams);
      lruKeys.add(key);
      cache.put(key,tfc);
      trimCache();
    }
    return tfc;
  }

  /**
   * Load {@link TotalFacetCounts} matching input parameters from the provided
   * outputFile and add them into the cache for the provided indexReader,
   * taxonomy, and facetIndexingParams. If a {@link TotalFacetCounts} for these
   * parameters already exists in the cache, it will be replaced by the loaded
   * one.
   * 
   * @param inputFile
   *          file from which to read the data
   * @param indexReader
   *          the documents index
   * @param taxonomy
   *          the taxonomy index
   * @param facetIndexingParams
   *          the facet indexing parameters
   * @throws IOException
   *           on error
   */
  public synchronized void load(File inputFile, IndexReader indexReader, TaxonomyReader taxonomy,
      FacetIndexingParams facetIndexingParams) throws IOException {
    if (!inputFile.isFile() || !inputFile.exists() || !inputFile.canRead()) {
      throw new IllegalArgumentException("Exepecting an existing readable file: "+inputFile);
    }
    TFCKey key = new TFCKey(indexReader, taxonomy, facetIndexingParams);
    TotalFacetCounts tfc = TotalFacetCounts.loadFromFile(inputFile, taxonomy, facetIndexingParams);
    cache.put(key,tfc);
    trimCache();
    markRecentlyUsed(key);
  }
  
  /**
   * Store the {@link TotalFacetCounts} matching input parameters into the
   * provided outputFile, making them available for a later call to
   * {@link #load(File, IndexReader, TaxonomyReader, FacetIndexingParams)}. If
   * these {@link TotalFacetCounts} are available in the cache, they are used.
   * But if they are not in the cache, this call will first compute them (which
   * will also add them to the cache).
   * 
   * @param outputFile
   *          file to store in.
   * @param indexReader
   *          the documents index
   * @param taxonomy
   *          the taxonomy index
   * @param facetIndexingParams
   *          the facet indexing parameters
   * @throws IOException
   *           on error
   * @see #load(File, IndexReader, TaxonomyReader, FacetIndexingParams)
   */
  public void store(File outputFile, IndexReader indexReader, TaxonomyReader taxonomy,
      FacetIndexingParams facetIndexingParams) throws IOException {
    File parentFile = outputFile.getParentFile();
    if (
        ( outputFile.exists() && (!outputFile.isFile()      || !outputFile.canWrite())) ||
        (!outputFile.exists() && (!parentFile.isDirectory() || !parentFile.canWrite()))
      ) {
      throw new IllegalArgumentException("Exepecting a writable file: "+outputFile);
    }
    TotalFacetCounts tfc = getTotalCounts(indexReader, taxonomy, facetIndexingParams);
    TotalFacetCounts.storeToFile(outputFile, tfc);  
  }
  
  private static class TFCKey {
    final IndexReader indexReader;
    final TaxonomyReader taxonomy;
    private final Iterable<CategoryListParams> clps;
    private final int hashCode;
    private final int nDels; // needed when a reader used for faceted search was just used for deletion. 
    final FacetIndexingParams facetIndexingParams;
    
    public TFCKey(IndexReader indexReader, TaxonomyReader taxonomy,
        FacetIndexingParams facetIndexingParams) {
      this.indexReader = indexReader;
      this.taxonomy = taxonomy;
      this.facetIndexingParams = facetIndexingParams;
      this.clps = facetIndexingParams.getAllCategoryListParams();
      this.nDels = indexReader.numDeletedDocs();
      hashCode = indexReader.hashCode() ^ taxonomy.hashCode();
    }
    
    @Override
    public int hashCode() {
      return hashCode;
    }
    
    @Override
    public boolean equals(Object other) {
      TFCKey o = (TFCKey) other; 
      if (indexReader != o.indexReader || taxonomy != o.taxonomy || nDels != o.nDels) {
        return false;
      }
      Iterator<CategoryListParams> it1 = clps.iterator();
      Iterator<CategoryListParams> it2 = o.clps.iterator();
      while (it1.hasNext() && it2.hasNext()) {
        if (!it1.next().equals(it2.next())) {
          return false;
        }
      }
      return it1.hasNext() == it2.hasNext();
    }
  }

  /**
   * Clear the cache.
   */
  public synchronized void clear() {
    cache.clear();
    lruKeys.clear();
  }
  
  /**
   * @return the maximal cache size
   */
  public int getCacheSize() {
    return maxCacheSize;
  }

  /**
   * Set the number of TotalFacetCounts arrays that will remain in memory cache.
   * <p>
   * If new size is smaller than current size, the cache is appropriately trimmed.
   * <p>
   * Minimal size is 1, so passing zero or negative size would result in size of 1.
   * @param size new size to set
   */
  public void setCacheSize(int size) {
    if (size < 1) size = 1;
    int origSize = maxCacheSize;
    maxCacheSize = size;
    if (maxCacheSize < origSize) { // need to trim only if the cache was reduced
      trimCache();
    }
  }
}

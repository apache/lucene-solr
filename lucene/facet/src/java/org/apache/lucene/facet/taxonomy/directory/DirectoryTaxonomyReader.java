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
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntUnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.LRUHashMap;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link TaxonomyReader} which retrieves stored taxonomy information from a {@link Directory}.
 *
 * <p>Reading from the on-disk index on every method call is too slow, so this implementation
 * employs caching: Some methods cache recent requests and their results, while other methods
 * prefetch all the data into memory and then provide answers directly from in-memory tables. See
 * the documentation of individual methods for comments on their performance.
 *
 * @lucene.experimental
 */
public class DirectoryTaxonomyReader extends TaxonomyReader implements Accountable {

  private static final Logger log = Logger.getLogger(DirectoryTaxonomyReader.class.getName());

  private static final int DEFAULT_CACHE_VALUE = 4000;

  // NOTE: very coarse estimate!
  private static final int BYTES_PER_CACHE_ENTRY =
      4 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + 4 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 8 * Character.BYTES;

  private final DirectoryTaxonomyWriter taxoWriter;
  private final long taxoEpoch; // used in doOpenIfChanged
  private final DirectoryReader indexReader;

  // TODO: test DoubleBarrelLRUCache and consider using it instead
  private LRUHashMap<FacetLabel, Integer> ordinalCache;
  private LRUHashMap<Integer, FacetLabel> categoryCache;

  private volatile TaxonomyIndexArrays taxoArrays;

  /**
   * Called only from {@link #doOpenIfChanged()}. If the taxonomy has been recreated, you should
   * pass {@code null} as the caches and parent/children arrays.
   */
  DirectoryTaxonomyReader(
      DirectoryReader indexReader,
      DirectoryTaxonomyWriter taxoWriter,
      LRUHashMap<FacetLabel, Integer> ordinalCache,
      LRUHashMap<Integer, FacetLabel> categoryCache,
      TaxonomyIndexArrays taxoArrays)
      throws IOException {
    this.indexReader = indexReader;
    this.taxoWriter = taxoWriter;
    this.taxoEpoch = taxoWriter == null ? -1 : taxoWriter.getTaxonomyEpoch();

    // use the same instance of the cache, note the protective code in getOrdinal and getPath
    this.ordinalCache =
        ordinalCache == null
            ? new LRUHashMap<FacetLabel, Integer>(DEFAULT_CACHE_VALUE)
            : ordinalCache;
    this.categoryCache =
        categoryCache == null
            ? new LRUHashMap<Integer, FacetLabel>(DEFAULT_CACHE_VALUE)
            : categoryCache;

    this.taxoArrays = taxoArrays != null ? new TaxonomyIndexArrays(indexReader, taxoArrays) : null;
  }

  /**
   * Open for reading a taxonomy stored in a given {@link Directory}.
   *
   * @param directory The {@link Directory} in which the taxonomy resides.
   * @throws CorruptIndexException if the Taxonomy is corrupt.
   * @throws IOException if another error occurred.
   */
  public DirectoryTaxonomyReader(Directory directory) throws IOException {
    indexReader = openIndexReader(directory);
    taxoWriter = null;
    taxoEpoch = -1;

    // These are the default cache sizes; they can be configured after
    // construction with the cache's setMaxSize() method
    ordinalCache = new LRUHashMap<>(DEFAULT_CACHE_VALUE);
    categoryCache = new LRUHashMap<>(DEFAULT_CACHE_VALUE);
  }

  /**
   * Opens a {@link DirectoryTaxonomyReader} over the given {@link DirectoryTaxonomyWriter} (for
   * NRT).
   *
   * @param taxoWriter The {@link DirectoryTaxonomyWriter} from which to obtain newly added
   *     categories, in real-time.
   */
  public DirectoryTaxonomyReader(DirectoryTaxonomyWriter taxoWriter) throws IOException {
    this.taxoWriter = taxoWriter;
    taxoEpoch = taxoWriter.getTaxonomyEpoch();
    indexReader = openIndexReader(taxoWriter.getInternalIndexWriter());

    // These are the default cache sizes; they can be configured after
    // construction with the cache's setMaxSize() method
    ordinalCache = new LRUHashMap<>(DEFAULT_CACHE_VALUE);
    categoryCache = new LRUHashMap<>(DEFAULT_CACHE_VALUE);
  }

  private synchronized void initTaxoArrays() throws IOException {
    if (taxoArrays == null) {
      // according to Java Concurrency in Practice, this might perform better on
      // some JVMs, because the array initialization doesn't happen on the
      // volatile member.
      TaxonomyIndexArrays tmpArrays = new TaxonomyIndexArrays(indexReader);
      taxoArrays = tmpArrays;
    }
  }

  @Override
  protected void doClose() throws IOException {
    indexReader.close();
    taxoArrays = null;
    // do not clear() the caches, as they may be used by other DTR instances.
    ordinalCache = null;
    categoryCache = null;
  }

  /**
   * Implements the opening of a new {@link DirectoryTaxonomyReader} instance if the taxonomy has
   * changed.
   *
   * <p><b>NOTE:</b> the returned {@link DirectoryTaxonomyReader} shares the ordinal and category
   * caches with this reader. This is not expected to cause any issues, unless the two instances
   * continue to live. The reader guarantees that the two instances cannot affect each other in
   * terms of correctness of the caches, however if the size of the cache is changed through {@link
   * #setCacheSize(int)}, it will affect both reader instances.
   */
  @Override
  protected DirectoryTaxonomyReader doOpenIfChanged() throws IOException {
    ensureOpen();

    // This works for both NRT and non-NRT readers (i.e. an NRT reader remains NRT).
    final DirectoryReader r2 = DirectoryReader.openIfChanged(indexReader);
    if (r2 == null) {
      return null; // no changes, nothing to do
    }

    // check if the taxonomy was recreated
    boolean success = false;
    try {
      boolean recreated = false;
      if (taxoWriter == null) {
        // not NRT, check epoch from commit data
        String t1 =
            indexReader.getIndexCommit().getUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH);
        String t2 = r2.getIndexCommit().getUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH);
        if (t1 == null) {
          if (t2 != null) {
            recreated = true;
          }
        } else if (!t1.equals(t2)) {
          // t1 != null and t2 must not be null b/c DirTaxoWriter always puts the commit data.
          // it's ok to use String.equals because we require the two epoch values to be the same.
          recreated = true;
        }
      } else {
        // NRT, compare current taxoWriter.epoch() vs the one that was given at construction
        if (taxoEpoch != taxoWriter.getTaxonomyEpoch()) {
          recreated = true;
        }
      }

      final DirectoryTaxonomyReader newtr;
      if (recreated) {
        // if recreated, do not reuse anything from this instace. the information
        // will be lazily computed by the new instance when needed.
        newtr = new DirectoryTaxonomyReader(r2, taxoWriter, null, null, null);
      } else {
        newtr =
            new DirectoryTaxonomyReader(r2, taxoWriter, ordinalCache, categoryCache, taxoArrays);
      }

      success = true;
      return newtr;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(r2);
      }
    }
  }

  /** Open the {@link DirectoryReader} from this {@link Directory}. */
  protected DirectoryReader openIndexReader(Directory directory) throws IOException {
    return DirectoryReader.open(directory);
  }

  /** Open the {@link DirectoryReader} from this {@link IndexWriter}. */
  protected DirectoryReader openIndexReader(IndexWriter writer) throws IOException {
    return DirectoryReader.open(writer);
  }

  /**
   * Expert: returns the underlying {@link DirectoryReader} instance that is used by this {@link
   * TaxonomyReader}.
   */
  DirectoryReader getInternalIndexReader() {
    ensureOpen();
    return indexReader;
  }

  @Override
  public ParallelTaxonomyArrays getParallelTaxonomyArrays() throws IOException {
    ensureOpen();
    if (taxoArrays == null) {
      initTaxoArrays();
    }
    return taxoArrays;
  }

  @Override
  public Map<String, String> getCommitUserData() throws IOException {
    ensureOpen();
    return indexReader.getIndexCommit().getUserData();
  }

  @Override
  public int getOrdinal(FacetLabel cp) throws IOException {
    ensureOpen();
    if (cp.length == 0) {
      return ROOT_ORDINAL;
    }

    // First try to find the answer in the LRU cache:
    synchronized (ordinalCache) {
      Integer res = ordinalCache.get(cp);
      if (res != null) {
        if (res.intValue() < indexReader.maxDoc()) {
          // Since the cache is shared with DTR instances allocated from
          // doOpenIfChanged, we need to ensure that the ordinal is one that
          // this DTR instance recognizes.
          return res.intValue();
        } else {
          // if we get here, it means that the category was found in the cache,
          // but is not recognized by this TR instance. Therefore there's no
          // need to continue search for the path on disk, because we won't find
          // it there too.
          return TaxonomyReader.INVALID_ORDINAL;
        }
      }
    }

    // If we're still here, we have a cache miss. We need to fetch the
    // value from disk, and then also put it in the cache:
    int ret = TaxonomyReader.INVALID_ORDINAL;
    PostingsEnum docs =
        MultiTerms.getTermPostingsEnum(
            indexReader,
            Consts.FULL,
            new BytesRef(FacetsConfig.pathToString(cp.components, cp.length)),
            0);
    if (docs != null && docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      ret = docs.docID();

      // we only store the fact that a category exists, not its inexistence.
      // This is required because the caches are shared with new DTR instances
      // that are allocated from doOpenIfChanged. Therefore, if we only store
      // information about found categories, we cannot accidently tell a new
      // generation of DTR that a category does not exist.
      synchronized (ordinalCache) {
        ordinalCache.put(cp, Integer.valueOf(ret));
      }
    }

    return ret;
  }

  @Override
  public FacetLabel getPath(int ordinal) throws IOException {
    ensureOpen();

    // Since the cache is shared with DTR instances allocated from
    // doOpenIfChanged, we need to ensure that the ordinal is one that this DTR
    // instance recognizes. Therefore we do this check up front, before we hit
    // the cache.
    int indexReaderMaxDoc = indexReader.maxDoc();
    checkOrdinalBounds(ordinal, indexReaderMaxDoc);

    FacetLabel ordinalPath = getPathFromCache(ordinal);
    if (ordinalPath != null) {
      return ordinalPath;
    }

    int readerIndex = ReaderUtil.subIndex(ordinal, indexReader.leaves());
    LeafReader leafReader = indexReader.leaves().get(readerIndex).reader();
    // TODO: Use LUCENE-9476 to get the bulk lookup API for extracting BinaryDocValues
    BinaryDocValues values = leafReader.getBinaryDocValues(Consts.FULL);

    FacetLabel ret;

    if (values == null
        || values.advanceExact(ordinal - indexReader.leaves().get(readerIndex).docBase) == false) {
      // The index uses the older StoredField format to store the mapping
      // On recreating the index, the values will be stored using the BinaryDocValuesField format
      Document doc = indexReader.document(ordinal);
      ret = new FacetLabel(FacetsConfig.stringToPath(doc.get(Consts.FULL)));
    } else {
      // The index uses the BinaryDocValuesField format to store the mapping
      ret = new FacetLabel(FacetsConfig.stringToPath(values.binaryValue().utf8ToString()));
    }

    synchronized (categoryCache) {
      categoryCache.put(ordinal, ret);
    }

    return ret;
  }

  private FacetLabel getPathFromCache(int ordinal) {
    // TODO: can we use an int-based hash impl, such as IntToObjectMap,
    // wrapped as LRU?
    synchronized (categoryCache) {
      return categoryCache.get(ordinal);
    }
  }

  private void checkOrdinalBounds(int ordinal, int indexReaderMaxDoc)
      throws IllegalArgumentException {
    if (ordinal < 0 || ordinal >= indexReaderMaxDoc) {
      throw new IllegalArgumentException(
          "ordinal "
              + ordinal
              + " is out of the range of the indexReader "
              + indexReader.toString());
    }
  }

  /**
   * Returns an array of FacetLabels for a given array of ordinals.
   *
   * <p>This API is generally faster than iteratively calling {@link #getPath(int)} over an array of
   * ordinals. It uses the {@link #getPath(int)} method iteratively when it detects that the index
   * was created using StoredFields (with no performance gains) and uses DocValues based iteration
   * when the index is based on DocValues.
   *
   * @param ordinals Array of ordinals that are assigned to categories inserted into the taxonomy
   *     index
   */
  public FacetLabel[] getBulkPath(int... ordinals) throws IOException {
    ensureOpen();

    int ordinalsLength = ordinals.length;
    FacetLabel[] bulkPath = new FacetLabel[ordinalsLength];
    // remember the original positions of ordinals before they are sorted
    int originalPosition[] = new int[ordinalsLength];
    Arrays.setAll(originalPosition, IntUnaryOperator.identity());
    int indexReaderMaxDoc = indexReader.maxDoc();

    for (int i = 0; i < ordinalsLength; i++) {
      // check whether the ordinal is valid before accessing the cache
      checkOrdinalBounds(ordinals[i], indexReaderMaxDoc);
      // check the cache before trying to find it in the index
      FacetLabel ordinalPath = getPathFromCache(ordinals[i]);
      if (ordinalPath != null) {
        bulkPath[i] = ordinalPath;
      }
    }

    // parallel sort the ordinals and originalPosition array based on the values in the ordinals
    // array
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        int x = ordinals[i];
        ordinals[i] = ordinals[j];
        ordinals[j] = x;

        x = originalPosition[i];
        originalPosition[i] = originalPosition[j];
        originalPosition[j] = x;
      }
      ;

      @Override
      public int compare(int i, int j) {
        return Integer.compare(ordinals[i], ordinals[j]);
      }
    }.sort(0, ordinalsLength);

    int readerIndex;
    int leafReaderMaxDoc = 0;
    int leafReaderDocBase = 0;
    LeafReader leafReader;
    LeafReaderContext leafReaderContext;
    BinaryDocValues values = null;

    for (int i = 0; i < ordinalsLength; i++) {
      if (bulkPath[originalPosition[i]] == null) {
        if (values == null || ordinals[i] >= leafReaderMaxDoc) {

          readerIndex = ReaderUtil.subIndex(ordinals[i], indexReader.leaves());
          leafReaderContext = indexReader.leaves().get(readerIndex);
          leafReader = leafReaderContext.reader();
          leafReaderMaxDoc = leafReader.maxDoc();
          leafReaderDocBase = leafReaderContext.docBase;
          values = leafReader.getBinaryDocValues(Consts.FULL);

          // this check is only needed once to confirm that the index uses BinaryDocValues
          boolean success = values.advanceExact(ordinals[i] - leafReaderDocBase);
          if (success == false) {
            return getBulkPathForOlderIndexes(ordinals);
          }
        }
        boolean success = values.advanceExact(ordinals[i] - leafReaderDocBase);
        assert success;
        bulkPath[originalPosition[i]] =
            new FacetLabel(FacetsConfig.stringToPath(values.binaryValue().utf8ToString()));
      }
    }

    for (int i = 0; i < ordinalsLength; i++) {
      synchronized (categoryCache) {
        categoryCache.put(ordinals[i], bulkPath[originalPosition[i]]);
      }
    }

    return bulkPath;
  }

  /**
   * This function is only used when the underlying taxonomy index was constructed using an older
   * (slower) StoredFields based codec (< 8.7). The {@link #getBulkPath(int...)} function calls it
   * internally when it realizes that the index uses StoredFields.
   */
  private FacetLabel[] getBulkPathForOlderIndexes(int... ordinals) throws IOException {
    FacetLabel[] bulkPath = new FacetLabel[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      bulkPath[i] = getPath(ordinals[i]);
    }

    return bulkPath;
  }

  @Override
  public int getSize() {
    ensureOpen();
    return indexReader.numDocs();
  }

  @Override
  public synchronized long ramBytesUsed() {
    ensureOpen();
    long ramBytesUsed = 0;
    for (LeafReaderContext ctx : indexReader.leaves()) {
      ramBytesUsed += ((SegmentReader) ctx.reader()).ramBytesUsed();
    }
    if (taxoArrays != null) {
      ramBytesUsed += taxoArrays.ramBytesUsed();
    }
    synchronized (categoryCache) {
      ramBytesUsed += BYTES_PER_CACHE_ENTRY * categoryCache.size();
    }

    synchronized (ordinalCache) {
      ramBytesUsed += BYTES_PER_CACHE_ENTRY * ordinalCache.size();
    }

    return ramBytesUsed;
  }

  @Override
  public synchronized Collection<Accountable> getChildResources() {
    final List<Accountable> resources = new ArrayList<>();
    long ramBytesUsed = 0;
    for (LeafReaderContext ctx : indexReader.leaves()) {
      ramBytesUsed += ((SegmentReader) ctx.reader()).ramBytesUsed();
    }
    resources.add(Accountables.namedAccountable("indexReader", ramBytesUsed));
    if (taxoArrays != null) {
      resources.add(Accountables.namedAccountable("taxoArrays", taxoArrays));
    }

    synchronized (categoryCache) {
      resources.add(
          Accountables.namedAccountable(
              "categoryCache", BYTES_PER_CACHE_ENTRY * categoryCache.size()));
    }

    synchronized (ordinalCache) {
      resources.add(
          Accountables.namedAccountable(
              "ordinalCache", BYTES_PER_CACHE_ENTRY * ordinalCache.size()));
    }

    return Collections.unmodifiableList(resources);
  }

  /**
   * setCacheSize controls the maximum allowed size of each of the caches used by {@link
   * #getPath(int)} and {@link #getOrdinal(FacetLabel)}.
   *
   * <p>Currently, if the given size is smaller than the current size of a cache, it will not
   * shrink, and rather we be limited to its current size.
   *
   * @param size the new maximum cache size, in number of entries.
   */
  public void setCacheSize(int size) {
    ensureOpen();
    synchronized (categoryCache) {
      categoryCache.setMaxSize(size);
    }
    synchronized (ordinalCache) {
      ordinalCache.setMaxSize(size);
    }
  }

  /**
   * Returns ordinal -&gt; label mapping, up to the provided max ordinal or number of ordinals,
   * whichever is smaller.
   */
  public String toString(int max) {
    ensureOpen();
    StringBuilder sb = new StringBuilder();
    int upperl = Math.min(max, indexReader.maxDoc());
    for (int i = 0; i < upperl; i++) {
      try {
        FacetLabel category = this.getPath(i);
        if (category == null) {
          sb.append(i).append(": NULL!! \n");
          continue;
        }
        if (category.length == 0) {
          sb.append(i).append(": EMPTY STRING!! \n");
          continue;
        }
        sb.append(i).append(": ").append(category.toString()).append("\n");
      } catch (IOException e) {
        if (log.isLoggable(Level.FINEST)) {
          log.log(Level.FINEST, e.getMessage(), e);
        }
      }
    }
    return sb.toString();
  }
}

package org.apache.lucene.facet.taxonomy.directory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.cl2o.Cl2oTaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.lru.LruTaxonomyWriterCache;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException; // javadocs
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException; // javadocs
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

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
 * {@link TaxonomyWriter} which uses a {@link Directory} to store the taxonomy
 * information on disk, and keeps an additional in-memory cache of some or all
 * categories.
 * <p>
 * In addition to the permanently-stored information in the {@link Directory},
 * efficiency dictates that we also keep an in-memory cache of <B>recently
 * seen</B> or <B>all</B> categories, so that we do not need to go back to disk
 * for every category addition to see which ordinal this category already has,
 * if any. A {@link TaxonomyWriterCache} object determines the specific caching
 * algorithm used.
 * <p>
 * This class offers some hooks for extending classes to control the
 * {@link IndexWriter} instance that is used. See {@link #openIndexWriter}.
 * 
 * @lucene.experimental
 */
public class DirectoryTaxonomyWriter implements TaxonomyWriter {
  
  /**
   * Property name of user commit data that contains the index epoch. The epoch
   * changes whenever the taxonomy is recreated (i.e. opened with
   * {@link OpenMode#CREATE}.
   * <p>
   * Applications should not use this property in their commit data because it
   * will be overridden by this taxonomy writer.
   */
  public static final String INDEX_EPOCH = "index.epoch";
  
  private final Directory dir;
  private final IndexWriter indexWriter;
  private final TaxonomyWriterCache cache;
  private final AtomicInteger cacheMisses = new AtomicInteger(0);
  
  // Records the taxonomy index epoch, updated on replaceTaxonomy as well.
  private long indexEpoch;
  
  private char delimiter = Consts.DEFAULT_DELIMITER;
  private SinglePositionTokenStream parentStream = new SinglePositionTokenStream(Consts.PAYLOAD_PARENT);
  private Field parentStreamField;
  private Field fullPathField;
  private int cacheMissesUntilFill = 11;
  private boolean shouldFillCache = true;
  
  // even though lazily initialized, not volatile so that access to it is
  // faster. we keep a volatile boolean init instead.
  private ReaderManager readerManager;
  private volatile boolean initializedReaderManager = false;
  private volatile boolean shouldRefreshReaderManager;
  
  /**
   * We call the cache "complete" if we know that every category in our
   * taxonomy is in the cache. When the cache is <B>not</B> complete, and
   * we can't find a category in the cache, we still need to look for it
   * in the on-disk index; Therefore when the cache is not complete, we
   * need to open a "reader" to the taxonomy index.
   * The cache becomes incomplete if it was never filled with the existing
   * categories, or if a put() to the cache ever returned true (meaning
   * that some of the cached data was cleared).
   */
  private volatile boolean cacheIsComplete;
  private volatile boolean isClosed = false;
  private volatile TaxonomyIndexArrays taxoArrays;
  private volatile int nextID;

  /** Reads the commit data from a Directory. */
  private static Map<String, String> readCommitData(Directory dir) throws IOException {
    SegmentInfos infos = new SegmentInfos();
    infos.read(dir);
    return infos.getUserData();
  }
  
  /**
   * Changes the character that the taxonomy uses in its internal storage as a
   * delimiter between category components. Do not use this method unless you
   * really know what you are doing. It has nothing to do with whatever
   * character the application may be using to represent categories for its own
   * use.
   * <p>
   * If you do use this method, make sure you call it before any other methods
   * that actually queries the taxonomy. Moreover, make sure you always pass the
   * same delimiter for all taxonomy writer and reader instances you create for
   * the same directory.
   */
  public void setDelimiter(char delimiter) {
    ensureOpen();
    this.delimiter = delimiter;
  }

  /**
   * Forcibly unlocks the taxonomy in the named directory.
   * <P>
   * Caution: this should only be used by failure recovery code, when it is
   * known that no other process nor thread is in fact currently accessing
   * this taxonomy.
   * <P>
   * This method is unnecessary if your {@link Directory} uses a
   * {@link NativeFSLockFactory} instead of the default
   * {@link SimpleFSLockFactory}. When the "native" lock is used, a lock
   * does not stay behind forever when the process using it dies. 
   */
  public static void unlock(Directory directory) throws IOException {
    IndexWriter.unlock(directory);
  }

  /**
   * Construct a Taxonomy writer.
   * 
   * @param directory
   *    The {@link Directory} in which to store the taxonomy. Note that
   *    the taxonomy is written directly to that directory (not to a
   *    subdirectory of it).
   * @param openMode
   *    Specifies how to open a taxonomy for writing: <code>APPEND</code>
   *    means open an existing index for append (failing if the index does
   *    not yet exist). <code>CREATE</code> means create a new index (first
   *    deleting the old one if it already existed).
   *    <code>APPEND_OR_CREATE</code> appends to an existing index if there
   *    is one, otherwise it creates a new index.
   * @param cache
   *    A {@link TaxonomyWriterCache} implementation which determines
   *    the in-memory caching policy. See for example
   *    {@link LruTaxonomyWriterCache} and {@link Cl2oTaxonomyWriterCache}.
   *    If null or missing, {@link #defaultTaxonomyWriterCache()} is used.
   * @throws CorruptIndexException
   *     if the taxonomy is corrupted.
   * @throws LockObtainFailedException
   *     if the taxonomy is locked by another writer. If it is known
   *     that no other concurrent writer is active, the lock might
   *     have been left around by an old dead process, and should be
   *     removed using {@link #unlock(Directory)}.
   * @throws IOException
   *     if another error occurred.
   */
  public DirectoryTaxonomyWriter(Directory directory, OpenMode openMode,
      TaxonomyWriterCache cache) throws IOException {

    dir = directory;
    IndexWriterConfig config = createIndexWriterConfig(openMode);
    indexWriter = openIndexWriter(dir, config);

    // verify (to some extent) that merge policy in effect would preserve category docids 
    assert !(indexWriter.getConfig().getMergePolicy() instanceof TieredMergePolicy) : 
      "for preserving category docids, merging none-adjacent segments is not allowed";
    
    // after we opened the writer, and the index is locked, it's safe to check
    // the commit data and read the index epoch
    openMode = config.getOpenMode();
    if (!DirectoryReader.indexExists(directory)) {
      indexEpoch = 1;
    } else {
      String epochStr = null;
      Map<String, String> commitData = readCommitData(directory);
      if (commitData != null) {
        epochStr = commitData.get(INDEX_EPOCH);
      }
      // no commit data, or no epoch in it means an old taxonomy, so set its epoch to 1, for lack
      // of a better value.
      indexEpoch = epochStr == null ? 1 : Long.parseLong(epochStr);
    }
    
    if (openMode == OpenMode.CREATE) {
      ++indexEpoch;
    }
    
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setOmitNorms(true);
    parentStreamField = new Field(Consts.FIELD_PAYLOADS, parentStream, ft);
    fullPathField = new StringField(Consts.FULL, "", Field.Store.YES);

    nextID = indexWriter.maxDoc();

    if (cache == null) {
      cache = defaultTaxonomyWriterCache();
    }
    this.cache = cache;

    if (nextID == 0) {
      cacheIsComplete = true;
      // Make sure that the taxonomy always contain the root category
      // with category id 0.
      addCategory(CategoryPath.EMPTY);
    } else {
      // There are some categories on the disk, which we have not yet
      // read into the cache, and therefore the cache is incomplete.
      // We choose not to read all the categories into the cache now,
      // to avoid terrible performance when a taxonomy index is opened
      // to add just a single category. We will do it later, after we
      // notice a few cache misses.
      cacheIsComplete = false;
    }
  }

  /**
   * Open internal index writer, which contains the taxonomy data.
   * <p>
   * Extensions may provide their own {@link IndexWriter} implementation or instance. 
   * <br><b>NOTE:</b> the instance this method returns will be closed upon calling
   * to {@link #close()}.
   * <br><b>NOTE:</b> the merge policy in effect must not merge none adjacent segments. See
   * comment in {@link #createIndexWriterConfig(IndexWriterConfig.OpenMode)} for the logic behind this.
   *  
   * @see #createIndexWriterConfig(IndexWriterConfig.OpenMode)
   * 
   * @param directory
   *          the {@link Directory} on top of which an {@link IndexWriter}
   *          should be opened.
   * @param config
   *          configuration for the internal index writer.
   */
  protected IndexWriter openIndexWriter(Directory directory, IndexWriterConfig config)
      throws IOException {
    return new IndexWriter(directory, config);
  }

  /**
   * Create the {@link IndexWriterConfig} that would be used for opening the internal index writer.
   * <br>Extensions can configure the {@link IndexWriter} as they see fit,
   * including setting a {@link org.apache.lucene.index.MergeScheduler merge-scheduler}, or
   * {@link org.apache.lucene.index.IndexDeletionPolicy deletion-policy}, different RAM size
   * etc.<br>
   * <br><b>NOTE:</b> internal docids of the configured index must not be altered.
   * For that, categories are never deleted from the taxonomy index.
   * In addition, merge policy in effect must not merge none adjacent segments.
   * 
   * @see #openIndexWriter(Directory, IndexWriterConfig)
   * 
   * @param openMode see {@link OpenMode}
   */
  protected IndexWriterConfig createIndexWriterConfig(OpenMode openMode) {
    // TODO: should we use a more optimized Codec, e.g. Pulsing (or write custom)?
    // The taxonomy has a unique structure, where each term is associated with one document
 
    // Make sure we use a MergePolicy which always merges adjacent segments and thus
    // keeps the doc IDs ordered as well (this is crucial for the taxonomy index).
    return new IndexWriterConfig(Version.LUCENE_43,
        null).setOpenMode(openMode).setMergePolicy(
        new LogByteSizeMergePolicy());
  }
  
  /** Opens a {@link ReaderManager} from the internal {@link IndexWriter}. */
  private void initReaderManager() throws IOException {
    if (!initializedReaderManager) {
      synchronized (this) {
        // verify that the taxo-writer hasn't been closed on us.
        ensureOpen();
        if (!initializedReaderManager) {
          readerManager = new ReaderManager(indexWriter, false);
          shouldRefreshReaderManager = false;
          initializedReaderManager = true;
        }
      }
    }
  }

  /**
   * Creates a new instance with a default cache as defined by
   * {@link #defaultTaxonomyWriterCache()}.
   */
  public DirectoryTaxonomyWriter(Directory directory, OpenMode openMode)
  throws IOException {
    this(directory, openMode, defaultTaxonomyWriterCache());
  }

  /**
   * Defines the default {@link TaxonomyWriterCache} to use in constructors
   * which do not specify one.
   * <P>  
   * The current default is {@link Cl2oTaxonomyWriterCache} constructed
   * with the parameters (1024, 0.15f, 3), i.e., the entire taxonomy is
   * cached in memory while building it.
   */
  public static TaxonomyWriterCache defaultTaxonomyWriterCache() {
    return new Cl2oTaxonomyWriterCache(1024, 0.15f, 3);
  }

  public DirectoryTaxonomyWriter(Directory d) throws IOException {
    this(d, OpenMode.CREATE_OR_APPEND);
  }

  /**
   * Frees used resources as well as closes the underlying {@link IndexWriter},
   * which commits whatever changes made to it to the underlying
   * {@link Directory}.
   */
  @Override
  public synchronized void close() throws IOException {
    if (!isClosed) {
      indexWriter.setCommitData(combinedCommitData(indexWriter.getCommitData()));
      indexWriter.commit();
      doClose();
    }
  }
  
  private void doClose() throws IOException {
    indexWriter.close();
    isClosed = true;
    closeResources();
  }

  /**
   * A hook for extending classes to close additional resources that were used.
   * The default implementation closes the {@link IndexReader} as well as the
   * {@link TaxonomyWriterCache} instances that were used. <br>
   * <b>NOTE:</b> if you override this method, you should include a
   * <code>super.closeResources()</code> call in your implementation.
   */
  protected synchronized void closeResources() throws IOException {
    if (initializedReaderManager) {
      readerManager.close();
      readerManager = null;
      initializedReaderManager = false;
    }
    if (cache != null) {
      cache.close();
    }
  }

  /**
   * Look up the given category in the cache and/or the on-disk storage,
   * returning the category's ordinal, or a negative number in case the
   * category does not yet exist in the taxonomy.
   */
  protected synchronized int findCategory(CategoryPath categoryPath) throws IOException {
    // If we can find the category in the cache, or we know the cache is
    // complete, we can return the response directly from it
    int res = cache.get(categoryPath);
    if (res >= 0 || cacheIsComplete) {
      return res;
    }

    cacheMisses.incrementAndGet();
    // After a few cache misses, it makes sense to read all the categories
    // from disk and into the cache. The reason not to do this on the first
    // cache miss (or even when opening the writer) is that it will
    // significantly slow down the case when a taxonomy is opened just to
    // add one category. The idea only spending a long time on reading
    // after enough time was spent on cache misses is known as an "online
    // algorithm".
    perhapsFillCache();
    res = cache.get(categoryPath);
    if (res >= 0 || cacheIsComplete) {
      // if after filling the cache from the info on disk, the category is in it
      // or the cache is complete, return whatever cache.get returned.
      return res;
    }

    // if we get here, it means the category is not in the cache, and it is not
    // complete, and therefore we must look for the category on disk.
    
    // We need to get an answer from the on-disk index.
    initReaderManager();

    int doc = -1;
    DirectoryReader reader = readerManager.acquire();
    try {
      final BytesRef catTerm = new BytesRef(categoryPath.toString(delimiter));
      TermsEnum termsEnum = null; // reuse
      DocsEnum docs = null; // reuse
      for (AtomicReaderContext ctx : reader.leaves()) {
        Terms terms = ctx.reader().terms(Consts.FULL);
        if (terms != null) {
          termsEnum = terms.iterator(termsEnum);
          if (termsEnum.seekExact(catTerm, true)) {
            // liveDocs=null because the taxonomy has no deletes
            docs = termsEnum.docs(null, docs, 0 /* freqs not required */);
            // if the term was found, we know it has exactly one document.
            doc = docs.nextDoc() + ctx.docBase;
            break;
          }
        }
      }
    } finally {
      readerManager.release(reader);
    }
    if (doc > 0) {
      addToCache(categoryPath, doc);
    }
    return doc;
  }

  @Override
  public int addCategory(CategoryPath categoryPath) throws IOException {
    ensureOpen();
    // check the cache outside the synchronized block. this results in better
    // concurrency when categories are there.
    int res = cache.get(categoryPath);
    if (res < 0) {
      // the category is not in the cache - following code cannot be executed in parallel.
      synchronized (this) {
        res = findCategory(categoryPath);
        if (res < 0) {
          // This is a new category, and we need to insert it into the index
          // (and the cache). Actually, we might also need to add some of
          // the category's ancestors before we can add the category itself
          // (while keeping the invariant that a parent is always added to
          // the taxonomy before its child). internalAddCategory() does all
          // this recursively
          res = internalAddCategory(categoryPath);
        }
      }
    }
    return res;
  }

  /**
   * Add a new category into the index (and the cache), and return its new
   * ordinal.
   * <p>
   * Actually, we might also need to add some of the category's ancestors
   * before we can add the category itself (while keeping the invariant that a
   * parent is always added to the taxonomy before its child). We do this by
   * recursion.
   */
  private int internalAddCategory(CategoryPath cp) throws IOException {
    // Find our parent's ordinal (recursively adding the parent category
    // to the taxonomy if it's not already there). Then add the parent
    // ordinal as payloads (rather than a stored field; payloads can be
    // more efficiently read into memory in bulk by LuceneTaxonomyReader)
    int parent;
    if (cp.length > 1) {
      CategoryPath parentPath = cp.subpath(cp.length - 1);
      parent = findCategory(parentPath);
      if (parent < 0) {
        parent = internalAddCategory(parentPath);
      }
    } else if (cp.length == 1) {
      parent = TaxonomyReader.ROOT_ORDINAL;
    } else {
      parent = TaxonomyReader.INVALID_ORDINAL;
    }
    int id = addCategoryDocument(cp, parent);

    return id;
  }

  /**
   * Verifies that this instance wasn't closed, or throws
   * {@link AlreadyClosedException} if it is.
   */
  protected final void ensureOpen() {
    if (isClosed) {
      throw new AlreadyClosedException("The taxonomy writer has already been closed");
    }
  }
  
  /**
   * Note that the methods calling addCategoryDocument() are synchornized, so
   * this method is effectively synchronized as well.
   */
  private int addCategoryDocument(CategoryPath categoryPath, int parent) throws IOException {
    // Before Lucene 2.9, position increments >=0 were supported, so we
    // added 1 to parent to allow the parent -1 (the parent of the root).
    // Unfortunately, starting with Lucene 2.9, after LUCENE-1542, this is
    // no longer enough, since 0 is not encoded consistently either (see
    // comment in SinglePositionTokenStream). But because we must be
    // backward-compatible with existing indexes, we can't just fix what
    // we write here (e.g., to write parent+2), and need to do a workaround
    // in the reader (which knows that anyway only category 0 has a parent
    // -1).    
    parentStream.set(Math.max(parent + 1, 1));
    Document d = new Document();
    d.add(parentStreamField);

    fullPathField.setStringValue(categoryPath.toString(delimiter));
    d.add(fullPathField);

    // Note that we do no pass an Analyzer here because the fields that are
    // added to the Document are untokenized or contains their own TokenStream.
    // Therefore the IndexWriter's Analyzer has no effect.
    indexWriter.addDocument(d);
    int id = nextID++;

    // added a category document, mark that ReaderManager is not up-to-date
    shouldRefreshReaderManager = true;
    
    // also add to the parent array
    taxoArrays = getTaxoArrays().add(id, parent);

    // NOTE: this line must be executed last, or else the cache gets updated
    // before the parents array (LUCENE-4596)
    addToCache(categoryPath, id);

    return id;
  }

  private static class SinglePositionTokenStream extends TokenStream {
    private CharTermAttribute termAtt;
    private PositionIncrementAttribute posIncrAtt;
    private boolean returned;
    public SinglePositionTokenStream(String word) {
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      termAtt.setEmpty().append(word);
      returned = true;
    }
    /**
     * Set the value we want to keep, as the position increment.
     * Note that when TermPositions.nextPosition() is later used to
     * retrieve this value, val-1 will be returned, not val.
     * <P>
     * IMPORTANT NOTE: Before Lucene 2.9, val>=0 were safe (for val==0,
     * the retrieved position would be -1). But starting with Lucene 2.9,
     * this unfortunately changed, and only val>0 are safe. val=0 can
     * still be used, but don't count on the value you retrieve later
     * (it could be 0 or -1, depending on circumstances or versions).
     * This change is described in Lucene's JIRA: LUCENE-1542. 
     */
    public void set(int val) {
      posIncrAtt.setPositionIncrement(val);
      returned = false;
    }
    @Override
    public boolean incrementToken() throws IOException {
      if (returned) {
        return false;
      }
      return returned = true;
    }
  }

  private void addToCache(CategoryPath categoryPath, int id) throws IOException {
    if (cache.put(categoryPath, id)) {
      // If cache.put() returned true, it means the cache was limited in
      // size, became full, and parts of it had to be evicted. It is
      // possible that a relatively-new category that isn't yet visible
      // to our 'reader' was evicted, and therefore we must now refresh 
      // the reader.
      refreshReaderManager();
      cacheIsComplete = false;
    }
  }

  private synchronized void refreshReaderManager() throws IOException {
    // this method is synchronized since it cannot happen concurrently with
    // addCategoryDocument -- when this method returns, we must know that the
    // reader manager's state is current. also, it sets shouldRefresh to false, 
    // and this cannot overlap with addCatDoc too.
    // NOTE: since this method is sync'ed, it can call maybeRefresh, instead of
    // maybeRefreshBlocking. If ever this is changed, make sure to change the
    // call too.
    if (shouldRefreshReaderManager && initializedReaderManager) {
      readerManager.maybeRefresh();
      shouldRefreshReaderManager = false;
    }
  }
  
  @Override
  public synchronized void commit() throws IOException {
    ensureOpen();
    indexWriter.setCommitData(combinedCommitData(indexWriter.getCommitData()));
    indexWriter.commit();
  }

  /** Combine original user data with the taxonomy epoch. */
  private Map<String,String> combinedCommitData(Map<String,String> commitData) {
    Map<String,String> m = new HashMap<String, String>();
    if (commitData != null) {
      m.putAll(commitData);
    }
    m.put(INDEX_EPOCH, Long.toString(indexEpoch));
    return m;
  }
  
  @Override
  public void setCommitData(Map<String,String> commitUserData) {
    indexWriter.setCommitData(combinedCommitData(commitUserData));
  }
  
  @Override
  public Map<String,String> getCommitData() {
    return combinedCommitData(indexWriter.getCommitData());
  }
  
  /**
   * prepare most of the work needed for a two-phase commit.
   * See {@link IndexWriter#prepareCommit}.
   */
  @Override
  public synchronized void prepareCommit() throws IOException {
    ensureOpen();
    indexWriter.setCommitData(combinedCommitData(indexWriter.getCommitData()));
    indexWriter.prepareCommit();
  }
  
  @Override
  public int getSize() {
    ensureOpen();
    return nextID;
  }
  
  /**
   * Set the number of cache misses before an attempt is made to read the entire
   * taxonomy into the in-memory cache.
   * <p>
   * This taxonomy writer holds an in-memory cache of recently seen categories
   * to speed up operation. On each cache-miss, the on-disk index needs to be
   * consulted. When an existing taxonomy is opened, a lot of slow disk reads
   * like that are needed until the cache is filled, so it is more efficient to
   * read the entire taxonomy into memory at once. We do this complete read
   * after a certain number (defined by this method) of cache misses.
   * <p>
   * If the number is set to {@code 0}, the entire taxonomy is read into the
   * cache on first use, without fetching individual categories first.
   * <p>
   * NOTE: it is assumed that this method is called immediately after the
   * taxonomy writer has been created.
   */
  public void setCacheMissesUntilFill(int i) {
    ensureOpen();
    cacheMissesUntilFill = i;
  }
  
  // we need to guarantee that if several threads call this concurrently, only
  // one executes it, and after it returns, the cache is updated and is either
  // complete or not.
  private synchronized void perhapsFillCache() throws IOException {
    if (cacheMisses.get() < cacheMissesUntilFill) {
      return;
    }
    
    if (!shouldFillCache) {
      // we already filled the cache once, there's no need to re-fill it
      return;
    }
    shouldFillCache = false;
    
    initReaderManager();

    boolean aborted = false;
    DirectoryReader reader = readerManager.acquire();
    try {
      TermsEnum termsEnum = null;
      DocsEnum docsEnum = null;
      for (AtomicReaderContext ctx : reader.leaves()) {
        Terms terms = ctx.reader().terms(Consts.FULL);
        if (terms != null) { // cannot really happen, but be on the safe side
          termsEnum = terms.iterator(termsEnum);
          while (termsEnum.next() != null) {
            if (!cache.isFull()) {
              BytesRef t = termsEnum.term();
              // Since we guarantee uniqueness of categories, each term has exactly
              // one document. Also, since we do not allow removing categories (and
              // hence documents), there are no deletions in the index. Therefore, it
              // is sufficient to call next(), and then doc(), exactly once with no
              // 'validation' checks.
              CategoryPath cp = new CategoryPath(t.utf8ToString(), delimiter);
              docsEnum = termsEnum.docs(null, docsEnum, DocsEnum.FLAG_NONE);
              boolean res = cache.put(cp, docsEnum.nextDoc() + ctx.docBase);
              assert !res : "entries should not have been evicted from the cache";
            } else {
              // the cache is full and the next put() will evict entries from it, therefore abort the iteration.
              aborted = true;
              break;
            }
          }
        }
        if (aborted) {
          break;
        }
      }
    } finally {
      readerManager.release(reader);
    }

    cacheIsComplete = !aborted;
    if (cacheIsComplete) {
      synchronized (this) {
        // everything is in the cache, so no need to keep readerManager open.
        // this block is executed in a sync block so that it works well with
        // initReaderManager called in parallel.
        readerManager.close();
        readerManager = null;
        initializedReaderManager = false;
      }
    }
  }

  private TaxonomyIndexArrays getTaxoArrays() throws IOException {
    if (taxoArrays == null) {
      synchronized (this) {
        if (taxoArrays == null) {
          initReaderManager();
          DirectoryReader reader = readerManager.acquire();
          try {
            // according to Java Concurrency, this might perform better on some
            // JVMs, since the object initialization doesn't happen on the
            // volatile member.
            TaxonomyIndexArrays tmpArrays = new TaxonomyIndexArrays(reader);
            taxoArrays = tmpArrays;
          } finally {
            readerManager.release(reader);
          }
        }
      }
    }
    return taxoArrays;
  }
  
  @Override
  public int getParent(int ordinal) throws IOException {
    ensureOpen();
    // Note: the following if() just enforces that a user can never ask
    // for the parent of a nonexistant category - even if the parent array
    // was allocated bigger than it really needs to be.
    if (ordinal >= nextID) {
      throw new ArrayIndexOutOfBoundsException("requested ordinal is bigger than the largest ordinal in the taxonomy");
    }
    
    int[] parents = getTaxoArrays().parents();
    assert ordinal < parents.length : "requested ordinal (" + ordinal + "); parents.length (" + parents.length + ") !";
    return parents[ordinal];
  }
  
  /**
   * Takes the categories from the given taxonomy directory, and adds the
   * missing ones to this taxonomy. Additionally, it fills the given
   * {@link OrdinalMap} with a mapping from the original ordinal to the new
   * ordinal.
   */
  public void addTaxonomy(Directory taxoDir, OrdinalMap map) throws IOException {
    ensureOpen();
    DirectoryReader r = DirectoryReader.open(taxoDir);
    try {
      final int size = r.numDocs();
      final OrdinalMap ordinalMap = map;
      ordinalMap.setSize(size);
      int base = 0;
      TermsEnum te = null;
      DocsEnum docs = null;
      for (final AtomicReaderContext ctx : r.leaves()) {
        final AtomicReader ar = ctx.reader();
        final Terms terms = ar.terms(Consts.FULL);
        te = terms.iterator(te);
        while (te.next() != null) {
          String value = te.term().utf8ToString();
          CategoryPath cp = new CategoryPath(value, delimiter);
          final int ordinal = addCategory(cp);
          docs = te.docs(null, docs, DocsEnum.FLAG_NONE);
          ordinalMap.addMapping(docs.nextDoc() + base, ordinal);
        }
        base += ar.maxDoc(); // no deletions, so we're ok
      }
      ordinalMap.addDone();
    } finally {
      r.close();
    }
  }

  /**
   * Mapping from old ordinal to new ordinals, used when merging indexes 
   * wit separate taxonomies.
   * <p> 
   * addToTaxonomies() merges one or more taxonomies into the given taxonomy
   * (this). An OrdinalMap is filled for each of the added taxonomies,
   * containing the new ordinal (in the merged taxonomy) of each of the
   * categories in the old taxonomy.
   * <P>  
   * There exist two implementations of OrdinalMap: MemoryOrdinalMap and
   * DiskOrdinalMap. As their names suggest, the former keeps the map in
   * memory and the latter in a temporary disk file. Because these maps will
   * later be needed one by one (to remap the counting lists), not all at the
   * same time, it is recommended to put the first taxonomy's map in memory,
   * and all the rest on disk (later to be automatically read into memory one
   * by one, when needed).
   */
  public static interface OrdinalMap {
    /**
     * Set the size of the map. This MUST be called before addMapping().
     * It is assumed (but not verified) that addMapping() will then be
     * called exactly 'size' times, with different origOrdinals between 0
     * and size-1.  
     */
    public void setSize(int size) throws IOException;
    public void addMapping(int origOrdinal, int newOrdinal) throws IOException;
    /**
     * Call addDone() to say that all addMapping() have been done.
     * In some implementations this might free some resources. 
     */
    public void addDone() throws IOException;
    /**
     * Return the map from the taxonomy's original (consecutive) ordinals
     * to the new taxonomy's ordinals. If the map has to be read from disk
     * and ordered appropriately, it is done when getMap() is called.
     * getMap() should only be called once, and only when the map is actually
     * needed. Calling it will also free all resources that the map might
     * be holding (such as temporary disk space), other than the returned int[].
     */
    public int[] getMap() throws IOException;
  }

  /**
   * {@link OrdinalMap} maintained in memory
   */
  public static final class MemoryOrdinalMap implements OrdinalMap {
    int[] map;
    @Override
    public void setSize(int taxonomySize) {
      map = new int[taxonomySize];
    }
    @Override
    public void addMapping(int origOrdinal, int newOrdinal) {
      map[origOrdinal] = newOrdinal;
    }
    @Override
    public void addDone() { /* nothing to do */ }
    @Override
    public int[] getMap() {
      return map;
    }
  }

  /**
   * {@link OrdinalMap} maintained on file system
   */
  public static final class DiskOrdinalMap implements OrdinalMap {
    File tmpfile;
    DataOutputStream out;

    public DiskOrdinalMap(File tmpfile) throws FileNotFoundException {
      this.tmpfile = tmpfile;
      out = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(tmpfile)));
    }

    @Override
    public void addMapping(int origOrdinal, int newOrdinal) throws IOException {
      out.writeInt(origOrdinal);
      out.writeInt(newOrdinal);
    }

    @Override
    public void setSize(int taxonomySize) throws IOException {
      out.writeInt(taxonomySize);
    }

    @Override
    public void addDone() throws IOException {
      if (out!=null) {
        out.close();
        out = null;
      }
    }

    int[] map = null;

    @Override
    public int[] getMap() throws IOException {
      if (map!=null) {
        return map;
      }
      addDone(); // in case this wasn't previously called
      DataInputStream in = new DataInputStream(new BufferedInputStream(
          new FileInputStream(tmpfile)));
      map = new int[in.readInt()];
      // NOTE: The current code assumes here that the map is complete,
      // i.e., every ordinal gets one and exactly one value. Otherwise,
      // we may run into an EOF here, or vice versa, not read everything.
      for (int i=0; i<map.length; i++) {
        int origordinal = in.readInt();
        int newordinal = in.readInt();
        map[origordinal] = newordinal;
      }
      in.close();
      // Delete the temporary file, which is no longer needed.
      if (!tmpfile.delete()) {
        tmpfile.deleteOnExit();
      }
      return map;
    }
  }

  /**
   * Rollback changes to the taxonomy writer and closes the instance. Following
   * this method the instance becomes unusable (calling any of its API methods
   * will yield an {@link AlreadyClosedException}).
   */
  @Override
  public synchronized void rollback() throws IOException {
    ensureOpen();
    indexWriter.rollback();
    doClose();
  }
  
  /**
   * Replaces the current taxonomy with the given one. This method should
   * generally be called in conjunction with
   * {@link IndexWriter#addIndexes(Directory...)} to replace both the taxonomy
   * as well as the search index content.
   */
  public synchronized void replaceTaxonomy(Directory taxoDir) throws IOException {
    // replace the taxonomy by doing IW optimized operations
    indexWriter.deleteAll();
    indexWriter.addIndexes(taxoDir);
    shouldRefreshReaderManager = true;
    initReaderManager(); // ensure that it's initialized
    refreshReaderManager();
    nextID = indexWriter.maxDoc();
    
    // need to clear the cache, so that addCategory won't accidentally return
    // old categories that are in the cache.
    cache.clear();
    cacheIsComplete = false;
    shouldFillCache = true;
    
    // update indexEpoch as a taxonomy replace is just like it has be recreated
    ++indexEpoch;
  }

  /** Returns the {@link Directory} of this taxonomy writer. */
  public Directory getDirectory() {
    return dir;
  }
  
  /**
   * Used by {@link DirectoryTaxonomyReader} to support NRT.
   * <p>
   * <b>NOTE:</b> you should not use the obtained {@link IndexWriter} in any
   * way, other than opening an IndexReader on it, or otherwise, the taxonomy
   * index may become corrupt!
   */
  final IndexWriter getInternalIndexWriter() {
    return indexWriter;
  }
  
  /** Expert: returns current index epoch, if this is a
   * near-real-time reader.  Used by {@link
   * DirectoryTaxonomyReader} to support NRT. 
   *
   * @lucene.internal */
  public final long getTaxonomyEpoch() {
    return indexEpoch;
  }
}

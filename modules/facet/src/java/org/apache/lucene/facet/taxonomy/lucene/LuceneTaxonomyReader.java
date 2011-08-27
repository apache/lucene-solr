package org.apache.lucene.facet.taxonomy.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.lucene.Consts.LoadFullPathOnly;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.collections.LRUHashMap;

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

/** 
 * LuceneTaxonomyReader is a {@link TaxonomyReader} which retrieves stored
 * taxonomy information from a separate Lucene index. By using a Lucene index,
 * rather than some specialized file format, we get for "free" its correctness
 * (especially regarding concurrency), and the ability to save it on any
 * implementation of Directory (and not just the file system).
 * <P>
 * Reading from the on-disk index on every method call is too slow, so this
 * implementation employs caching: Some methods cache recent requests and
 * their results, while other methods prefetch all the data into memory
 * and then provide answers directly from in-memory tables. See the
 * documentation of individual methods for comments on their performance.
 * 
 * @lucene.experimental
 */
public class LuceneTaxonomyReader implements TaxonomyReader {

  private static final Logger logger = Logger.getLogger(LuceneTaxonomyReader.class.getName());
  
  private IndexReader indexReader;

  // The following lock is used to allow multiple threads to read from the
  // index concurrently, while having them block during the very short
  // critical moment of refresh() (see comments below). Note, however, that
  // we only read from the index when we don't have the entry in our cache,
  // and the caches are locked separately.
  private ReadWriteLock indexReaderLock = new ReentrantReadWriteLock();

  // The following are the limited-size LRU caches used to cache the latest
  // results from getOrdinal() and getCategoryCache().
  // Because LRUHashMap is not thread-safe, we need to synchronize on this
  // object when using it. Unfortunately, this is not optimal under heavy
  // contention because it means that while one thread is using the cache
  // (reading or modifying) others are blocked from using it - or even
  // starting to do benign things like calculating the hash function. A more
  // efficient approach would be to use a non-locking (as much as possible)
  // concurrent solution, along the lines of java.util.concurrent.ConcurrentHashMap
  // but with LRU semantics.
  // However, even in the current sub-optimal implementation we do not make
  // the mistake of locking out readers while waiting for disk in a cache
  // miss - below, we do not hold cache lock while reading missing data from
  // disk.
  private final LRUHashMap<String, Integer> getOrdinalCache;
  private final LRUHashMap<Integer, String> getCategoryCache;

  // getParent() needs to be extremely efficient, to the point that we need
  // to fetch all the data in advance into memory, and answer these calls
  // from memory. Currently we use a large integer array, which is
  // initialized when the taxonomy is opened, and potentially enlarged
  // when it is refresh()ed.
  // These arrays are not syncrhonized. Rather, the reference to the array
  // is volatile, and the only writing operation (refreshPrefetchArrays)
  // simply creates a new array and replaces the reference. The volatility
  // of the reference ensures the correct atomic replacement and its
  // visibility properties (the content of the array is visible when the
  // new reference is visible).
  private ParentArray parentArray;

  private char delimiter = Consts.DEFAULT_DELIMITER;

  /**
   * Open for reading a taxonomy stored in a given {@link Directory}.
   * @param directory
   *    The {@link Directory} in which to the taxonomy lives. Note that
   *    the taxonomy is read directly to that directory (not from a
   *    subdirectory of it).
   * @throws CorruptIndexException if the Taxonomy is corrupted.
   * @throws IOException if another error occurred.
   */
  public LuceneTaxonomyReader(Directory directory)
  throws CorruptIndexException, IOException {
    this.indexReader = openIndexReader(directory);

    // These are the default cache sizes; they can be configured after
    // construction with the cache's setMaxSize() method
    getOrdinalCache = new LRUHashMap<String, Integer>(4000);
    getCategoryCache = new LRUHashMap<Integer, String>(4000);

    // TODO (Facet): consider lazily create parent array it when asked, not in the constructor
    parentArray = new ParentArray();
    parentArray.refresh(indexReader);
  }

  protected IndexReader openIndexReader(Directory directory) throws CorruptIndexException, IOException {
    return IndexReader.open(directory);
  }

  // convenience constructors... deprecated because they cause confusion
  // because they use parent directory instead of the actual directory.
  private Directory ourDirectory = null; // remember directory to close later, but only if we opened it here
  /**
   * Open for reading a taxonomy stored in a subdirectory of a given
   * directory on the file system.
   * @param parentDir The parent directory of the taxonomy's directory
   * (usually this would be the directory holding the index).
   * @param name The name of the taxonomy, and the subdirectory holding it. 
   * @throws CorruptIndexException if the Taxonomy is corrupted.
   * @throws IOException if another error occurred.
   */  
  @Deprecated
  public LuceneTaxonomyReader(File parentDir, String name)
  throws CorruptIndexException, IOException {
    this(FSDirectory.open(new File(parentDir, name)));
    ourDirectory = indexReader.directory(); // remember to close the directory we opened
  }

  /**
   * Open for reading a taxonomy stored in a subdirectory of a given
   * directory on the file system.
   * @param parentDir The parent directory of the taxonomy's directory.
   * @param name The name of the taxonomy, and the subdirectory holding it. 
   * @throws CorruptIndexException if the Taxonomy is corrupted.
   * @throws IOException if another error occurred.
   */  
  @Deprecated
  public LuceneTaxonomyReader(String parentDir, String name)
  throws CorruptIndexException, IOException {
    this(FSDirectory.open(new File(parentDir, name)));
    ourDirectory = indexReader.directory(); // rememebr to close the directory we opened
  }

  /**
   * setCacheSize controls the maximum allowed size of each of the caches
   * used by {@link #getPath(int)} and {@link #getOrdinal(CategoryPath)}.
   * <P>
   * Currently, if the given size is smaller than the current size of
   * a cache, it will not shrink, and rather we be limited to its current
   * size.
   * @param size the new maximum cache size, in number of entries.
   */
  public void setCacheSize(int size) {
    synchronized(getCategoryCache) {
      getCategoryCache.setMaxSize(size);
    }
    synchronized(getOrdinalCache) {
      getOrdinalCache.setMaxSize(size);
    }
  }

  /**
   * setDelimiter changes the character that the taxonomy uses in its
   * internal storage as a delimiter between category components. Do not
   * use this method unless you really know what you are doing.
   * <P>
   * If you do use this method, make sure you call it before any other
   * methods that actually queries the taxonomy. Moreover, make sure you
   * always pass the same delimiter for all LuceneTaxonomyWriter and
   * LuceneTaxonomyReader objects you create.
   */
  public void setDelimiter(char delimiter) {
    this.delimiter = delimiter;
  }

  public int getOrdinal(CategoryPath categoryPath) throws IOException {
    if (categoryPath.length()==0) {
      return ROOT_ORDINAL;
    }
    String path = categoryPath.toString(delimiter);

    // First try to find the answer in the LRU cache:
    synchronized(getOrdinalCache) {
      Integer res = getOrdinalCache.get(path);
      if (res!=null) {
        return res.intValue();
      }
    }

    // If we're still here, we have a cache miss. We need to fetch the
    // value from disk, and then also put it in the cache:
    int ret = TaxonomyReader.INVALID_ORDINAL;
    try {
      indexReaderLock.readLock().lock();
      // TODO (Facet): avoid Multi*?
      Bits liveDocs = MultiFields.getLiveDocs(indexReader);
      DocsEnum docs = MultiFields.getTermDocsEnum(indexReader, liveDocs, Consts.FULL, new BytesRef(path));
      if (docs != null && docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        ret = docs.docID();
      }
    } finally {
      indexReaderLock.readLock().unlock();
    }

    // Put the new value in the cache. Note that it is possible that while
    // we were doing the above fetching (without the cache locked), some
    // other thread already added the same category to the cache. We do
    // not care about this possibilty, as LRUCache replaces previous values
    // of the same keys (it doesn't store duplicates).
    synchronized(getOrdinalCache) {
      // GB: new Integer(int); creates a new object each and every time.
      // Integer.valueOf(int) might not (See JavaDoc). 
      getOrdinalCache.put(path, Integer.valueOf(ret));
    }

    return ret;
  }

  public CategoryPath getPath(int ordinal) throws CorruptIndexException, IOException {
    // TODO (Facet): Currently, the LRU cache we use (getCategoryCache) holds
    // strings with delimiters, not CategoryPath objects, so even if
    // we have a cache hit, we need to process the string and build a new
    // CategoryPath object every time. What is preventing us from putting
    // the actual CategoryPath object in the cache is the fact that these
    // objects are mutable. So we should create an immutable (read-only)
    // interface that CategoryPath implements, and this method should
    // return this interface, not the writable CategoryPath.
    String label = getLabel(ordinal);
    if (label==null) {
      return null;  
    }
    return new CategoryPath(label, delimiter);
  }

  public boolean getPath(int ordinal, CategoryPath result) throws CorruptIndexException, IOException {
    String label = getLabel(ordinal);
    if (label==null) {
      return false;
    }
    result.clear();
    result.add(label, delimiter);
    return true;
  }

  private String getLabel(int catID) throws CorruptIndexException, IOException {
    // First try to find the answer in the LRU cache. It is very
    // unfortunate that we need to allocate an Integer object here -
    // it would have been better if we used a hash table specifically
    // designed for int keys...
    // GB: new Integer(int); creates a new object each and every time.
    // Integer.valueOf(int) might not (See JavaDoc). 
    Integer catIDInteger = Integer.valueOf(catID);

    synchronized(getCategoryCache) {
      String res = getCategoryCache.get(catIDInteger);
      if (res!=null) {
        return res;
      }
    }

    // If we're still here, we have a cache miss. We need to fetch the
    // value from disk, and then also put it in the cache:
    String ret;
    try {
      indexReaderLock.readLock().lock();
      // The taxonomy API dictates that if we get an invalid category
      // ID, we should return null, If we don't check this here, we
      // can some sort of an exception from the document() call below.
      // NOTE: Currently, we *do not* cache this return value; There
      // isn't much point to do so, because checking the validity of
      // the docid doesn't require disk access - just comparing with
      // the number indexReader.maxDoc().
      if (catID<0 || catID>=indexReader.maxDoc()) {
        return null;
      }
      final LoadFullPathOnly loader = new LoadFullPathOnly();
      indexReader.document(catID, loader);
      ret = loader.getFullPath();
    } finally {
      indexReaderLock.readLock().unlock();
    }
    // Put the new value in the cache. Note that it is possible that while
    // we were doing the above fetching (without the cache locked), some
    // other thread already added the same category to the cache. We do
    // not care about this possibility, as LRUCache replaces previous
    // values of the same keys (it doesn't store duplicates).
    synchronized (getCategoryCache) {
      getCategoryCache.put(catIDInteger, ret);
    }

    return ret;
  }

  public int getParent(int ordinal) {
    // Note how we don't need to hold the read lock to do the following,
    // because the array reference is volatile, ensuring the correct
    // visibility and ordering: if we get the new reference, the new
    // data is also visible to this thread.
    return getParentArray()[ordinal];
  }

  /**
   * getParentArray() returns an int array of size getSize() listing the
   * ordinal of the parent category of each category in the taxonomy.
   * <P>
   * The caller can hold on to the array it got indefinitely - it is
   * guaranteed that no-one else will modify it. The other side of the
   * same coin is that the caller must treat the array it got as read-only
   * and <B>not modify it</B>, because other callers might have gotten the
   * same array too, and getParent() calls are also answered from the
   * same array.
   * <P>
   * The getParentArray() call is extremely efficient, merely returning
   * a reference to an array that already exists. For a caller that plans
   * to call getParent() for many categories, using getParentArray() and
   * the array it returns is a somewhat faster approach because it avoids
   * the overhead of method calls and volatile dereferencing.
   * <P>
   * If you use getParentArray() instead of getParent(), remember that
   * the array you got is (naturally) not modified after a refresh(),
   * so you should always call getParentArray() again after a refresh().
   */

  public int[] getParentArray() {
    // Note how we don't need to hold the read lock to do the following,
    // because the array reference is volatile, ensuring the correct
    // visibility and ordering: if we get the new reference, the new
    // data is also visible to this thread.
    return parentArray.getArray();
  }

  // Note that refresh() is synchronized (it is the only synchronized
  // method in this class) to ensure that it never gets called concurrently
  // with itself.
  public synchronized void refresh() throws IOException {
    /*
     * Since refresh() can be a lengthy operation, it is very important that we
     * avoid locking out all readers for its duration. This is why we don't hold
     * the indexReaderLock write lock for the entire duration of this method. In
     * fact, it is enough to hold it only during a single assignment! Other
     * comments in this method will explain this.
     */

    // note that the lengthy operation indexReader.reopen() does not
    // modify the reader, so we can do it without holding a lock. We can
    // safely read indexReader without holding the write lock, because
    // no other thread can be writing at this time (this method is the
    // only possible writer, and it is "synchronized" to avoid this case).
    IndexReader r2 = indexReader.reopen();
    if (indexReader != r2) {
      IndexReader oldreader = indexReader;
      // we can close the old searcher, but need to synchronize this
      // so that we don't close it in the middle that another routine
      // is reading from it.
      indexReaderLock.writeLock().lock();
      indexReader = r2;
      indexReaderLock.writeLock().unlock();
      // We can close the old reader, but need to be certain that we
      // don't close it while another method is reading from it.
      // Luckily, we can be certain of that even without putting the
      // oldreader.close() in the locked section. The reason is that
      // after lock() succeeded above, we know that all existing readers
      // had finished (this is what a read-write lock ensures). New
      // readers, starting after the unlock() we just did, already got
      // the new indexReader we set above. So nobody can be possibly
      // using the old indexReader, and we can close it:
      oldreader.close();

      // We prefetch some of the arrays to make requests much faster.
      // Let's refresh these prefetched arrays; This refresh is much
      // is made more efficient by assuming that it is enough to read
      // the values for new categories (old categories could not have been
      // changed or deleted)
      // Note that this this done without the write lock being held,
      // which means that it is possible that during a refresh(), a
      // reader will have some methods (like getOrdinal and getCategory)
      // return fresh information, while getParent()
      // (only to be prefetched now) still return older information.
      // We consider this to be acceptable. The important thing,
      // however, is that refreshPrefetchArrays() itself writes to
      // the arrays in a correct manner (see discussion there)
      parentArray.refresh(indexReader);

      // Remove any INVALID_ORDINAL values from the ordinal cache,
      // because it is possible those are now answered by the new data!
      Iterator<Entry<String, Integer>> i = getOrdinalCache.entrySet().iterator();
      while (i.hasNext()) {
        Entry<String, Integer> e = i.next();
        if (e.getValue().intValue() == INVALID_ORDINAL) {
          i.remove();
        }
      }
    }
  }

  public void close() throws IOException {
    indexReader.close();
    if (ourDirectory!=null) {
      ourDirectory.close();
    }
  }

  public int getSize() {
    indexReaderLock.readLock().lock();
    try {
      return indexReader.numDocs();
    } finally {
      indexReaderLock.readLock().unlock();
    }
  }

  public Map<String, String> getCommitUserData() {
    return indexReader.getCommitUserData();
  }
  
  private ChildrenArrays childrenArrays;
  Object childrenArraysRebuild = new Object();

  public ChildrenArrays getChildrenArrays() {
    // Check if the taxonomy grew since we built the array, and if it
    // did, create new (and larger) arrays and fill them as required.
    // We do all this under a lock, two prevent to concurrent calls to
    // needlessly do the same array building at the same time.
    synchronized(childrenArraysRebuild) {
      int num = getSize();
      int first;
      if (childrenArrays==null) {
        first = 0;
      } else {
        first = childrenArrays.getYoungestChildArray().length;
      }
      // If the taxonomy hasn't grown, we can return the existing object
      // immediately
      if (first == num) {
        return childrenArrays;
      }
      // Otherwise, build new arrays for a new ChildrenArray object.
      // These arrays start with an enlarged copy of the previous arrays,
      // and then are modified to take into account the new categories:
      int[] newYoungestChildArray = new int[num];
      int[] newOlderSiblingArray = new int[num];
      // In Java 6, we could just do Arrays.copyOf()...
      if (childrenArrays!=null) {
        System.arraycopy(childrenArrays.getYoungestChildArray(), 0,
            newYoungestChildArray, 0, childrenArrays.getYoungestChildArray().length);
        System.arraycopy(childrenArrays.getOlderSiblingArray(), 0,
            newOlderSiblingArray, 0, childrenArrays.getOlderSiblingArray().length);
      }
      int[] parents = getParentArray();
      for (int i=first; i<num; i++) {
        newYoungestChildArray[i] = INVALID_ORDINAL;
      }
      // In the loop below we can ignore the root category (0) because
      // it has no parent
      if (first==0) {
        first = 1;
        newOlderSiblingArray[0] = INVALID_ORDINAL;
      }
      for (int i=first; i<num; i++) {
        // Note that parents[i] is always < i, so the right-hand-side of
        // the following line is already set when we get here.
        newOlderSiblingArray[i] = newYoungestChildArray[parents[i]];
        newYoungestChildArray[parents[i]] = i;
      }
      // Finally switch to the new arrays
      childrenArrays = new ChildrenArraysImpl(newYoungestChildArray,
          newOlderSiblingArray);
      return childrenArrays;
    }
  }

  public String toString(int max) {
    StringBuilder sb = new StringBuilder();
    int upperl = Math.min(max, this.indexReader.maxDoc());
    for (int i = 0; i < upperl; i++) {
      try {
        CategoryPath category = this.getPath(i);
        if (category == null) {
          sb.append(i + ": NULL!! \n");
          continue;
        } 
        if (category.length() == 0) {
          sb.append(i + ": EMPTY STRING!! \n");
          continue;
        }
        sb.append(i +": "+category.toString()+"\n");
      } catch (IOException e) {
        if (logger.isLoggable(Level.FINEST)) {
          logger.log(Level.FINEST, e.getMessage(), e);
        }
      }
    }
    return sb.toString();
  }

  private static final class ChildrenArraysImpl implements ChildrenArrays {
    private int[] youngestChildArray, olderSiblingArray;
    public ChildrenArraysImpl(int[] youngestChildArray, int[] olderSiblingArray) {
      this.youngestChildArray = youngestChildArray;
      this.olderSiblingArray = olderSiblingArray;
    }
    public int[] getOlderSiblingArray() {
      return olderSiblingArray;
    }
    public int[] getYoungestChildArray() {
      return youngestChildArray;
    }    
  }

  /**
   * Expert:  This method is only for expert use.
   * Note also that any call to refresh() will invalidate the returned reader,
   * so the caller needs to take care of appropriate locking.
   * 
   * @return lucene indexReader
   */
  IndexReader getInternalIndexReader() {
    return this.indexReader;
  }

  /**
   * Expert: decreases the refCount of this TaxonomyReader instance. 
   * If the refCount drops to 0, then pending changes (if any) are 
   * committed to the taxonomy index and this reader is closed. 
   * @throws IOException 
   */
  public void decRef() throws IOException {
    this.indexReader.decRef();
  }
  
  /**
   * Expert: returns the current refCount for this taxonomy reader
   */
  public int getRefCount() {
    return this.indexReader.getRefCount();
  }
  
  /**
   * Expert: increments the refCount of this TaxonomyReader instance. 
   * RefCounts are used to determine when a taxonomy reader can be closed 
   * safely, i.e. as soon as there are no more references. 
   * Be sure to always call a corresponding decRef(), in a finally clause; 
   * otherwise the reader may never be closed. 
   */
  public void incRef() {
    this.indexReader.incRef();
  }
}

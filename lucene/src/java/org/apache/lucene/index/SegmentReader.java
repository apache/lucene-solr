package org.apache.lucene.index;

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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.FieldCache; // javadocs
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CloseableThreadLocal;

/**
 * @lucene.experimental
 */
public final class SegmentReader extends IndexReader implements Cloneable {
  private final boolean readOnly;

  private SegmentInfo si;
  private final ReaderContext readerContext = new AtomicReaderContext(this);
  final CloseableThreadLocal<StoredFieldsReader> fieldsReaderLocal = new FieldsReaderLocal();
  final CloseableThreadLocal<TermVectorsReader> termVectorsLocal = new CloseableThreadLocal<TermVectorsReader>();

  volatile BitVector liveDocs = null;
  volatile Object combinedCoreAndDeletesKey;
  AtomicInteger liveDocsRef = null;
  boolean hasChanges = false;

  // TODO: remove deletions from SR
  private int pendingDeleteCount;
  private boolean rollbackHasChanges = false;
  private SegmentInfo rollbackSegmentInfo;
  private int rollbackPendingDeleteCount;
  // end TODO

  SegmentCoreReaders core;

  /**
   * Sets the initial value 
   */
  private class FieldsReaderLocal extends CloseableThreadLocal<StoredFieldsReader> {
    @Override
    protected StoredFieldsReader initialValue() {
      return core.getFieldsReaderOrig().clone();
    }
  }
  
  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(SegmentInfo si, int termInfosIndexDivisor, IOContext context) throws CorruptIndexException, IOException {
    return get(true, si, true, termInfosIndexDivisor, context);
  }

  // TODO: remove deletions from SR
  static SegmentReader getRW(SegmentInfo si, boolean doOpenStores, int termInfosIndexDivisor, IOContext context) throws CorruptIndexException, IOException {
    return get(false, si, doOpenStores, termInfosIndexDivisor, context);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private static SegmentReader get(boolean readOnly,
                                  SegmentInfo si,
                                  boolean doOpenStores,
                                  int termInfosIndexDivisor,
                                  IOContext context)
    throws CorruptIndexException, IOException {
    
    SegmentReader instance = new SegmentReader(readOnly, si);
    boolean success = false;
    try {
      instance.core = new SegmentCoreReaders(instance, si.dir, si, context, termInfosIndexDivisor);
      if (doOpenStores) {
        instance.core.openDocStores(si);
      }
      instance.loadLiveDocs(context);
      success = true;
    } finally {

      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above.  In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        instance.doClose();
      }
    }
    return instance;
  }

  private SegmentReader(boolean readOnly, SegmentInfo si) {
    this.readOnly = readOnly;
    this.si = si;
  }

  void openDocStores() throws IOException {
    core.openDocStores(si);
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return liveDocs;
  }

  private boolean checkLiveCounts() throws IOException {
    final int recomputedCount = liveDocs.getRecomputedCount();
    // First verify BitVector is self consistent:
    assert liveDocs.count() == recomputedCount : "live count=" + liveDocs.count() + " vs recomputed count=" + recomputedCount;

    assert si.getDelCount() == si.docCount - recomputedCount :
      "delete count mismatch: info=" + si.getDelCount() + " vs BitVector=" + (si.docCount-recomputedCount);

    // Verify # deletes does not exceed maxDoc for this
    // segment:
    assert si.getDelCount() <= maxDoc() : 
      "delete count mismatch: " + recomputedCount + ") exceeds max doc (" + maxDoc() + ") for segment " + si.name;

    return true;
  }

  private void loadLiveDocs(IOContext context) throws IOException {
    // NOTE: the bitvector is stored using the regular directory, not cfs
    if (si.hasDeletions()) {
      liveDocs = new BitVector(directory(), si.getDelFileName(), new IOContext(context, true));
      liveDocsRef = new AtomicInteger(1);
      assert checkLiveCounts();
      if (liveDocs.size() != si.docCount) {
        throw new CorruptIndexException("document count mismatch: deleted docs count " + liveDocs.size() + " vs segment doc count " + si.docCount + " segment=" + si.name);
      }
    } else {
      assert si.getDelCount() == 0;
    }
    // we need a key reflecting actual deletes (if existent or not):
    combinedCoreAndDeletesKey = new Object();
  }

  /** Clones are always in readOnly mode */
  @Override
  public final synchronized Object clone() {
    try {
      return reopenSegment(si, true);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  // used by DirectoryReader:
  synchronized SegmentReader reopenSegment(SegmentInfo si, boolean doClone) throws CorruptIndexException, IOException {
    ensureOpen();
    boolean deletionsUpToDate = (this.si.hasDeletions() == si.hasDeletions()) 
                                  && (!si.hasDeletions() || this.si.getDelFileName().equals(si.getDelFileName()));

    // if we're cloning we need to run through the reopenSegment logic
    // also if both old and new readers aren't readonly, we clone to avoid sharing modifications
    if (deletionsUpToDate && !doClone && readOnly) {
      return null;
    }    

    // When cloning, the incoming SegmentInfos should not
    // have any changes in it:
    assert !doClone || (deletionsUpToDate);

    // clone reader
    SegmentReader clone = new SegmentReader(true, si);

    boolean success = false;
    try {
      core.incRef();
      clone.core = core;
      clone.pendingDeleteCount = pendingDeleteCount;
      clone.combinedCoreAndDeletesKey = combinedCoreAndDeletesKey;

      if (doClone) {
        if (liveDocs != null) {
          liveDocsRef.incrementAndGet();
          clone.liveDocs = liveDocs;
          clone.liveDocsRef = liveDocsRef;
        }
      } else {
        if (!deletionsUpToDate) {
          // load deleted docs
          assert clone.liveDocs == null;
          clone.loadLiveDocs(IOContext.READ);
        } else if (liveDocs != null) {
          liveDocsRef.incrementAndGet();
          clone.liveDocs = liveDocs;
          clone.liveDocsRef = liveDocsRef;
        }
      }
      success = true;
    } finally {
      if (!success) {
        // An exception occurred during reopen, we have to decRef the norms
        // that we incRef'ed already and close singleNormsStream and FieldsReader
        clone.decRef();
      }
    }
    
    return clone;
  }

  /** @lucene.internal */
  public StoredFieldsReader getFieldsReader() {
    return fieldsReaderLocal.get();
  }

  @Override
  protected void doClose() throws IOException {
    if (hasChanges) {
      doCommit();
    }
    
    termVectorsLocal.close();
    fieldsReaderLocal.close();
    
    if (liveDocs != null) {
      liveDocsRef.decrementAndGet();
      // null so if an app hangs on to us we still free most ram
      liveDocs = null;
    }

    if (core != null) {
      core.decRef();
    }
  }

  @Override
  public boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return liveDocs != null;
  }

  List<String> files() throws IOException {
    return new ArrayList<String>(si.files());
  }
  
  FieldInfos fieldInfos() {
    return core.fieldInfos;
  }

  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    if (docID < 0 || docID >= maxDoc()) {       
      throw new IllegalArgumentException("docID must be >= 0 and < maxDoc=" + maxDoc() + " (got docID=" + docID + ")");
    }
    getFieldsReader().visitDocument(docID, visitor);
  }

  @Override
  public Fields fields() throws IOException {
    ensureOpen();
    return core.fields;
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    if (liveDocs != null) {
      return liveDocs.count();
    } else {
      return maxDoc();
    }
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return si.docCount;
  }

  /**
   * @see IndexReader#getFieldNames(org.apache.lucene.index.IndexReader.FieldOption)
   */
  @Override
  public Collection<String> getFieldNames(IndexReader.FieldOption fieldOption) {
    ensureOpen();

    Set<String> fieldSet = new HashSet<String>();
    for (FieldInfo fi : core.fieldInfos) {
      if (fieldOption == IndexReader.FieldOption.ALL) {
        fieldSet.add(fi.name);
      }
      else if (!fi.isIndexed && fieldOption == IndexReader.FieldOption.UNINDEXED) {
        fieldSet.add(fi.name);
      }
      else if (fi.indexOptions == IndexOptions.DOCS_ONLY && fieldOption == IndexReader.FieldOption.OMIT_TERM_FREQ_AND_POSITIONS) {
        fieldSet.add(fi.name);
      }
      else if (fi.indexOptions == IndexOptions.DOCS_AND_FREQS && fieldOption == IndexReader.FieldOption.OMIT_POSITIONS) {
        fieldSet.add(fi.name);
      }
      else if (fi.storePayloads && fieldOption == IndexReader.FieldOption.STORES_PAYLOADS) {
        fieldSet.add(fi.name);
      }
      else if (fi.isIndexed && fieldOption == IndexReader.FieldOption.INDEXED) {
        fieldSet.add(fi.name);
      }
      else if (fi.isIndexed && fi.storeTermVector == false && fieldOption == IndexReader.FieldOption.INDEXED_NO_TERMVECTOR) {
        fieldSet.add(fi.name);
      }
      else if (fi.storeTermVector == true &&
               fi.storePositionWithTermVector == false &&
               fi.storeOffsetWithTermVector == false &&
               fieldOption == IndexReader.FieldOption.TERMVECTOR) {
        fieldSet.add(fi.name);
      }
      else if (fi.isIndexed && fi.storeTermVector && fieldOption == IndexReader.FieldOption.INDEXED_WITH_TERMVECTOR) {
        fieldSet.add(fi.name);
      }
      else if (fi.storePositionWithTermVector && fi.storeOffsetWithTermVector == false && fieldOption == IndexReader.FieldOption.TERMVECTOR_WITH_POSITION) {
        fieldSet.add(fi.name);
      }
      else if (fi.storeOffsetWithTermVector && fi.storePositionWithTermVector == false && fieldOption == IndexReader.FieldOption.TERMVECTOR_WITH_OFFSET) {
        fieldSet.add(fi.name);
      }
      else if ((fi.storeOffsetWithTermVector && fi.storePositionWithTermVector) &&
                fieldOption == IndexReader.FieldOption.TERMVECTOR_WITH_POSITION_OFFSET) {
        fieldSet.add(fi.name);
      } 
      else if (fi.hasDocValues() && fieldOption == IndexReader.FieldOption.DOC_VALUES) {
        fieldSet.add(fi.name);
      }
    }
    return fieldSet;
  }

  @Override
  public boolean hasNorms(String field) {
    ensureOpen();
    FieldInfo fi = core.fieldInfos.fieldInfo(field);
    return fi != null && fi.isIndexed && !fi.omitNorms;
  }

  @Override
  public byte[] norms(String field) throws IOException {
    ensureOpen();
    return core.norms.norms(field);
  }

  /**
   * Create a clone from the initial TermVectorsReader and store it in the ThreadLocal.
   * @return TermVectorsReader
   * @lucene.internal
   */
  public TermVectorsReader getTermVectorsReader() {
    TermVectorsReader tvReader = termVectorsLocal.get();
    if (tvReader == null) {
      TermVectorsReader orig = core.getTermVectorsReaderOrig();
      if (orig == null) {
        return null;
      } else {
        tvReader = orig.clone();
      }
      termVectorsLocal.set(tvReader);
    }
    return tvReader;
  }

  TermVectorsReader getTermVectorsReaderOrig() {
    return core.getTermVectorsReaderOrig();
  }
  
  /** Return a term frequency vector for the specified document and field. The
   *  vector returned contains term numbers and frequencies for all terms in
   *  the specified field of this document, if the field had storeTermVector
   *  flag set.  If the flag was not set, the method returns null.
   * @throws IOException
   */
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null) {
      return null;
    }
    return termVectorsReader.get(docID);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    if (hasChanges) {
      buffer.append('*');
    }
    buffer.append(si.toString(core.dir, pendingDeleteCount));
    return buffer.toString();
  }
  
  @Override
  public ReaderContext getTopReaderContext() {
    ensureOpen();
    return readerContext;
  }

  /**
   * Return the name of the segment this reader is reading.
   */
  public String getSegmentName() {
    return core.segment;
  }
  
  /**
   * Return the SegmentInfo of the segment this reader is reading.
   */
  SegmentInfo getSegmentInfo() {
    return si;
  }

  void setSegmentInfo(SegmentInfo info) {
    si = info;
  }

  /** Returns the directory this index resides in. */
  @Override
  public Directory directory() {
    // Don't ensureOpen here -- in certain cases, when a
    // cloned/reopened reader needs to commit, it may call
    // this method on the closed original reader
    return core.dir;
  }

  // This is necessary so that cloned SegmentReaders (which
  // share the underlying postings data) will map to the
  // same entry in the FieldCache.  See LUCENE-1579.
  @Override
  public final Object getCoreCacheKey() {
    return core;
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return combinedCoreAndDeletesKey;
  }
  
  @Override
  public int getTermInfosIndexDivisor() {
    return core.termsIndexDivisor;
  }
  
  @Override
  public DocValues docValues(String field) throws IOException {
    ensureOpen();
    final PerDocProducer perDoc = core.perDocProducer;
    if (perDoc == null) {
      return null;
    }
    return perDoc.docValues(field);
  }

  /**
   * Clones the deleteDocs BitVector.  May be overridden by subclasses. New and experimental.
   * @param bv BitVector to clone
   * @return New BitVector
   */
  // TODO: remove deletions from SR
  BitVector cloneDeletedDocs(BitVector bv) {
    ensureOpen();
    return (BitVector)bv.clone();
  }

  // TODO: remove deletions from SR
  void doCommit() throws IOException {
    assert hasChanges;
    startCommit();
    boolean success = false;
    try {
      commitChanges();
      success = true;
    } finally {
      if (!success) {
        rollbackCommit();
      }
    }
  }

  // TODO: remove deletions from SR
  private void startCommit() {
    rollbackSegmentInfo = (SegmentInfo) si.clone();
    rollbackHasChanges = hasChanges;
    rollbackPendingDeleteCount = pendingDeleteCount;
  }

  // TODO: remove deletions from SR
  private void rollbackCommit() {
    si.reset(rollbackSegmentInfo);
    hasChanges = rollbackHasChanges;
    pendingDeleteCount = rollbackPendingDeleteCount;
  }

  // TODO: remove deletions from SR
  private synchronized void commitChanges() throws IOException {
    si.advanceDelGen();

    assert liveDocs.length() == si.docCount;

    // We can write directly to the actual name (vs to a
    // .tmp & renaming it) because the file is not live
    // until segments file is written:
    final String delFileName = si.getDelFileName();
    boolean success = false;
    try {
      liveDocs.write(directory(), delFileName, IOContext.DEFAULT);
      success = true;
    } finally {
      if (!success) {
        try {
          directory().deleteFile(delFileName);
        } catch (Throwable t) {
          // suppress this so we keep throwing the
          // original exception
        }
      }
    }
    si.setDelCount(si.getDelCount()+pendingDeleteCount);
    pendingDeleteCount = 0;
    assert (maxDoc()-liveDocs.count()) == si.getDelCount(): "delete count mismatch during commit: info=" + si.getDelCount() + " vs BitVector=" + (maxDoc()-liveDocs.count());
    hasChanges = false;
  }

  // TODO: remove deletions from SR
  synchronized void deleteDocument(int docNum) throws IOException {
    if (readOnly)
      throw new UnsupportedOperationException("this SegmentReader is read only");
    hasChanges = true;
    if (liveDocs == null) {
      liveDocs = new BitVector(maxDoc());
      liveDocs.setAll();
      liveDocsRef = new AtomicInteger(1);
    }
    // there is more than 1 SegmentReader with a reference to this
    // liveDocs BitVector so decRef the current liveDocsRef,
    // clone the BitVector, create a new liveDocsRef
    if (liveDocsRef.get() > 1) {
      AtomicInteger oldRef = liveDocsRef;
      liveDocs = cloneDeletedDocs(liveDocs);
      liveDocsRef = new AtomicInteger(1);
      oldRef.decrementAndGet();
    }
    // we need a key reflecting actual deletes (if existent or not):
    combinedCoreAndDeletesKey = new Object();
    // liveDocs are now dirty:
    if (liveDocs.getAndClear(docNum)) {
      pendingDeleteCount++;
    }
  }
  
  /**
   * Called when the shared core for this SegmentReader
   * is closed.
   * <p>
   * This listener is called only once all SegmentReaders 
   * sharing the same core are closed.  At this point it 
   * is safe for apps to evict this reader from any caches 
   * keyed on {@link #getCoreCacheKey}.  This is the same 
   * interface that {@link FieldCache} uses, internally, 
   * to evict entries.</p>
   * 
   * @lucene.experimental
   */
  public static interface CoreClosedListener {
    public void onClose(SegmentReader owner);
  }
  
  /** Expert: adds a CoreClosedListener to this reader's shared core */
  public void addCoreClosedListener(CoreClosedListener listener) {
    ensureOpen();
    core.coreClosedListeners.add(listener);
  }
  
  /** Expert: removes a CoreClosedListener from this reader's shared core */
  public void removeCoreClosedListener(CoreClosedListener listener) {
    ensureOpen();
    core.coreClosedListeners.remove(listener);
  }
}

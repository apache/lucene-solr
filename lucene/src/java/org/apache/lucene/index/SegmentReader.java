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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.StringHelper;

/**
 * @lucene.experimental
 */
public class SegmentReader extends IndexReader implements Cloneable {
  protected boolean readOnly;

  private SegmentInfo si;
  private final ReaderContext readerContext = new AtomicReaderContext(this);
  CloseableThreadLocal<FieldsReader> fieldsReaderLocal = new FieldsReaderLocal();
  CloseableThreadLocal<TermVectorsReader> termVectorsLocal = new CloseableThreadLocal<TermVectorsReader>();

  volatile BitVector liveDocs;
  AtomicInteger liveDocsRef = null;
  private boolean liveDocsDirty = false;
  private boolean normsDirty = false;

  // TODO: we should move this tracking into SegmentInfo;
  // this way SegmentInfo.toString shows pending deletes
  private int pendingDeleteCount;

  private boolean rollbackHasChanges = false;
  private boolean rollbackDeletedDocsDirty = false;
  private boolean rollbackNormsDirty = false;
  private SegmentInfo rollbackSegmentInfo;
  private int rollbackPendingDeleteCount;

  // optionally used for the .nrm file shared by multiple norms
  IndexInput singleNormStream;
  AtomicInteger singleNormRef;

  SegmentCoreReaders core;

  /**
   * Sets the initial value 
   */
  private class FieldsReaderLocal extends CloseableThreadLocal<FieldsReader> {
    @Override
    protected FieldsReader initialValue() {
      return (FieldsReader) core.getFieldsReaderOrig().clone();
    }
  }

  Map<String,SegmentNorms> norms = new HashMap<String,SegmentNorms>();
  
  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(boolean readOnly, SegmentInfo si, int termInfosIndexDivisor, IOContext context) throws CorruptIndexException, IOException {
    return get(readOnly, si.dir, si, true, termInfosIndexDivisor, context);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(boolean readOnly,
                                  Directory dir,
                                  SegmentInfo si,
                                  boolean doOpenStores,
                                  int termInfosIndexDivisor,
                                  IOContext context)
    throws CorruptIndexException, IOException {
    
    SegmentReader instance = new SegmentReader();
    instance.readOnly = readOnly;
    instance.si = si;
    boolean success = false;

    try {
      instance.core = new SegmentCoreReaders(instance, dir, si, context, termInfosIndexDivisor);
      if (doOpenStores) {
        instance.core.openDocStores(si);
      }
      instance.loadLiveDocs(context);
      instance.openNorms(instance.core.cfsDir, context);
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

  void openDocStores() throws IOException {
    core.openDocStores(si);
  }

  @Override
  public Bits getLiveDocs() {
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
    if (hasDeletions(si)) {
      liveDocs = new BitVector(directory(), si.getDelFileName(), new IOContext(context, true));
      liveDocsRef = new AtomicInteger(1);
      assert checkLiveCounts();
      if (liveDocs.size() != si.docCount) {
        throw new CorruptIndexException("document count mismatch: deleted docs count " + liveDocs.size() + " vs segment doc count " + si.docCount + " segment=" + si.name);
      }
    } else
      assert si.getDelCount() == 0;
  }
  
  /**
   * Clones the norm bytes.  May be overridden by subclasses.  New and experimental.
   * @param bytes Byte array to clone
   * @return New BitVector
   */
  protected byte[] cloneNormBytes(byte[] bytes) {
    byte[] cloneBytes = new byte[bytes.length];
    System.arraycopy(bytes, 0, cloneBytes, 0, bytes.length);
    return cloneBytes;
  }
  
  /**
   * Clones the deleteDocs BitVector.  May be overridden by subclasses. New and experimental.
   * @param bv BitVector to clone
   * @return New BitVector
   */
  protected BitVector cloneDeletedDocs(BitVector bv) {
    return (BitVector)bv.clone();
  }

  @Override
  public final synchronized Object clone() {
    try {
      return clone(readOnly); // Preserve current readOnly
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public final synchronized IndexReader clone(boolean openReadOnly) throws CorruptIndexException, IOException {
    return reopenSegment(si, true, openReadOnly);
  }

  @Override
  public synchronized IndexReader reopen()
    throws CorruptIndexException, IOException {
    return reopenSegment(si, false, readOnly);
  }

  @Override
  public synchronized IndexReader reopen(boolean openReadOnly)
    throws CorruptIndexException, IOException {
    return reopenSegment(si, false, openReadOnly);
  }

  synchronized SegmentReader reopenSegment(SegmentInfo si, boolean doClone, boolean openReadOnly) throws CorruptIndexException, IOException {
    boolean deletionsUpToDate = (this.si.hasDeletions() == si.hasDeletions()) 
                                  && (!si.hasDeletions() || this.si.getDelFileName().equals(si.getDelFileName()));
    boolean normsUpToDate = true;
    
    Set<Integer> fieldNormsChanged = new HashSet<Integer>();
    for (FieldInfo fi : core.fieldInfos) {
      int fieldNumber = fi.number;
      if (!this.si.getNormFileName(fieldNumber).equals(si.getNormFileName(fieldNumber))) {
        normsUpToDate = false;
        fieldNormsChanged.add(fieldNumber);
      }
    }

    // if we're cloning we need to run through the reopenSegment logic
    // also if both old and new readers aren't readonly, we clone to avoid sharing modifications
    if (normsUpToDate && deletionsUpToDate && !doClone && openReadOnly && readOnly) {
      return this;
    }    

    // When cloning, the incoming SegmentInfos should not
    // have any changes in it:
    assert !doClone || (normsUpToDate && deletionsUpToDate);

    // clone reader
    SegmentReader clone = new SegmentReader();

    boolean success = false;
    try {
      core.incRef();
      clone.core = core;
      clone.readOnly = openReadOnly;
      clone.si = si;
      clone.pendingDeleteCount = pendingDeleteCount;
      clone.readerFinishedListeners = readerFinishedListeners;

      if (!openReadOnly && hasChanges) {
        // My pending changes transfer to the new reader
        clone.liveDocsDirty = liveDocsDirty;
        clone.normsDirty = normsDirty;
        clone.hasChanges = hasChanges;
        hasChanges = false;
      }
      
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

      clone.norms = new HashMap<String,SegmentNorms>();

      // Clone norms
      for (FieldInfo fi : core.fieldInfos) {
        // Clone unchanged norms to the cloned reader
        if (doClone || !fieldNormsChanged.contains(fi.number)) {
          final String curField = fi.name;
          SegmentNorms norm = this.norms.get(curField);
          if (norm != null)
            clone.norms.put(curField, (SegmentNorms) norm.clone());
        }
      }

      // If we are not cloning, then this will open anew
      // any norms that have changed:
      clone.openNorms(si.getUseCompoundFile() ? core.getCFSReader() : directory(), IOContext.DEFAULT);

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

  @Override
  protected void doCommit(Map<String,String> commitUserData) throws IOException {
    if (hasChanges) {
      startCommit();
      boolean success = false;
      try {
        commitChanges(commitUserData);
        success = true;
      } finally {
        if (!success) {
          rollbackCommit();
        }
      }
    }
  }

  private synchronized void commitChanges(Map<String,String> commitUserData) throws IOException {
    if (liveDocsDirty) {               // re-write deleted
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
    } else {
      assert pendingDeleteCount == 0;
    }

    if (normsDirty) {               // re-write norms
      si.initNormGen();
      for (final SegmentNorms norm : norms.values()) {
        if (norm.dirty) {
          norm.reWrite(si);
        }
      }
    }
    liveDocsDirty = false;
    normsDirty = false;
    hasChanges = false;
  }

  FieldsReader getFieldsReader() {
    return fieldsReaderLocal.get();
  }

  @Override
  protected void doClose() throws IOException {
    termVectorsLocal.close();
    fieldsReaderLocal.close();
    
    if (liveDocs != null) {
      liveDocsRef.decrementAndGet();
      // null so if an app hangs on to us we still free most ram
      liveDocs = null;
    }

    for (final SegmentNorms norm : norms.values()) {
      norm.decRef();
    }
    if (core != null) {
      core.decRef();
    }
  }

  static boolean hasDeletions(SegmentInfo si) throws IOException {
    // Don't call ensureOpen() here (it could affect performance)
    return si.hasDeletions();
  }

  @Override
  public boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return liveDocs != null;
  }

  static boolean usesCompoundFile(SegmentInfo si) throws IOException {
    return si.getUseCompoundFile();
  }

  static boolean hasSeparateNorms(SegmentInfo si) throws IOException {
    return si.hasSeparateNorms();
  }

  @Override
  protected void doDelete(int docNum) {
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
    liveDocsDirty = true;
    if (liveDocs.getAndClear(docNum)) {
      pendingDeleteCount++;
    }
  }

  @Override
  protected void doUndeleteAll() {
    liveDocsDirty = false;
    if (liveDocs != null) {
      assert liveDocsRef != null;
      liveDocsRef.decrementAndGet();
      liveDocs = null;
      liveDocsRef = null;
      pendingDeleteCount = 0;
      si.clearDelGen();
      si.setDelCount(0);
    } else {
      assert liveDocsRef == null;
      assert pendingDeleteCount == 0;
    }
  }

  List<String> files() throws IOException {
    return new ArrayList<String>(si.files());
  }
  
  FieldInfos fieldInfos() {
    return core.fieldInfos;
  }

  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    getFieldsReader().visitDocument(docID, visitor);
  }

  @Override
  public Fields fields() throws IOException {
    return core.fields;
  }

  @Override
  public int docFreq(String field, BytesRef term) throws IOException {
    ensureOpen();

    Terms terms = core.fields.terms(field);
    if (terms != null) {
      return terms.docFreq(term);
    } else {
      return 0;
    }
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
    }
    return fieldSet;
  }

  @Override
  public boolean hasNorms(String field) {
    ensureOpen();
    return norms.containsKey(field);
  }

  @Override
  public byte[] norms(String field) throws IOException {
    ensureOpen();
    final SegmentNorms norm = norms.get(field);
    if (norm == null) {
      // not indexed, or norms not stored
      return null;  
    }
    return norm.bytes();
  }

  @Override
  protected void doSetNorm(int doc, String field, byte value)
          throws IOException {
    SegmentNorms norm = norms.get(field);
    if (norm == null) {
      // field does not store norms
      throw new IllegalStateException("Cannot setNorm for field " + field + ": norms were omitted");
    }

    normsDirty = true;
    norm.copyOnWrite()[doc] = value;                    // set the value
  }

  private void openNorms(Directory cfsDir, IOContext context) throws IOException {
    long nextNormSeek = SegmentNorms.NORMS_HEADER.length; //skip header (header unused for now)
    int maxDoc = maxDoc();
    for (FieldInfo fi : core.fieldInfos) {
      if (norms.containsKey(fi.name)) {
        // in case this SegmentReader is being re-opened, we might be able to
        // reuse some norm instances and skip loading them here
        continue;
      }
      if (fi.isIndexed && !fi.omitNorms) {
        Directory d = directory();
        String fileName = si.getNormFileName(fi.number);
        if (!si.hasSeparateNorms(fi.number)) {
          d = cfsDir;
        }
        
        // singleNormFile means multiple norms share this file
        boolean singleNormFile = IndexFileNames.matchesExtension(fileName, IndexFileNames.NORMS_EXTENSION);
        IndexInput normInput = null;
        long normSeek;

        if (singleNormFile) {
          normSeek = nextNormSeek;
          if (singleNormStream == null) {
            singleNormStream = d.openInput(fileName, context);
            singleNormRef = new AtomicInteger(1);
          } else {
            singleNormRef.incrementAndGet();
          }
          // All norms in the .nrm file can share a single IndexInput since
          // they are only used in a synchronized context.
          // If this were to change in the future, a clone could be done here.
          normInput = singleNormStream;
        } else {
          normInput = d.openInput(fileName, context);
          // if the segment was created in 3.2 or after, we wrote the header for sure,
          // and don't need to do the sketchy file size check. otherwise, we check 
          // if the size is exactly equal to maxDoc to detect a headerless file.
          // NOTE: remove this check in Lucene 5.0!
          String version = si.getVersion();
          final boolean isUnversioned = 
            (version == null || StringHelper.getVersionComparator().compare(version, "3.2") < 0)
            && normInput.length() == maxDoc();
          if (isUnversioned) {
            normSeek = 0;
          } else {
            normSeek = SegmentNorms.NORMS_HEADER.length;
          }
        }

        norms.put(fi.name, new SegmentNorms(normInput, fi.number, normSeek, this));
        nextNormSeek += maxDoc; // increment also if some norms are separate
      }
    }
  }

  // for testing only
  boolean normsClosed() {
    if (singleNormStream != null) {
      return false;
    }
    for (final SegmentNorms norm : norms.values()) {
      if (norm.refCount > 0) {
        return false;
      }
    }
    return true;
  }

  // for testing only
  boolean normsClosed(String field) {
    return norms.get(field).refCount == 0;
  }

  /**
   * Create a clone from the initial TermVectorsReader and store it in the ThreadLocal.
   * @return TermVectorsReader
   */
  TermVectorsReader getTermVectorsReader() {
    TermVectorsReader tvReader = termVectorsLocal.get();
    if (tvReader == null) {
      TermVectorsReader orig = core.getTermVectorsReaderOrig();
      if (orig == null) {
        return null;
      } else {
        try {
          tvReader = (TermVectorsReader) orig.clone();
        } catch (CloneNotSupportedException cnse) {
          return null;
        }
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
  public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
    // Check if this field is invalid or has no stored term vector
    ensureOpen();
    FieldInfo fi = core.fieldInfos.fieldInfo(field);
    if (fi == null || !fi.storeTermVector) 
      return null;
    
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null)
      return null;
    
    return termVectorsReader.get(docNumber, field);
  }


  @Override
  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    FieldInfo fi = core.fieldInfos.fieldInfo(field);
    if (fi == null || !fi.storeTermVector)
      return;

    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null) {
      return;
    }


    termVectorsReader.get(docNumber, field, mapper);
  }


  @Override
  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    ensureOpen();

    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null)
      return;

    termVectorsReader.get(docNumber, mapper);
  }

  /** Return an array of term frequency vectors for the specified document.
   *  The array contains a vector for each vectorized field in the document.
   *  Each vector vector contains term numbers and frequencies for all terms
   *  in a given vectorized field.
   *  If no such fields existed, the method returns null.
   * @throws IOException
   */
  @Override
  public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
    ensureOpen();
    
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null)
      return null;
    
    return termVectorsReader.get(docNumber);
  }
  
  /** {@inheritDoc} */
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

  void startCommit() {
    rollbackSegmentInfo = (SegmentInfo) si.clone();
    rollbackHasChanges = hasChanges;
    rollbackDeletedDocsDirty = liveDocsDirty;
    rollbackNormsDirty = normsDirty;
    rollbackPendingDeleteCount = pendingDeleteCount;
    for (SegmentNorms norm : norms.values()) {
      norm.rollbackDirty = norm.dirty;
    }
  }

  void rollbackCommit() {
    si.reset(rollbackSegmentInfo);
    hasChanges = rollbackHasChanges;
    liveDocsDirty = rollbackDeletedDocsDirty;
    normsDirty = rollbackNormsDirty;
    pendingDeleteCount = rollbackPendingDeleteCount;
    for (SegmentNorms norm : norms.values()) {
      norm.dirty = norm.rollbackDirty;
    }
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
  public int getTermInfosIndexDivisor() {
    return core.termsIndexDivisor;
  }

  @Override
  protected void readerFinished() {
    // Do nothing here -- we have more careful control on
    // when to notify that a SegmentReader has finished,
    // because a given core is shared across many cloned
    // SegmentReaders.  We only notify once that core is no
    // longer used (all SegmentReaders sharing it have been
    // closed).
  }
  
  @Override
  public PerDocValues perDocValues() throws IOException {
    return core.perDocProducer;
  }
}

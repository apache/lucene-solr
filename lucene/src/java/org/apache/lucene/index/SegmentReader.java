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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
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
  private int readBufferSize;
  private final ReaderContext readerContext = new AtomicReaderContext(this);
  CloseableThreadLocal<FieldsReader> fieldsReaderLocal = new FieldsReaderLocal();
  CloseableThreadLocal<TermVectorsReader> termVectorsLocal = new CloseableThreadLocal<TermVectorsReader>();

  volatile BitVector deletedDocs;
  AtomicInteger deletedDocsRef = null;
  private boolean deletedDocsDirty = false;
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

  CoreReaders core;

  // Holds core readers that are shared (unchanged) when
  // SegmentReader is cloned or reopened
  static final class CoreReaders {

    // Counts how many other reader share the core objects
    // (freqStream, proxStream, tis, etc.) of this reader;
    // when coreRef drops to 0, these core objects may be
    // closed.  A given instance of SegmentReader may be
    // closed, even those it shares core objects with other
    // SegmentReaders:
    private final AtomicInteger ref = new AtomicInteger(1);

    final String segment;
    final FieldInfos fieldInfos;

    final FieldsProducer fields;
    
    final Directory dir;
    final Directory cfsDir;
    final int readBufferSize;
    final int termsIndexDivisor;

    private final SegmentReader origInstance;

    FieldsReader fieldsReaderOrig;
    TermVectorsReader termVectorsReaderOrig;
    CompoundFileReader cfsReader;
    CompoundFileReader storeCFSReader;

    CoreReaders(SegmentReader origInstance, Directory dir, SegmentInfo si, int readBufferSize, int termsIndexDivisor) throws IOException {

      if (termsIndexDivisor == 0) {
        throw new IllegalArgumentException("indexDivisor must be < 0 (don't load terms index) or greater than 0 (got 0)");
      }

      segment = si.name;
      final SegmentCodecs segmentCodecs = si.getSegmentCodecs();
      this.readBufferSize = readBufferSize;
      this.dir = dir;

      boolean success = false;

      try {
        Directory dir0 = dir;
        if (si.getUseCompoundFile()) {
          cfsReader = new CompoundFileReader(dir, IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION), readBufferSize);
          dir0 = cfsReader;
        }
        cfsDir = dir0;
        si.loadFieldInfos(cfsDir, false); // prevent opening the CFS to load fieldInfos
        fieldInfos = si.getFieldInfos();
        
        this.termsIndexDivisor = termsIndexDivisor;
        
        // Ask codec for its Fields
        fields = segmentCodecs.codec().fieldsProducer(new SegmentReadState(cfsDir, si, fieldInfos, readBufferSize, termsIndexDivisor));
        assert fields != null;

        success = true;
      } finally {
        if (!success) {
          decRef();
        }
      }

      // Must assign this at the end -- if we hit an
      // exception above core, we don't want to attempt to
      // purge the FieldCache (will hit NPE because core is
      // not assigned yet).
      this.origInstance = origInstance;
    }

    synchronized TermVectorsReader getTermVectorsReaderOrig() {
      return termVectorsReaderOrig;
    }

    synchronized FieldsReader getFieldsReaderOrig() {
      return fieldsReaderOrig;
    }

    synchronized void incRef() {
      ref.incrementAndGet();
    }

    synchronized Directory getCFSReader() {
      return cfsReader;
    }

    synchronized void decRef() throws IOException {

      if (ref.decrementAndGet() == 0) {

        if (fields != null) {
          fields.close();
        }

        if (termVectorsReaderOrig != null) {
          termVectorsReaderOrig.close();
        }
  
        if (fieldsReaderOrig != null) {
          fieldsReaderOrig.close();
        }
  
        if (cfsReader != null) {
          cfsReader.close();
        }
  
        if (storeCFSReader != null) {
          storeCFSReader.close();
        }

        // Now, notify any ReaderFinished listeners:
        if (origInstance != null) {
          origInstance.notifyReaderFinishedListeners();
        }
      }
    }

    synchronized void openDocStores(SegmentInfo si) throws IOException {

      assert si.name.equals(segment);

      if (fieldsReaderOrig == null) {
        final Directory storeDir;
        if (si.getDocStoreOffset() != -1) {
          if (si.getDocStoreIsCompoundFile()) {
            assert storeCFSReader == null;
            storeCFSReader = new CompoundFileReader(dir,
                IndexFileNames.segmentFileName(si.getDocStoreSegment(), "", IndexFileNames.COMPOUND_FILE_STORE_EXTENSION),
                                                    readBufferSize);
            storeDir = storeCFSReader;
            assert storeDir != null;
          } else {
            storeDir = dir;
            assert storeDir != null;
          }
        } else if (si.getUseCompoundFile()) {
          // In some cases, we were originally opened when CFS
          // was not used, but then we are asked to open doc
          // stores after the segment has switched to CFS
          if (cfsReader == null) {
            cfsReader = new CompoundFileReader(dir, IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION), readBufferSize);
          }
          storeDir = cfsReader;
          assert storeDir != null;
        } else {
          storeDir = dir;
          assert storeDir != null;
        }

        final String storesSegment = si.getDocStoreSegment();
        fieldsReaderOrig = new FieldsReader(storeDir, storesSegment, fieldInfos, readBufferSize,
                                            si.getDocStoreOffset(), si.docCount);

        // Verify two sources of "maxDoc" agree:
        if (si.getDocStoreOffset() == -1 && fieldsReaderOrig.size() != si.docCount) {
          throw new CorruptIndexException("doc counts differ for segment " + segment + ": fieldsReader shows " + fieldsReaderOrig.size() + " but segmentInfo shows " + si.docCount);
        }

        if (si.getHasVectors()) { // open term vector files only as needed
          termVectorsReaderOrig = new TermVectorsReader(storeDir, storesSegment, fieldInfos, readBufferSize, si.getDocStoreOffset(), si.docCount);
        }
      }
    }
  }

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
  public static SegmentReader get(boolean readOnly, SegmentInfo si, int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return get(readOnly, si.dir, si, BufferedIndexInput.BUFFER_SIZE, true, termInfosIndexDivisor);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(boolean readOnly,
                                  Directory dir,
                                  SegmentInfo si,
                                  int readBufferSize,
                                  boolean doOpenStores,
                                  int termInfosIndexDivisor)
    throws CorruptIndexException, IOException {
    
    SegmentReader instance = new SegmentReader();
    instance.readOnly = readOnly;
    instance.si = si;
    instance.readBufferSize = readBufferSize;

    boolean success = false;

    try {
      instance.core = new CoreReaders(instance, dir, si, readBufferSize, termInfosIndexDivisor);
      if (doOpenStores) {
        instance.core.openDocStores(si);
      }
      instance.loadDeletedDocs();
      instance.openNorms(instance.core.cfsDir, readBufferSize);
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
  public Bits getDeletedDocs() {
    return deletedDocs;
  }

  private boolean checkDeletedCounts() throws IOException {
    final int recomputedCount = deletedDocs.getRecomputedCount();
     
    assert deletedDocs.count() == recomputedCount : "deleted count=" + deletedDocs.count() + " vs recomputed count=" + recomputedCount;

    assert si.getDelCount() == recomputedCount : 
    "delete count mismatch: info=" + si.getDelCount() + " vs BitVector=" + recomputedCount;

    // Verify # deletes does not exceed maxDoc for this
    // segment:
    assert si.getDelCount() <= maxDoc() : 
    "delete count mismatch: " + recomputedCount + ") exceeds max doc (" + maxDoc() + ") for segment " + si.name;

    return true;
  }

  private void loadDeletedDocs() throws IOException {
    // NOTE: the bitvector is stored using the regular directory, not cfs
    if (hasDeletions(si)) {
      deletedDocs = new BitVector(directory(), si.getDelFileName());
      deletedDocsRef = new AtomicInteger(1);
      assert checkDeletedCounts();
      if (deletedDocs.size() != si.docCount) {
        throw new CorruptIndexException("document count mismatch: deleted docs count " + deletedDocs.size() + " vs segment doc count " + si.docCount + " segment=" + si.name);
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
      clone.readBufferSize = readBufferSize;
      clone.pendingDeleteCount = pendingDeleteCount;
      clone.readerFinishedListeners = readerFinishedListeners;

      if (!openReadOnly && hasChanges) {
        // My pending changes transfer to the new reader
        clone.deletedDocsDirty = deletedDocsDirty;
        clone.normsDirty = normsDirty;
        clone.hasChanges = hasChanges;
        hasChanges = false;
      }
      
      if (doClone) {
        if (deletedDocs != null) {
          deletedDocsRef.incrementAndGet();
          clone.deletedDocs = deletedDocs;
          clone.deletedDocsRef = deletedDocsRef;
        }
      } else {
        if (!deletionsUpToDate) {
          // load deleted docs
          assert clone.deletedDocs == null;
          clone.loadDeletedDocs();
        } else if (deletedDocs != null) {
          deletedDocsRef.incrementAndGet();
          clone.deletedDocs = deletedDocs;
          clone.deletedDocsRef = deletedDocsRef;
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
      clone.openNorms(si.getUseCompoundFile() ? core.getCFSReader() : directory(), readBufferSize);

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
    if (deletedDocsDirty) {               // re-write deleted
      si.advanceDelGen();

      assert deletedDocs.length() == si.docCount;

      // We can write directly to the actual name (vs to a
      // .tmp & renaming it) because the file is not live
      // until segments file is written:
      final String delFileName = si.getDelFileName();
      boolean success = false;
      try {
        deletedDocs.write(directory(), delFileName);
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
      assert deletedDocs.count() == si.getDelCount(): "delete count mismatch during commit: info=" + si.getDelCount() + " vs BitVector=" + deletedDocs.count();
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
    deletedDocsDirty = false;
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
    
    if (deletedDocs != null) {
      deletedDocsRef.decrementAndGet();
      // null so if an app hangs on to us we still free most ram
      deletedDocs = null;
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
    return deletedDocs != null;
  }

  static boolean usesCompoundFile(SegmentInfo si) throws IOException {
    return si.getUseCompoundFile();
  }

  static boolean hasSeparateNorms(SegmentInfo si) throws IOException {
    return si.hasSeparateNorms();
  }

  @Override
  protected void doDelete(int docNum) {
    if (deletedDocs == null) {
      deletedDocs = new BitVector(maxDoc());
      deletedDocsRef = new AtomicInteger(1);
    }
    // there is more than 1 SegmentReader with a reference to this
    // deletedDocs BitVector so decRef the current deletedDocsRef,
    // clone the BitVector, create a new deletedDocsRef
    if (deletedDocsRef.get() > 1) {
      AtomicInteger oldRef = deletedDocsRef;
      deletedDocs = cloneDeletedDocs(deletedDocs);
      deletedDocsRef = new AtomicInteger(1);
      oldRef.decrementAndGet();
    }
    deletedDocsDirty = true;
    if (!deletedDocs.getAndSet(docNum)) {
      pendingDeleteCount++;
    }
  }

  @Override
  protected void doUndeleteAll() {
    deletedDocsDirty = false;
    if (deletedDocs != null) {
      assert deletedDocsRef != null;
      deletedDocsRef.decrementAndGet();
      deletedDocs = null;
      deletedDocsRef = null;
      pendingDeleteCount = 0;
      si.clearDelGen();
      si.setDelCount(0);
    } else {
      assert deletedDocsRef == null;
      assert pendingDeleteCount == 0;
    }
  }

  List<String> files() throws IOException {
    return new ArrayList<String>(si.files());
  }
  
  FieldInfos fieldInfos() {
    return core.fieldInfos;
  }

  @Override
  public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    ensureOpen();
    return getFieldsReader().doc(n, fieldSelector);
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
    int n = maxDoc();
    if (deletedDocs != null)
      n -= deletedDocs.count();
    return n;
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
      else if (fi.omitTermFreqAndPositions && fieldOption == IndexReader.FieldOption.OMIT_TERM_FREQ_AND_POSITIONS) {
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
    if (norm == null)                             // not an indexed field
      return;

    normsDirty = true;
    norm.copyOnWrite()[doc] = value;                    // set the value
  }

  private void openNorms(Directory cfsDir, int readBufferSize) throws IOException {
    long nextNormSeek = SegmentMerger.NORMS_HEADER.length; //skip header (header unused for now)
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
            singleNormStream = d.openInput(fileName, readBufferSize);
            singleNormRef = new AtomicInteger(1);
          } else {
            singleNormRef.incrementAndGet();
          }
          // All norms in the .nrm file can share a single IndexInput since
          // they are only used in a synchronized context.
          // If this were to change in the future, a clone could be done here.
          normInput = singleNormStream;
        } else {
          normInput = d.openInput(fileName);
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
            normSeek = SegmentMerger.NORMS_HEADER.length;
          }
        }

        norms.put(fi.name, new SegmentNorms(normInput, fi.number, normSeek, this));
        nextNormSeek += maxDoc; // increment also if some norms are separate
      }
    }
  }

  // NOTE: only called from IndexWriter when a near
  // real-time reader is opened, or applyDeletes is run,
  // sharing a segment that's still being merged.  This
  // method is not thread safe, and relies on the
  // synchronization in IndexWriter
  void loadTermsIndex(int indexDivisor) throws IOException {
    core.fields.loadTermsIndex(indexDivisor);
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
    rollbackDeletedDocsDirty = deletedDocsDirty;
    rollbackNormsDirty = normsDirty;
    rollbackPendingDeleteCount = pendingDeleteCount;
    for (SegmentNorms norm : norms.values()) {
      norm.rollbackDirty = norm.dirty;
    }
  }

  void rollbackCommit() {
    si.reset(rollbackSegmentInfo);
    hasChanges = rollbackHasChanges;
    deletedDocsDirty = rollbackDeletedDocsDirty;
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
}

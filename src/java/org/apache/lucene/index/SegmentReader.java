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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.CloseableThreadLocal;

/**
 * @version $Id$
 */
class SegmentReader extends DirectoryIndexReader {
  private String segment;
  private SegmentInfo si;
  private int readBufferSize;

  FieldInfos fieldInfos;
  private FieldsReader fieldsReader;

  TermInfosReader tis;
  TermVectorsReader termVectorsReaderOrig = null;
  CloseableThreadLocal termVectorsLocal = new CloseableThreadLocal();

  BitVector deletedDocs = null;
  private boolean deletedDocsDirty = false;
  private boolean normsDirty = false;
  private boolean undeleteAll = false;
  private int pendingDeleteCount;

  private boolean rollbackDeletedDocsDirty = false;
  private boolean rollbackNormsDirty = false;
  private boolean rollbackUndeleteAll = false;
  private int rollbackPendingDeleteCount;
  private boolean readOnly;

  IndexInput freqStream;
  IndexInput proxStream;

  // optionally used for the .nrm file shared by multiple norms
  private IndexInput singleNormStream;

  // Compound File Reader when based on a compound file segment
  CompoundFileReader cfsReader = null;
  CompoundFileReader storeCFSReader = null;
  
  // indicates the SegmentReader with which the resources are being shared,
  // in case this is a re-opened reader
  private SegmentReader referencedSegmentReader = null;
  
  private class Norm {
    volatile int refCount;
    boolean useSingleNormStream;
    
    public synchronized void incRef() {
      assert refCount > 0;
      refCount++;
    }

    public synchronized void decRef() throws IOException {
      assert refCount > 0;
      if (refCount == 1) {
        close();
      }
      refCount--;

    }
    
    public Norm(IndexInput in, boolean useSingleNormStream, int number, long normSeek)
    {
      refCount = 1;
      this.in = in;
      this.number = number;
      this.normSeek = normSeek;
      this.useSingleNormStream = useSingleNormStream;
    }

    private IndexInput in;
    private byte[] bytes;
    private boolean dirty;
    private int number;
    private long normSeek;
    private boolean rollbackDirty;

    private void reWrite(SegmentInfo si) throws IOException {
      // NOTE: norms are re-written in regular directory, not cfs
      si.advanceNormGen(this.number);
      IndexOutput out = directory().createOutput(si.getNormFileName(this.number));
      try {
        out.writeBytes(bytes, maxDoc());
      } finally {
        out.close();
      }
      this.dirty = false;
    }
    
    /** Closes the underlying IndexInput for this norm.
     * It is still valid to access all other norm properties after close is called.
     * @throws IOException
     */
    private synchronized void close() throws IOException {
      if (in != null && !useSingleNormStream) {
        in.close();
      }
      in = null;
    }
  }
  
  /**
   * Increments the RC of this reader, as well as
   * of all norms this reader is using
   */
  public synchronized void incRef() {
    super.incRef();
    Iterator it = norms.values().iterator();
    while (it.hasNext()) {
      Norm norm = (Norm) it.next();
      norm.incRef();
    }
  }
  
  /**
   * only increments the RC of this reader, not tof 
   * he norms. This is important whenever a reopen()
   * creates a new SegmentReader that doesn't share
   * the norms with this one 
   */
  private synchronized void incRefReaderNotNorms() {
    super.incRef();
  }

  public synchronized void decRef() throws IOException {
    super.decRef();
    Iterator it = norms.values().iterator();
    while (it.hasNext()) {
      Norm norm = (Norm) it.next();
      norm.decRef();
    }
  }
  
  private synchronized void decRefReaderNotNorms() throws IOException {
    super.decRef();
  }
  
  Map norms = new HashMap();
  
  /** The class which implements SegmentReader. */
  private static Class IMPL;
  static {
    try {
      String name =
        System.getProperty("org.apache.lucene.SegmentReader.class",
                           SegmentReader.class.getName());
      IMPL = Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("cannot load SegmentReader class: " + e, e);
    } catch (SecurityException se) {
      try {
        IMPL = Class.forName(SegmentReader.class.getName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("cannot load default SegmentReader class: " + e, e);
      }
    }
  }

  private static Class READONLY_IMPL;
  static {
    try {
      String name =
        System.getProperty("org.apache.lucene.ReadOnlySegmentReader.class",
                           ReadOnlySegmentReader.class.getName());
      READONLY_IMPL = Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("cannot load ReadOnlySegmentReader class: " + e, e);
    } catch (SecurityException se) {
      try {
        READONLY_IMPL = Class.forName(ReadOnlySegmentReader.class.getName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("cannot load default ReadOnlySegmentReader class: " + e, e);
      }
    }
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(SegmentInfo si) throws CorruptIndexException, IOException {
    return get(READ_ONLY_DEFAULT, si.dir, si, null, false, false, BufferedIndexInput.BUFFER_SIZE, true);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(boolean readOnly, SegmentInfo si) throws CorruptIndexException, IOException {
    return get(readOnly, si.dir, si, null, false, false, BufferedIndexInput.BUFFER_SIZE, true);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  static SegmentReader get(SegmentInfo si, boolean doOpenStores) throws CorruptIndexException, IOException {
    return get(READ_ONLY_DEFAULT, si.dir, si, null, false, false, BufferedIndexInput.BUFFER_SIZE, doOpenStores);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(SegmentInfo si, int readBufferSize) throws CorruptIndexException, IOException {
    return get(READ_ONLY_DEFAULT, si.dir, si, null, false, false, readBufferSize, true);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  static SegmentReader get(SegmentInfo si, int readBufferSize, boolean doOpenStores) throws CorruptIndexException, IOException {
    return get(READ_ONLY_DEFAULT, si.dir, si, null, false, false, readBufferSize, doOpenStores);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  static SegmentReader get(boolean readOnly, SegmentInfo si, int readBufferSize, boolean doOpenStores) throws CorruptIndexException, IOException {
    return get(readOnly, si.dir, si, null, false, false, readBufferSize, doOpenStores);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(boolean readOnly, SegmentInfos sis, SegmentInfo si,
                                  boolean closeDir) throws CorruptIndexException, IOException {
    return get(readOnly, si.dir, si, sis, closeDir, true, BufferedIndexInput.BUFFER_SIZE, true);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(Directory dir, SegmentInfo si,
                                  SegmentInfos sis,
                                  boolean closeDir, boolean ownDir,
                                  int readBufferSize)
    throws CorruptIndexException, IOException {
    return get(READ_ONLY_DEFAULT, dir, si, sis, closeDir, ownDir, readBufferSize, true);
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static SegmentReader get(boolean readOnly,
                                  Directory dir,
                                  SegmentInfo si,
                                  SegmentInfos sis,
                                  boolean closeDir, boolean ownDir,
                                  int readBufferSize,
                                  boolean doOpenStores)
    throws CorruptIndexException, IOException {
    SegmentReader instance;
    try {
      if (readOnly)
        instance = (SegmentReader)READONLY_IMPL.newInstance();
      else
        instance = (SegmentReader)IMPL.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("cannot load SegmentReader class: " + e, e);
    }
    instance.init(dir, sis, closeDir, readOnly);
    instance.initialize(si, readBufferSize, doOpenStores);
    return instance;
  }

  private void initialize(SegmentInfo si, int readBufferSize, boolean doOpenStores) throws CorruptIndexException, IOException {
    segment = si.name;
    this.si = si;
    this.readBufferSize = readBufferSize;

    boolean success = false;

    try {
      // Use compound file directory for some files, if it exists
      Directory cfsDir = directory();
      if (si.getUseCompoundFile()) {
        cfsReader = new CompoundFileReader(directory(), segment + "." + IndexFileNames.COMPOUND_FILE_EXTENSION, readBufferSize);
        cfsDir = cfsReader;
      }

      final Directory storeDir;

      if (doOpenStores) {
        if (si.getDocStoreOffset() != -1) {
          if (si.getDocStoreIsCompoundFile()) {
            storeCFSReader = new CompoundFileReader(directory(), si.getDocStoreSegment() + "." + IndexFileNames.COMPOUND_FILE_STORE_EXTENSION, readBufferSize);
            storeDir = storeCFSReader;
          } else {
            storeDir = directory();
          }
        } else {
          storeDir = cfsDir;
        }
      } else
        storeDir = null;

      fieldInfos = new FieldInfos(cfsDir, segment + ".fnm");

      boolean anyProx = false;
      final int numFields = fieldInfos.size();
      for(int i=0;!anyProx && i<numFields;i++)
        if (!fieldInfos.fieldInfo(i).omitTf)
          anyProx = true;

      final String fieldsSegment;

      if (si.getDocStoreOffset() != -1)
        fieldsSegment = si.getDocStoreSegment();
      else
        fieldsSegment = segment;

      if (doOpenStores) {
        fieldsReader = new FieldsReader(storeDir, fieldsSegment, fieldInfos, readBufferSize,
                                        si.getDocStoreOffset(), si.docCount);

        // Verify two sources of "maxDoc" agree:
        if (si.getDocStoreOffset() == -1 && fieldsReader.size() != si.docCount) {
          throw new CorruptIndexException("doc counts differ for segment " + si.name + ": fieldsReader shows " + fieldsReader.size() + " but segmentInfo shows " + si.docCount);
        }
      }

      tis = new TermInfosReader(cfsDir, segment, fieldInfos, readBufferSize);
      
      loadDeletedDocs();

      // make sure that all index files have been read or are kept open
      // so that if an index update removes them we'll still have them
      freqStream = cfsDir.openInput(segment + ".frq", readBufferSize);
      if (anyProx)
        proxStream = cfsDir.openInput(segment + ".prx", readBufferSize);
      openNorms(cfsDir, readBufferSize);

      if (doOpenStores && fieldInfos.hasVectors()) { // open term vector files only as needed
        final String vectorsSegment;
        if (si.getDocStoreOffset() != -1)
          vectorsSegment = si.getDocStoreSegment();
        else
          vectorsSegment = segment;
        termVectorsReaderOrig = new TermVectorsReader(storeDir, vectorsSegment, fieldInfos, readBufferSize, si.getDocStoreOffset(), si.docCount);
      }
      success = true;
    } finally {

      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above.  In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        doClose();
      }
    }
  }
  
  private void loadDeletedDocs() throws IOException {
    // NOTE: the bitvector is stored using the regular directory, not cfs
    if (hasDeletions(si)) {
      deletedDocs = new BitVector(directory(), si.getDelFileName());
     
      assert si.getDelCount() == deletedDocs.count() : 
        "delete count mismatch: info=" + si.getDelCount() + " vs BitVector=" + deletedDocs.count();

      // Verify # deletes does not exceed maxDoc for this
      // segment:
      assert si.getDelCount() <= maxDoc() : 
        "delete count mismatch: " + deletedDocs.count() + ") exceeds max doc (" + maxDoc() + ") for segment " + si.name;

    } else
      assert si.getDelCount() == 0;
  }
  
  protected synchronized DirectoryIndexReader doReopen(SegmentInfos infos) throws CorruptIndexException, IOException {
    DirectoryIndexReader newReader;
    
    if (infos.size() == 1) {
      SegmentInfo si = infos.info(0);
      if (segment.equals(si.name) && si.getUseCompoundFile() == SegmentReader.this.si.getUseCompoundFile()) {
        newReader = reopenSegment(si);
      } else { 
        // segment not referenced anymore, reopen not possible
        // or segment format changed
        newReader = SegmentReader.get(readOnly, infos, infos.info(0), false);
      }
    } else {
      if (readOnly)
        return new ReadOnlyMultiSegmentReader(directory, infos, closeDirectory, new SegmentReader[] {this}, null, null);
      else
        return new MultiSegmentReader(directory, infos, closeDirectory, new SegmentReader[] {this}, null, null, false);
    }
    
    return newReader;
  }
  
  synchronized SegmentReader reopenSegment(SegmentInfo si) throws CorruptIndexException, IOException {
    boolean deletionsUpToDate = (this.si.hasDeletions() == si.hasDeletions()) 
                                  && (!si.hasDeletions() || this.si.getDelFileName().equals(si.getDelFileName()));
    boolean normsUpToDate = true;

    
    boolean[] fieldNormsChanged = new boolean[fieldInfos.size()];
    if (normsUpToDate) {
      for (int i = 0; i < fieldInfos.size(); i++) {
        if (!this.si.getNormFileName(i).equals(si.getNormFileName(i))) {
          normsUpToDate = false;
          fieldNormsChanged[i] = true;
        }
      }
    }

    if (normsUpToDate && deletionsUpToDate) {
      return this;
    }    
    

      // clone reader
    SegmentReader clone;
    if (readOnly) 
      clone = new ReadOnlySegmentReader();
    else
      clone = new SegmentReader();

    boolean success = false;
    try {
      clone.readOnly = readOnly;
      clone.directory = directory;
      clone.si = si;
      clone.segment = segment;
      clone.readBufferSize = readBufferSize;
      clone.cfsReader = cfsReader;
      clone.storeCFSReader = storeCFSReader;
  
      clone.fieldInfos = fieldInfos;
      clone.tis = tis;
      clone.freqStream = freqStream;
      clone.proxStream = proxStream;
      clone.termVectorsReaderOrig = termVectorsReaderOrig;
  
      
      // we have to open a new FieldsReader, because it is not thread-safe
      // and can thus not be shared among multiple SegmentReaders
      // TODO: Change this in case FieldsReader becomes thread-safe in the future
      final String fieldsSegment;
  
      Directory storeDir = directory();
      
      if (si.getDocStoreOffset() != -1) {
        fieldsSegment = si.getDocStoreSegment();
        if (storeCFSReader != null) {
          storeDir = storeCFSReader;
        }
      } else {
        fieldsSegment = segment;
        if (cfsReader != null) {
          storeDir = cfsReader;
        }
      }
  
      if (fieldsReader != null) {
        clone.fieldsReader = new FieldsReader(storeDir, fieldsSegment, fieldInfos, readBufferSize,
                                        si.getDocStoreOffset(), si.docCount);
      }
      
      
      if (!deletionsUpToDate) {
        // load deleted docs
        clone.deletedDocs = null;
        clone.loadDeletedDocs();
      } else {
        clone.deletedDocs = this.deletedDocs;
      }
  
      clone.norms = new HashMap();
      if (!normsUpToDate) {
        // load norms
        for (int i = 0; i < fieldNormsChanged.length; i++) {
          // copy unchanged norms to the cloned reader and incRef those norms
          if (!fieldNormsChanged[i]) {
            String curField = fieldInfos.fieldInfo(i).name;
            Norm norm = (Norm) this.norms.get(curField);
            norm.incRef();
            clone.norms.put(curField, norm);
          }
        }
        
        clone.openNorms(si.getUseCompoundFile() ? cfsReader : directory(), readBufferSize);
      } else {
        Iterator it = norms.keySet().iterator();
        while (it.hasNext()) {
          String field = (String) it.next();
          Norm norm = (Norm) norms.get(field);
          norm.incRef();
          clone.norms.put(field, norm);
        }
      }
  
      if (clone.singleNormStream == null) {
        for (int i = 0; i < fieldInfos.size(); i++) {
          FieldInfo fi = fieldInfos.fieldInfo(i);
          if (fi.isIndexed && !fi.omitNorms) {
            Directory d = si.getUseCompoundFile() ? cfsReader : directory();
            String fileName = si.getNormFileName(fi.number);
            if (si.hasSeparateNorms(fi.number)) {
              continue;
            }  
  
            if (fileName.endsWith("." + IndexFileNames.NORMS_EXTENSION)) {
              clone.singleNormStream = d.openInput(fileName, readBufferSize);    
              break;
            }
          }
        }  
      }    
  
      success = true;
    } finally {
      if (this.referencedSegmentReader != null) {
        // this reader shares resources with another SegmentReader,
        // so we increment the other readers refCount. We don't
        // increment the refCount of the norms because we did
        // that already for the shared norms
        clone.referencedSegmentReader = this.referencedSegmentReader;
        referencedSegmentReader.incRefReaderNotNorms();
      } else {
        // this reader wasn't reopened, so we increment this
        // readers refCount
        clone.referencedSegmentReader = this;
        incRefReaderNotNorms();
      }
      
      if (!success) {
        // An exception occured during reopen, we have to decRef the norms
        // that we incRef'ed already and close singleNormsStream and FieldsReader
        clone.decRef();
      }
    }
    
    return clone;
  }

  protected void commitChanges() throws IOException {
    if (deletedDocsDirty) {               // re-write deleted
      si.advanceDelGen();

      // We can write directly to the actual name (vs to a
      // .tmp & renaming it) because the file is not live
      // until segments file is written:
      deletedDocs.write(directory(), si.getDelFileName());
      
      si.setDelCount(si.getDelCount()+pendingDeleteCount);
    }
    if (undeleteAll && si.hasDeletions()) {
      si.clearDelGen();
      si.setDelCount(0);
    }
    if (normsDirty) {               // re-write norms
      si.setNumFields(fieldInfos.size());
      Iterator it = norms.values().iterator();
      while (it.hasNext()) {
        Norm norm = (Norm) it.next();
        if (norm.dirty) {
          norm.reWrite(si);
        }
      }
    }
    deletedDocsDirty = false;
    normsDirty = false;
    undeleteAll = false;
  }

  FieldsReader getFieldsReader() {
    return fieldsReader;
  }

  protected void doClose() throws IOException {
    boolean hasReferencedReader = (referencedSegmentReader != null);

    termVectorsLocal.close();

    if (hasReferencedReader) {
      referencedSegmentReader.decRefReaderNotNorms();
      referencedSegmentReader = null;
    }

    deletedDocs = null;

    // close the single norms stream
    if (singleNormStream != null) {
      // we can close this stream, even if the norms
      // are shared, because every reader has it's own 
      // singleNormStream
      singleNormStream.close();
      singleNormStream = null;
    }
    
    // re-opened SegmentReaders have their own instance of FieldsReader
    if (fieldsReader != null) {
      fieldsReader.close();
    }

    if (!hasReferencedReader) { 
      // close everything, nothing is shared anymore with other readers
      if (tis != null) {
        tis.close();
      }
  
      if (freqStream != null)
        freqStream.close();
      if (proxStream != null)
        proxStream.close();
  
      if (termVectorsReaderOrig != null)
        termVectorsReaderOrig.close();
  
      if (cfsReader != null)
        cfsReader.close();
  
      if (storeCFSReader != null)
        storeCFSReader.close();
      
      // maybe close directory
      super.doClose();
    }
  }

  static boolean hasDeletions(SegmentInfo si) throws IOException {
    // Don't call ensureOpen() here (it could affect performance)
    return si.hasDeletions();
  }

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

  protected void doDelete(int docNum) {
    if (deletedDocs == null)
      deletedDocs = new BitVector(maxDoc());
    deletedDocsDirty = true;
    undeleteAll = false;
    if (!deletedDocs.getAndSet(docNum))
      pendingDeleteCount++;
  }

  protected void doUndeleteAll() {
      deletedDocs = null;
      deletedDocsDirty = false;
      undeleteAll = true;
  }

  List files() throws IOException {
    return new ArrayList(si.files());
  }

  public TermEnum terms() {
    ensureOpen();
    return tis.terms();
  }

  public TermEnum terms(Term t) throws IOException {
    ensureOpen();
    return tis.terms(t);
  }

  FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public synchronized Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    ensureOpen();
    if (isDeleted(n))
      throw new IllegalArgumentException
              ("attempt to access a deleted document");
    return fieldsReader.doc(n, fieldSelector);
  }

  public synchronized boolean isDeleted(int n) {
    return (deletedDocs != null && deletedDocs.get(n));
  }

  public TermDocs termDocs() throws IOException {
    ensureOpen();
    return new SegmentTermDocs(this);
  }

  public TermPositions termPositions() throws IOException {
    ensureOpen();
    return new SegmentTermPositions(this);
  }

  public int docFreq(Term t) throws IOException {
    ensureOpen();
    TermInfo ti = tis.get(t);
    if (ti != null)
      return ti.docFreq;
    else
      return 0;
  }

  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    int n = maxDoc();
    if (deletedDocs != null)
      n -= deletedDocs.count();
    return n;
  }

  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return si.docCount;
  }

  public void setTermInfosIndexDivisor(int indexDivisor) throws IllegalStateException {
    tis.setIndexDivisor(indexDivisor);
  }

  public int getTermInfosIndexDivisor() {
    return tis.getIndexDivisor();
  }

  /**
   * @see IndexReader#getFieldNames(IndexReader.FieldOption fldOption)
   */
  public Collection getFieldNames(IndexReader.FieldOption fieldOption) {
    ensureOpen();

    Set fieldSet = new HashSet();
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fieldOption == IndexReader.FieldOption.ALL) {
        fieldSet.add(fi.name);
      }
      else if (!fi.isIndexed && fieldOption == IndexReader.FieldOption.UNINDEXED) {
        fieldSet.add(fi.name);
      }
      else if (fi.omitTf && fieldOption == IndexReader.FieldOption.OMIT_TF) {
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


  public synchronized boolean hasNorms(String field) {
    ensureOpen();
    return norms.containsKey(field);
  }

  static byte[] createFakeNorms(int size) {
    byte[] ones = new byte[size];
    Arrays.fill(ones, DefaultSimilarity.encodeNorm(1.0f));
    return ones;
  }

  private byte[] ones;
  private byte[] fakeNorms() {
    if (ones==null) ones=createFakeNorms(maxDoc());
    return ones;
  }

  // can return null if norms aren't stored
  protected synchronized byte[] getNorms(String field) throws IOException {
    Norm norm = (Norm) norms.get(field);
    if (norm == null) return null;  // not indexed, or norms not stored
    synchronized(norm) {
      if (norm.bytes == null) {                     // value not yet read
        byte[] bytes = new byte[maxDoc()];
        norms(field, bytes, 0);
        norm.bytes = bytes;                         // cache it
        // it's OK to close the underlying IndexInput as we have cached the
        // norms and will never read them again.
        norm.close();
      }
      return norm.bytes;
    }
  }

  // returns fake norms if norms aren't available
  public synchronized byte[] norms(String field) throws IOException {
    ensureOpen();
    byte[] bytes = getNorms(field);
    if (bytes==null) bytes=fakeNorms();
    return bytes;
  }

  protected void doSetNorm(int doc, String field, byte value)
          throws IOException {
    Norm norm = (Norm) norms.get(field);
    if (norm == null)                             // not an indexed field
      return;

    norm.dirty = true;                            // mark it dirty
    normsDirty = true;

    norms(field)[doc] = value;                    // set the value
  }

  /** Read norms into a pre-allocated array. */
  public synchronized void norms(String field, byte[] bytes, int offset)
    throws IOException {

    ensureOpen();
    Norm norm = (Norm) norms.get(field);
    if (norm == null) {
      System.arraycopy(fakeNorms(), 0, bytes, offset, maxDoc());
      return;
    }
    
    synchronized(norm) {
      if (norm.bytes != null) {                     // can copy from cache
        System.arraycopy(norm.bytes, 0, bytes, offset, maxDoc());
        return;
      }

    // Read from disk.  norm.in may be shared across  multiple norms and
    // should only be used in a synchronized context.
      IndexInput normStream;
      if (norm.useSingleNormStream) {
        normStream = singleNormStream;
      } else {
        normStream = norm.in;
      }
      normStream.seek(norm.normSeek);
      normStream.readBytes(bytes, offset, maxDoc());
    }
  }


  private void openNorms(Directory cfsDir, int readBufferSize) throws IOException {
    long nextNormSeek = SegmentMerger.NORMS_HEADER.length; //skip header (header unused for now)
    int maxDoc = maxDoc();
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
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
        boolean singleNormFile = fileName.endsWith("." + IndexFileNames.NORMS_EXTENSION);
        IndexInput normInput = null;
        long normSeek;

        if (singleNormFile) {
          normSeek = nextNormSeek;
          if (singleNormStream==null) {
            singleNormStream = d.openInput(fileName, readBufferSize);
          }
          // All norms in the .nrm file can share a single IndexInput since
          // they are only used in a synchronized context.
          // If this were to change in the future, a clone could be done here.
          normInput = singleNormStream;
        } else {
          normSeek = 0;
          normInput = d.openInput(fileName);
        }

        norms.put(fi.name, new Norm(normInput, singleNormFile, fi.number, normSeek));
        nextNormSeek += maxDoc; // increment also if some norms are separate
      }
    }
  }

  // for testing only
  boolean normsClosed() {
    if (singleNormStream != null) {
      return false;
    }
    Iterator it = norms.values().iterator();
    while (it.hasNext()) {
      Norm norm = (Norm) it.next();
      if (norm.refCount > 0) {
        return false;
      }
    }
    return true;
  }
  
  // for testing only
  boolean normsClosed(String field) {
      Norm norm = (Norm) norms.get(field);
      return norm.refCount == 0;
  }

  /**
   * Create a clone from the initial TermVectorsReader and store it in the ThreadLocal.
   * @return TermVectorsReader
   */
  private TermVectorsReader getTermVectorsReader() {
    assert termVectorsReaderOrig != null;
    TermVectorsReader tvReader = (TermVectorsReader)termVectorsLocal.get();
    if (tvReader == null) {
      try {
        tvReader = (TermVectorsReader)termVectorsReaderOrig.clone();
      } catch (CloneNotSupportedException cnse) {
        return null;
      }
      termVectorsLocal.set(tvReader);
    }
    return tvReader;
  }
  
  /** Return a term frequency vector for the specified document and field. The
   *  vector returned contains term numbers and frequencies for all terms in
   *  the specified field of this document, if the field had storeTermVector
   *  flag set.  If the flag was not set, the method returns null.
   * @throws IOException
   */
  public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
    // Check if this field is invalid or has no stored term vector
    ensureOpen();
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null || !fi.storeTermVector || termVectorsReaderOrig == null) 
      return null;
    
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null)
      return null;
    
    return termVectorsReader.get(docNumber, field);
  }


  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null || !fi.storeTermVector || termVectorsReaderOrig == null)
      return;

    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null)
    {
      return;
    }


    termVectorsReader.get(docNumber, field, mapper);
  }


  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    if (termVectorsReaderOrig == null)
      return;

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
  public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
    ensureOpen();
    if (termVectorsReaderOrig == null)
      return null;
    
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null)
      return null;
    
    return termVectorsReader.get(docNumber);
  }
  
  /** Returns the field infos of this segment */
  FieldInfos fieldInfos() {
    return fieldInfos;
  }
  
  /**
   * Return the name of the segment this reader is reading.
   */
  String getSegmentName() {
    return segment;
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
    super.startCommit();
    rollbackDeletedDocsDirty = deletedDocsDirty;
    rollbackNormsDirty = normsDirty;
    rollbackUndeleteAll = undeleteAll;
    rollbackPendingDeleteCount = pendingDeleteCount;
    Iterator it = norms.values().iterator();
    while (it.hasNext()) {
      Norm norm = (Norm) it.next();
      norm.rollbackDirty = norm.dirty;
    }
  }

  void rollbackCommit() {
    super.rollbackCommit();
    deletedDocsDirty = rollbackDeletedDocsDirty;
    normsDirty = rollbackNormsDirty;
    undeleteAll = rollbackUndeleteAll;
    pendingDeleteCount = rollbackPendingDeleteCount;
    Iterator it = norms.values().iterator();
    while (it.hasNext()) {
      Norm norm = (Norm) it.next();
      norm.dirty = norm.rollbackDirty;
    }
  }
}

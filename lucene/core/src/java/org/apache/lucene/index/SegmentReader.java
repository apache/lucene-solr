package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

// javadocs

/**
 * IndexReader implementation over a single segment. 
 * <p>
 * Instances pointing to the same segment (but with different deletes, etc)
 * may share the same core data.
 * @lucene.experimental
 */
public final class SegmentReader extends AtomicReader {

  private final SegmentInfoPerCommit si;
  private final Bits liveDocs;

  // Normally set to si.docCount - si.delDocCount, unless we
  // were created as an NRT reader from IW, in which case IW
  // tells us the docCount:
  private final int numDocs;

  final SegmentCoreReaders core;
  final SegmentCoreReaders[] updates;
  
  private final IOContext context;
  private Fields fields;
  private FieldInfos fieldInfos;
  private StoredFieldsReader fieldsReader;
  private TermVectorsReader termVectorsReader;
  private Map<String,FieldGenerationReplacements> replacementsMap;
  
  /**
   * Constructs a new SegmentReader with a new core.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  // TODO: why is this public?
  public SegmentReader(SegmentInfoPerCommit si, int termInfosIndexDivisor, IOContext context) throws IOException {
    this.si = si;
    this.context = context;
    core = new SegmentCoreReaders(this, si.info, -1, context, termInfosIndexDivisor);
    updates = initUpdates(si, termInfosIndexDivisor, context);
    boolean success = false;
    try {
      if (si.hasDeletions()) {
        // NOTE: the bitvector is stored using the regular directory, not cfs
        liveDocs = si.info.getCodec().liveDocsFormat().readLiveDocs(directory(), si, new IOContext(IOContext.READ, true));
      } else {
        assert si.getDelCount() == 0;
        liveDocs = null;
      }
      numDocs = si.info.getDocCount() - si.getDelCount();
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above.  In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        core.decRef();
        if (updates != null) {
          for (int i = 0; i < updates.length; i++) {
            updates[i].decRef();
          }
        }
      }
    }
  }
  
  /**
   * Create new SegmentReader sharing core from a previous SegmentReader and
   * loading new live docs from a new deletes file. Used by openIfChanged.
   */
  SegmentReader(SegmentInfoPerCommit si, SegmentCoreReaders core,
      SegmentCoreReaders[] updates, IOContext context) throws IOException {
    this(si, context, core, updates, si.info.getCodec().liveDocsFormat()
        .readLiveDocs(si.info.dir, si, context), si.info.getDocCount()
        - si.getDelCount());
  }
  
  /**
   * Create new SegmentReader sharing core from a previous SegmentReader and
   * using the provided in-memory liveDocs. Used by IndexWriter to provide a new
   * NRT reader
   */
  SegmentReader(SegmentInfoPerCommit si, IOContext context,
      SegmentCoreReaders core, SegmentCoreReaders[] updates, Bits liveDocs, int numDocs) {
    this.si = si;
    this.context = context;
    this.core = core;
    core.incRef();
    this.updates = updates;
    // TODO : handle NRT updates, add field liveUpdates
    
    assert liveDocs != null;
    this.liveDocs = liveDocs;

    this.numDocs = numDocs;
  }
  
  private SegmentCoreReaders[] initUpdates(SegmentInfoPerCommit si, int termInfosIndexDivisor,
      IOContext context) throws IOException {
    if (si.hasUpdates()) {
      SegmentCoreReaders[] newUpdates = new SegmentCoreReaders[(int) si
          .getUpdateGen()];
      for (int i = 0; i < newUpdates.length; i++) {
        newUpdates[i] = new SegmentCoreReaders(this, si.info, i + 1, context,
            termInfosIndexDivisor);
      }
      return newUpdates;
    }
    return null;
  }
  
  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return liveDocs;
  }

  @Override
  protected void doClose() throws IOException {
    //System.out.println("SR.close seg=" + si);
    core.decRef();
    if (updates != null) {
      for (int i = 0; i < updates.length; i++) {
        updates[i].decRef();
      }
    }
  }

  @Override
  public FieldInfos getFieldInfos() {
    ensureOpen();
    if (updates == null) {
      return core.fieldInfos;
    }
    
    // need to create FieldInfos combining core and updates infos
    final FieldInfos.Builder builder = new FieldInfos.Builder();
    builder.add(core.fieldInfos);
    for (final SegmentCoreReaders update : updates) {
      builder.add(update.fieldInfos);
    }
    fieldInfos = builder.finish();
    return fieldInfos;
  }
  
  /**
   * Expert: retrieve thread-private {@link StoredFieldsReader}
   * 
   * @lucene.internal
   */
  public StoredFieldsReader getFieldsReader() throws IOException {
    ensureOpen();
    if (updates == null) {
      return core.fieldsReaderLocal.get();
    }
    
    synchronized (updates) {
      if (fieldsReader == null) {
        // generate readers array
        StoredFieldsReader[] allReaders = new StoredFieldsReader[updates.length + 1];
        allReaders[0] = core.fieldsReaderLocal.get();
        for (int i = 0; i < updates.length; i++) {
          allReaders[i + 1] = updates[i].fieldsReaderLocal.get();
        }
        
        // generate replacements map
        if (replacementsMap == null) {
          generateReplacementsMap();
        }
        
        fieldsReader = new StackedStoredFieldsReader(allReaders,
            replacementsMap);
      }
    }
    
    return fieldsReader;
  }
  
  private synchronized void generateReplacementsMap() throws IOException {
    if (replacementsMap == null) {
      Set<String> visitedFields = new HashSet<String>();
      replacementsMap = new HashMap<String,FieldGenerationReplacements>();
      for (String field : core.fields) {
        addReplacements(field);
        visitedFields.add(field);
      }
      for (int i = 0; i < updates.length; i++) {
        for (String field : updates[i].fields) {
          if (!visitedFields.contains(field)) {
            addReplacements(field);
            visitedFields.add(field);
          }
        }
      }
    }
  }
  
  private void addReplacements(String field) throws IOException {
    final FieldGenerationReplacements replacements = si.info.getCodec()
        .generationReplacementsFormat()
        .readGenerationReplacements(field, si, context);
    if (replacements != null) {
      replacementsMap.put(field, replacements);
    }
  }
  
  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    checkBounds(docID);
    getFieldsReader().visitDocument(docID, visitor, null);
  }

  @Override
  public Fields fields() throws IOException {
    ensureOpen();
    if (fields == null) {
      if (updates == null || updates.length == 0) {
        return core.fields;
      }
      
      // generate fields array
      Fields[] fieldsArray = new Fields[updates.length + 1];
      fieldsArray[0] = core.fields;
      for (int i = 0; i < updates.length; i++) {
        fieldsArray[i + 1] = updates[i].fields;
      }
      
      // generate replacements map
      if (replacementsMap == null) {
        generateReplacementsMap();
      }
      
      fields = new StackedFields(fieldsArray, replacementsMap, -1);
    }
    return fields;
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return si.info.getDocCount();
  }
  
  /**
   * Expert: retrieve thread-private {@link TermVectorsReader}
   * 
   * @lucene.internal
   */
  public TermVectorsReader getTermVectorsReader() throws IOException {
    ensureOpen();
    if (updates == null) {
      return core.termVectorsLocal.get();
    }
    if (termVectorsReader == null) {
      setStackedTermVectorsReader();
    }
    
    return termVectorsReader;
  }
  
  private synchronized void setStackedTermVectorsReader() throws IOException {
    if (termVectorsReader == null) {
      // generate readers array
      TermVectorsReader[] tvReaders = new TermVectorsReader[updates.length + 1];
      tvReaders[0] = core.termVectorsLocal.get();
      for (int i = 0; i < updates.length; i++) {
        tvReaders[i + 1] = updates[i].termVectorsLocal.get();
      }
      
      // generate replacements map
      if (replacementsMap == null) {
        generateReplacementsMap();
      }
      
      termVectorsReader = new StackedTermVectorsReader(tvReaders,
          replacementsMap, -1);
    }
  }
  
  private void checkBounds(int docID) {
    if (docID < 0 || docID >= maxDoc()) {
      throw new IndexOutOfBoundsException("docID must be >= 0 and < maxDoc="
          + maxDoc() + " (got docID=" + docID + ")");
    }
  }
  
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    if (updates == null) {
      // no updates, delegate to core
      checkBounds(docID);
      final TermVectorsReader coreReader = core.termVectorsLocal.get();
      if (coreReader == null) {
        return null;
      }
      return coreReader.get(docID);
    }
    // generate fields array, only fields of the given docID
    Fields[] fields = new Fields[updates.length + 1];
    final TermVectorsReader coreReader = core.termVectorsLocal.get();
    if (coreReader != null) {
      checkBounds(docID);
      fields[0] = coreReader.get(docID);
    }
    for (int i = 0; i < updates.length; i++) {
      final TermVectorsReader updateReader = updates[i].termVectorsLocal.get();
      if (updateReader != null) {
        checkBounds(docID);
        fields[i + 1] = updateReader.get(docID);
      }
    }
    
    // generate replacements map
    if (replacementsMap == null) {
      generateReplacementsMap();
    }
    
    return new StackedFields(fields, replacementsMap, docID);
  }

  @Override
  public String toString() {
    // SegmentInfo.toString takes dir and number of
    // *pending* deletions; so we reverse compute that here:
    return si.toString(si.info.dir, si.info.getDocCount() - numDocs - si.getDelCount());
  }
  
  /**
   * Return the name of the segment this reader is reading.
   */
  public String getSegmentName() {
    return si.info.name;
  }
  
  /**
   * Return the SegmentInfoPerCommit of the segment this reader is reading.
   */
  public SegmentInfoPerCommit getSegmentInfo() {
    return si;
  }

  /** Returns the directory this index resides in. */
  public Directory directory() {
    // Don't ensureOpen here -- in certain cases, when a
    // cloned/reopened reader needs to commit, it may call
    // this method on the closed original reader
    return si.info.dir;
  }

  // This is necessary so that cloned SegmentReaders (which
  // share the underlying postings data) will map to the
  // same entry in the FieldCache.  See LUCENE-1579.
  @Override
  public Object getCoreCacheKey() {
    return core;
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return this;
  }

  /** Returns term infos index divisor originally passed to
   *  {@link #SegmentReader(SegmentInfoPerCommit, int, IOContext)}. */
  public int getTermInfosIndexDivisor() {
    return core.termsIndexDivisor;
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    return core.getNumericDocValues(field);
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    return core.getBinaryDocValues(field);
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    return core.getSortedDocValues(field);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    return core.getSortedSetDocValues(field);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    NumericDocValues normValues = core.getNormValues(field);
    if (updates != null) {
      for (final SegmentCoreReaders updateReader : updates) {
        NumericDocValues updateNormValues = updateReader.getNormValues(field);
        if (updateNormValues != null) {
          normValues = updateNormValues;
        }
      }
    }
    return normValues;
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
    /** Invoked when the shared core of the provided {@link
     *  SegmentReader} has closed. */
    public void onClose(SegmentReader owner);
  }
  
  /** Expert: adds a CoreClosedListener to this reader's shared core */
  public void addCoreClosedListener(CoreClosedListener listener) {
    ensureOpen();
    core.addCoreClosedListener(listener);
  }
  
  /** Expert: removes a CoreClosedListener from this reader's shared core */
  public void removeCoreClosedListener(CoreClosedListener listener) {
    ensureOpen();
    core.removeCoreClosedListener(listener);
  }
}

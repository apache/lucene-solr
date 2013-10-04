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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.RefCount;

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

  final CloseableThreadLocal<Map<String,Object>> docValuesLocal = new CloseableThreadLocal<Map<String,Object>>() {
    @Override
    protected Map<String,Object> initialValue() {
      return new HashMap<String,Object>();
    }
  };

  final CloseableThreadLocal<Map<String,Bits>> docsWithFieldLocal = new CloseableThreadLocal<Map<String,Bits>>() {
    @Override
    protected Map<String,Bits> initialValue() {
      return new HashMap<String,Bits>();
    }
  };

  final Map<String,DocValuesProducer> dvProducers = new HashMap<String,DocValuesProducer>();
  final Map<Long,RefCount<DocValuesProducer>> genDVProducers = new HashMap<Long,RefCount<DocValuesProducer>>();

  final FieldInfos fieldInfos;
  
  /**
   * Constructs a new SegmentReader with a new core.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  // TODO: why is this public?
  public SegmentReader(SegmentInfoPerCommit si, IOContext context) throws IOException {
    this.si = si;
    // TODO if the segment uses CFS, we may open the CFS file twice: once for
    // reading the FieldInfos (if they are not gen'd) and second time by
    // SegmentCoreReaders. We can open the CFS here and pass to SCR, but then it
    // results in less readable code (resource not closed where it was opened).
    // Best if we could somehow read FieldInfos in SCR but not keep it there, but
    // constructors don't allow returning two things...
    fieldInfos = readFieldInfos(si);
    core = new SegmentCoreReaders(this, si.info.dir, si, context);

    boolean success = false;
    final Codec codec = si.info.getCodec();
    try {
      if (si.hasDeletions()) {
        // NOTE: the bitvector is stored using the regular directory, not cfs
        liveDocs = codec.liveDocsFormat().readLiveDocs(directory(), si, IOContext.READONCE);
      } else {
        assert si.getDelCount() == 0;
        liveDocs = null;
      }
      numDocs = si.info.getDocCount() - si.getDelCount();
      
      if (fieldInfos.hasDocValues()) {
        final Directory dir = core.cfsReader != null ? core.cfsReader : si.info.dir;
        final DocValuesFormat dvFormat = codec.docValuesFormat();
        // initialize the per generation numericDVProducers and put the correct
        // DVProducer for each field
        final Map<Long,List<FieldInfo>> genInfos = getGenInfos();
        
//        System.out.println("[" + Thread.currentThread().getName() + "] SR.init: new reader: " + si + "; gens=" + genInfos.keySet());

        for (Entry<Long,List<FieldInfo>> e : genInfos.entrySet()) {
          Long gen = e.getKey();
          List<FieldInfo> infos = e.getValue();
          RefCount<DocValuesProducer> dvp = genDVProducers.get(gen);
          if (dvp == null) {
            dvp = newDocValuesProducer(si, context, dir, dvFormat, gen, infos);
            assert dvp != null;
            genDVProducers.put(gen, dvp);
          }
          for (FieldInfo fi : infos) {
            dvProducers.put(fi.name, dvp.get());
          }
        }
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

  /** Create new SegmentReader sharing core from a previous
   *  SegmentReader and loading new live docs from a new
   *  deletes file.  Used by openIfChanged. */
  SegmentReader(SegmentInfoPerCommit si, SegmentReader sr) throws IOException {
    this(si, sr,
         si.info.getCodec().liveDocsFormat().readLiveDocs(si.info.dir, si, IOContext.READONCE),
         si.info.getDocCount() - si.getDelCount());
  }

  /** Create new SegmentReader sharing core from a previous
   *  SegmentReader and using the provided in-memory
   *  liveDocs.  Used by IndexWriter to provide a new NRT
   *  reader */
  SegmentReader(SegmentInfoPerCommit si, SegmentReader sr, Bits liveDocs, int numDocs) throws IOException {
    this.si = si;
    this.liveDocs = liveDocs;
    this.numDocs = numDocs;
    this.core = sr.core;
    core.incRef();
    
//    System.out.println("[" + Thread.currentThread().getName() + "] SR.init: sharing reader: " + sr + " for gens=" + sr.genDVProducers.keySet());
    
    // increment refCount of DocValuesProducers that are used by this reader
    boolean success = false;
    try {
      final Codec codec = si.info.getCodec();
      if (si.getFieldInfosGen() == -1) {
        fieldInfos = sr.fieldInfos;
      } else {
        fieldInfos = readFieldInfos(si);
      }
      
      if (fieldInfos.hasDocValues()) {
        final Directory dir = core.cfsReader != null ? core.cfsReader : si.info.dir;
        
        final DocValuesFormat dvFormat = codec.docValuesFormat();
        final Map<Long,List<FieldInfo>> genInfos = getGenInfos();
        
        for (Entry<Long,List<FieldInfo>> e : genInfos.entrySet()) {
          Long gen = e.getKey();
          List<FieldInfo> infos = e.getValue();
          RefCount<DocValuesProducer> dvp = genDVProducers.get(gen);
          if (dvp == null) {
            // check if this DVP gen is used by the given reader
            dvp = sr.genDVProducers.get(gen);
            if (dvp != null) {
              // gen used by given reader, incRef its DVP
              dvp.incRef();
//              System.out.println("[" + Thread.currentThread().getName() + "] SR.init: sharing DVP for gen=" + gen + " refCount=" + dvp.getRefCount());
            } else {
              // this gen is not used by given reader, initialize a new one
              dvp = newDocValuesProducer(si, IOContext.READ, dir, dvFormat, gen, infos);
//              System.out.println("[" + Thread.currentThread().getName() + "] SR.init: new DVP for gen=" + gen + " refCount=" + dvp.getRefCount());
            }
            assert dvp != null;
            genDVProducers.put(gen, dvp);
          }
          for (FieldInfo fi : infos) {
            dvProducers.put(fi.name, dvp.get());
          }
        }
      }
      success = true;
    } finally {
      if (!success) {
        doClose();
      }
    }
  }

  /**
   * Reads the most recent {@link FieldInfos} of the given segment info.
   * 
   * @lucene.internal
   */
  static FieldInfos readFieldInfos(SegmentInfoPerCommit info) throws IOException {
    final Directory dir;
    final boolean closeDir;
    if (info.getFieldInfosGen() == -1 && info.info.getUseCompoundFile()) {
      // no fieldInfos gen and segment uses a compound file
      dir = new CompoundFileDirectory(info.info.dir,
          IndexFileNames.segmentFileName(info.info.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION),
          IOContext.READONCE,
          false);
      closeDir = true;
    } else {
      // gen'd FIS are read outside CFS, or the segment doesn't use a compound file
      dir = info.info.dir;
      closeDir = false;
    }
    
    try {
      final String segmentSuffix = info.getFieldInfosGen() == -1 ? "" : Long.toString(info.getFieldInfosGen(), Character.MAX_RADIX);
      return info.info.getCodec().fieldInfosFormat().getFieldInfosReader().read(dir, info.info.name, segmentSuffix, IOContext.READONCE);
    } finally {
      if (closeDir) {
        dir.close();
      }
    }
  }
  
  // returns a gen->List<FieldInfo> mapping. Fields without DV updates have gen=-1
  private Map<Long,List<FieldInfo>> getGenInfos() {
    final Map<Long,List<FieldInfo>> genInfos = new HashMap<Long,List<FieldInfo>>();
    for (FieldInfo fi : fieldInfos) {
      if (fi.getDocValuesType() == null) {
        continue;
      }
      long gen = fi.getDocValuesGen();
      List<FieldInfo> infos = genInfos.get(gen);
      if (infos == null) {
        infos = new ArrayList<FieldInfo>();
        genInfos.put(gen, infos);
      }
      infos.add(fi);
    }
    return genInfos;
  }

  private RefCount<DocValuesProducer> newDocValuesProducer(SegmentInfoPerCommit si, IOContext context, Directory dir, 
      DocValuesFormat dvFormat, Long gen, List<FieldInfo> infos) throws IOException {
    Directory dvDir = dir;
    String segmentSuffix = "";
    if (gen.longValue() != -1) {
      dvDir = si.info.dir; // gen'd files are written outside CFS, so use SegInfo directory
      segmentSuffix = Long.toString(gen.longValue(), Character.MAX_RADIX);
    }

    // set SegmentReadState to list only the fields that are relevant to that gen
    SegmentReadState srs = new SegmentReadState(dvDir, si.info, new FieldInfos(infos.toArray(new FieldInfo[infos.size()])), context, segmentSuffix);
    return new RefCount<DocValuesProducer>(dvFormat.fieldsProducer(srs)) {
      @Override
      protected void release() throws IOException {
        object.close();
      }
    };
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return liveDocs;
  }

  @Override
  protected void doClose() throws IOException {
    //System.out.println("SR.close seg=" + si);
    try {
      core.decRef();
    } finally {
      Throwable t = null;
      for (RefCount<DocValuesProducer> dvp : genDVProducers.values()) {
        try {
          dvp.decRef();
        } catch (Throwable th) {
          if (t != null) {
            t = th;
          }
        }
      }
      if (t != null) {
        if (t instanceof IOException) throw (IOException) t;
        if (t instanceof RuntimeException) throw (RuntimeException) t;
        if (t instanceof Error) throw (Error) t;
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public FieldInfos getFieldInfos() {
    ensureOpen();
    return fieldInfos;
  }

  /** Expert: retrieve thread-private {@link
   *  StoredFieldsReader}
   *  @lucene.internal */
  public StoredFieldsReader getFieldsReader() {
    ensureOpen();
    return core.fieldsReaderLocal.get();
  }
  
  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    checkBounds(docID);
    getFieldsReader().visitDocument(docID, visitor);
  }

  @Override
  public Fields fields() {
    ensureOpen();
    return core.fields;
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

  /** Expert: retrieve thread-private {@link
   *  TermVectorsReader}
   *  @lucene.internal */
  public TermVectorsReader getTermVectorsReader() {
    ensureOpen();
    return core.termVectorsLocal.get();
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null) {
      return null;
    }
    checkBounds(docID);
    return termVectorsReader.get(docID);
  }
  
  private void checkBounds(int docID) {
    if (docID < 0 || docID >= maxDoc()) {       
      throw new IndexOutOfBoundsException("docID must be >= 0 and < maxDoc=" + maxDoc() + " (got docID=" + docID + ")");
    }
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
    // NOTE: if this every changes, be sure to fix
    // SegmentCoreReader's ownerCoreCacheKey to match!
    return core;
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return this;
  }

  // returns the FieldInfo that corresponds to the given field and type, or
  // null if the field does not exist, or not indexed as the requested
  // DovDocValuesType.
  private FieldInfo getDVField(String field, DocValuesType type) {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() == null) {
      // Field was not indexed with doc values
      return null;
    }
    if (fi.getDocValuesType() != type) {
      // Field DocValues are different than requested type
      return null;
    }

    return fi;
  }
  
  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.NUMERIC);
    if (fi == null) {
      return null;
    }

    DocValuesProducer dvProducer = dvProducers.get(field);
    assert dvProducer != null;

    Map<String,Object> dvFields = docValuesLocal.get();

    NumericDocValues dvs = (NumericDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = dvProducer.getNumeric(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() == null) {
      // Field was not indexed with doc values
      return null;
    }

    DocValuesProducer dvProducer = dvProducers.get(field);
    assert dvProducer != null;

    Map<String,Bits> dvFields = docsWithFieldLocal.get();

    Bits dvs = dvFields.get(field);
    if (dvs == null) {
      dvs = dvProducer.getDocsWithField(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.BINARY);
    if (fi == null) {
      return null;
    }

    DocValuesProducer dvProducer = dvProducers.get(field);
    assert dvProducer != null;

    Map<String,Object> dvFields = docValuesLocal.get();

    BinaryDocValues dvs = (BinaryDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = dvProducer.getBinary(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.SORTED);
    if (fi == null) {
      return null;
    }

    DocValuesProducer dvProducer = dvProducers.get(field);
    assert dvProducer != null;

    Map<String,Object> dvFields = docValuesLocal.get();

    SortedDocValues dvs = (SortedDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = dvProducer.getSorted(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.SORTED_SET);
    if (fi == null) {
      return null;
    }

    DocValuesProducer dvProducer = dvProducers.get(field);
    assert dvProducer != null;

    Map<String,Object> dvFields = docValuesLocal.get();

    SortedSetDocValues dvs = (SortedSetDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = dvProducer.getSortedSet(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null || !fi.hasNorms()) {
      // Field does not exist or does not index norms
      return null;
    }
    return core.getNormValues(fi);
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
    /** Invoked when the shared core of the original {@code
     *  SegmentReader} has closed. */
    public void onClose(Object ownerCoreCacheKey);
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
  
  private long dvRamBytesUsed() {
    long ramBytesUsed = 0;
    for (RefCount<DocValuesProducer> dvp : genDVProducers.values()) {
      ramBytesUsed += dvp.get().ramBytesUsed();
    }
    return ramBytesUsed;
  }

  /** Returns approximate RAM Bytes used */
  public long ramBytesUsed() {
    ensureOpen();
    long ramBytesUsed = dvRamBytesUsed();
    if (core != null) {
      ramBytesUsed += core.ramBytesUsed();
    }
    return ramBytesUsed;
  }
}

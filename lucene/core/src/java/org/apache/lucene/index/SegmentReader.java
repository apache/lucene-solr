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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * IndexReader implementation over a single segment. 
 * <p>
 * Instances pointing to the same segment (but with different deletes, etc)
 * may share the same core data.
 * @lucene.experimental
 */
public final class SegmentReader extends CodecReader {
       
  private final SegmentCommitInfo si;
  // this is the original SI that IW uses internally but it's mutated behind the scenes
  // and we don't want this SI to be used for anything. Yet, IW needs this to do maintainance
  // and lookup pooled readers etc.
  private final SegmentCommitInfo originalSi;
  private final LeafMetaData metaData;
  private final Bits liveDocs;

  // Normally set to si.maxDoc - si.delDocCount, unless we
  // were created as an NRT reader from IW, in which case IW
  // tells us the number of live docs:
  private final int numDocs;

  final SegmentCoreReaders core;
  final SegmentDocValues segDocValues;

  /** True if we are holding RAM only liveDocs or DV updates, i.e. the SegmentCommitInfo delGen doesn't match our liveDocs. */
  final boolean isNRT;
  
  final DocValuesProducer docValuesProducer;
  final FieldInfos fieldInfos;
  
  /**
   * Constructs a new SegmentReader with a new core.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  SegmentReader(SegmentCommitInfo si, int createdVersionMajor, IOContext context) throws IOException {
    this.si = si.clone();
    this.originalSi = si;
    this.metaData = new LeafMetaData(createdVersionMajor, si.info.getMinVersion(), si.info.getIndexSort());

    // We pull liveDocs/DV updates from disk:
    this.isNRT = false;
    
    core = new SegmentCoreReaders(si.info.dir, si, context);
    segDocValues = new SegmentDocValues();
    
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
      numDocs = si.info.maxDoc() - si.getDelCount();
      
      fieldInfos = initFieldInfos();
      docValuesProducer = initDocValuesProducer();

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
  SegmentReader(SegmentCommitInfo si, SegmentReader sr) throws IOException {
    this(si, sr,
         si.hasDeletions() ? si.info.getCodec().liveDocsFormat().readLiveDocs(si.info.dir, si, IOContext.READONCE) : null,
         si.info.maxDoc() - si.getDelCount(), false);
  }

  /** Create new SegmentReader sharing core from a previous
   *  SegmentReader and using the provided in-memory
   *  liveDocs.  Used by IndexWriter to provide a new NRT
   *  reader */
  SegmentReader(SegmentCommitInfo si, SegmentReader sr, Bits liveDocs, int numDocs) throws IOException {
    this(si, sr, liveDocs, numDocs, true);
  }
    
  /** Create new SegmentReader sharing core from a previous
   *  SegmentReader and using the provided liveDocs, and recording
   *  whether those liveDocs were carried in ram (isNRT=true). */
  SegmentReader(SegmentCommitInfo si, SegmentReader sr, Bits liveDocs, int numDocs, boolean isNRT) throws IOException {
    if (numDocs > si.info.maxDoc()) {
      throw new IllegalArgumentException("numDocs=" + numDocs + " but maxDoc=" + si.info.maxDoc());
    }
    if (liveDocs != null && liveDocs.length() != si.info.maxDoc()) {
      throw new IllegalArgumentException("maxDoc=" + si.info.maxDoc() + " but liveDocs.size()=" + liveDocs.length());
    }
    this.si = si.clone();
    this.originalSi = si;
    this.metaData = sr.getMetaData();
    this.liveDocs = liveDocs;
    this.isNRT = isNRT;
    this.numDocs = numDocs;
    this.core = sr.core;
    core.incRef();
    this.segDocValues = sr.segDocValues;

    boolean success = false;
    try {
      fieldInfos = initFieldInfos();
      docValuesProducer = initDocValuesProducer();
      success = true;
    } finally {
      if (!success) {
        doClose();
      }
    }
  }

  /**
   * init most recent DocValues for the current commit
   */
  private DocValuesProducer initDocValuesProducer() throws IOException {

    if (fieldInfos.hasDocValues() == false) {
      return null;
    } else {
      Directory dir;
      if (core.cfsReader != null) {
        dir = core.cfsReader;
      } else {
        dir = si.info.dir;
      }
      if (si.hasFieldUpdates()) {
        return new SegmentDocValuesProducer(si, dir, core.coreFieldInfos, fieldInfos, segDocValues);
      } else {
        // simple case, no DocValues updates
        return segDocValues.getDocValuesProducer(-1L, si, dir, fieldInfos);
      }
    }
  }
  
  /**
   * init most recent FieldInfos for the current commit
   */
  private FieldInfos initFieldInfos() throws IOException {
    if (!si.hasFieldUpdates()) {
      return core.coreFieldInfos;
    } else {
      // updates always outside of CFS
      FieldInfosFormat fisFormat = si.info.getCodec().fieldInfosFormat();
      final String segmentSuffix = Long.toString(si.getFieldInfosGen(), Character.MAX_RADIX);
      return fisFormat.read(si.info.dir, si.info, segmentSuffix, IOContext.READONCE);
    }
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
      if (docValuesProducer instanceof SegmentDocValuesProducer) {
        segDocValues.decRef(((SegmentDocValuesProducer)docValuesProducer).dvGens);
      } else if (docValuesProducer != null) {
        segDocValues.decRef(Collections.singletonList(-1L));
      }
    }
  }

  @Override
  public FieldInfos getFieldInfos() {
    ensureOpen();
    return fieldInfos;
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return si.info.maxDoc();
  }

  @Override
  public TermVectorsReader getTermVectorsReader() {
    ensureOpen();
    return core.termVectorsLocal.get();
  }

  @Override
  public StoredFieldsReader getFieldsReader() {
    ensureOpen();
    return core.fieldsReaderLocal.get();
  }
  
  @Override
  public PointsReader getPointsReader() {
    ensureOpen();
    return core.pointsReader;
  }

  @Override
  public NormsProducer getNormsReader() {
    ensureOpen();
    return core.normsProducer;
  }
  
  @Override
  public DocValuesProducer getDocValuesReader() {
    ensureOpen();
    return docValuesProducer;
  }

  @Override
  public FieldsProducer getPostingsReader() {
    ensureOpen();
    return core.fields;
  }

  @Override
  public String toString() {
    // SegmentInfo.toString takes dir and number of
    // *pending* deletions; so we reverse compute that here:
    return si.toString(si.info.maxDoc() - numDocs - si.getDelCount());
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
  public SegmentCommitInfo getSegmentInfo() {
    return si;
  }

  /** Returns the directory this index resides in. */
  public Directory directory() {
    // Don't ensureOpen here -- in certain cases, when a
    // cloned/reopened reader needs to commit, it may call
    // this method on the closed original reader
    return si.info.dir;
  }

  private final Set<ClosedListener> readerClosedListeners = new CopyOnWriteArraySet<>();

  @Override
  void notifyReaderClosedListeners() throws IOException {
    synchronized(readerClosedListeners) {
      IOUtils.applyToAll(readerClosedListeners, l -> l.onClose(readerCacheHelper.getKey()));
    }
  }

  private final IndexReader.CacheHelper readerCacheHelper = new IndexReader.CacheHelper() {
    private final IndexReader.CacheKey cacheKey = new IndexReader.CacheKey();

    @Override
    public CacheKey getKey() {
      return cacheKey;
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      ensureOpen();
      readerClosedListeners.add(listener);
    }
  };

  @Override
  public CacheHelper getReaderCacheHelper() {
    return readerCacheHelper;
  }

  /** Wrap the cache helper of the core to add ensureOpen() calls that make
   *  sure users do not register closed listeners on closed indices. */
  private final IndexReader.CacheHelper coreCacheHelper = new IndexReader.CacheHelper() {

    @Override
    public CacheKey getKey() {
      return core.getCacheHelper().getKey();
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      ensureOpen();
      core.getCacheHelper().addClosedListener(listener);
    }
  };

  @Override
  public CacheHelper getCoreCacheHelper() {
    return coreCacheHelper;
  }

  @Override
  public LeafMetaData getMetaData() {
    return metaData;
  }

  /**
   * Returns the original SegmentInfo passed to the segment reader on creation time.
   * {@link #getSegmentInfo()} returns a clone of this instance.
   */
  SegmentCommitInfo getOriginalSegmentInfo() {
    return originalSi;
  }
}

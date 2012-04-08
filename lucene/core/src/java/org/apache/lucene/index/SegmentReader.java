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

import org.apache.lucene.store.Directory;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.search.FieldCache; // javadocs
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

/**
 * IndexReader implementation over a single segment. 
 * <p>
 * Instances pointing to the same segment (but with different deletes, etc)
 * may share the same core data.
 * @lucene.experimental
 */
public final class SegmentReader extends AtomicReader {

  private final SegmentInfo si;
  private final Bits liveDocs;

  // Normally set to si.docCount - si.delDocCount, unless we
  // were created as an NRT reader from IW, in which case IW
  // tells us the docCount:
  private final int numDocs;

  final SegmentCoreReaders core;

  /**
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public SegmentReader(SegmentInfo si, int termInfosIndexDivisor, IOContext context) throws IOException {
    this.si = si;
    core = new SegmentCoreReaders(this, si.dir, si, context, termInfosIndexDivisor);
    boolean success = false;
    try {
      if (si.hasDeletions()) {
        // NOTE: the bitvector is stored using the regular directory, not cfs
        liveDocs = si.getCodec().liveDocsFormat().readLiveDocs(directory(), si, new IOContext(IOContext.READ, true));
      } else {
        assert si.getDelCount() == 0;
        liveDocs = null;
      }
      numDocs = si.docCount - si.getDelCount();
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above.  In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        core.decRef();
      }
    }
  }

  // Create new SegmentReader sharing core from a previous
  // SegmentReader and loading new live docs from a new
  // deletes file.  Used by openIfChanged.
  SegmentReader(SegmentInfo si, SegmentCoreReaders core, IOContext context) throws IOException {
    this(si, core, si.getCodec().liveDocsFormat().readLiveDocs(si.dir, si, context), si.docCount - si.getDelCount());
  }

  // Create new SegmentReader sharing core from a previous
  // SegmentReader and using the provided in-memory
  // liveDocs.  Used by IndexWriter to provide a new NRT
  // reader:
  SegmentReader(SegmentInfo si, SegmentCoreReaders core, Bits liveDocs, int numDocs) throws IOException {
    this.si = si;
    this.core = core;
    core.incRef();

    assert liveDocs != null;
    this.liveDocs = liveDocs;

    this.numDocs = numDocs;
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
  }

  @Override
  public boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return liveDocs != null;
  }

  @Override
  public FieldInfos getFieldInfos() {
    ensureOpen();
    return core.fieldInfos;
  }

  /** @lucene.internal */
  public StoredFieldsReader getFieldsReader() {
    ensureOpen();
    return core.fieldsReaderLocal.get();
  }
  
  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
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
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return si.docCount;
  }

  /** @lucene.internal */
  public TermVectorsReader getTermVectorsReader() {
    ensureOpen();
    return core.termVectorsLocal.get();
  }

  /** Return a term frequency vector for the specified document and field. The
   *  vector returned contains term numbers and frequencies for all terms in
   *  the specified field of this document, if the field had storeTermVector
   *  flag set.  If the flag was not set, the method returns null.
   * @throws IOException
   */
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null) {
      return null;
    }
    return termVectorsReader.get(docID);
  }

  @Override
  public String toString() {
    // SegmentInfo.toString takes dir and number of
    // *pending* deletions; so we reverse compute that here:
    return si.toString(si.dir, si.docCount - numDocs - si.getDelCount());
  }
  
  /**
   * Return the name of the segment this reader is reading.
   */
  public String getSegmentName() {
    return si.name;
  }
  
  /**
   * Return the SegmentInfo of the segment this reader is reading.
   */
  SegmentInfo getSegmentInfo() {
    return si;
  }

  /** Returns the directory this index resides in. */
  public Directory directory() {
    // Don't ensureOpen here -- in certain cases, when a
    // cloned/reopened reader needs to commit, it may call
    // this method on the closed original reader
    return si.dir;
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
  
  @Override
  public DocValues normValues(String field) throws IOException {
    ensureOpen();
    final PerDocProducer perDoc = core.norms;
    if (perDoc == null) {
      return null;
    }
    return perDoc.docValues(field);
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
    core.addCoreClosedListener(listener);
  }
  
  /** Expert: removes a CoreClosedListener from this reader's shared core */
  public void removeCoreClosedListener(CoreClosedListener listener) {
    ensureOpen();
    core.removeCoreClosedListener(listener);
  }
}

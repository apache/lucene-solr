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
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

final class DocumentsWriterPerThread implements Accountable {

  /**
   * The IndexingChain must define the {@link #getChain(int, SegmentInfo, Directory, FieldInfos.Builder, LiveIndexWriterConfig, Consumer)} method
   * which returns the DocConsumer that the DocumentsWriter calls to process the
   * documents.
   */
  abstract static class IndexingChain {
    abstract DocConsumer getChain(int indexCreatedVersionMajor, SegmentInfo segmentInfo, Directory directory,
                                  FieldInfos.Builder fieldInfos, LiveIndexWriterConfig indexWriterConfig,
                                  Consumer<Throwable> abortingExceptionConsumer);
  }

  private Throwable abortingException;

  private void onAbortingException(Throwable throwable) {
    assert throwable != null : "aborting exception must not be null";
    assert abortingException == null: "aborting exception has already been set";
    abortingException = throwable;
  }

  final boolean isAborted() {
    return aborted;
  }

  static final IndexingChain defaultIndexingChain = new IndexingChain() {

    @Override
    DocConsumer getChain(int indexCreatedVersionMajor, SegmentInfo segmentInfo, Directory directory,
                         FieldInfos.Builder fieldInfos, LiveIndexWriterConfig indexWriterConfig,
                         Consumer<Throwable> abortingExceptionConsumer) {
      return new DefaultIndexingChain(indexCreatedVersionMajor, segmentInfo, directory, fieldInfos, indexWriterConfig, abortingExceptionConsumer);
    }
  };

  static final class FlushedSegment {
    final SegmentCommitInfo segmentInfo;
    final FieldInfos fieldInfos;
    final FrozenBufferedUpdates segmentUpdates;
    final FixedBitSet liveDocs;
    final Sorter.DocMap sortMap;
    final int delCount;

    private FlushedSegment(InfoStream infoStream, SegmentCommitInfo segmentInfo, FieldInfos fieldInfos,
                           BufferedUpdates segmentUpdates, FixedBitSet liveDocs, int delCount, Sorter.DocMap sortMap) {
      this.segmentInfo = segmentInfo;
      this.fieldInfos = fieldInfos;
      this.segmentUpdates = segmentUpdates != null && segmentUpdates.any() ? new FrozenBufferedUpdates(infoStream, segmentUpdates, segmentInfo) : null;
      this.liveDocs = liveDocs;
      this.delCount = delCount;
      this.sortMap = sortMap;
    }
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush. */
  void abort() throws IOException{
    aborted = true;
    pendingNumDocs.addAndGet(-numDocsInRAM);
    try {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "now abort");
      }
      try {
        consumer.abort();
      } finally {
        pendingUpdates.clear();
      }
    } finally {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "done abort");
      }
    }
  }
  private final static boolean INFO_VERBOSE = false;
  final Codec codec;
  final TrackingDirectoryWrapper directory;
  private final DocConsumer consumer;

  // Updates for our still-in-RAM (to be flushed next) segment
  private final BufferedUpdates pendingUpdates;
  private final SegmentInfo segmentInfo;     // Current segment we are working on
  private boolean aborted = false;   // True if we aborted
  private SetOnce<Boolean> flushPending = new SetOnce<>();
  private volatile long lastCommittedBytesUsed;
  private SetOnce<Boolean> hasFlushed = new SetOnce<>();

  private final FieldInfos.Builder fieldInfos;
  private final InfoStream infoStream;
  private int numDocsInRAM;
  final DocumentsWriterDeleteQueue deleteQueue;
  private final DeleteSlice deleteSlice;
  private final NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
  private final AtomicLong pendingNumDocs;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final boolean enableTestPoints;
  private final ReentrantLock lock = new ReentrantLock();
  private int[] deleteDocIDs = new int[0];
  private int numDeletedDocIds = 0;

  DocumentsWriterPerThread(int indexVersionCreated, String segmentName, Directory directoryOrig, Directory directory,
                           LiveIndexWriterConfig indexWriterConfig, DocumentsWriterDeleteQueue deleteQueue,
                           FieldInfos.Builder fieldInfos, AtomicLong pendingNumDocs, boolean enableTestPoints) {
    this.directory = new TrackingDirectoryWrapper(directory);
    this.fieldInfos = fieldInfos;
    this.indexWriterConfig = indexWriterConfig;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.codec = indexWriterConfig.getCodec();
    this.pendingNumDocs = pendingNumDocs;
    pendingUpdates = new BufferedUpdates(segmentName);
    this.deleteQueue = Objects.requireNonNull(deleteQueue);
    assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
    deleteSlice = deleteQueue.newSlice();
   
    segmentInfo = new SegmentInfo(directoryOrig, Version.LATEST, Version.LATEST, segmentName, -1, false, codec, Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), indexWriterConfig.getIndexSort());
    assert numDocsInRAM == 0;
    if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
      infoStream.message("DWPT", Thread.currentThread().getName() + " init seg=" + segmentName + " delQueue=" + deleteQueue);  
    }
    this.enableTestPoints = enableTestPoints;
    consumer = indexWriterConfig.getIndexingChain().getChain(indexVersionCreated, segmentInfo, this.directory, fieldInfos, indexWriterConfig, this::onAbortingException);
  }
  
  final void testPoint(String message) {
    if (enableTestPoints) {
      assert infoStream.isEnabled("TP"); // don't enable unless you need them.
      infoStream.message("TP", message);
    }
  }

  /** Anything that will add N docs to the index should reserve first to
   *  make sure it's allowed. */
  private void reserveOneDoc() {
    if (pendingNumDocs.incrementAndGet() > IndexWriter.getActualMaxDocs()) {
      // Reserve failed: put the one doc back and throw exc:
      pendingNumDocs.decrementAndGet();
      throw new IllegalArgumentException("number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs());
    }
  }

  long updateDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, DocumentsWriterDeleteQueue.Node<?> deleteNode, DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
    try {
      testPoint("DocumentsWriterPerThread addDocuments start");
      assert abortingException == null: "DWPT has hit aborting exception but is still indexing";
      if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", Thread.currentThread().getName() + " update delTerm=" + deleteNode + " docID=" + numDocsInRAM + " seg=" + segmentInfo.name);
      }
      final int docsInRamBefore = numDocsInRAM;
      boolean allDocsIndexed = false;
      try {
        for (Iterable<? extends IndexableField> doc : docs) {
          // Even on exception, the document is still added (but marked
          // deleted), so we don't need to un-reserve at that point.
          // Aborting exceptions will actually "lose" more than one
          // document, so the counter will be "wrong" in that case, but
          // it's very hard to fix (we can't easily distinguish aborting
          // vs non-aborting exceptions):
          reserveOneDoc();
          consumer.processDocument(numDocsInRAM++, doc);
        }
        allDocsIndexed = true;
        return finishDocuments(deleteNode, docsInRamBefore);
      } finally {
        if (!allDocsIndexed && !aborted) {
          // the iterator threw an exception that is not aborting
          // go and mark all docs from this block as deleted
          deleteLastDocs(numDocsInRAM - docsInRamBefore);
        }
      }
    } finally {
      maybeAbort("updateDocuments", flushNotifications);
    }
  }
  
  private long finishDocuments(DocumentsWriterDeleteQueue.Node<?> deleteNode, int docIdUpTo) {
    /*
     * here we actually finish the document in two steps 1. push the delete into
     * the queue and update our slice. 2. increment the DWPT private document
     * id.
     * 
     * the updated slice we get from 1. holds all the deletes that have occurred
     * since we updated the slice the last time.
     */
    // Apply delTerm only after all indexing has
    // succeeded, but apply it only to docs prior to when
    // this batch started:
    long seqNo;
    if (deleteNode != null) {
      seqNo = deleteQueue.add(deleteNode, deleteSlice);
      assert deleteSlice.isTail(deleteNode) : "expected the delete term as the tail item";
      deleteSlice.apply(pendingUpdates, docIdUpTo);
      return seqNo;
    } else {
      seqNo = deleteQueue.updateSlice(deleteSlice);
      if (seqNo < 0) {
        seqNo = -seqNo;
        deleteSlice.apply(pendingUpdates, docIdUpTo);
      } else {
        deleteSlice.reset();
      }
    }

    return seqNo;
  }

  // This method marks the last N docs as deleted. This is used
  // in the case of a non-aborting exception. There are several cases
  // where we fail a document ie. due to an exception during analysis
  // that causes the doc to be rejected but won't cause the DWPT to be
  // stale nor the entire IW to abort and shutdown. In such a case
  // we only mark these docs as deleted and turn it into a livedocs
  // during flush
  private void deleteLastDocs(int docCount) {
    int from = numDocsInRAM-docCount;
    int to = numDocsInRAM;
    deleteDocIDs = ArrayUtil.grow(deleteDocIDs, numDeletedDocIds + (to-from));
    for (int docId = from; docId < to; docId++) {
      deleteDocIDs[numDeletedDocIds++] = docId;
    }
    // NOTE: we do not trigger flush here.  This is
    // potentially a RAM leak, if you have an app that tries
    // to add docs but every single doc always hits a
    // non-aborting exception.  Allowing a flush here gets
    // very messy because we are only invoked when handling
    // exceptions so to do this properly, while handling an
    // exception we'd have to go off and flush new deletes
    // which is risky (likely would hit some other
    // confounding exception).
  }

  /**
   * Returns the number of RAM resident documents in this {@link DocumentsWriterPerThread}
   */
  public int getNumDocsInRAM() {
    // public for FlushPolicy
    return numDocsInRAM;
  }

  /**
   * Prepares this DWPT for flushing. This method will freeze and return the
   * {@link DocumentsWriterDeleteQueue}s global buffer and apply all pending
   * deletes to this DWPT.
   */
  FrozenBufferedUpdates prepareFlush() {
    assert numDocsInRAM > 0;
    final FrozenBufferedUpdates globalUpdates = deleteQueue.freezeGlobalBuffer(deleteSlice);
    /* deleteSlice can possibly be null if we have hit non-aborting exceptions during indexing and never succeeded 
    adding a document. */
    if (deleteSlice != null) {
      // apply all deletes before we flush and release the delete slice
      deleteSlice.apply(pendingUpdates, numDocsInRAM);
      assert deleteSlice.isEmpty();
      deleteSlice.reset();
    }
    return globalUpdates;
  }

  /** Flush all pending docs to a new segment */
  FlushedSegment flush(DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
    assert flushPending.get() == Boolean.TRUE;
    assert numDocsInRAM > 0;
    assert deleteSlice.isEmpty() : "all deletes must be applied in prepareFlush";
    segmentInfo.setMaxDoc(numDocsInRAM);
    final SegmentWriteState flushState = new SegmentWriteState(infoStream, directory, segmentInfo, fieldInfos.finish(),
        pendingUpdates, new IOContext(new FlushInfo(numDocsInRAM, lastCommittedBytesUsed)));
    final double startMBUsed = lastCommittedBytesUsed / 1024. / 1024.;

    // Apply delete-by-docID now (delete-byDocID only
    // happens when an exception is hit processing that
    // doc, eg if analyzer has some problem w/ the text):
    if (numDeletedDocIds > 0) {
      flushState.liveDocs = new FixedBitSet(numDocsInRAM);
      flushState.liveDocs.set(0, numDocsInRAM);
      for (int i = 0; i < numDeletedDocIds; i++) {
        flushState.liveDocs.clear(deleteDocIDs[i]);
      }
      flushState.delCountOnFlush = numDeletedDocIds;
      deleteDocIDs = new int[0];
    }

    if (aborted) {
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "flush: skip because aborting is set");
      }
      return null;
    }

    long t0 = System.nanoTime();

    if (infoStream.isEnabled("DWPT")) {
      infoStream.message("DWPT", "flush postings as segment " + flushState.segmentInfo.name + " numDocs=" + numDocsInRAM);
    }
    final Sorter.DocMap sortMap;
    try {
      DocIdSetIterator softDeletedDocs;
      if (indexWriterConfig.getSoftDeletesField() != null) {
        softDeletedDocs = consumer.getHasDocValues(indexWriterConfig.getSoftDeletesField());
      } else {
        softDeletedDocs = null;
      }
      sortMap = consumer.flush(flushState);
      if (softDeletedDocs == null) {
        flushState.softDelCountOnFlush = 0;
      } else {
        flushState.softDelCountOnFlush = PendingSoftDeletes.countSoftDeletes(softDeletedDocs, flushState.liveDocs);
        assert flushState.segmentInfo.maxDoc() >= flushState.softDelCountOnFlush + flushState.delCountOnFlush;
      }
      // We clear this here because we already resolved them (private to this segment) when writing postings:
      pendingUpdates.clearDeleteTerms();
      segmentInfo.setFiles(new HashSet<>(directory.getCreatedFiles()));

      final SegmentCommitInfo segmentInfoPerCommit = new SegmentCommitInfo(segmentInfo, 0, flushState.softDelCountOnFlush, -1L, -1L, -1L, StringHelper.randomId());
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "new segment has " + (flushState.liveDocs == null ? 0 : flushState.delCountOnFlush) + " deleted docs");
        infoStream.message("DWPT", "new segment has " + flushState.softDelCountOnFlush + " soft-deleted docs");
        infoStream.message("DWPT", "new segment has " +
            (flushState.fieldInfos.hasVectors() ? "vectors" : "no vectors") + "; " +
            (flushState.fieldInfos.hasNorms() ? "norms" : "no norms") + "; " +
            (flushState.fieldInfos.hasDocValues() ? "docValues" : "no docValues") + "; " +
            (flushState.fieldInfos.hasProx() ? "prox" : "no prox") + "; " +
            (flushState.fieldInfos.hasFreq() ? "freqs" : "no freqs"));
        infoStream.message("DWPT", "flushedFiles=" + segmentInfoPerCommit.files());
        infoStream.message("DWPT", "flushed codec=" + codec);
      }

      final BufferedUpdates segmentDeletes;
      if (pendingUpdates.deleteQueries.isEmpty() && pendingUpdates.numFieldUpdates.get() == 0) {
        pendingUpdates.clear();
        segmentDeletes = null;
      } else {
        segmentDeletes = pendingUpdates;
      }

      if (infoStream.isEnabled("DWPT")) {
        final double newSegmentSize = segmentInfoPerCommit.sizeInBytes() / 1024. / 1024.;
        infoStream.message("DWPT", "flushed: segment=" + segmentInfo.name +
            " ramUsed=" + nf.format(startMBUsed) + " MB" +
            " newFlushedSize=" + nf.format(newSegmentSize) + " MB" +
            " docs/MB=" + nf.format(flushState.segmentInfo.maxDoc() / newSegmentSize));
      }

      assert segmentInfo != null;

      FlushedSegment fs = new FlushedSegment(infoStream, segmentInfoPerCommit, flushState.fieldInfos,
          segmentDeletes, flushState.liveDocs, flushState.delCountOnFlush, sortMap);
      sealFlushedSegment(fs, sortMap, flushNotifications);
      if (infoStream.isEnabled("DWPT")) {
        infoStream.message("DWPT", "flush time " + ((System.nanoTime() - t0) / 1000000.0) + " msec");
      }
      return fs;
    } catch (Throwable t) {
      onAbortingException(t);
      throw t;
    } finally {
      maybeAbort("flush", flushNotifications);
      hasFlushed.set(Boolean.TRUE);
    }
  }

  private void maybeAbort(String location, DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
    if (abortingException != null && aborted == false) {
      // if we are already aborted don't do anything here
      try {
        abort();
      } finally {
        // whatever we do here we have to fire this tragic event up.
        flushNotifications.onTragicEvent(abortingException, location);
      }
    }
  }
  
  private final Set<String> filesToDelete = new HashSet<>();
  
  Set<String> pendingFilesToDelete() {
    return filesToDelete;
  }

  private FixedBitSet sortLiveDocs(Bits liveDocs, Sorter.DocMap sortMap) {
    assert liveDocs != null && sortMap != null;
    FixedBitSet sortedLiveDocs = new FixedBitSet(liveDocs.length());
    sortedLiveDocs.set(0, liveDocs.length());
    for (int i = 0; i < liveDocs.length(); i++) {
      if (liveDocs.get(i) == false) {
        sortedLiveDocs.clear(sortMap.oldToNew(i));
      }
    }
    return sortedLiveDocs;
  }

  /**
   * Seals the {@link SegmentInfo} for the new flushed segment and persists
   * the deleted documents {@link FixedBitSet}.
   */
  void sealFlushedSegment(FlushedSegment flushedSegment, Sorter.DocMap sortMap, DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
    assert flushedSegment != null;
    SegmentCommitInfo newSegment = flushedSegment.segmentInfo;

    IndexWriter.setDiagnostics(newSegment.info, IndexWriter.SOURCE_FLUSH);
    
    IOContext context = new IOContext(new FlushInfo(newSegment.info.maxDoc(), newSegment.sizeInBytes()));

    boolean success = false;
    try {
      
      if (indexWriterConfig.getUseCompoundFile()) {
        Set<String> originalFiles = newSegment.info.files();
        // TODO: like addIndexes, we are relying on createCompoundFile to successfully cleanup...
        IndexWriter.createCompoundFile(infoStream, new TrackingDirectoryWrapper(directory), newSegment.info, context, flushNotifications::deleteUnusedFiles);
        filesToDelete.addAll(originalFiles);
        newSegment.info.setUseCompoundFile(true);
      }

      // Have codec write SegmentInfo.  Must do this after
      // creating CFS so that 1) .si isn't slurped into CFS,
      // and 2) .si reflects useCompoundFile=true change
      // above:
      codec.segmentInfoFormat().write(directory, newSegment.info, context);

      // TODO: ideally we would freeze newSegment here!!
      // because any changes after writing the .si will be
      // lost... 

      // Must write deleted docs after the CFS so we don't
      // slurp the del file into CFS:
      if (flushedSegment.liveDocs != null) {
        final int delCount = flushedSegment.delCount;
        assert delCount > 0;
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message("DWPT", "flush: write " + delCount + " deletes gen=" + flushedSegment.segmentInfo.getDelGen());
        }

        // TODO: we should prune the segment if it's 100%
        // deleted... but merge will also catch it.

        // TODO: in the NRT case it'd be better to hand
        // this del vector over to the
        // shortly-to-be-opened SegmentReader and let it
        // carry the changes; there's no reason to use
        // filesystem as intermediary here.
          
        SegmentCommitInfo info = flushedSegment.segmentInfo;
        Codec codec = info.info.getCodec();
        final FixedBitSet bits;
        if (sortMap == null) {
          bits = flushedSegment.liveDocs;
        } else {
          bits = sortLiveDocs(flushedSegment.liveDocs, sortMap);
        }
        codec.liveDocsFormat().writeLiveDocs(bits, directory, info, delCount, context);
        newSegment.setDelCount(delCount);
        newSegment.advanceDelGen();
      }

      success = true;
    } finally {
      if (!success) {
        if (infoStream.isEnabled("DWPT")) {
          infoStream.message("DWPT",
                             "hit exception creating compound file for newly flushed segment " + newSegment.info.name);
        }
      }
    }
  }

  /** Get current segment info we are writing. */
  SegmentInfo getSegmentInfo() {
    return segmentInfo;
  }

  @Override
  public long ramBytesUsed() {
    assert lock.isHeldByCurrentThread();
    return (deleteDocIDs.length  * Integer.BYTES)+ pendingUpdates.ramBytesUsed() + consumer.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    assert lock.isHeldByCurrentThread();
    return Collections.unmodifiableList(Arrays.asList(pendingUpdates, consumer));
  }

  @Override
  public String toString() {
    return "DocumentsWriterPerThread [pendingDeletes=" + pendingUpdates
      + ", segment=" + (segmentInfo != null ? segmentInfo.name : "null") + ", aborted=" + aborted + ", numDocsInRAM="
        + numDocsInRAM + ", deleteQueue=" + deleteQueue + ", " + numDeletedDocIds + " deleted docIds" + "]";
  }


  /**
   * Returns true iff this DWPT is marked as flush pending
   */
  boolean isFlushPending() {
    return flushPending.get() == Boolean.TRUE;
  }

  /**
   * Sets this DWPT as flush pending. This can only be set once.
   */
  void setFlushPending() {
    flushPending.set(Boolean.TRUE);
  }


  /**
   * Returns the last committed bytes for this DWPT. This method can be called
   * without acquiring the DWPTs lock.
   */
  long getLastCommittedBytesUsed() {
    return lastCommittedBytesUsed;
  }

  /**
   * Commits the current {@link #ramBytesUsed()} and stores it's value for later reuse.
   * The last committed bytes used can be retrieved via {@link #getLastCommittedBytesUsed()}
   * @return the delta between the current {@link #ramBytesUsed()} and the current {@link #getLastCommittedBytesUsed()}
   */
  long commitLastBytesUsed() {
    assert isHeldByCurrentThread();
    long delta = ramBytesUsed() - lastCommittedBytesUsed;
    lastCommittedBytesUsed += delta;
    return delta;
  }

  /**
   * Locks this DWPT for exclusive access.
   * @see ReentrantLock#lock()
   */
  void lock() {
    lock.lock();
  }

  /**
   * Acquires the DWPT's lock only if it is not held by another thread at the time
   * of invocation.
   * @return true if the lock was acquired.
   * @see ReentrantLock#tryLock()
   */
  boolean tryLock() {
    return lock.tryLock();
  }

  /**
   * Returns true if the DWPT's lock is held by the current thread
   * @see ReentrantLock#isHeldByCurrentThread()
   */
  boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }

  /**
   * Unlocks the DWPT's lock
   * @see ReentrantLock#unlock()
   */
  void unlock() {
    lock.unlock();
  }

  /**
   * Returns <code>true</code> iff this DWPT has been flushed
   */
  boolean hasFlushed() {
    return hasFlushed.get() == Boolean.TRUE;
  }
}

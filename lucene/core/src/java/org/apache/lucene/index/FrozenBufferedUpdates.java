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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds buffered deletes and updates by term or query, once pushed. Pushed
 * deletes/updates are write-once, so we shift to more memory efficient data
 * structure to hold them.  We don't hold docIDs because these are applied on
 * flush.
 */
final class FrozenBufferedUpdates {

  /* NOTE: we now apply this frozen packet immediately on creation, yet this process is heavy, and runs
   * in multiple threads, and this compression is sizable (~8.3% of the original size), so it's important
   * we run this before applying the deletes/updates. */

  /* Query we often undercount (say 24 bytes), plus int. */
  final static int BYTES_PER_DEL_QUERY = RamUsageEstimator.NUM_BYTES_OBJECT_REF + Integer.BYTES + 24;
  
  // Terms, in sorted order:
  final PrefixCodedTerms deleteTerms;

  // Parallel array of deleted query, and the docIDUpto for each
  final Query[] deleteQueries;
  final int[] deleteQueryLimits;
  
  /** Counts down once all deletes/updates have been applied */
  public final CountDownLatch applied = new CountDownLatch(1);
  private final ReentrantLock applyLock = new ReentrantLock();
  private final Map<String, FieldUpdatesBuffer> fieldUpdates;

  /** How many total documents were deleted/updated. */
  public long totalDelCount;
  private final int fieldUpdatesCount;
  
  final int bytesUsed;
  final int numTermDeletes;

  private long delGen = -1; // assigned by BufferedUpdatesStream once pushed

  final SegmentCommitInfo privateSegment;  // non-null iff this frozen packet represents 
                                   // a segment private deletes. in that case is should
                                   // only have Queries and doc values updates
  private final InfoStream infoStream;

  public FrozenBufferedUpdates(InfoStream infoStream, BufferedUpdates updates, SegmentCommitInfo privateSegment) {
    this.infoStream = infoStream;
    this.privateSegment = privateSegment;
    assert updates.deleteDocIDs.isEmpty();
    assert privateSegment == null || updates.deleteTerms.isEmpty() : "segment private packet should only have del queries"; 
    Term termsArray[] = updates.deleteTerms.keySet().toArray(new Term[updates.deleteTerms.size()]);
    ArrayUtil.timSort(termsArray);
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    for (Term term : termsArray) {
      builder.add(term);
    }
    deleteTerms = builder.finish();
    
    deleteQueries = new Query[updates.deleteQueries.size()];
    deleteQueryLimits = new int[updates.deleteQueries.size()];
    int upto = 0;
    for(Map.Entry<Query,Integer> ent : updates.deleteQueries.entrySet()) {
      deleteQueries[upto] = ent.getKey();
      deleteQueryLimits[upto] = ent.getValue();
      upto++;
    }
    // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
    // so that it maps to all fields it affects, sorted by their docUpto, and traverse
    // that Term only once, applying the update to all fields that still need to be
    // updated.
    this.fieldUpdates = Map.copyOf(updates.fieldUpdates);
    this.fieldUpdatesCount = updates.numFieldUpdates.get();

    bytesUsed = (int) ((deleteTerms.ramBytesUsed() + deleteQueries.length * BYTES_PER_DEL_QUERY)
        + updates.fieldUpdatesBytesUsed.get());
    
    numTermDeletes = updates.numTermDeletes.get();
    if (infoStream != null && infoStream.isEnabled("BD")) {
      infoStream.message("BD", String.format(Locale.ROOT,
                                             "compressed %d to %d bytes (%.2f%%) for deletes/updates; private segment %s",
                                             updates.ramBytesUsed(), bytesUsed, 100.*bytesUsed/updates.ramBytesUsed(),
                                             privateSegment));
    }
  }

  /** Returns the {@link SegmentCommitInfo} that this packet is supposed to apply its deletes to, or null
   *  if the private segment was already merged away. */
  private List<SegmentCommitInfo> getInfosToApply(IndexWriter writer) {
    assert Thread.holdsLock(writer);
    final List<SegmentCommitInfo> infos;
    if (privateSegment != null) {
      if (writer.segmentCommitInfoExist(privateSegment)) {
        infos = Collections.singletonList(privateSegment);
      }else {
        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", "private segment already gone; skip processing updates");
        }
        infos = null;
      }
    } else {
      infos = writer.listOfSegmentCommitInfos();
    }
    return infos;
  }

  /** Translates a frozen packet of delete term/query, or doc values
   *  updates, into their actual docIDs in the index, and applies the change.  This is a heavy
   *  operation and is done concurrently by incoming indexing threads.
   *  This method will return immediately without blocking if another thread is currently
   *  applying the package. In order to ensure the packet has been applied, {@link #forceApply(IndexWriter)}
   *  must be called.
   *  */
  @SuppressWarnings("try")
  boolean tryApply(IndexWriter writer) throws IOException {
    if (applyLock.tryLock()) {
      try {
        forceApply(writer);
        return true;
      } finally {
        applyLock.unlock();
      }
    }
    return false;
  }

  /** Translates a frozen packet of delete term/query, or doc values
   *  updates, into their actual docIDs in the index, and applies the change.  This is a heavy
   *  operation and is done concurrently by incoming indexing threads.
   *  */
  void forceApply(IndexWriter writer) throws IOException {
    applyLock.lock();
    try {
      if (applied.getCount() == 0) {
        // already done
        return;
      }
      long startNS = System.nanoTime();

      assert any();

      Set<SegmentCommitInfo> seenSegments = new HashSet<>();

      int iter = 0;
      int totalSegmentCount = 0;
      long totalDelCount = 0;

      boolean finished = false;

      // Optimistic concurrency: assume we are free to resolve the deletes against all current segments in the index, despite that
      // concurrent merges are running.  Once we are done, we check to see if a merge completed while we were running.  If so, we must retry
      // resolving against the newly merged segment(s).  Eventually no merge finishes while we were running and we are done.
      while (true) {
        String messagePrefix;
        if (iter == 0) {
          messagePrefix = "";
        } else {
          messagePrefix = "iter " + iter;
        }

        long iterStartNS = System.nanoTime();

        long mergeGenStart = writer.mergeFinishedGen.get();

        Set<String> delFiles = new HashSet<>();
        BufferedUpdatesStream.SegmentState[] segStates;

        synchronized (writer) {
          List<SegmentCommitInfo> infos = getInfosToApply(writer);
          if (infos == null) {
            break;
          }

          for (SegmentCommitInfo info : infos) {
            delFiles.addAll(info.files());
          }

          // Must open while holding IW lock so that e.g. segments are not merged
          // away, dropped from 100% deletions, etc., before we can open the readers
          segStates = openSegmentStates(writer, infos, seenSegments, delGen());

          if (segStates.length == 0) {

            if (infoStream.isEnabled("BD")) {
              infoStream.message("BD", "packet matches no segments");
            }
            break;
          }

          if (infoStream.isEnabled("BD")) {
            infoStream.message("BD", String.format(Locale.ROOT,
                messagePrefix + "now apply del packet (%s) to %d segments, mergeGen %d",
                this, segStates.length, mergeGenStart));
          }

          totalSegmentCount += segStates.length;

          // Important, else IFD may try to delete our files while we are still using them,
          // if e.g. a merge finishes on some of the segments we are resolving on:
          writer.deleter.incRef(delFiles);
        }

        AtomicBoolean success = new AtomicBoolean();
        long delCount;
        try (Closeable finalizer = () -> finishApply(writer, segStates, success.get(), delFiles)) {
          assert finalizer != null; // access the finalizer to prevent a warning
          // don't hold IW monitor lock here so threads are free concurrently resolve deletes/updates:
          delCount = apply(segStates);
          success.set(true);
        }

        // Since we just resolved some more deletes/updates, now is a good time to write them:
        writer.writeSomeDocValuesUpdates();

        // It's OK to add this here, even if the while loop retries, because delCount only includes newly
        // deleted documents, on the segments we didn't already do in previous iterations:
        totalDelCount += delCount;

        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", String.format(Locale.ROOT,
              messagePrefix + "done inner apply del packet (%s) to %d segments; %d new deletes/updates; took %.3f sec",
              this, segStates.length, delCount, (System.nanoTime() - iterStartNS) / 1000000000.));
        }
        if (privateSegment != null) {
          // No need to retry for a segment-private packet: the merge that folds in our private segment already waits for all deletes to
          // be applied before it kicks off, so this private segment must already not be in the set of merging segments

          break;
        }

        // Must sync on writer here so that IW.mergeCommit is not running concurrently, so that if we exit, we know mergeCommit will succeed
        // in pulling all our delGens into a merge:
        synchronized (writer) {
          long mergeGenCur = writer.mergeFinishedGen.get();

          if (mergeGenCur == mergeGenStart) {

            // Must do this while still holding IW lock else a merge could finish and skip carrying over our updates:

            // Record that this packet is finished:
            writer.finished(this);

            finished = true;

            // No merge finished while we were applying, so we are done!
            break;
          }
        }

        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", messagePrefix + "concurrent merges finished; move to next iter");
        }

        // A merge completed while we were running.  In this case, that merge may have picked up some of the updates we did, but not
        // necessarily all of them, so we cycle again, re-applying all our updates to the newly merged segment.

        iter++;
      }

      if (finished == false) {
        // Record that this packet is finished:
        writer.finished(this);
      }

      if (infoStream.isEnabled("BD")) {
        String message = String.format(Locale.ROOT,
            "done apply del packet (%s) to %d segments; %d new deletes/updates; took %.3f sec",
            this, totalSegmentCount, totalDelCount, (System.nanoTime() - startNS) / 1000000000.);
        if (iter > 0) {
          message += "; " + (iter + 1) + " iters due to concurrent merges";
        }
        message += "; " + writer.getPendingUpdatesCount() + " packets remain";
        infoStream.message("BD", message);
      }
    } finally {
      applyLock.unlock();
    }
  }

  /** Opens SegmentReader and inits SegmentState for each segment. */
  private static BufferedUpdatesStream.SegmentState[] openSegmentStates(IndexWriter writer, List<SegmentCommitInfo> infos,
                                                                       Set<SegmentCommitInfo> alreadySeenSegments, long delGen) throws IOException {
    List<BufferedUpdatesStream.SegmentState> segStates = new ArrayList<>();
    try {
      for (SegmentCommitInfo info : infos) {
        if (info.getBufferedDeletesGen() <= delGen && alreadySeenSegments.contains(info) == false) {
          segStates.add(new BufferedUpdatesStream.SegmentState(writer.getPooledInstance(info, true), writer::release, info));
          alreadySeenSegments.add(info);
        }
      }
    } catch (Throwable t) {
      try {
        IOUtils.close(segStates);
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }

    return segStates.toArray(new BufferedUpdatesStream.SegmentState[0]);
  }

  /** Close segment states previously opened with openSegmentStates. */
  public static BufferedUpdatesStream.ApplyDeletesResult closeSegmentStates(IndexWriter writer, BufferedUpdatesStream.SegmentState[] segStates, boolean success) throws IOException {
    List<SegmentCommitInfo> allDeleted = null;
    long totDelCount = 0;
    try {
      for (BufferedUpdatesStream.SegmentState segState : segStates) {
        if (success) {
          totDelCount += segState.rld.getDelCount() - segState.startDelCount;
          int fullDelCount = segState.rld.getDelCount();
          assert fullDelCount <= segState.rld.info.info.maxDoc() : fullDelCount + " > " + segState.rld.info.info.maxDoc();
          if (segState.rld.isFullyDeleted() && writer.getConfig().getMergePolicy().keepFullyDeletedSegment(() -> segState.reader) == false) {
            if (allDeleted == null) {
              allDeleted = new ArrayList<>();
            }
            allDeleted.add(segState.reader.getOriginalSegmentInfo());
          }
        }
      }
    } finally {
      IOUtils.close(segStates);
    }
    if (writer.infoStream.isEnabled("BD")) {
      writer.infoStream.message("BD", "closeSegmentStates: " + totDelCount + " new deleted documents; pool " + writer.getPendingUpdatesCount()+ " packets; bytesUsed=" + writer.getReaderPoolRamBytesUsed());
    }

    return new BufferedUpdatesStream.ApplyDeletesResult(totDelCount > 0, allDeleted);
  }

  private void finishApply(IndexWriter writer, BufferedUpdatesStream.SegmentState[] segStates,
                           boolean success, Set<String> delFiles) throws IOException {
    assert applyLock.isHeldByCurrentThread();
    synchronized (writer) {

      BufferedUpdatesStream.ApplyDeletesResult result;
      try {
        result = closeSegmentStates(writer, segStates, success);
      } finally {
        // Matches the incRef we did above, but we must do the decRef after closing segment states else
        // IFD can't delete still-open files
        writer.deleter.decRef(delFiles);
      }

      if (result.anyDeletes) {
          writer.maybeMerge.set(true);
          writer.checkpoint();
      }

      if (result.allDeleted != null) {
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "drop 100% deleted segments: " + writer.segString(result.allDeleted));
        }
        for (SegmentCommitInfo info : result.allDeleted) {
          writer.dropDeletedSegment(info);
        }
        writer.checkpoint();
      }
    }
  }

  /** Applies pending delete-by-term, delete-by-query and doc values updates to all segments in the index, returning
   *  the number of new deleted or updated documents. */
  private long apply(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {
    assert applyLock.isHeldByCurrentThread();
    if (delGen == -1) {
      // we were not yet pushed
      throw new IllegalArgumentException("gen is not yet set; call BufferedUpdatesStream.push first");
    }

    assert applied.getCount() != 0;

    if (privateSegment != null) {
      assert segStates.length == 1;
      assert privateSegment == segStates[0].reader.getOriginalSegmentInfo();
    }

    totalDelCount += applyTermDeletes(segStates);
    totalDelCount += applyQueryDeletes(segStates);
    totalDelCount += applyDocValuesUpdates(segStates);

    return totalDelCount;
  }

  private long applyDocValuesUpdates(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

    if (fieldUpdates.isEmpty()) {
      return 0;
    }

    long startNS = System.nanoTime();

    long updateCount = 0;

    for (BufferedUpdatesStream.SegmentState segState : segStates) {

      if (delGen < segState.delGen) {
        // segment is newer than this deletes packet
        continue;
      }

      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }
      final boolean isSegmentPrivateDeletes = privateSegment != null;
      if (fieldUpdates.isEmpty() == false) {
        updateCount += applyDocValuesUpdates(segState, fieldUpdates, delGen, isSegmentPrivateDeletes);
      }

    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyDocValuesUpdates %.1f msec for %d segments, %d field updates; %d new updates",
                                       (System.nanoTime()-startNS)/1000000.,
                                       segStates.length,
                                       fieldUpdatesCount,
                                       updateCount));
    }

    return updateCount;
  }

  private static long applyDocValuesUpdates(BufferedUpdatesStream.SegmentState segState,
                                            Map<String, FieldUpdatesBuffer> updates,
                                            long delGen,
                                            boolean segmentPrivateDeletes) throws IOException {

    // TODO: we can process the updates per DV field, from last to first so that
    // if multiple terms affect same document for the same field, we add an update
    // only once (that of the last term). To do that, we can keep a bitset which
    // marks which documents have already been updated. So e.g. if term T1
    // updates doc 7, and then we process term T2 and it updates doc 7 as well,
    // we don't apply the update since we know T1 came last and therefore wins
    // the update.
    // We can also use that bitset as 'liveDocs' to pass to TermEnum.docs(), so
    // that these documents aren't even returned.

    long updateCount = 0;

    // We first write all our updates private, and only in the end publish to the ReadersAndUpdates */
    final List<DocValuesFieldUpdates> resolvedUpdates = new ArrayList<>();
    for (Map.Entry<String, FieldUpdatesBuffer> fieldUpdate : updates.entrySet()) {
      String updateField = fieldUpdate.getKey();
      DocValuesFieldUpdates dvUpdates = null;
      FieldUpdatesBuffer value = fieldUpdate.getValue();
      boolean isNumeric = value.isNumeric();
      FieldUpdatesBuffer.BufferedUpdateIterator iterator = value.iterator();
      FieldUpdatesBuffer.BufferedUpdate bufferedUpdate;
      TermDocsIterator termDocsIterator = new TermDocsIterator(segState.reader, false);
      while ((bufferedUpdate = iterator.next()) != null) {
        // TODO: we traverse the terms in update order (not term order) so that we
        // apply the updates in the correct order, i.e. if two terms update the
        // same document, the last one that came in wins, irrespective of the
        // terms lexical order.
        // we can apply the updates in terms order if we keep an updatesGen (and
        // increment it with every update) and attach it to each NumericUpdate. Note
        // that we cannot rely only on docIDUpto because an app may send two updates
        // which will get same docIDUpto, yet will still need to respect the order
        // those updates arrived.
        // TODO: we could at least *collate* by field?
        final DocIdSetIterator docIdSetIterator = termDocsIterator.nextTerm(bufferedUpdate.termField, bufferedUpdate.termValue);
        if (docIdSetIterator != null) {
          final int limit;
          if (delGen == segState.delGen) {
            assert segmentPrivateDeletes;
            limit = bufferedUpdate.docUpTo;
          } else {
            limit = Integer.MAX_VALUE;
          }
          final BytesRef binaryValue;
          final long longValue;
          if (bufferedUpdate.hasValue == false) {
            longValue = -1;
            binaryValue = null;
          } else {
            longValue = bufferedUpdate.numericValue;
            binaryValue = bufferedUpdate.binaryValue;
          }
           termDocsIterator.getDocs();
          if (dvUpdates == null) {
            if (isNumeric) {
              if (value.hasSingleValue()) {
                dvUpdates = new NumericDocValuesFieldUpdates
                    .SingleValueNumericDocValuesFieldUpdates(delGen, updateField, segState.reader.maxDoc(),
                    value.getNumericValue(0));
              } else {
                dvUpdates = new NumericDocValuesFieldUpdates(delGen, updateField, value.getMinNumeric(),
                    value.getMaxNumeric(), segState.reader.maxDoc());
              }
            } else {
              dvUpdates = new BinaryDocValuesFieldUpdates(delGen, updateField, segState.reader.maxDoc());
            }
            resolvedUpdates.add(dvUpdates);
          }
          final IntConsumer docIdConsumer;
          final DocValuesFieldUpdates update = dvUpdates;
          if (bufferedUpdate.hasValue == false) {
            docIdConsumer = doc -> update.reset(doc);
          } else if (isNumeric) {
            docIdConsumer = doc -> update.add(doc, longValue);
          } else {
            docIdConsumer = doc -> update.add(doc, binaryValue);
          }
          final Bits acceptDocs = segState.rld.getLiveDocs();
          if (segState.rld.sortMap != null && segmentPrivateDeletes) {
            // This segment was sorted on flush; we must apply seg-private deletes carefully in this case:
            int doc;
            while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (acceptDocs == null || acceptDocs.get(doc)) {
                // The limit is in the pre-sorted doc space:
                if (segState.rld.sortMap.newToOld(doc) < limit) {
                  docIdConsumer.accept(doc);
                  updateCount++;
                }
              }
            }
          } else {
            int doc;
            while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (doc >= limit) {
                break; // no more docs that can be updated for this term
              }
              if (acceptDocs == null || acceptDocs.get(doc)) {
                docIdConsumer.accept(doc);
                updateCount++;
              }
            }
          }
        }
      }
    }

    // now freeze & publish:
    for (DocValuesFieldUpdates update : resolvedUpdates) {
      if (update.any()) {
        update.finish();
        segState.rld.addDVUpdate(update);
      }
    }

    return updateCount;
  }

  // Delete by query
  private long applyQueryDeletes(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

    if (deleteQueries.length == 0) {
      return 0;
    }

    long startNS = System.nanoTime();

    long delCount = 0;
    for (BufferedUpdatesStream.SegmentState segState : segStates) {

      if (delGen < segState.delGen) {
        // segment is newer than this deletes packet
        continue;
      }
      
      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }

      final LeafReaderContext readerContext = segState.reader.getContext();
      for (int i = 0; i < deleteQueries.length; i++) {
        Query query = deleteQueries[i];
        int limit;
        if (delGen == segState.delGen) {
          assert privateSegment != null;
          limit = deleteQueryLimits[i];
        } else {
          limit = Integer.MAX_VALUE;
        }
        final IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        searcher.setQueryCache(null);
        query = searcher.rewrite(query);
        final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1);
        final Scorer scorer = weight.scorer(readerContext);
        if (scorer != null) {
          final DocIdSetIterator it = scorer.iterator();
          if (segState.rld.sortMap != null && limit != Integer.MAX_VALUE) {
            assert privateSegment != null;
            // This segment was sorted on flush; we must apply seg-private deletes carefully in this case:
            int docID;
            while ((docID = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              // The limit is in the pre-sorted doc space:
              if (segState.rld.sortMap.newToOld(docID) < limit) {
                if (segState.rld.delete(docID)) {
                  delCount++;
                }
              }
            }
          } else {
            int docID;
            while ((docID = it.nextDoc()) < limit) {
              if (segState.rld.delete(docID)) {
                delCount++;
              }
            }
          }
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyQueryDeletes took %.2f msec for %d segments and %d queries; %d new deletions",
                                       (System.nanoTime()-startNS)/1000000.,
                                       segStates.length,
                                       deleteQueries.length,
                                       delCount));
    }
    
    return delCount;
  }
  
  private long applyTermDeletes(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

    if (deleteTerms.size() == 0) {
      return 0;
    }

    // We apply segment-private deletes on flush:
    assert privateSegment == null;

    long startNS = System.nanoTime();

    long delCount = 0;

    for (BufferedUpdatesStream.SegmentState segState : segStates) {
      assert segState.delGen != delGen: "segState.delGen=" + segState.delGen + " vs this.gen=" + delGen;
      if (segState.delGen > delGen) {
        // our deletes don't apply to this segment
        continue;
      }
      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }

      FieldTermIterator iter = deleteTerms.iterator();
      BytesRef delTerm;
      TermDocsIterator termDocsIterator = new TermDocsIterator(segState.reader, true);
      while ((delTerm = iter.next()) != null) {
        final DocIdSetIterator iterator = termDocsIterator.nextTerm(iter.field(), delTerm);
        if (iterator != null) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            // NOTE: there is no limit check on the docID
            // when deleting by Term (unlike by Query)
            // because on flush we apply all Term deletes to
            // each segment.  So all Term deleting here is
            // against prior segments:
            if (segState.rld.delete(docID)) {
              delCount++;
            }
          }
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyTermDeletes took %.2f msec for %d segments and %d del terms; %d new deletions",
                                       (System.nanoTime()-startNS)/1000000.,
                                       segStates.length,
                                       deleteTerms.size(),
                                       delCount));
    }

    return delCount;
  }
  
  public void setDelGen(long delGen) {
    assert this.delGen == -1: "delGen was already previously set to " + this.delGen;
    this.delGen = delGen;
    deleteTerms.setDelGen(delGen);
  }
  
  public long delGen() {
    assert delGen != -1;
    return delGen;
  }

  @Override
  public String toString() {
    String s = "delGen=" + delGen;
    if (numTermDeletes != 0) {
      s += " numDeleteTerms=" + numTermDeletes;
      if (numTermDeletes != deleteTerms.size()) {
        s += " (" + deleteTerms.size() + " unique)";
      }
    }
    if (deleteQueries.length != 0) {
      s += " numDeleteQueries=" + deleteQueries.length;
    }
    if (fieldUpdates.size() > 0) {
      s += " fieldUpdates=" + fieldUpdatesCount;
    }
    if (bytesUsed != 0) {
      s += " bytesUsed=" + bytesUsed;
    }
    if (privateSegment != null) {
      s += " privateSegment=" + privateSegment;
    }

    return s;
  }
  
  boolean any() {
    return deleteTerms.size() > 0 || deleteQueries.length > 0 || fieldUpdatesCount > 0 ;
  }

  /**
   * This class helps iterating a term dictionary and consuming all the docs for each terms.
   * It accepts a field, value tuple and returns a {@link DocIdSetIterator} if the field has an entry
   * for the given value. It has an optimized way of iterating the term dictionary if the terms are
   * passed in sorted order and makes sure terms and postings are reused as much as possible.
   */
  static final class TermDocsIterator {
    private final TermsProvider provider;
    private String field;
    private TermsEnum termsEnum;
    private PostingsEnum postingsEnum;
    private final boolean sortedTerms;
    private BytesRef readerTerm;
    private BytesRef lastTerm; // only set with asserts

    @FunctionalInterface
    interface TermsProvider {
      Terms terms(String field) throws IOException;
    }

    TermDocsIterator(Fields fields, boolean sortedTerms) {
      this(fields::terms, sortedTerms);
    }

    TermDocsIterator(LeafReader reader, boolean sortedTerms) {
      this(reader::terms, sortedTerms);
    }

    private TermDocsIterator(TermsProvider provider, boolean sortedTerms) {
      this.sortedTerms = sortedTerms;
      this.provider = provider;
    }

    private void setField(String field) throws IOException {
      if (this.field == null || this.field.equals(field) == false) {
        this.field = field;

        Terms terms = provider.terms(field);
        if (terms != null) {
          termsEnum = terms.iterator();
          if (sortedTerms) {
            assert (lastTerm = null) == null; // need to reset otherwise we fail the assertSorted below since we sort per field
            readerTerm = termsEnum.next();
          }
        } else {
          termsEnum = null;
        }
      }
    }

    DocIdSetIterator nextTerm(String field, BytesRef term) throws IOException {
      setField(field);
      if (termsEnum != null) {
        if (sortedTerms) {
          assert assertSorted(term);
          // in the sorted case we can take advantage of the "seeking forward" property
          // this allows us depending on the term dict impl to reuse data-structures internally
          // which speed up iteration over terms and docs significantly.
          int cmp = term.compareTo(readerTerm);
          if (cmp < 0) {
            return null; // requested term does not exist in this segment
          } else if (cmp == 0) {
            return getDocs();
          } else if (cmp > 0) {
            TermsEnum.SeekStatus status = termsEnum.seekCeil(term);
            switch (status) {
              case FOUND:
                return getDocs();
              case NOT_FOUND:
                readerTerm = termsEnum.term();
                return null;
              case END:
                // no more terms in this segment
                termsEnum = null;
                return null;
              default:
                throw new AssertionError("unknown status");
            }
          }
        } else if (termsEnum.seekExact(term)) {
          return getDocs();
        }
      }
      return null;
    }

    private boolean assertSorted(BytesRef term) {
      assert sortedTerms;
      assert lastTerm == null || term.compareTo(lastTerm) >= 0 : "boom: " + term.utf8ToString() + " last: " + lastTerm.utf8ToString();
      lastTerm = BytesRef.deepCopyOf(term);
      return true;
    }

    private DocIdSetIterator getDocs() throws IOException {
      assert termsEnum != null;
      return postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
    }
  }

}

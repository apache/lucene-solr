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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;

import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
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
  
  // numeric DV update term and their updates
  final byte[] numericDVUpdates;
  
  // binary DV update term and their updates
  final byte[] binaryDVUpdates;

  private final int numericDVUpdateCount;
  private final int binaryDVUpdateCount;

  /** Counts down once all deletes/updates have been applied */
  public final CountDownLatch applied = new CountDownLatch(1);

  /** How many total documents were deleted/updated. */
  public long totalDelCount;
  
  final int bytesUsed;
  final int numTermDeletes;

  private long delGen = -1; // assigned by BufferedUpdatesStream once pushed

  final SegmentCommitInfo privateSegment;  // non-null iff this frozen packet represents 
                                   // a segment private deletes. in that case is should
                                   // only have Queries and doc values updates
  private final InfoStream infoStream;

  public FrozenBufferedUpdates(InfoStream infoStream, BufferedUpdates updates, SegmentCommitInfo privateSegment) throws IOException {
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
    Counter counter = Counter.newCounter();
    // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
    // so that it maps to all fields it affects, sorted by their docUpto, and traverse
    // that Term only once, applying the update to all fields that still need to be
    // updated.
    numericDVUpdates = freezeDVUpdates(updates.numericUpdates, counter::addAndGet);
    numericDVUpdateCount = (int)counter.get();
    counter.addAndGet(-counter.get());
    assert counter.get() == 0;
    // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
    // so that it maps to all fields it affects, sorted by their docUpto, and traverse
    // that Term only once, applying the update to all fields that still need to be
    // updated. 
    binaryDVUpdates = freezeDVUpdates(updates.binaryUpdates, counter::addAndGet);
    binaryDVUpdateCount = (int)counter.get();

    bytesUsed = (int) (deleteTerms.ramBytesUsed() + deleteQueries.length * BYTES_PER_DEL_QUERY 
                       + numericDVUpdates.length + binaryDVUpdates.length);
    
    numTermDeletes = updates.numTermDeletes.get();
    if (infoStream != null && infoStream.isEnabled("BD")) {
      infoStream.message("BD", String.format(Locale.ROOT,
                                             "compressed %d to %d bytes (%.2f%%) for deletes/updates; private segment %s",
                                             updates.bytesUsed.get(), bytesUsed, 100.*bytesUsed/updates.bytesUsed.get(),
                                             privateSegment));
    }
  }

  private static <T extends DocValuesUpdate> byte[] freezeDVUpdates(Map<String,LinkedHashMap<Term, T>> dvUpdates,
                                                                    IntConsumer updateSizeConsumer)
    throws IOException {
    // TODO: we could do better here, e.g. collate the updates by field
    // so if you are updating 2 fields interleaved we don't keep writing the field strings
    try (RAMOutputStream out = new RAMOutputStream()) {
      String lastTermField = null;
      String lastUpdateField = null;
      for (LinkedHashMap<Term, T> updates : dvUpdates.values()) {
        updateSizeConsumer.accept(updates.size());
        for (T update : updates.values()) {
          int code = update.term.bytes().length << 3;

          String termField = update.term.field();
          if (termField.equals(lastTermField) == false) {
            code |= 1;
          }
          String updateField = update.field;
          if (updateField.equals(lastUpdateField) == false) {
            code |= 2;
          }
          if (update.hasValue()) {
            code |= 4;
          }
          out.writeVInt(code);
          out.writeVInt(update.docIDUpto);
          if (termField.equals(lastTermField) == false) {
            out.writeString(termField);
            lastTermField = termField;
          }
          if (updateField.equals(lastUpdateField) == false) {
            out.writeString(updateField);
            lastUpdateField = updateField;
          }
          out.writeBytes(update.term.bytes().bytes, update.term.bytes().offset, update.term.bytes().length);
          if (update.hasValue()) {
            update.writeTo(out);
          }
        }
      }
      byte[] bytes = new byte[(int) out.getFilePointer()];
      out.writeTo(bytes, 0);
      return bytes;
    }
  }

  /** Returns the {@link SegmentCommitInfo} that this packet is supposed to apply its deletes to, or null
   *  if the private segment was already merged away. */
  private List<SegmentCommitInfo> getInfosToApply(IndexWriter writer) {
    assert Thread.holdsLock(writer);
    List<SegmentCommitInfo> infos;
    if (privateSegment != null) {
      if (writer.segmentInfos.indexOf(privateSegment) == -1) {
        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", "private segment already gone; skip processing updates");
        }
        return null;
      } else {
        infos = Collections.singletonList(privateSegment);
      }
    } else {
      infos = writer.segmentInfos.asList();
    }
    return infos;
  }

  /** Translates a frozen packet of delete term/query, or doc values
   *  updates, into their actual docIDs in the index, and applies the change.  This is a heavy
   *  operation and is done concurrently by incoming indexing threads. */
  @SuppressWarnings("try")
  public synchronized void apply(IndexWriter writer) throws IOException {
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
        message += "; " + (iter+1) + " iters due to concurrent merges";
      }
      message += "; " + writer.getPendingUpdatesCount() + " packets remain";
      infoStream.message("BD", message);
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
    final List<BufferedUpdatesStream.SegmentState> segmentStates = Arrays.asList(segStates);
    for (BufferedUpdatesStream.SegmentState segState : segmentStates) {
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
    IOUtils.close(segmentStates);
    if (writer.infoStream.isEnabled("BD")) {
      writer.infoStream.message("BD", "closeSegmentStates: " + totDelCount + " new deleted documents; pool " + writer.getPendingUpdatesCount()+ " packets; bytesUsed=" + writer.getReaderPoolRamBytesUsed());
    }

    return new BufferedUpdatesStream.ApplyDeletesResult(totDelCount > 0, allDeleted);
  }

  private void finishApply(IndexWriter writer, BufferedUpdatesStream.SegmentState[] segStates,
                           boolean success, Set<String> delFiles) throws IOException {
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
  private synchronized long apply(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

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

    if (numericDVUpdates.length == 0 && binaryDVUpdates.length == 0) {
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
      if (numericDVUpdates.length > 0) {
        updateCount += applyDocValuesUpdates(segState, numericDVUpdates, true, delGen, isSegmentPrivateDeletes);
      }

      if (binaryDVUpdates.length > 0) {
        updateCount += applyDocValuesUpdates(segState, binaryDVUpdates, false, delGen, isSegmentPrivateDeletes);
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyDocValuesUpdates %.1f msec for %d segments, %d numeric updates and %d binary updates; %d new updates",
                                       (System.nanoTime()-startNS)/1000000.,
                                       segStates.length,
                                       numericDVUpdateCount,
                                       binaryDVUpdateCount,
                                       updateCount));
    }

    return updateCount;
  }

  private static long applyDocValuesUpdates(BufferedUpdatesStream.SegmentState segState, byte[] updates,
                                            boolean isNumeric, long delGen,
                                            boolean segmentPrivateDeletes) throws IOException {

    TermsEnum termsEnum = null;
    PostingsEnum postingsEnum = null;

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
    Map<String, DocValuesFieldUpdates> holder = new HashMap<>();

    ByteArrayDataInput in = new ByteArrayDataInput(updates);

    String termField = null;
    String updateField = null;
    BytesRef term = new BytesRef();
    term.bytes = new byte[16];
    
    BytesRef scratch = new BytesRef();
    scratch.bytes = new byte[16];
    
    while (in.getPosition() != updates.length) {
      int code = in.readVInt();
      int docIDUpto = in.readVInt();
      term.length = code >> 3;
      
      if ((code & 1) != 0) {
        termField = in.readString();
      }
      if ((code & 2) != 0) {
        updateField = in.readString();
      }
      boolean hasValue = (code & 4) != 0;

      if (term.bytes.length < term.length) {
        term.bytes = ArrayUtil.grow(term.bytes, term.length);
      }
      in.readBytes(term.bytes, 0, term.length);

      final int limit;
      if (delGen == segState.delGen) {
        assert segmentPrivateDeletes;
        limit = docIDUpto;
      } else {
        limit = Integer.MAX_VALUE;
      }
        
      // TODO: we traverse the terms in update order (not term order) so that we
      // apply the updates in the correct order, i.e. if two terms udpate the
      // same document, the last one that came in wins, irrespective of the
      // terms lexical order.
      // we can apply the updates in terms order if we keep an updatesGen (and
      // increment it with every update) and attach it to each NumericUpdate. Note
      // that we cannot rely only on docIDUpto because an app may send two updates
      // which will get same docIDUpto, yet will still need to respect the order
      // those updates arrived.

      // TODO: we could at least *collate* by field?

      // This is the field used to resolve to docIDs, e.g. an "id" field, not the doc values field we are updating!
      if ((code & 1) != 0) {
        Terms terms = segState.reader.terms(termField);
        if (terms != null) {
          termsEnum = terms.iterator();
        } else {
          termsEnum = null;
        }
      }

      final BytesRef binaryValue;
      final long longValue;
      if (hasValue == false) {
        longValue = -1;
        binaryValue = null;
      } else if (isNumeric) {
        longValue = NumericDocValuesUpdate.readFrom(in);
        binaryValue = null;
      } else {
        longValue = -1;
        binaryValue = BinaryDocValuesUpdate.readFrom(in, scratch);
      }

      if (termsEnum == null) {
        // no terms in this segment for this field
        continue;
      }

      if (termsEnum.seekExact(term)) {
        // we don't need term frequencies for this
        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
        DocValuesFieldUpdates dvUpdates = holder.get(updateField);
        if (dvUpdates == null) {
          if (isNumeric) {
            dvUpdates = new NumericDocValuesFieldUpdates(delGen, updateField, segState.reader.maxDoc());
          } else {
            dvUpdates = new BinaryDocValuesFieldUpdates(delGen, updateField, segState.reader.maxDoc());
          }
          holder.put(updateField, dvUpdates);
        }
        final IntConsumer docIdConsumer;
        final DocValuesFieldUpdates update = dvUpdates;
        if (hasValue == false) {
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
          while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
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
          while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
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

    // now freeze & publish:
    for (DocValuesFieldUpdates update : holder.values()) {
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
        final Weight weight = searcher.createWeight(query, false, 1);
        final Scorer scorer = weight.scorer(readerContext);
        if (scorer != null) {
          final DocIdSetIterator it = scorer.iterator();

          int docID;
          while ((docID = it.nextDoc()) < limit)  {
            if (segState.rld.delete(docID)) {
              delCount++;
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
      String field = null;
      TermsEnum termsEnum = null;
      BytesRef readerTerm = null;
      PostingsEnum postingsEnum = null;
      while ((delTerm = iter.next()) != null) {

        if (iter.field() != field) {
          // field changed
          field = iter.field();
          Terms terms = segState.reader.terms(field);
          if (terms != null) {
            termsEnum = terms.iterator();
            readerTerm = termsEnum.next();
          } else {
            termsEnum = null;
          }
        }

        if (termsEnum != null) {
          int cmp = delTerm.compareTo(readerTerm);
          if (cmp < 0) {
            // TODO: can we advance across del terms here?
            // move to next del term
            continue;
          } else if (cmp == 0) {
            // fall through
          } else if (cmp > 0) {
            TermsEnum.SeekStatus status = termsEnum.seekCeil(delTerm);
            if (status == TermsEnum.SeekStatus.FOUND) {
              // fall through
            } else if (status == TermsEnum.SeekStatus.NOT_FOUND) {
              readerTerm = termsEnum.term();
              continue;
            } else {
              // TODO: can we advance to next field in deleted terms?
              // no more terms in this segment
              termsEnum = null;
              continue;
            }
          }

          // we don't need term frequencies for this
          postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);

          assert postingsEnum != null;

          int docID;
          while ((docID = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

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
    if (numericDVUpdates.length > 0) {
      s += " numNumericDVUpdates=" + numericDVUpdateCount;
    }
    if (binaryDVUpdates.length > 0) {
      s += " numBinaryDVUpdates=" + binaryDVUpdateCount;
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
    return deleteTerms.size() > 0 || deleteQueries.length > 0 || numericDVUpdates.length > 0 || binaryDVUpdates.length > 0;
  }
}

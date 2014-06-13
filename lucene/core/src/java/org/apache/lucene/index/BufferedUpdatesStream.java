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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;

/* Tracks the stream of {@link BufferedDeletes}.
 * When DocumentsWriterPerThread flushes, its buffered
 * deletes and updates are appended to this stream.  We later
 * apply them (resolve them to the actual
 * docIDs, per segment) when a merge is started
 * (only to the to-be-merged segments).  We
 * also apply to all segments when NRT reader is pulled,
 * commit/close is called, or when too many deletes or  updates are
 * buffered and must be flushed (by RAM usage or by count).
 *
 * Each packet is assigned a generation, and each flushed or
 * merged segment is also assigned a generation, so we can
 * track which BufferedDeletes packets to apply to any given
 * segment. */

class BufferedUpdatesStream implements Accountable {

  // TODO: maybe linked list?
  private final List<FrozenBufferedUpdates> updates = new ArrayList<>();

  // Starts at 1 so that SegmentInfos that have never had
  // deletes applied (whose bufferedDelGen defaults to 0)
  // will be correct:
  private long nextGen = 1;

  // used only by assert
  private Term lastDeleteTerm;

  private final InfoStream infoStream;
  private final AtomicLong bytesUsed = new AtomicLong();
  private final AtomicInteger numTerms = new AtomicInteger();

  public BufferedUpdatesStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  // Appends a new packet of buffered deletes to the stream,
  // setting its generation:
  public synchronized long push(FrozenBufferedUpdates packet) {
    /*
     * The insert operation must be atomic. If we let threads increment the gen
     * and push the packet afterwards we risk that packets are out of order.
     * With DWPT this is possible if two or more flushes are racing for pushing
     * updates. If the pushed packets get our of order would loose documents
     * since deletes are applied to the wrong segments.
     */
    packet.setDelGen(nextGen++);
    assert packet.any();
    assert checkDeleteStats();
    assert packet.delGen() < nextGen;
    assert updates.isEmpty() || updates.get(updates.size()-1).delGen() < packet.delGen() : "Delete packets must be in order";
    updates.add(packet);
    numTerms.addAndGet(packet.numTermDeletes);
    bytesUsed.addAndGet(packet.bytesUsed);
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "push deletes " + packet + " delGen=" + packet.delGen() + " packetCount=" + updates.size() + " totBytesUsed=" + bytesUsed.get());
    }
    assert checkDeleteStats();
    return packet.delGen();
  }

  public synchronized void clear() {
    updates.clear();
    nextGen = 1;
    numTerms.set(0);
    bytesUsed.set(0);
  }

  public boolean any() {
    return bytesUsed.get() != 0;
  }

  public int numTerms() {
    return numTerms.get();
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get();
  }

  public static class ApplyDeletesResult {
    
    // True if any actual deletes took place:
    public final boolean anyDeletes;

    // Current gen, for the merged segment:
    public final long gen;

    // If non-null, contains segments that are 100% deleted
    public final List<SegmentCommitInfo> allDeleted;

    ApplyDeletesResult(boolean anyDeletes, long gen, List<SegmentCommitInfo> allDeleted) {
      this.anyDeletes = anyDeletes;
      this.gen = gen;
      this.allDeleted = allDeleted;
    }
  }

  // Sorts SegmentInfos from smallest to biggest bufferedDelGen:
  private static final Comparator<SegmentCommitInfo> sortSegInfoByDelGen = new Comparator<SegmentCommitInfo>() {
    @Override
    public int compare(SegmentCommitInfo si1, SegmentCommitInfo si2) {
      final long cmp = si1.getBufferedDeletesGen() - si2.getBufferedDeletesGen();
      if (cmp > 0) {
        return 1;
      } else if (cmp < 0) {
        return -1;
      } else {
        return 0;
      }
    }
  };
  
  /** Resolves the buffered deleted Term/Query/docIDs, into
   *  actual deleted docIDs in the liveDocs MutableBits for
   *  each SegmentReader. */
  public synchronized ApplyDeletesResult applyDeletesAndUpdates(IndexWriter.ReaderPool readerPool, List<SegmentCommitInfo> infos) throws IOException {
    final long t0 = System.currentTimeMillis();

    if (infos.size() == 0) {
      return new ApplyDeletesResult(false, nextGen++, null);
    }

    assert checkDeleteStats();

    if (!any()) {
      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", "applyDeletes: no deletes; skipping");
      }
      return new ApplyDeletesResult(false, nextGen++, null);
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "applyDeletes: infos=" + infos + " packetCount=" + updates.size());
    }

    final long gen = nextGen++;

    List<SegmentCommitInfo> infos2 = new ArrayList<>();
    infos2.addAll(infos);
    Collections.sort(infos2, sortSegInfoByDelGen);

    CoalescedUpdates coalescedUpdates = null;
    boolean anyNewDeletes = false;

    int infosIDX = infos2.size()-1;
    int delIDX = updates.size()-1;

    List<SegmentCommitInfo> allDeleted = null;

    while (infosIDX >= 0) {
      //System.out.println("BD: cycle delIDX=" + delIDX + " infoIDX=" + infosIDX);

      final FrozenBufferedUpdates packet = delIDX >= 0 ? updates.get(delIDX) : null;
      final SegmentCommitInfo info = infos2.get(infosIDX);
      final long segGen = info.getBufferedDeletesGen();

      if (packet != null && segGen < packet.delGen()) {
//        System.out.println("  coalesce");
        if (coalescedUpdates == null) {
          coalescedUpdates = new CoalescedUpdates();
        }
        if (!packet.isSegmentPrivate) {
          /*
           * Only coalesce if we are NOT on a segment private del packet: the segment private del packet
           * must only applied to segments with the same delGen.  Yet, if a segment is already deleted
           * from the SI since it had no more documents remaining after some del packets younger than
           * its segPrivate packet (higher delGen) have been applied, the segPrivate packet has not been
           * removed.
           */
          coalescedUpdates.update(packet);
        }

        delIDX--;
      } else if (packet != null && segGen == packet.delGen()) {
        assert packet.isSegmentPrivate : "Packet and Segments deletegen can only match on a segment private del packet gen=" + segGen;
        //System.out.println("  eq");

        // Lock order: IW -> BD -> RP
        assert readerPool.infoIsLive(info);
        final ReadersAndUpdates rld = readerPool.get(info, true);
        final SegmentReader reader = rld.getReader(IOContext.READ);
        int delCount = 0;
        final boolean segAllDeletes;
        try {
          final DocValuesFieldUpdates.Container dvUpdates = new DocValuesFieldUpdates.Container();
          if (coalescedUpdates != null) {
            //System.out.println("    del coalesced");
            delCount += applyTermDeletes(coalescedUpdates.termsIterable(), rld, reader);
            delCount += applyQueryDeletes(coalescedUpdates.queriesIterable(), rld, reader);
            applyDocValuesUpdates(coalescedUpdates.numericDVUpdates, rld, reader, dvUpdates);
            applyDocValuesUpdates(coalescedUpdates.binaryDVUpdates, rld, reader, dvUpdates);
          }
          //System.out.println("    del exact");
          // Don't delete by Term here; DocumentsWriterPerThread
          // already did that on flush:
          delCount += applyQueryDeletes(packet.queriesIterable(), rld, reader);
          applyDocValuesUpdates(Arrays.asList(packet.numericDVUpdates), rld, reader, dvUpdates);
          applyDocValuesUpdates(Arrays.asList(packet.binaryDVUpdates), rld, reader, dvUpdates);
          if (dvUpdates.any()) {
            rld.writeFieldUpdates(info.info.dir, dvUpdates);
          }
          final int fullDelCount = rld.info.getDelCount() + rld.getPendingDeleteCount();
          assert fullDelCount <= rld.info.info.getDocCount();
          segAllDeletes = fullDelCount == rld.info.info.getDocCount();
        } finally {
          rld.release(reader);
          readerPool.release(rld);
        }
        anyNewDeletes |= delCount > 0;

        if (segAllDeletes) {
          if (allDeleted == null) {
            allDeleted = new ArrayList<>();
          }
          allDeleted.add(info);
        }

        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", "seg=" + info + " segGen=" + segGen + " segDeletes=[" + packet + "]; coalesced deletes=[" + (coalescedUpdates == null ? "null" : coalescedUpdates) + "] newDelCount=" + delCount + (segAllDeletes ? " 100% deleted" : ""));
        }

        if (coalescedUpdates == null) {
          coalescedUpdates = new CoalescedUpdates();
        }
        
        /*
         * Since we are on a segment private del packet we must not
         * update the coalescedDeletes here! We can simply advance to the 
         * next packet and seginfo.
         */
        delIDX--;
        infosIDX--;
        info.setBufferedDeletesGen(gen);

      } else {
        //System.out.println("  gt");

        if (coalescedUpdates != null) {
          // Lock order: IW -> BD -> RP
          assert readerPool.infoIsLive(info);
          final ReadersAndUpdates rld = readerPool.get(info, true);
          final SegmentReader reader = rld.getReader(IOContext.READ);
          int delCount = 0;
          final boolean segAllDeletes;
          try {
            delCount += applyTermDeletes(coalescedUpdates.termsIterable(), rld, reader);
            delCount += applyQueryDeletes(coalescedUpdates.queriesIterable(), rld, reader);
            DocValuesFieldUpdates.Container dvUpdates = new DocValuesFieldUpdates.Container();
            applyDocValuesUpdates(coalescedUpdates.numericDVUpdates, rld, reader, dvUpdates);
            applyDocValuesUpdates(coalescedUpdates.binaryDVUpdates, rld, reader, dvUpdates);
            if (dvUpdates.any()) {
              rld.writeFieldUpdates(info.info.dir, dvUpdates);
            }
            final int fullDelCount = rld.info.getDelCount() + rld.getPendingDeleteCount();
            assert fullDelCount <= rld.info.info.getDocCount();
            segAllDeletes = fullDelCount == rld.info.info.getDocCount();
          } finally {
            rld.release(reader);
            readerPool.release(rld);
          }
          anyNewDeletes |= delCount > 0;

          if (segAllDeletes) {
            if (allDeleted == null) {
              allDeleted = new ArrayList<>();
            }
            allDeleted.add(info);
          }

          if (infoStream.isEnabled("BD")) {
            infoStream.message("BD", "seg=" + info + " segGen=" + segGen + " coalesced deletes=[" + coalescedUpdates + "] newDelCount=" + delCount + (segAllDeletes ? " 100% deleted" : ""));
          }
        }
        info.setBufferedDeletesGen(gen);

        infosIDX--;
      }
    }

    assert checkDeleteStats();
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "applyDeletes took " + (System.currentTimeMillis()-t0) + " msec");
    }
    // assert infos != segmentInfos || !any() : "infos=" + infos + " segmentInfos=" + segmentInfos + " any=" + any;

    return new ApplyDeletesResult(anyNewDeletes, gen, allDeleted);
  }

  synchronized long getNextGen() {
    return nextGen++;
  }

  // Lock order IW -> BD
  /* Removes any BufferedDeletes that we no longer need to
   * store because all segments in the index have had the
   * deletes applied. */
  public synchronized void prune(SegmentInfos segmentInfos) {
    assert checkDeleteStats();
    long minGen = Long.MAX_VALUE;
    for(SegmentCommitInfo info : segmentInfos) {
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
    }

    if (infoStream.isEnabled("BD")) {
      Directory dir;
      if (segmentInfos.size() > 0) {
        dir = segmentInfos.info(0).info.dir;
      } else {
        dir = null;
      }
      infoStream.message("BD", "prune sis=" + segmentInfos.toString(dir) + " minGen=" + minGen + " packetCount=" + updates.size());
    }
    final int limit = updates.size();
    for(int delIDX=0;delIDX<limit;delIDX++) {
      if (updates.get(delIDX).delGen() >= minGen) {
        prune(delIDX);
        assert checkDeleteStats();
        return;
      }
    }

    // All deletes pruned
    prune(limit);
    assert !any();
    assert checkDeleteStats();
  }

  private synchronized void prune(int count) {
    if (count > 0) {
      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", "pruneDeletes: prune " + count + " packets; " + (updates.size() - count) + " packets remain");
      }
      for(int delIDX=0;delIDX<count;delIDX++) {
        final FrozenBufferedUpdates packet = updates.get(delIDX);
        numTerms.addAndGet(-packet.numTermDeletes);
        assert numTerms.get() >= 0;
        bytesUsed.addAndGet(-packet.bytesUsed);
        assert bytesUsed.get() >= 0;
      }
      updates.subList(0, count).clear();
    }
  }

  // Delete by Term
  private synchronized long applyTermDeletes(Iterable<Term> termsIter, ReadersAndUpdates rld, SegmentReader reader) throws IOException {
    long delCount = 0;
    Fields fields = reader.fields();
    if (fields == null) {
      // This reader has no postings
      return 0;
    }

    TermsEnum termsEnum = null;

    String currentField = null;
    DocsEnum docs = null;

    assert checkDeleteTerm(null);

    boolean any = false;

    //System.out.println(Thread.currentThread().getName() + " del terms reader=" + reader);
    for (Term term : termsIter) {
      // Since we visit terms sorted, we gain performance
      // by re-using the same TermsEnum and seeking only
      // forwards
      if (!term.field().equals(currentField)) {
        assert currentField == null || currentField.compareTo(term.field()) < 0;
        currentField = term.field();
        Terms terms = fields.terms(currentField);
        if (terms != null) {
          termsEnum = terms.iterator(termsEnum);
        } else {
          termsEnum = null;
        }
      }

      if (termsEnum == null) {
        continue;
      }
      assert checkDeleteTerm(term);

      // System.out.println("  term=" + term);

      if (termsEnum.seekExact(term.bytes())) {
        // we don't need term frequencies for this
        DocsEnum docsEnum = termsEnum.docs(rld.getLiveDocs(), docs, DocsEnum.FLAG_NONE);
        //System.out.println("BDS: got docsEnum=" + docsEnum);

        if (docsEnum != null) {
          while (true) {
            final int docID = docsEnum.nextDoc();
            //System.out.println(Thread.currentThread().getName() + " del term=" + term + " doc=" + docID);
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }   
            if (!any) {
              rld.initWritableLiveDocs();
              any = true;
            }
            // NOTE: there is no limit check on the docID
            // when deleting by Term (unlike by Query)
            // because on flush we apply all Term deletes to
            // each segment.  So all Term deleting here is
            // against prior segments:
            if (rld.delete(docID)) {
              delCount++;
            }
          }
        }
      }
    }

    return delCount;
  }

  // DocValues updates
  private synchronized void applyDocValuesUpdates(Iterable<? extends DocValuesUpdate> updates, 
      ReadersAndUpdates rld, SegmentReader reader, DocValuesFieldUpdates.Container dvUpdatesContainer) throws IOException {
    Fields fields = reader.fields();
    if (fields == null) {
      // This reader has no postings
      return;
    }

    // TODO: we can process the updates per DV field, from last to first so that
    // if multiple terms affect same document for the same field, we add an update
    // only once (that of the last term). To do that, we can keep a bitset which
    // marks which documents have already been updated. So e.g. if term T1
    // updates doc 7, and then we process term T2 and it updates doc 7 as well,
    // we don't apply the update since we know T1 came last and therefore wins
    // the update.
    // We can also use that bitset as 'liveDocs' to pass to TermEnum.docs(), so
    // that these documents aren't even returned.
    
    String currentField = null;
    TermsEnum termsEnum = null;
    DocsEnum docs = null;
    
    //System.out.println(Thread.currentThread().getName() + " numericDVUpdate reader=" + reader);
    for (DocValuesUpdate update : updates) {
      Term term = update.term;
      int limit = update.docIDUpto;
      
      // TODO: we traverse the terms in update order (not term order) so that we
      // apply the updates in the correct order, i.e. if two terms udpate the
      // same document, the last one that came in wins, irrespective of the
      // terms lexical order.
      // we can apply the updates in terms order if we keep an updatesGen (and
      // increment it with every update) and attach it to each NumericUpdate. Note
      // that we cannot rely only on docIDUpto because an app may send two updates
      // which will get same docIDUpto, yet will still need to respect the order
      // those updates arrived.
      
      if (!term.field().equals(currentField)) {
        // if we change the code to process updates in terms order, enable this assert
//        assert currentField == null || currentField.compareTo(term.field()) < 0;
        currentField = term.field();
        Terms terms = fields.terms(currentField);
        if (terms != null) {
          termsEnum = terms.iterator(termsEnum);
        } else {
          termsEnum = null;
          continue; // no terms in that field
        }
      }

      if (termsEnum == null) {
        continue;
      }
      // System.out.println("  term=" + term);

      if (termsEnum.seekExact(term.bytes())) {
        // we don't need term frequencies for this
        DocsEnum docsEnum = termsEnum.docs(rld.getLiveDocs(), docs, DocsEnum.FLAG_NONE);
      
        //System.out.println("BDS: got docsEnum=" + docsEnum);

        DocValuesFieldUpdates dvUpdates = dvUpdatesContainer.getUpdates(update.field, update.type);
        if (dvUpdates == null) {
          dvUpdates = dvUpdatesContainer.newUpdates(update.field, update.type, reader.maxDoc());
        }
        int doc;
        while ((doc = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          //System.out.println(Thread.currentThread().getName() + " numericDVUpdate term=" + term + " doc=" + docID);
          if (doc >= limit) {
            break; // no more docs that can be updated for this term
          }
          dvUpdates.add(doc, update.value);
        }
      }
    }
  }
  
  public static class QueryAndLimit {
    public final Query query;
    public final int limit;
    public QueryAndLimit(Query query, int limit) {
      this.query = query;
      this.limit = limit;
    }
  }

  // Delete by query
  private static long applyQueryDeletes(Iterable<QueryAndLimit> queriesIter, ReadersAndUpdates rld, final SegmentReader reader) throws IOException {
    long delCount = 0;
    final AtomicReaderContext readerContext = reader.getContext();
    boolean any = false;
    for (QueryAndLimit ent : queriesIter) {
      Query query = ent.query;
      int limit = ent.limit;
      final DocIdSet docs = new QueryWrapperFilter(query).getDocIdSet(readerContext, reader.getLiveDocs());
      if (docs != null) {
        final DocIdSetIterator it = docs.iterator();
        if (it != null) {
          while(true)  {
            int doc = it.nextDoc();
            if (doc >= limit) {
              break;
            }

            if (!any) {
              rld.initWritableLiveDocs();
              any = true;
            }

            if (rld.delete(doc)) {
              delCount++;
            }
          }
        }
      }
    }

    return delCount;
  }

  // used only by assert
  private boolean checkDeleteTerm(Term term) {
    if (term != null) {
      assert lastDeleteTerm == null || term.compareTo(lastDeleteTerm) > 0: "lastTerm=" + lastDeleteTerm + " vs term=" + term;
    }
    // TODO: we re-use term now in our merged iterable, but we shouldn't clone, instead copy for this assert
    lastDeleteTerm = term == null ? null : new Term(term.field(), BytesRef.deepCopyOf(term.bytes));
    return true;
  }

  // only for assert
  private boolean checkDeleteStats() {
    int numTerms2 = 0;
    long bytesUsed2 = 0;
    for(FrozenBufferedUpdates packet : updates) {
      numTerms2 += packet.numTermDeletes;
      bytesUsed2 += packet.bytesUsed;
    }
    assert numTerms2 == numTerms.get(): "numTerms2=" + numTerms2 + " vs " + numTerms.get();
    assert bytesUsed2 == bytesUsed.get(): "bytesUsed2=" + bytesUsed2 + " vs " + bytesUsed;
    return true;
  }
}

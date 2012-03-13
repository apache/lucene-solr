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
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;

/* Tracks the stream of {@link BufferedDeletes}.
 * When DocumentsWriterPerThread flushes, its buffered
 * deletes are appended to this stream.  We later
 * apply these deletes (resolve them to the actual
 * docIDs, per segment) when a merge is started
 * (only to the to-be-merged segments).  We
 * also apply to all segments when NRT reader is pulled,
 * commit/close is called, or when too many deletes are
 * buffered and must be flushed (by RAM usage or by count).
 *
 * Each packet is assigned a generation, and each flushed or
 * merged segment is also assigned a generation, so we can
 * track which BufferedDeletes packets to apply to any given
 * segment. */

class BufferedDeletesStream {

  // TODO: maybe linked list?
  private final List<FrozenBufferedDeletes> deletes = new ArrayList<FrozenBufferedDeletes>();

  // Starts at 1 so that SegmentInfos that have never had
  // deletes applied (whose bufferedDelGen defaults to 0)
  // will be correct:
  private long nextGen = 1;

  // used only by assert
  private Term lastDeleteTerm;

  private final InfoStream infoStream;
  private final AtomicLong bytesUsed = new AtomicLong();
  private final AtomicInteger numTerms = new AtomicInteger();

  public BufferedDeletesStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  // Appends a new packet of buffered deletes to the stream,
  // setting its generation:
  public synchronized long push(FrozenBufferedDeletes packet) {
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
    assert deletes.isEmpty() || deletes.get(deletes.size()-1).delGen() < packet.delGen() : "Delete packets must be in order";
    deletes.add(packet);
    numTerms.addAndGet(packet.numTermDeletes);
    bytesUsed.addAndGet(packet.bytesUsed);
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "push deletes " + packet + " delGen=" + packet.delGen() + " packetCount=" + deletes.size() + " totBytesUsed=" + bytesUsed.get());
    }
    assert checkDeleteStats();
    return packet.delGen();
  }

  public synchronized void clear() {
    deletes.clear();
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

  public long bytesUsed() {
    return bytesUsed.get();
  }

  public static class ApplyDeletesResult {
    // True if any actual deletes took place:
    public final boolean anyDeletes;

    // Current gen, for the merged segment:
    public final long gen;

    // If non-null, contains segments that are 100% deleted
    public final List<SegmentInfo> allDeleted;

    ApplyDeletesResult(boolean anyDeletes, long gen, List<SegmentInfo> allDeleted) {
      this.anyDeletes = anyDeletes;
      this.gen = gen;
      this.allDeleted = allDeleted;
    }
  }

  // Sorts SegmentInfos from smallest to biggest bufferedDelGen:
  private static final Comparator<SegmentInfo> sortSegInfoByDelGen = new Comparator<SegmentInfo>() {
    @Override
    public int compare(SegmentInfo si1, SegmentInfo si2) {
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
  public synchronized ApplyDeletesResult applyDeletes(IndexWriter.ReaderPool readerPool, List<SegmentInfo> infos) throws IOException {
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
      infoStream.message("BD", "applyDeletes: infos=" + infos + " packetCount=" + deletes.size());
    }

    List<SegmentInfo> infos2 = new ArrayList<SegmentInfo>();
    infos2.addAll(infos);
    Collections.sort(infos2, sortSegInfoByDelGen);

    CoalescedDeletes coalescedDeletes = null;
    boolean anyNewDeletes = false;

    int infosIDX = infos2.size()-1;
    int delIDX = deletes.size()-1;

    List<SegmentInfo> allDeleted = null;

    while (infosIDX >= 0) {
      //System.out.println("BD: cycle delIDX=" + delIDX + " infoIDX=" + infosIDX);

      final FrozenBufferedDeletes packet = delIDX >= 0 ? deletes.get(delIDX) : null;
      final SegmentInfo info = infos2.get(infosIDX);
      final long segGen = info.getBufferedDeletesGen();

      if (packet != null && segGen < packet.delGen()) {
        //System.out.println("  coalesce");
        if (coalescedDeletes == null) {
          coalescedDeletes = new CoalescedDeletes();
        }
        if (!packet.isSegmentPrivate) {
          /*
           * Only coalesce if we are NOT on a segment private del packet: the segment private del packet
           * must only applied to segments with the same delGen.  Yet, if a segment is already deleted
           * from the SI since it had no more documents remaining after some del packets younger than
           * its segPrivate packet (higher delGen) have been applied, the segPrivate packet has not been
           * removed.
           */
          coalescedDeletes.update(packet);
        }

        delIDX--;
      } else if (packet != null && segGen == packet.delGen()) {
        assert packet.isSegmentPrivate : "Packet and Segments deletegen can only match on a segment private del packet gen=" + segGen;
        //System.out.println("  eq");

        // Lock order: IW -> BD -> RP
        assert readerPool.infoIsLive(info);
        final ReadersAndLiveDocs rld = readerPool.get(info, true);
        final SegmentReader reader = rld.getReader(IOContext.READ);
        int delCount = 0;
        final boolean segAllDeletes;
        try {
          if (coalescedDeletes != null) {
            //System.out.println("    del coalesced");
            delCount += applyTermDeletes(coalescedDeletes.termsIterable(), rld, reader);
            delCount += applyQueryDeletes(coalescedDeletes.queriesIterable(), rld, reader);
          }
          //System.out.println("    del exact");
          // Don't delete by Term here; DocumentsWriterPerThread
          // already did that on flush:
          delCount += applyQueryDeletes(packet.queriesIterable(), rld, reader);
          final int fullDelCount = rld.info.getDelCount() + rld.getPendingDeleteCount();
          assert fullDelCount <= rld.info.docCount;
          segAllDeletes = fullDelCount == rld.info.docCount;
        } finally {
          rld.release(reader);
          readerPool.release(rld);
        }
        anyNewDeletes |= delCount > 0;

        if (segAllDeletes) {
          if (allDeleted == null) {
            allDeleted = new ArrayList<SegmentInfo>();
          }
          allDeleted.add(info);
        }

        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", "seg=" + info + " segGen=" + segGen + " segDeletes=[" + packet + "]; coalesced deletes=[" + (coalescedDeletes == null ? "null" : coalescedDeletes) + "] newDelCount=" + delCount + (segAllDeletes ? " 100% deleted" : ""));
        }

        if (coalescedDeletes == null) {
          coalescedDeletes = new CoalescedDeletes();
        }
        
        /*
         * Since we are on a segment private del packet we must not
         * update the coalescedDeletes here! We can simply advance to the 
         * next packet and seginfo.
         */
        delIDX--;
        infosIDX--;
        info.setBufferedDeletesGen(nextGen);

      } else {
        //System.out.println("  gt");

        if (coalescedDeletes != null) {
          // Lock order: IW -> BD -> RP
          assert readerPool.infoIsLive(info);
          final ReadersAndLiveDocs rld = readerPool.get(info, true);
          final SegmentReader reader = rld.getReader(IOContext.READ);
          int delCount = 0;
          final boolean segAllDeletes;
          try {
            delCount += applyTermDeletes(coalescedDeletes.termsIterable(), rld, reader);
            delCount += applyQueryDeletes(coalescedDeletes.queriesIterable(), rld, reader);
            final int fullDelCount = rld.info.getDelCount() + rld.getPendingDeleteCount();
            assert fullDelCount <= rld.info.docCount;
            segAllDeletes = fullDelCount == rld.info.docCount;
          } finally {   
            rld.release(reader);
            readerPool.release(rld);
          }
          anyNewDeletes |= delCount > 0;

          if (segAllDeletes) {
            if (allDeleted == null) {
              allDeleted = new ArrayList<SegmentInfo>();
            }
            allDeleted.add(info);
          }

          if (infoStream.isEnabled("BD")) {
            infoStream.message("BD", "seg=" + info + " segGen=" + segGen + " coalesced deletes=[" + (coalescedDeletes == null ? "null" : coalescedDeletes) + "] newDelCount=" + delCount + (segAllDeletes ? " 100% deleted" : ""));
          }
        }
        info.setBufferedDeletesGen(nextGen);

        infosIDX--;
      }
    }

    assert checkDeleteStats();
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "applyDeletes took " + (System.currentTimeMillis()-t0) + " msec");
    }
    // assert infos != segmentInfos || !any() : "infos=" + infos + " segmentInfos=" + segmentInfos + " any=" + any;

    return new ApplyDeletesResult(anyNewDeletes, nextGen++, allDeleted);
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
    for(SegmentInfo info : segmentInfos) {
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "prune sis=" + segmentInfos + " minGen=" + minGen + " packetCount=" + deletes.size());
    }
    final int limit = deletes.size();
    for(int delIDX=0;delIDX<limit;delIDX++) {
      if (deletes.get(delIDX).delGen() >= minGen) {
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
        infoStream.message("BD", "pruneDeletes: prune " + count + " packets; " + (deletes.size() - count) + " packets remain");
      }
      for(int delIDX=0;delIDX<count;delIDX++) {
        final FrozenBufferedDeletes packet = deletes.get(delIDX);
        numTerms.addAndGet(-packet.numTermDeletes);
        assert numTerms.get() >= 0;
        bytesUsed.addAndGet(-packet.bytesUsed);
        assert bytesUsed.get() >= 0;
      }
      deletes.subList(0, count).clear();
    }
  }

  // Delete by Term
  private synchronized long applyTermDeletes(Iterable<Term> termsIter, ReadersAndLiveDocs rld, SegmentReader reader) throws IOException {
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
          termsEnum = terms.iterator(null);
        } else {
          termsEnum = null;
        }
      }

      if (termsEnum == null) {
        continue;
      }
      assert checkDeleteTerm(term);

      // System.out.println("  term=" + term);

      if (termsEnum.seekExact(term.bytes(), false)) {
        DocsEnum docsEnum = termsEnum.docs(rld.getLiveDocs(), docs, false);
        //System.out.println("BDS: got docsEnum=" + docsEnum);

        if (docsEnum != null) {
          while (true) {
            final int docID = docsEnum.nextDoc();
            //System.out.println(Thread.currentThread().getName() + " del term=" + term + " doc=" + docID);
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }   
            // NOTE: there is no limit check on the docID
            // when deleting by Term (unlike by Query)
            // because on flush we apply all Term deletes to
            // each segment.  So all Term deleting here is
            // against prior segments:
            if (!any) {
              rld.initWritableLiveDocs();
              any = true;
            }
            if (rld.delete(docID)) {
              delCount++;
            }
          }
        }
      }
    }

    return delCount;
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
  private static long applyQueryDeletes(Iterable<QueryAndLimit> queriesIter, ReadersAndLiveDocs rld, final SegmentReader reader) throws IOException {
    long delCount = 0;
    final AtomicReaderContext readerContext = reader.getTopReaderContext();
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
    for(FrozenBufferedDeletes packet : deletes) {
      numTerms2 += packet.numTermDeletes;
      bytesUsed2 += packet.bytesUsed;
    }
    assert numTerms2 == numTerms.get(): "numTerms2=" + numTerms2 + " vs " + numTerms.get();
    assert bytesUsed2 == bytesUsed.get(): "bytesUsed2=" + bytesUsed2 + " vs " + bytesUsed;
    return true;
  }
}

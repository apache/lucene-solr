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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/** Tracks the stream of {@link FrozenBufferedUpdates}.
 * When DocumentsWriterPerThread flushes, its buffered
 * deletes and updates are appended to this stream and immediately
 * resolved (to actual docIDs, per segment) using the indexing
 * thread that triggered the flush for concurrency.  When a
 * merge kicks off, we sync to ensure all resolving packets
 * complete.  We also apply to all segments when NRT reader is pulled,
 * commit/close is called, or when too many deletes or updates are
 * buffered and must be flushed (by RAM usage or by count).
 *
 * Each packet is assigned a generation, and each flushed or
 * merged segment is also assigned a generation, so we can
 * track which BufferedDeletes packets to apply to any given
 * segment. */

class BufferedUpdatesStream implements Accountable {

  private final Set<FrozenBufferedUpdates> updates = new HashSet<>();

  // Starts at 1 so that SegmentInfos that have never had
  // deletes applied (whose bufferedDelGen defaults to 0)
  // will be correct:
  private long nextGen = 1;

  private final FinishedSegments finishedSegments;
  private final InfoStream infoStream;
  private final AtomicLong bytesUsed = new AtomicLong();
  private final AtomicInteger numTerms = new AtomicInteger();
  private final IndexWriter writer;
  private boolean closed;

  public BufferedUpdatesStream(IndexWriter writer) {
    this.writer = writer;
    this.infoStream = writer.infoStream;
    this.finishedSegments = new FinishedSegments(infoStream);
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

    updates.add(packet);
    numTerms.addAndGet(packet.numTermDeletes);
    bytesUsed.addAndGet(packet.bytesUsed);
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", String.format(Locale.ROOT, "push new packet (%s), packetCount=%d, bytesUsed=%.3f MB", packet, updates.size(), bytesUsed.get()/1024./1024.));
    }
    assert checkDeleteStats();

    return packet.delGen();
  }

  public synchronized int getPendingUpdatesCount() {
    return updates.size();
  }

  /** Only used by IW.rollback */
  public synchronized void clear() {
    updates.clear();
    nextGen = 1;
    finishedSegments.clear();
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

  private synchronized void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("already closed");
    }
  }

  public static class ApplyDeletesResult {
    
    // True if any actual deletes took place:
    public final boolean anyDeletes;

    // If non-null, contains segments that are 100% deleted
    public final List<SegmentCommitInfo> allDeleted;

    ApplyDeletesResult(boolean anyDeletes, List<SegmentCommitInfo> allDeleted) {
      this.anyDeletes = anyDeletes;
      this.allDeleted = allDeleted;
    }
  }

  /** Waits for all in-flight packets, which are already being resolved concurrently
   *  by indexing threads, to finish.  Returns true if there were any 
   *  new deletes or updates.  This is called for refresh, commit. */
  public void waitApplyAll() throws IOException {

    assert Thread.holdsLock(writer) == false;
    
    final long t0 = System.nanoTime();

    Set<FrozenBufferedUpdates> waitFor;
    synchronized (this) {
      waitFor = new HashSet<>(updates);
    }

    waitApply(waitFor);
  }

  /** Returns true if this delGen is still running. */
  public boolean stillRunning(long delGen) {
    return finishedSegments.stillRunning(delGen);
  }

  public void finishedSegment(long delGen) {
    finishedSegments.finishedSegment(delGen);
  }
  
  /** Called by indexing threads once they are fully done resolving all deletes for the provided
   *  delGen.  We track the completed delGens and record the maximum delGen for which all prior
   *  delGens, inclusive, are completed, so that it's safe for doc values updates to apply and write. */

  public synchronized void finished(FrozenBufferedUpdates packet) {
    // TODO: would be a bit more memory efficient to track this per-segment, so when each segment writes it writes all packets finished for
    // it, rather than only recording here, across all segments.  But, more complex code, and more CPU, and maybe not so much impact in
    // practice?
    assert packet.applied.getCount() == 1: "packet=" + packet;

    packet.applied.countDown();

    updates.remove(packet);
    numTerms.addAndGet(-packet.numTermDeletes);
    assert numTerms.get() >= 0: "numTerms=" + numTerms + " packet=" + packet;
    
    bytesUsed.addAndGet(-packet.bytesUsed);

    finishedSegment(packet.delGen());
  }

  /** All frozen packets up to and including this del gen are guaranteed to be finished. */
  public long getCompletedDelGen() {
    return finishedSegments.getCompletedDelGen();
  }   

  /** Waits only for those in-flight packets that apply to these merge segments.  This is
   *  called when a merge needs to finish and must ensure all deletes to the merging
   *  segments are resolved. */
  public void waitApplyForMerge(List<SegmentCommitInfo> mergeInfos) throws IOException {
    assert Thread.holdsLock(writer) == false;

    final long t0 = System.nanoTime();

    long maxDelGen = Long.MIN_VALUE;
    for (SegmentCommitInfo info : mergeInfos) {
      maxDelGen = Math.max(maxDelGen, info.getBufferedDeletesGen());
    }

    Set<FrozenBufferedUpdates> waitFor = new HashSet<>();
    synchronized (this) {
      for (FrozenBufferedUpdates packet : updates) {
        if (packet.delGen() <= maxDelGen) {
          // We must wait for this packet before finishing the merge because its
          // deletes apply to a subset of the segments being merged:
          waitFor.add(packet);
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "waitApplyForMerge: " + waitFor.size() + " packets, " + mergeInfos.size() + " merging segments");
    }
    
    waitApply(waitFor);
  }

  private void waitApply(Set<FrozenBufferedUpdates> waitFor) throws IOException {

    long startNS = System.nanoTime();

    int packetCount = waitFor.size();

    if (waitFor.isEmpty()) {
      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", "waitApply: no deletes to apply");
      }
      return;
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "waitApply: " + waitFor.size() + " packets: " + waitFor);
    }

    long totalDelCount = 0;
    for (FrozenBufferedUpdates packet : waitFor) {
      // Frozen packets are now resolved, concurrently, by the indexing threads that
      // create them, by adding a DocumentsWriter.ResolveUpdatesEvent to the events queue,
      // but if we get here and the packet is not yet resolved, we resolve it now ourselves:
      packet.apply(writer);
      totalDelCount += packet.totalDelCount;
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "waitApply: done %d packets; totalDelCount=%d; totBytesUsed=%d; took %.2f msec",
                                       packetCount,
                                       totalDelCount,
                                       bytesUsed.get(),
                                       (System.nanoTime() - startNS) / 1000000.));
    }
  }

  synchronized long getNextGen() {
    return nextGen++;
  }

  /** Holds all per-segment internal state used while resolving deletions. */
  public static final class SegmentState {
    final long delGen;
    final ReadersAndUpdates rld;
    final SegmentReader reader;
    final int startDelCount;

    TermsEnum termsEnum;
    PostingsEnum postingsEnum;
    BytesRef term;

    public SegmentState(IndexWriter.ReaderPool pool, SegmentCommitInfo info) throws IOException {
      rld = pool.get(info, true);
      startDelCount = rld.getPendingDeleteCount();
      reader = rld.getReader(IOContext.READ);
      delGen = info.getBufferedDeletesGen();
    }

    public void finish(IndexWriter.ReaderPool pool) throws IOException {
      try {
        rld.release(reader);
      } finally {
        pool.release(rld);
      }
    }

    @Override
    public String toString() {
      return "SegmentState(" + rld.info + ")";
    }
  }

  /** Opens SegmentReader and inits SegmentState for each segment. */
  public SegmentState[] openSegmentStates(IndexWriter.ReaderPool pool, List<SegmentCommitInfo> infos,
                                          Set<SegmentCommitInfo> alreadySeenSegments, long delGen) throws IOException {
    ensureOpen();

    List<SegmentState> segStates = new ArrayList<>();
    try {
      for (SegmentCommitInfo info : infos) {
        if (info.getBufferedDeletesGen() <= delGen && alreadySeenSegments.contains(info) == false) {
          segStates.add(new SegmentState(pool, info));
          alreadySeenSegments.add(info);
        }
      }
    } catch (Throwable t) {
      for(SegmentState segState : segStates) {
        try {
          segState.finish(pool);
        } catch (Throwable th) {
          t.addSuppressed(th);
        }
      }
      throw t;
    }
    
    return segStates.toArray(new SegmentState[0]);
  }

  /** Close segment states previously opened with openSegmentStates. */
  public ApplyDeletesResult closeSegmentStates(IndexWriter.ReaderPool pool, SegmentState[] segStates, boolean success) throws IOException {
    List<SegmentCommitInfo> allDeleted = null;
    long totDelCount = 0;
    final List<SegmentState> segmentStates = Arrays.asList(segStates);
    for (SegmentState segState : segmentStates) {
      if (success) {
        totDelCount += segState.rld.getPendingDeleteCount() - segState.startDelCount;
        int fullDelCount = segState.rld.info.getDelCount() + segState.rld.getPendingDeleteCount();
        assert fullDelCount <= segState.rld.info.info.maxDoc() : fullDelCount + " > " + segState.rld.info.info.maxDoc();
        if (segState.rld.isFullyDeleted()) {
          if (allDeleted == null) {
            allDeleted = new ArrayList<>();
          }
          allDeleted.add(segState.reader.getSegmentInfo());
        }
      }
    }
    IOUtils.applyToAll(segmentStates, s -> s.finish(pool));
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "closeSegmentStates: " + totDelCount + " new deleted documents; pool " + updates.size() + " packets; bytesUsed=" + pool.ramBytesUsed());
    }

    return new ApplyDeletesResult(totDelCount > 0, allDeleted);      
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

  /** Tracks the contiguous range of packets that have finished resolving.  We need this because the packets
   *  are concurrently resolved, and we can only write to disk the contiguous completed
   *  packets. */
  private static class FinishedSegments {

    /** Largest del gen, inclusive, for which all prior packets have finished applying. */
    private long completedDelGen;

    /** This lets us track the "holes" in the current frontier of applying del
     *  gens; once the holes are filled in we can advance completedDelGen. */
    private final Set<Long> finishedDelGens = new HashSet<>();

    private final InfoStream infoStream;

    public FinishedSegments(InfoStream infoStream) {
      this.infoStream = infoStream;
    }

    public synchronized void clear() {
      finishedDelGens.clear();
      completedDelGen = 0;
    }

    public synchronized boolean stillRunning(long delGen) {
      return delGen > completedDelGen && finishedDelGens.contains(delGen) == false;
    }

    public synchronized long getCompletedDelGen() {
      return completedDelGen;
    }

    public synchronized void finishedSegment(long delGen) {
      finishedDelGens.add(delGen);
      while (true) {
        if (finishedDelGens.contains(completedDelGen + 1)) {
          finishedDelGens.remove(completedDelGen + 1);
          completedDelGen++;
        } else {
          break;
        }
      }

      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", "finished packet delGen=" + delGen + " now completedDelGen=" + completedDelGen);
      }
    }
  }
}

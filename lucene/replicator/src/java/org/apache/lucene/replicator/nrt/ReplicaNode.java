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

package org.apache.lucene.replicator.nrt;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.IOUtils;

/** Replica node, that pulls index changes from the primary node by copying newly flushed or merged index files.
 * 
 *  @lucene.experimental */

public abstract class ReplicaNode extends Node {

  ReplicaFileDeleter deleter;

  /** IncRef'd files in the current commit point: */
  private final Collection<String> lastCommitFiles = new HashSet<>();

  /** IncRef'd files in the current NRT point: */
  protected final Collection<String> lastNRTFiles = new HashSet<>();

  /** Currently running merge pre-copy jobs */
  protected final Set<CopyJob> mergeCopyJobs = Collections.synchronizedSet(new HashSet<>());

  /** Non-null when we are currently copying files from a new NRT point: */
  protected CopyJob curNRTCopy;

  /** We hold this to ensure an external IndexWriter cannot also open on our directory: */
  private final Lock writeFileLock;

  /** Merged segment files that we pre-copied, but have not yet made visible in a new NRT point. */
  final Set<String> pendingMergeFiles = Collections.synchronizedSet(new HashSet<String>());

  /** Primary gen last time we successfully replicated: */
  protected long lastPrimaryGen;

  public ReplicaNode(int id, Directory dir, SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
    super(id, dir, searcherFactory, printStream);

    if (dir instanceof FSDirectory && ((FSDirectory) dir).checkPendingDeletions()) {
      throw new IllegalArgumentException("Directory " + dir + " still has pending deleted files; cannot initialize IndexWriter");
    }

    boolean success = false;

    try {
      message("top: init replica dir=" + dir);

      // Obtain a write lock on this index since we "act like" an IndexWriter, to prevent any other IndexWriter or ReplicaNode from using it:
      writeFileLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME);
      
      state = "init";
      deleter = new ReplicaFileDeleter(this, dir);
      success = true;
    } catch (Throwable t) {
      message("exc on init:");
      t.printStackTrace(printStream);
      throw t;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  /** Start up this replica, which possibly requires heavy copying of files from the primary node, if we were down for a long time */
  protected synchronized void start(long curPrimaryGen) throws IOException {

    if (state.equals("init") == false) {
      throw new IllegalStateException("already started");
    }

    message("top: now start");
    try {

      // Figure out what state our local index is in now:
      String segmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(dir);

      // Also look for any pending_segments_N, in case we crashed mid-commit.  We must "inflate" our infos gen to at least this, since
      // otherwise we may wind up re-using the pending_segments_N file name on commit, and then our deleter can get angry because it still
      // wants to delete this file:
      long maxPendingGen = -1;
      for(String fileName : dir.listAll()) {
        if (fileName.startsWith(IndexFileNames.PENDING_SEGMENTS)) {
          long gen = Long.parseLong(fileName.substring(IndexFileNames.PENDING_SEGMENTS.length()+1), Character.MAX_RADIX);
          if (gen > maxPendingGen) {
            maxPendingGen = gen;
          }
        }
      }

      SegmentInfos infos;
      if (segmentsFileName == null) {
        // No index here yet:
        infos = new SegmentInfos();
        message("top: init: no segments in index");
      } else {
        message("top: init: read existing segments commit " + segmentsFileName);
        infos = SegmentInfos.readCommit(dir, segmentsFileName);
        message("top: init: segments: " + infos.toString() + " version=" + infos.getVersion());
        Collection<String> indexFiles = infos.files(false);

        lastCommitFiles.add(segmentsFileName);
        lastCommitFiles.addAll(indexFiles);

        // Always protect the last commit:
        deleter.incRef(lastCommitFiles);

        lastNRTFiles.addAll(indexFiles);
        deleter.incRef(lastNRTFiles);
        message("top: commitFiles=" + lastCommitFiles);
        message("top: nrtFiles=" + lastNRTFiles);
      }

      message("top: delete unknown files on init: all files=" + Arrays.toString(dir.listAll()));
      deleter.deleteUnknownFiles(segmentsFileName);
      message("top: done delete unknown files on init: all files=" + Arrays.toString(dir.listAll()));

      String s = infos.getUserData().get(PRIMARY_GEN_KEY);
      long myPrimaryGen;
      if (s == null) {
        assert infos.size() == 0;
        myPrimaryGen = -1;
      } else {
        myPrimaryGen = Long.parseLong(s);
      }
      message("top: myPrimaryGen=" + myPrimaryGen);

      boolean doCommit;

      if (infos.size() > 0 && myPrimaryGen != -1 && myPrimaryGen != curPrimaryGen) {

        assert myPrimaryGen < curPrimaryGen;

        // Primary changed while we were down.  In this case, we must sync from primary before opening a reader, because it's possible current
        // files we have will need to be overwritten with different ones (if index rolled back and "forked"), and we can't overwrite open
        // files on Windows:

        final long initSyncStartNS = System.nanoTime();

        message("top: init: primary changed while we were down myPrimaryGen=" + myPrimaryGen +
                " vs curPrimaryGen=" + curPrimaryGen +
                "; sync now before mgr init");

        // Try until we succeed in copying over the latest NRT point:
        CopyJob job = null;

        // We may need to overwrite files referenced by our latest commit, either right now on initial sync, or on a later sync.  To make
        // sure the index is never even in an "apparently" corrupt state (where an old segments_N references invalid files) we forcefully
        // remove the commit now, and refuse to start the replica if this delete fails:
        message("top: now delete starting commit point " + segmentsFileName);

        // If this throws exc (e.g. due to virus checker), we cannot start this replica:
        assert deleter.getRefCount(segmentsFileName) == 1;
        deleter.decRef(Collections.singleton(segmentsFileName));

        if (dir instanceof FSDirectory && ((FSDirectory) dir).checkPendingDeletions()) {
          // If e.g. virus checker blocks us from deleting, we absolutely cannot start this node else there is a definite window during
          // which if we carsh, we cause corruption:
          throw new RuntimeException("replica cannot start: existing segments file=" + segmentsFileName + " must be removed in order to start, but the file delete failed");
        }

        // So we don't later try to decRef it (illegally) again:
        boolean didRemove = lastCommitFiles.remove(segmentsFileName);
        assert didRemove;

        while (true) {
          job = newCopyJob("sync on startup replica=" + name() + " myVersion=" + infos.getVersion(),
                           null,
                           null,
                           true,
                           null);
          job.start();

          message("top: init: sync sis.version=" + job.getCopyState().version);

          // Force this copy job to finish while we wait, now.  Note that this can be very time consuming!
          // NOTE: newNRTPoint detects we are still in init (mgr is null) and does not cancel our copy if a flush happens
          try {
            job.runBlocking();
            job.finish();

            // Success!
            break;
          } catch (IOException ioe) {
            job.cancel("startup failed", ioe);
            if (ioe.getMessage().contains("checksum mismatch after file copy")) {
              // OK-ish
              message("top: failed to copy: " + ioe + "; retrying");
            } else {
              throw ioe;
            }
          }
        }

        lastPrimaryGen = job.getCopyState().primaryGen;
        byte[] infosBytes = job.getCopyState().infosBytes;

        SegmentInfos syncInfos = SegmentInfos.readCommit(dir,
                                                         new BufferedChecksumIndexInput(new ByteArrayIndexInput("SegmentInfos", job.getCopyState().infosBytes)),
                                                         job.getCopyState().gen);

        // Must always commit to a larger generation than what's currently in the index:
        syncInfos.updateGeneration(infos);
        infos = syncInfos;

        assert infos.getVersion() == job.getCopyState().version;
        message("  version=" + infos.getVersion() + " segments=" + infos.toString());
        message("top: init: incRef nrtFiles=" + job.getFileNames());
        deleter.incRef(job.getFileNames());
        message("top: init: decRef lastNRTFiles=" + lastNRTFiles);
        deleter.decRef(lastNRTFiles);

        lastNRTFiles.clear();
        lastNRTFiles.addAll(job.getFileNames());

        message("top: init: set lastNRTFiles=" + lastNRTFiles);
        lastFileMetaData = job.getCopyState().files;
        message(String.format(Locale.ROOT, "top: %d: start: done sync: took %.3fs for %s, opened NRT reader version=%d",
                              id,
                              (System.nanoTime()-initSyncStartNS)/1000000000.0,
                              bytesToString(job.getTotalBytesCopied()),
                              job.getCopyState().version));

        doCommit = true;
      } else {
        doCommit = false;
        lastPrimaryGen = curPrimaryGen;
        message("top: same primary as before");
      }

      if (infos.getGeneration() < maxPendingGen) {
        message("top: move infos generation from " + infos.getGeneration() + " to " + maxPendingGen);
        infos.setNextWriteGeneration(maxPendingGen);
      }

      // Notify primary we started, to give it a chance to send any warming merges our way to reduce NRT latency of first sync:
      sendNewReplica();

      // Finally, we are open for business, since our index now "agrees" with the primary:
      mgr = new SegmentInfosSearcherManager(dir, this, infos, searcherFactory);

      IndexSearcher searcher = mgr.acquire();
      try {
        // TODO: this is test specific:
        int hitCount = searcher.count(new TermQuery(new Term("marker", "marker")));
        message("top: marker count=" + hitCount + " version=" + ((DirectoryReader) searcher.getIndexReader()).getVersion());
      } finally {
        mgr.release(searcher);
      }

      // Must commit after init mgr:
      if (doCommit) {
        // Very important to commit what we just sync'd over, because we removed the pre-existing commit point above if we had to
        // overwrite any files it referenced:
        commit();
      }

      message("top: done start");
      state = "idle";
    } catch (Throwable t) {
      if (t.getMessage().startsWith("replica cannot start") == false) {
        message("exc on start:");
        t.printStackTrace(printStream);
      } else {
        dir.close();
      }
      IOUtils.reThrow(t);
    }
  }
  
  final Object commitLock = new Object();

  @Override
  public void commit() throws IOException {

    synchronized(commitLock) {

      SegmentInfos infos;
      Collection<String> indexFiles;

      synchronized (this) {
        infos = ((SegmentInfosSearcherManager) mgr).getCurrentInfos();
        indexFiles = infos.files(false);
        deleter.incRef(indexFiles);
      }

      message("top: commit primaryGen=" + lastPrimaryGen + " infos=" + infos.toString() + " files=" + indexFiles);

      // fsync all index files we are now referencing
      dir.sync(indexFiles);

      Map<String,String> commitData = new HashMap<>();
      commitData.put(PRIMARY_GEN_KEY, Long.toString(lastPrimaryGen));
      commitData.put(VERSION_KEY, Long.toString(getCurrentSearchingVersion()));
      infos.setUserData(commitData, false);

      // write and fsync a new segments_N
      infos.commit(dir);

      // Notify current infos (which may have changed while we were doing dir.sync above) what generation we are up to; this way future
      // commits are guaranteed to go to the next (unwritten) generations:
      if (mgr != null) {
        ((SegmentInfosSearcherManager) mgr).getCurrentInfos().updateGeneration(infos);
      }
      String segmentsFileName = infos.getSegmentsFileName();
      message("top: commit wrote segments file " + segmentsFileName + " version=" + infos.getVersion() + " sis=" + infos.toString() + " commitData=" + commitData);
      deleter.incRef(Collections.singletonList(segmentsFileName));
      message("top: commit decRef lastCommitFiles=" + lastCommitFiles);
      deleter.decRef(lastCommitFiles);
      lastCommitFiles.clear();
      lastCommitFiles.addAll(indexFiles);
      lastCommitFiles.add(segmentsFileName);
      message("top: commit version=" + infos.getVersion() + " files now " + lastCommitFiles);
    }
  }

  void finishNRTCopy(CopyJob job, long startNS) throws IOException {
    CopyState copyState = job.getCopyState();
    message("top: finishNRTCopy: version=" + copyState.version + (job.getFailed() ? " FAILED" : "") + " job=" + job);

    // NOTE: if primary crashed while we were still copying then the job will hit an exc trying to read bytes for the files from the primary node,
    // and the job will be marked as failed here:

    synchronized (this) {

      if ("syncing".equals(state)) {
        state = "idle";
      }

      if (curNRTCopy == job) {
        message("top: now clear curNRTCopy; job=" + job);
        curNRTCopy = null;
      } else {
        assert job.getFailed();
        message("top: skip clear curNRTCopy: we were cancelled; job=" + job);
      }

      if (job.getFailed()) {
        return;
      }

      // Does final file renames:
      job.finish();

      // Turn byte[] back to SegmentInfos:
      byte[] infosBytes = copyState.infosBytes;
      SegmentInfos infos = SegmentInfos.readCommit(dir,
                                                   new BufferedChecksumIndexInput(new ByteArrayIndexInput("SegmentInfos", copyState.infosBytes)),
                                                   copyState.gen);
      assert infos.getVersion() == copyState.version;

      message("  version=" + infos.getVersion() + " segments=" + infos.toString());

      // Cutover to new searcher:
      if (mgr != null) {
        ((SegmentInfosSearcherManager) mgr).setCurrentInfos(infos);
      }

      // Must first incRef new NRT files, then decRef old ones, to make sure we don't remove an NRT file that's in common to both:
      Collection<String> newFiles = copyState.files.keySet();
      message("top: incRef newNRTFiles=" + newFiles);
      deleter.incRef(newFiles);

      // If any of our new files were previously copied merges, we clear them now, so we don't try to later delete a non-existent file:
      pendingMergeFiles.removeAll(newFiles);
      message("top: after remove from pending merges pendingMergeFiles=" + pendingMergeFiles);

      message("top: decRef lastNRTFiles=" + lastNRTFiles);
      deleter.decRef(lastNRTFiles);
      lastNRTFiles.clear();
      lastNRTFiles.addAll(newFiles);
      message("top: set lastNRTFiles=" + lastNRTFiles);

      // At this point we can remove any completed merge segment files that we still do not reference.  This can happen when a merge
      // finishes, copies its files out to us, but is then merged away (or dropped due to 100% deletions) before we ever cutover to it
      // in an NRT point:
      if (copyState.completedMergeFiles.isEmpty() == false) {
        message("now remove-if-not-ref'd completed merge files: " + copyState.completedMergeFiles);
        for(String fileName : copyState.completedMergeFiles) {
          if (pendingMergeFiles.contains(fileName)) {
            pendingMergeFiles.remove(fileName);
            deleter.deleteIfNoRef(fileName);
          }
        }
      }

      lastFileMetaData = copyState.files;
    }

    int markerCount;
    IndexSearcher s = mgr.acquire();
    try {
      markerCount = s.count(new TermQuery(new Term("marker", "marker")));
    } finally {
      mgr.release(s);
    }

    message(String.format(Locale.ROOT, "top: done sync: took %.3fs for %s, opened NRT reader version=%d markerCount=%d",
                          (System.nanoTime()-startNS)/1000000000.0,
                          bytesToString(job.getTotalBytesCopied()),
                          copyState.version,
                          markerCount));
  }

  /** Start a background copying job, to copy the specified files from the current primary node.  If files is null then the latest copy
   *  state should be copied.  If prevJob is not null, then the new copy job is replacing it and should 1) cancel the previous one, and
   *  2) optionally salvage e.g. partially copied and, shared with the new copy job, files. */
  protected abstract CopyJob newCopyJob(String reason, Map<String,FileMetaData> files, Map<String,FileMetaData> prevFiles,
                                        boolean highPriority, CopyJob.OnceDone onceDone) throws IOException;

  /** Runs this job async'd */
  protected abstract void launch(CopyJob job);

  /** Tell primary we (replica) just started, so primary can tell us to warm any already warming merges.  This lets us keep low nrt refresh
   *  time for the first nrt sync after we started. */
  protected abstract void sendNewReplica() throws IOException;

  /** Call this to notify this replica node that a new NRT infos is available on the primary.
   *  We kick off a job (runs in the background) to copy files across, and open a new reader once that's done. */
  public synchronized CopyJob newNRTPoint(long newPrimaryGen, long version) throws IOException {

    if (isClosed()) {
      throw new AlreadyClosedException("this replica is closed: state=" + state);
    }

    // Cutover (possibly) to new primary first, so we discard any pre-copied merged segments up front, before checking for which files need
    // copying.  While it's possible the pre-copied merged segments could still be useful to us, in the case that the new primary is either
    // the same primary (just e.g. rebooted), or a promoted replica that had a newer NRT point than we did that included the pre-copied
    // merged segments, it's still a bit risky to rely solely on checksum/file length to catch the difference, so we defensively discard
    // here and re-copy in that case:
    maybeNewPrimary(newPrimaryGen);

    // Caller should not "publish" us until we have finished .start():
    assert mgr != null;

    if ("idle".equals(state)) {
      state = "syncing";
    }

    long curVersion = getCurrentSearchingVersion();

    message("top: start sync sis.version=" + version);

    if (version == curVersion) {
      // Caller releases the CopyState:
      message("top: new NRT point has same version as current; skipping");
      return null;
    }

    if (version < curVersion) {
      // This can happen, if two syncs happen close together, and due to thread scheduling, the incoming older version runs after the newer version
      message("top: new NRT point (version=" + version + ") is older than current (version=" + version + "); skipping");
      return null;
    }

    final long startNS = System.nanoTime();

    message("top: newNRTPoint");
    CopyJob job = null;
    try {
      job = newCopyJob("NRT point sync version=" + version,
                       null,
                       lastFileMetaData,
                       true,
                       new CopyJob.OnceDone() {
                         @Override
                         public void run(CopyJob job) {
                           try {
                             finishNRTCopy(job, startNS);
                           } catch (IOException ioe) {
                             throw new RuntimeException(ioe);
                           }
                         }
                       });
    } catch (NodeCommunicationException nce) {
      // E.g. primary could crash/close when we are asking it for the copy state:
      message("top: ignoring communication exception creating CopyJob: " + nce);
      //nce.printStackTrace(printStream);
      if (state.equals("syncing")) {
        state = "idle";
      }
      return null;
    }

    assert newPrimaryGen == job.getCopyState().primaryGen;

    Collection<String> newNRTFiles = job.getFileNames();

    message("top: newNRTPoint: job files=" + newNRTFiles);

    if (curNRTCopy != null) {
      job.transferAndCancel(curNRTCopy);
      assert curNRTCopy.getFailed();
    }

    curNRTCopy = job;

    for(String fileName : curNRTCopy.getFileNamesToCopy()) {
      assert lastCommitFiles.contains(fileName) == false: "fileName=" + fileName + " is in lastCommitFiles and is being copied?";
      synchronized (mergeCopyJobs) {
        for (CopyJob mergeJob : mergeCopyJobs) {
          if (mergeJob.getFileNames().contains(fileName)) {
            // TODO: we could maybe transferAndCancel here?  except CopyJob can't transferAndCancel more than one currently
            message("top: now cancel merge copy job=" + mergeJob + ": file " + fileName + " is now being copied via NRT point");
            mergeJob.cancel("newNRTPoint is copying over the same file", null);
          }
        }
      }
    }

    try {
      job.start();
    } catch (NodeCommunicationException nce) {
      // E.g. primary could crash/close when we are asking it for the copy state:
      message("top: ignoring exception starting CopyJob: " + nce);
      nce.printStackTrace(printStream);
      if (state.equals("syncing")) {
        state = "idle";
      }
      return null;
    }

    // Runs in the background jobs thread, maybe slowly/throttled, and calls finishSync once it's done:
    launch(curNRTCopy);
    return curNRTCopy;
  }

  public synchronized boolean isCopying() {
    return curNRTCopy != null;
  }

  @Override
  public boolean isClosed() {
    return "closed".equals(state) || "closing".equals(state) || "crashing".equals(state) || "crashed".equals(state);
  }

  @Override
  public void close() throws IOException {
    message("top: now close");

    synchronized (this) {
      state = "closing";
      if (curNRTCopy != null) {
        curNRTCopy.cancel("closing", null);
      }
    }

    synchronized (this) {
      message("top: close mgr");
      mgr.close();

      message("top: decRef lastNRTFiles=" + lastNRTFiles);
      deleter.decRef(lastNRTFiles);
      lastNRTFiles.clear();

      // NOTE: do not decRef these!
      lastCommitFiles.clear();

      message("top: delete if no ref pendingMergeFiles=" + pendingMergeFiles);
      for(String fileName : pendingMergeFiles) {
        deleter.deleteIfNoRef(fileName);
      }
      pendingMergeFiles.clear();
    
      message("top: close dir");
      IOUtils.close(writeFileLock, dir);
    }
    message("top: done close");
    state = "closed";
  }

  /** Called when the primary changed */
  protected synchronized void maybeNewPrimary(long newPrimaryGen) throws IOException {
    if (newPrimaryGen != lastPrimaryGen) {
      message("top: now change lastPrimaryGen from " + lastPrimaryGen + " to " + newPrimaryGen + " pendingMergeFiles=" + pendingMergeFiles);

      message("top: delete if no ref pendingMergeFiles=" + pendingMergeFiles);
      for(String fileName : pendingMergeFiles) {
        deleter.deleteIfNoRef(fileName);
      }

      assert newPrimaryGen > lastPrimaryGen: "newPrimaryGen=" + newPrimaryGen + " vs lastPrimaryGen=" + lastPrimaryGen;
      lastPrimaryGen = newPrimaryGen;
      pendingMergeFiles.clear();
    } else {
      message("top: keep current lastPrimaryGen=" + lastPrimaryGen);
    }
  }

  protected synchronized CopyJob launchPreCopyMerge(AtomicBoolean finished, long newPrimaryGen, Map<String,FileMetaData> files) throws IOException {

    CopyJob job;

    maybeNewPrimary(newPrimaryGen);
    final long primaryGenStart = lastPrimaryGen;
    Set<String> fileNames = files.keySet();
    message("now pre-copy warm merge files=" + fileNames + " primaryGen=" + newPrimaryGen);

    for(String fileName : fileNames) {
      assert pendingMergeFiles.contains(fileName) == false: "file \"" + fileName + "\" is already being warmed!";
      assert lastNRTFiles.contains(fileName) == false: "file \"" + fileName + "\" is already NRT visible!";
    }

    job = newCopyJob("warm merge on " + name() + " filesNames=" + fileNames,
                     files, null, false,
                     new CopyJob.OnceDone() {

                       @Override
                       public void run(CopyJob job) throws IOException {
                         // Signals that this replica has finished
                         mergeCopyJobs.remove(job);
                         message("done warming merge " + fileNames + " failed?=" + job.getFailed());
                         synchronized(this) {
                           if (job.getFailed() == false) {
                             if (lastPrimaryGen != primaryGenStart) {
                               message("merge pre copy finished but primary has changed; cancelling job files=" + fileNames);
                               job.cancel("primary changed during merge copy", null);
                             } else {
                               boolean abort = false;
                               for (String fileName : fileNames) {
                                 if (lastNRTFiles.contains(fileName)) {
                                   message("abort merge finish: file " + fileName + " is referenced by last NRT point");
                                   abort = true;
                                 }
                                 if (lastCommitFiles.contains(fileName)) {
                                   message("abort merge finish: file " + fileName + " is referenced by last commit point");
                                   abort = true;
                                 }
                               }
                               if (abort) {
                                 // Even though in newNRTPoint we have similar logic, which cancels any merge copy jobs if an NRT point
                                 // shows up referencing the files we are warming (because primary got impatient and gave up on us), we also
                                 // need it here in case replica is way far behind and fails to even receive the merge pre-copy request
                                 // until after the newNRTPoint referenced those files:
                                 job.cancel("merged segment was separately copied via NRT point", null);
                               } else {
                                 job.finish();
                                 message("merge pre copy finished files=" + fileNames);
                                 for(String fileName : fileNames) {
                                   assert pendingMergeFiles.contains(fileName) == false : "file \"" + fileName + "\" is already in pendingMergeFiles";
                                   message("add file " + fileName + " to pendingMergeFiles");
                                   pendingMergeFiles.add(fileName);
                                 }
                               }
                             }
                           } else {
                             message("merge copy finished with failure");
                           }
                         }
                         finished.set(true);
                       }
                     });

    job.start();

    // When warming a merge we better not already have any of these files copied!
    assert job.getFileNamesToCopy().size() == files.size();

    mergeCopyJobs.add(job);
    launch(job);

    return job;
  }

  public IndexOutput createTempOutput(String prefix, String suffix, IOContext ioContext) throws IOException {
    return dir.createTempOutput(prefix, suffix, IOContext.DEFAULT);
  }

  /** Compares incoming per-file identity (id, checksum, header, footer) versus what we have locally and returns the subset of the incoming
   *  files that need copying */
  public List<Map.Entry<String,FileMetaData>> getFilesToCopy(Map<String,FileMetaData> files) throws IOException {

    List<Map.Entry<String,FileMetaData>> toCopy = new ArrayList<>();
    for (Map.Entry<String,FileMetaData> ent : files.entrySet()) {
      String fileName = ent.getKey();
      FileMetaData fileMetaData = ent.getValue();
      if (fileIsIdentical(fileName, fileMetaData) == false) {
        toCopy.add(ent);
      }
    }

    return toCopy;
  }

  /** Carefully determine if the file on the primary, identified by its {@code String fileName} along with the {@link FileMetaData}
   * "summarizing" its contents, is precisely the same file that we have locally.  If the file does not exist locally, or if its its header
   * (inclues the segment id), length, footer (including checksum) differ, then this returns false, else true. */
  private boolean fileIsIdentical(String fileName, FileMetaData srcMetaData) throws IOException {

    FileMetaData destMetaData = readLocalFileMetaData(fileName);
    if (destMetaData == null) {
      // Something went wrong in reading the file (it's corrupt, truncated, does not exist, etc.):
      return false;
    }

    if (Arrays.equals(destMetaData.header, srcMetaData.header) == false ||
        Arrays.equals(destMetaData.footer, srcMetaData.footer) == false) {
      // Segment name was reused!  This is rare but possible and otherwise devastating:
      if (Node.VERBOSE_FILES) {
        message("file " + fileName + ": will copy [header/footer is different]");
      }
      return false;
    } else {
      return true;
    }
  }

  private ConcurrentMap<String,Boolean> copying = new ConcurrentHashMap<>();

  // Used only to catch bugs, ensuring a given file name is only ever being copied bye one job:
  public void startCopyFile(String name) {
    if (copying.putIfAbsent(name, Boolean.TRUE) != null) {
      throw new IllegalStateException("file " + name + " is being copied in two places!");
    }
  }

  public void finishCopyFile(String name) {
    if (copying.remove(name) == null) {
      throw new IllegalStateException("file " + name + " was not actually being copied?");
    }
  }
}

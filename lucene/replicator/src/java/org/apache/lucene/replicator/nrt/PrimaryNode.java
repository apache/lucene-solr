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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ThreadInterruptedException;

/*
 * This just asks IndexWriter to open new NRT reader, in order to publish a new NRT point.  This could be improved, if we separated out 1)
 * nrt flush (and incRef the SIS) from 2) opening a new reader, but this is tricky with IW's concurrency, and it would also be hard-ish to share
 * IW's reader pool with our searcher manager.  So we do the simpler solution now, but that adds some unecessary latency to NRT refresh on
 * replicas since step 2) could otherwise be done concurrently with replicas copying files over.
 */

/** Node that holds an IndexWriter, indexing documents into its local index.
 *
 * @lucene.experimental */

public abstract class PrimaryNode extends Node {

  // Current NRT segment infos, incRef'd with IndexWriter.deleter:
  private SegmentInfos curInfos;

  protected final IndexWriter writer;

  // IncRef'd state of the last published NRT point; when a replica comes asking, we give it this as the current NRT point:
  private CopyState copyState;

  protected final long primaryGen;

  /** Contains merged segments that have been copied to all running replicas (as of when that merge started warming). */
  final Set<String> finishedMergedFiles = Collections.synchronizedSet(new HashSet<String>());

  private final AtomicInteger copyingCount = new AtomicInteger();

  public PrimaryNode(IndexWriter writer, int id, long primaryGen, long forcePrimaryVersion,
                     SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
    super(id, writer.getDirectory(), searcherFactory, printStream);
    message("top: now init primary");
    this.writer = writer;
    this.primaryGen = primaryGen;

    try {
      // So that when primary node's IndexWriter finishes a merge, but before it cuts over to the merged segment,
      // it copies it out to the replicas.  This ensures the whole system's NRT latency remains low even when a
      // large merge completes:
      writer.getConfig().setMergedSegmentWarmer(new PreCopyMergedSegmentWarmer(this));

      message("IWC:\n" + writer.getConfig());
      message("dir:\n" + writer.getDirectory());
      message("commitData: " + writer.getLiveCommitData());

      // Record our primaryGen in the userData, and set initial version to 0:
      Map<String,String> commitData = new HashMap<>();
      Iterable<Map.Entry<String,String>> iter = writer.getLiveCommitData();
      if (iter != null) {
        for(Map.Entry<String,String> ent : iter) {
          commitData.put(ent.getKey(), ent.getValue());
        }
      }
      commitData.put(PRIMARY_GEN_KEY, Long.toString(primaryGen));
      if (commitData.get(VERSION_KEY) == null) {
        commitData.put(VERSION_KEY, "0");
        message("add initial commitData version=0");
      } else {
        message("keep current commitData version=" + commitData.get(VERSION_KEY));
      }
      writer.setLiveCommitData(commitData.entrySet(), false);

      // We forcefully advance the SIS version to an unused future version.  This is necessary if the previous primary crashed and we are
      // starting up on an "older" index, else versions can be illegally reused but show different results:
      if (forcePrimaryVersion != -1) {
        message("now forcePrimaryVersion to version=" + forcePrimaryVersion);
        writer.advanceSegmentInfosVersion(forcePrimaryVersion);
      }

      mgr = new SearcherManager(writer, true, true, searcherFactory);
      setCurrentInfos(Collections.<String>emptySet());
      message("init: infos version=" + curInfos.getVersion());

      IndexSearcher s = mgr.acquire();
      try {
        // TODO: this is test code specific!!
        message("init: marker count: " + s.count(new TermQuery(new Term("marker", "marker"))));
      } finally {
        mgr.release(s);
      }

    } catch (Throwable t) {
      message("init: exception");
      t.printStackTrace(printStream);
      throw new RuntimeException(t);
    }
  }

  /** Returns the current primary generation, which is incremented each time a new primary is started for this index */
  public long getPrimaryGen() {
    return primaryGen;
  }

  // TODO: in the future, we should separate "flush" (returns an incRef'd SegmentInfos) from "refresh" (open new NRT reader from
  // IndexWriter) so that the latter can be done concurrently while copying files out to replicas, minimizing the refresh time from the
  // replicas.  But fixing this is tricky because e.g. IndexWriter may complete a big merge just after returning the incRef'd SegmentInfos
  // and before we can open a new reader causing us to close the just-merged readers only to then open them again from the (now stale)
  // SegmentInfos.  To fix this "properly" I think IW.inc/decRefDeleter must also incread the ReaderPool entry

  /** Flush all index operations to disk and opens a new near-real-time reader.
   *  new NRT point, to make the changes visible to searching.  Returns true if there were changes. */
  public boolean flushAndRefresh() throws IOException {
    message("top: now flushAndRefresh");
    Set<String> completedMergeFiles;
    synchronized(finishedMergedFiles) {
      completedMergeFiles = Collections.unmodifiableSet(new HashSet<>(finishedMergedFiles));
    }
    mgr.maybeRefreshBlocking();
    boolean result = setCurrentInfos(completedMergeFiles);
    if (result) {
      message("top: opened NRT reader version=" + curInfos.getVersion());
      finishedMergedFiles.removeAll(completedMergeFiles);
      message("flushAndRefresh: version=" + curInfos.getVersion() + " completedMergeFiles=" + completedMergeFiles + " finishedMergedFiles=" + finishedMergedFiles);
    } else {
      message("top: no changes in flushAndRefresh; still version=" + curInfos.getVersion());
    }
    return result;
  }

  public long getCopyStateVersion() {
    return copyState.version;
  }

  public synchronized long getLastCommitVersion() {
    Iterable<Map.Entry<String,String>> iter = writer.getLiveCommitData();
    assert iter != null;
    for(Map.Entry<String,String> ent : iter) {
      if (ent.getKey().equals(VERSION_KEY)) {
        return Long.parseLong(ent.getValue());
      }
    }

    // In ctor we always install an initial version:
    throw new AssertionError("missing VERSION_KEY");
  }

  @Override
  public void commit() throws IOException {
    Map<String,String> commitData = new HashMap<>();
    commitData.put(PRIMARY_GEN_KEY, Long.toString(primaryGen));
    // TODO (opto): it's a bit wasteful that we put "last refresh" version here, not the actual version we are committing, because it means
    // on xlog replay we are replaying more ops than necessary.
    commitData.put(VERSION_KEY, Long.toString(copyState.version));
    message("top: commit commitData=" + commitData);
    writer.setLiveCommitData(commitData.entrySet(), false);
    writer.commit();
  }

  /** IncRef the current CopyState and return it */
  public synchronized CopyState getCopyState() throws IOException {
    ensureOpen(false);
    //message("top: getCopyState replicaID=" + replicaID + " replicaNodeID=" + replicaNodeID + " version=" + curInfos.getVersion() + " infos=" + curInfos.toString());
    assert curInfos == copyState.infos;
    writer.incRefDeleter(copyState.infos);
    int count = copyingCount.incrementAndGet();
    assert count > 0;
    return copyState;
  }

  /** Called once replica is done (or failed) copying an NRT point */
  public void releaseCopyState(CopyState copyState) throws IOException {
    //message("top: releaseCopyState version=" + copyState.version);
    assert copyState.infos != null;
    writer.decRefDeleter(copyState.infos);
    int count = copyingCount.decrementAndGet();
    assert count >= 0;
  }

  @Override
  public boolean isClosed() {
    return isClosed(false);
  }

  boolean isClosed(boolean allowClosing) {
    return "closed".equals(state) || (allowClosing == false && "closing".equals(state));
  }

  private void ensureOpen(boolean allowClosing) {
    if (isClosed(allowClosing)) {
      throw new AlreadyClosedException(state);
    }
  }

  /** Steals incoming infos refCount; returns true if there were changes. */
  private synchronized boolean setCurrentInfos(Set<String> completedMergeFiles) throws IOException {

    IndexSearcher searcher = null;
    SegmentInfos infos;
    try {
      searcher = mgr.acquire();
      infos = ((StandardDirectoryReader) searcher.getIndexReader()).getSegmentInfos();
      // TODO: this is test code specific!!
      message("setCurrentInfos: marker count: " + searcher.count(new TermQuery(new Term("marker", "marker"))) + " version=" + infos.getVersion() + " searcher=" + searcher);
    } finally {
      if (searcher != null) {
        mgr.release(searcher);
      }
    }
    if (curInfos != null && infos.getVersion() == curInfos.getVersion()) {
      // no change
      message("top: skip switch to infos: version=" + infos.getVersion() + " is unchanged: " + infos.toString());
      return false;
    }

    SegmentInfos oldInfos = curInfos;
    writer.incRefDeleter(infos);
    curInfos = infos;
    if (oldInfos != null) {
      writer.decRefDeleter(oldInfos);
    }

    message("top: switch to infos=" + infos.toString() + " version=" + infos.getVersion());

    // Serialize the SegmentInfos:
    RAMOutputStream out = new RAMOutputStream(new RAMFile(), true);
    infos.write(dir, out);
    byte[] infosBytes = new byte[(int) out.getFilePointer()];
    out.writeTo(infosBytes, 0);

    Map<String,FileMetaData> filesMetaData = new HashMap<String,FileMetaData>();
    for(SegmentCommitInfo info : infos) {
      for(String fileName : info.files()) {
        FileMetaData metaData = readLocalFileMetaData(fileName);
        // NOTE: we hold a refCount on this infos, so this file better exist:
        assert metaData != null;
        assert filesMetaData.containsKey(fileName) == false;
        filesMetaData.put(fileName, metaData);
      }
    }

    lastFileMetaData = Collections.unmodifiableMap(filesMetaData);

    message("top: set copyState primaryGen=" + primaryGen + " version=" + infos.getVersion() + " files=" + filesMetaData.keySet());
    copyState = new CopyState(lastFileMetaData,
                              infos.getVersion(), infos.getGeneration(), infosBytes, completedMergeFiles,
                              primaryGen, curInfos);
    return true;
  }

  private synchronized void waitForAllRemotesToClose() throws IOException {

    // Wait for replicas to finish or crash:
    while (true) {
      int count = copyingCount.get();
      if (count == 0) {
        return;
      }
      message("pendingCopies: " + count);

      try {
        wait(10);
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
  }

  @Override
  public void close() throws IOException {
    state = "closing";
    message("top: close primary");

    synchronized (this) {
      waitForAllRemotesToClose();
      if (curInfos != null) {
        writer.decRefDeleter(curInfos);
        curInfos = null;
      }
    }

    mgr.close();

    writer.rollback();
    dir.close();

    state = "closed";
  }

  /** Called when a merge has finished, but before IW switches to the merged segment */
  protected abstract void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String,FileMetaData> files) throws IOException;
}

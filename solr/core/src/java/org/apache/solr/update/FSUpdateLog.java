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

package org.apache.solr.update;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.RunUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/** @lucene.experimental */
class NullUpdateLog extends UpdateLog {
  @Override
  public void init(PluginInfo info) {
  }

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
  }

  @Override
  public void add(AddUpdateCommand cmd) {
  }

  @Override
  public void delete(DeleteUpdateCommand cmd) {
  }

  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) {
  }

  @Override
  public void preCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public void postCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public void preSoftCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public void postSoftCommit(CommitUpdateCommand cmd) {
  }

  @Override
  public Object lookup(BytesRef indexedId) {
    return null;
  }

  @Override
  public Long lookupVersion(BytesRef indexedId) {
    return null;
  }

  @Override
  public void close() {
  }

  @Override
  public VersionInfo getVersionInfo() {
    return null;
  }

  @Override
  public void finish(SyncLevel synclevel) {
  }

  @Override
  public Future<RecoveryInfo> recoverFromLog() {
    return null;
  }

  @Override
  public void bufferUpdates() {
  }

  @Override
  public Future<FSUpdateLog.RecoveryInfo> applyBufferedUpdates() {
    return null;
  }

  @Override
  public State getState() {
    return State.ACTIVE;
  }

}

/** @lucene.experimental */
public class FSUpdateLog extends UpdateLog {

  public static String TLOG_NAME="tlog";

  long id = -1;
  private State state = State.ACTIVE;

  private TransactionLog tlog;
  private TransactionLog prevTlog;

  private Map<BytesRef,LogPtr> map = new HashMap<BytesRef, LogPtr>();
  private Map<BytesRef,LogPtr> prevMap;  // used while committing/reopening is happening
  private Map<BytesRef,LogPtr> prevMap2;  // used while committing/reopening is happening
  private TransactionLog prevMapLog;  // the transaction log used to look up entries found in prevMap
  private TransactionLog prevMapLog2;  // the transaction log used to look up entries found in prevMap

  private final int numDeletesToKeep = 1000;
  // keep track of deletes only... this is not updated on an add
  private LinkedHashMap<BytesRef, LogPtr> oldDeletes = new LinkedHashMap<BytesRef, LogPtr>(numDeletesToKeep) {
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > numDeletesToKeep;
    }
  };


  private String[] tlogFiles;
  private File tlogDir;
  private Collection<String> globalStrings;

  private String dataDir;
  private String lastDataDir;

  private VersionInfo versionInfo;

  private SyncLevel defaultSyncLevel = SyncLevel.FLUSH;

  private volatile UpdateHandler uhandler;    // a core reload can change this reference!

  @Override
  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  @Override
  public void init(PluginInfo info) {
    dataDir = (String)info.initArgs.get("dir");
  }

  public void init(UpdateHandler uhandler, SolrCore core) {
    if (dataDir == null || dataDir.length()==0) {
      dataDir = core.getDataDir();
    }

    this.uhandler = uhandler;

    if (dataDir.equals(lastDataDir)) {
      // on a normal reopen, we currently shouldn't have to do anything
      return;
    }
    lastDataDir = dataDir;
    tlogDir = new File(dataDir, TLOG_NAME);
    tlogDir.mkdirs();
    tlogFiles = getLogList(tlogDir);
    id = getLastLogId() + 1;   // add 1 since we will create a new log for the next update

    versionInfo = new VersionInfo(uhandler, 256);
  }

  static class LogPtr {
    final long pointer;
    final long version;

    public LogPtr(long pointer, long version) {
      this.pointer = pointer;
      this.version = version;
    }

    public String toString() {
      return "LogPtr(" + pointer + ")";
    }
  }

  public static String[] getLogList(File directory) {
    final String prefix = TLOG_NAME+'.';
    String[] names = directory.list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith(prefix);
      }
    });
    Arrays.sort(names);
    return names;
  }


  public long getLastLogId() {
    if (id != -1) return id;
    if (tlogFiles.length == 0) return -1;
    String last = tlogFiles[tlogFiles.length-1];
    return Long.parseLong(last.substring(TLOG_NAME.length()+1));
  }


  @Override
  public void add(AddUpdateCommand cmd) {
    // don't log if we are replaying from another log
    // TODO: we currently need to log to maintain correct versioning, rtg, etc
    // if ((cmd.getFlags() & UpdateCommand.REPLAY) != 0) return;

    synchronized (this) {
      long pos = -1;

    // don't log if we are replaying from another log
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog();
        pos = tlog.write(cmd);
      }

      // TODO: in the future we could support a real position for a REPLAY update.
      // Only currently would be useful for RTG while in recovery mode though.
      LogPtr ptr = new LogPtr(pos, cmd.getVersion());

      // only update our map if we're not buffering
      if ((cmd.getFlags() & UpdateCommand.BUFFERING) == 0) {
        map.put(cmd.getIndexedId(), ptr);
      }

      // SolrCore.verbose("TLOG: added id " + cmd.getPrintableId() + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
    }
  }

  @Override
  public void delete(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();

    synchronized (this) {
      long pos = -1;

      // don't log if we are replaying from another log
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog();
        pos = tlog.writeDelete(cmd);
      }

      LogPtr ptr = new LogPtr(pos, cmd.version);

      // only update our map if we're not buffering
      if ((cmd.getFlags() & UpdateCommand.BUFFERING) == 0) {
        map.put(br, ptr);

        oldDeletes.put(br, ptr);
      }

      // SolrCore.verbose("TLOG: added delete for id " + cmd.id + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
    }
  }

  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) {
    synchronized (this) {
      long pos = -1;
    // don't log if we are replaying from another log
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {

        ensureLog();
        // TODO: how to support realtime-get, optimistic concurrency, or anything else in this case?
        // Maybe we shouldn't?
        // realtime-get could just do a reopen of the searcher
        // optimistic concurrency? Maybe we shouldn't support deleteByQuery w/ optimistic concurrency
        pos = tlog.writeDeleteByQuery(cmd);
      }

      LogPtr ptr = new LogPtr(pos, cmd.getVersion());
      // SolrCore.verbose("TLOG: added deleteByQuery " + cmd.query + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
    }
  }


  private void newMap() {
    prevMap2 = prevMap;
    prevMapLog2 = prevMapLog;

    prevMap = map;
    prevMapLog = tlog;

    map = new HashMap<BytesRef, LogPtr>();
  }

  private void clearOldMaps() {
    prevMap = null;
    prevMap2 = null;
  }

  @Override
  public void preCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      // since we're changing the log, we must change the map.
      newMap();

      // since document additions can happen concurrently with commit, create
      // a new transaction log first so that we know the old one is definitely
      // in the index.
      prevTlog = tlog;
      tlog = null;
      id++;

      if (prevTlog != null) {
        globalStrings = prevTlog.getGlobalStrings();
      }
    }
  }

  @Override
  public void postCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      if (prevTlog != null) {
        prevTlog.decref();
        prevTlog = null;
      }
    }
  }

  @Override
  public void preSoftCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      if (!cmd.softCommit) return;  // already handled this at the start of the hard commit
      newMap();

      // start adding documents to a new map since we won't know if
      // any added documents will make it into this commit or not.
      // But we do know that any updates already added will definitely
      // show up in the latest reader after the commit succeeds.
      map = new HashMap<BytesRef, LogPtr>();
      // SolrCore.verbose("TLOG: preSoftCommit: prevMap="+ System.identityHashCode(prevMap) + " new map=" + System.identityHashCode(map));
    }
  }

  @Override
  public void postSoftCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      // We can clear out all old maps now that a new searcher has been opened.
      // This currently only works since DUH2 synchronizes around preCommit to avoid
      // it being called in the middle of a preSoftCommit, postSoftCommit sequence.
      // If this DUH2 synchronization were to be removed, preSoftCommit should
      // record what old maps were created and only remove those.
      clearOldMaps();
      // SolrCore.verbose("TLOG: postSoftCommit: disposing of prevMap="+ System.identityHashCode(prevMap));
    }
  }

  @Override
  public Object lookup(BytesRef indexedId) {
    LogPtr entry;
    TransactionLog lookupLog;

    synchronized (this) {
      entry = map.get(indexedId);
      lookupLog = tlog;  // something found in "map" will always be in "tlog"
      // SolrCore.verbose("TLOG: lookup: for id ",indexedId.utf8ToString(),"in map",System.identityHashCode(map),"got",entry,"lookupLog=",lookupLog);
      if (entry == null && prevMap != null) {
        entry = prevMap.get(indexedId);
        // something found in prevMap will always be found in preMapLog (which could be tlog or prevTlog)
        lookupLog = prevMapLog;
        // SolrCore.verbose("TLOG: lookup: for id ",indexedId.utf8ToString(),"in prevMap",System.identityHashCode(map),"got",entry,"lookupLog=",lookupLog);
      }
      if (entry == null && prevMap2 != null) {
        entry = prevMap2.get(indexedId);
        // something found in prevMap2 will always be found in preMapLog2 (which could be tlog or prevTlog)
        lookupLog = prevMapLog2;
        // SolrCore.verbose("TLOG: lookup: for id ",indexedId.utf8ToString(),"in prevMap2",System.identityHashCode(map),"got",entry,"lookupLog=",lookupLog);
      }

      if (entry == null) {
        return null;
      }
      lookupLog.incref();
    }

    try {
      // now do the lookup outside of the sync block for concurrency
      return lookupLog.lookup(entry.pointer);
    } finally {
      lookupLog.decref();
    }

  }

  // This method works like realtime-get... it only guarantees to return the latest
  // version of the *completed* update.  There can be updates in progress concurrently
  // that have already grabbed higher version numbers.  Higher level coordination or
  // synchronization is needed for stronger guarantees (as VersionUpdateProcessor does).
  @Override
  public Long lookupVersion(BytesRef indexedId) {
    LogPtr entry;
    TransactionLog lookupLog;

    synchronized (this) {
      entry = map.get(indexedId);
      lookupLog = tlog;  // something found in "map" will always be in "tlog"
      // SolrCore.verbose("TLOG: lookup ver: for id ",indexedId.utf8ToString(),"in map",System.identityHashCode(map),"got",entry,"lookupLog=",lookupLog);
      if (entry == null && prevMap != null) {
        entry = prevMap.get(indexedId);
        // something found in prevMap will always be found in preMapLog (which could be tlog or prevTlog)
        lookupLog = prevMapLog;
        // SolrCore.verbose("TLOG: lookup ver: for id ",indexedId.utf8ToString(),"in prevMap",System.identityHashCode(map),"got",entry,"lookupLog=",lookupLog);
      }
      if (entry == null && prevMap2 != null) {
        entry = prevMap2.get(indexedId);
        // something found in prevMap2 will always be found in preMapLog2 (which could be tlog or prevTlog)
        lookupLog = prevMapLog2;
        // SolrCore.verbose("TLOG: lookup ver: for id ",indexedId.utf8ToString(),"in prevMap2",System.identityHashCode(map),"got",entry,"lookupLog=",lookupLog);
      }
    }

    if (entry != null) {
      return entry.version;
    }

    // Now check real index
    Long version = versionInfo.getVersionFromIndex(indexedId);

    if (version != null) {
      return version;
    }

    // We can't get any version info for deletes from the index, so if the doc
    // wasn't found, check a cache of recent deletes.

    synchronized (this) {
      entry = oldDeletes.get(indexedId);
    }

    if (entry != null) {
      return entry.version;
    }

    return null;
  }

  @Override
  public void finish(SyncLevel syncLevel) {
    if (syncLevel == null) {
      syncLevel = defaultSyncLevel;
    }
    if (syncLevel == SyncLevel.NONE) {
      return;
    }

    TransactionLog currLog;
    synchronized (this) {
      currLog = tlog;
      if (currLog == null) return;
      currLog.incref();
    }

    try {
      if (tlog != null) {
        tlog.finish(syncLevel);
      }
    } finally {
      currLog.decref();
    }
  }

  @Override
  public Future<RecoveryInfo> recoverFromLog() {
    recoveryInfo = new RecoveryInfo();
    if (tlogFiles.length == 0) return null;
    TransactionLog oldTlog = null;

    oldTlog = new TransactionLog( new File(tlogDir, tlogFiles[tlogFiles.length-1]), null, true );
    ExecutorCompletionService<RecoveryInfo> cs = new ExecutorCompletionService<RecoveryInfo>(recoveryExecutor);
    LogReplayer replayer = new LogReplayer(oldTlog, false);

    versionInfo.blockUpdates();
    try {
      state = State.REPLAYING;
    } finally {
      versionInfo.unblockUpdates();
    }

    return cs.submit(replayer, recoveryInfo);

  }


  private void ensureLog() {
    if (tlog == null) {
      String newLogName = String.format("%s.%019d", TLOG_NAME, id);
      tlog = new TransactionLog(new File(tlogDir, newLogName), globalStrings);
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      if (prevTlog != null) {
        prevTlog.decref();
      }
      if (tlog != null) {
        tlog.decref();
      }

      recoveryExecutor.shutdownNow();
    }
  }

  @Override
  public void bufferUpdates() {
    assert state == State.ACTIVE;
    recoveryInfo = new RecoveryInfo();

    // block all updates to eliminate race conditions
    // reading state and acting on it in the update processor
    versionInfo.blockUpdates();
    try {
      state = State.BUFFERING;
    } finally {
      versionInfo.unblockUpdates();
    }
    recoveryInfo = new RecoveryInfo();
  }

  /** Returns the Future to wait on, or null if no replay was needed */
  @Override
  public Future<RecoveryInfo> applyBufferedUpdates() {
    assert state == State.BUFFERING;

    // block all updates to eliminate race conditions
    // reading state and acting on it in the update processor
    versionInfo.blockUpdates();
    try {
      if (state != State.BUFFERING) return null;
      state = State.APPLYING_BUFFERED;

      // handle case when no log was even created because no updates
      // were received.
      if (tlog == null) {
        state = State.ACTIVE;
        return null;
      }

    } finally {
      versionInfo.unblockUpdates();
    }

    tlog.incref();
    if (recoveryExecutor.isShutdown()) {
      throw new RuntimeException("executore is not running...");
    }
    ExecutorCompletionService<RecoveryInfo> cs = new ExecutorCompletionService<RecoveryInfo>(recoveryExecutor);
    LogReplayer replayer = new LogReplayer(tlog, true);
    return cs.submit(replayer, recoveryInfo);
  }

  @Override
  public State getState() {
    return state;
  }


  public static Runnable testing_logReplayHook;  // called before each log read
  public static Runnable testing_logReplayFinishHook;  // called when log replay has finished



  private RecoveryInfo recoveryInfo;

  // TODO: do we let the log replayer run across core reloads?
  class LogReplayer implements Runnable {
    TransactionLog translog;
    TransactionLog.LogReader tlogReader;
    boolean activeLog;
    boolean finishing = false;  // state where we lock out other updates and finish those updates that snuck in before we locked


    public LogReplayer(TransactionLog translog, boolean activeLog) {
      this.translog = translog;
      this.activeLog = activeLog;
    }

    @Override
    public void run() {
      try {

        uhandler.core.log.warn("Starting log replay " + tlogReader);

        tlogReader = translog.getReader();

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(DistributedUpdateProcessor.SEEN_LEADER, true);
        SolrQueryRequest req = new LocalSolrQueryRequest(uhandler.core, params);
        SolrQueryResponse rsp = new SolrQueryResponse();

        // NOTE: we don't currently handle a core reload during recovery.  This would cause the core
        // to change underneath us.

        // TODO: use the standard request factory?  We won't get any custom configuration instantiating this way.
        RunUpdateProcessorFactory runFac = new RunUpdateProcessorFactory();
        DistributedUpdateProcessorFactory magicFac = new DistributedUpdateProcessorFactory();
        runFac.init(new NamedList());
        magicFac.init(new NamedList());

        UpdateRequestProcessor proc = magicFac.getInstance(req, rsp, runFac.getInstance(req, rsp, null));

        long commitVersion = 0;

        for(;;) {
          Object o = null;

          try {
            if (testing_logReplayHook != null) testing_logReplayHook.run();
            o = tlogReader.next();
            if (o == null && activeLog) {
              if (!finishing) {
                // block to prevent new adds, but don't immediately unlock since
                // we could be starved from ever completing recovery.  Only unlock
                // after we've finished this recovery.
                // NOTE: our own updates won't be blocked since the thread holding a write lock can
                // lock a read lock.
                versionInfo.blockUpdates();
                finishing = true;
                o = tlogReader.next();
              } else {
                // we had previously blocked updates, so this "null" from the log is final.

                // Wait until our final commit to change the state and unlock.
                // This is only so no new updates are written to the current log file, and is
                // only an issue if we crash before the commit (and we are paying attention
                // to incomplete log files).
                //
                // versionInfo.unblockUpdates();
              }
            }
          } catch (InterruptedException e) {
            SolrException.log(log,e);
          } catch (IOException e) {
            SolrException.log(log,e);
          }

          if (o == null) break;

          try {

            // should currently be a List<Oper,Ver,Doc/Id>
            List entry = (List)o;

            int oper = (Integer)entry.get(0);
            long version = (Long) entry.get(1);

            switch (oper) {
              case UpdateLog.ADD:
              {
                recoveryInfo.adds++;
                // byte[] idBytes = (byte[]) entry.get(2);
                SolrInputDocument sdoc = (SolrInputDocument)entry.get(entry.size()-1);
                AddUpdateCommand cmd = new AddUpdateCommand(req);
                // cmd.setIndexedId(new BytesRef(idBytes));
                cmd.solrDoc = sdoc;
                cmd.setVersion(version);
                cmd.setFlags(UpdateCommand.REPLAY | UpdateCommand.IGNORE_AUTOCOMMIT);
                proc.processAdd(cmd);
                break;

                // TODO: updates need to go through versioning code for handing reorders? (for replicas at least,
                // depending on how they recover.
              }
              case UpdateLog.DELETE:
              {
                recoveryInfo.deletes++;
                byte[] idBytes = (byte[]) entry.get(2);
                DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
                cmd.setIndexedId(new BytesRef(idBytes));
                cmd.setVersion(version);
                cmd.setFlags(UpdateCommand.REPLAY | UpdateCommand.IGNORE_AUTOCOMMIT);
                proc.processDelete(cmd);
                break;
              }

              case UpdateLog.DELETE_BY_QUERY:
              {
                recoveryInfo.deleteByQuery++;
                String query = (String)entry.get(2);
                DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
                cmd.query = query;
                cmd.setVersion(version);
                cmd.setFlags(UpdateCommand.REPLAY | UpdateCommand.IGNORE_AUTOCOMMIT);
                proc.processDelete(cmd);
                break;
              }

              case UpdateLog.COMMIT:
              {
                // currently we don't log commits
                commitVersion = version;
                break;
              }

              default:
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
            }
          } catch (IOException ex) {
            recoveryInfo.errors++;
            log.warn("IOException reading log", ex);
            // could be caused by an incomplete flush if recovering from log
          } catch (ClassCastException cl) {
            recoveryInfo.errors++;
            log.warn("Unexpected log entry or corrupt log.  Entry=" + o, cl);
            // would be caused by a corrupt transaction log
          } catch (Exception ex) {
            recoveryInfo.errors++;
            log.warn("Exception replaying log", ex);
            // something wrong with the request?
          }
        }

        CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
        cmd.setVersion(commitVersion);
        cmd.softCommit = false;
        cmd.waitSearcher = true;
        cmd.setFlags(UpdateCommand.REPLAY);
        try {
          uhandler.commit(cmd);
        } catch (IOException ex) {
          recoveryInfo.errors++;
          log.error("Replay exception: final commit.", ex);
        }

        try {
          proc.finish();
        } catch (IOException ex) {
          recoveryInfo.errors++;
          log.error("Replay exception: finish()", ex);
        }

        tlogReader.close();
        translog.decref();

      } catch (Exception e) {
        recoveryInfo.errors++;
        SolrException.log(log,e);
      } finally {
        // change the state while updates are still blocked to prevent races
        state = State.ACTIVE;
        if (finishing) {
          versionInfo.unblockUpdates();
        }
      }

      log.warn("Ending log replay " + tlogReader);

      if (testing_logReplayFinishHook != null) testing_logReplayFinishHook.run();
    }
  }

  // nocommit: i made this not static for my test that doesn't reinit statics after restart...
   ThreadPoolExecutor recoveryExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
      1, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

}




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

package org.apache.solr.update;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase.FROMLEADER;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** @lucene.experimental */
public class UpdateLog implements PluginInfoInitialized {
  private static final long STATUS_TIME = TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
  public static String LOG_FILENAME_PATTERN = "%s.%019d";
  public static String TLOG_NAME="tlog";

  public static Logger log = LoggerFactory.getLogger(UpdateLog.class);
  public boolean debug = log.isDebugEnabled();
  public boolean trace = log.isTraceEnabled();

  // TODO: hack
  public FileSystem getFs() {
    return null;
  }

  public enum SyncLevel { NONE, FLUSH, FSYNC;
    public static SyncLevel getSyncLevel(String level){
      if (level == null) {
        return SyncLevel.FLUSH;
      }
      try{
        return SyncLevel.valueOf(level.toUpperCase(Locale.ROOT));
      } catch(Exception ex){
        log.warn("There was an error reading the SyncLevel - default to " + SyncLevel.FLUSH, ex);
        return SyncLevel.FLUSH;
      }
    }
  }
  public enum State { REPLAYING, BUFFERING, APPLYING_BUFFERED, ACTIVE }

  public static final int ADD = 0x01;
  public static final int DELETE = 0x02;
  public static final int DELETE_BY_QUERY = 0x03;
  public static final int COMMIT = 0x04;
  // Flag indicating that this is a buffered operation, and that a gap exists before buffering started.
  // for example, if full index replication starts and we are buffering updates, then this flag should
  // be set to indicate that replaying the log would not bring us into sync (i.e. peersync should
  // fail if this flag is set on the last update in the tlog).
  public static final int FLAG_GAP = 0x10;
  public static final int OPERATION_MASK = 0x0f;  // mask off flags to get the operation

  public static class RecoveryInfo {
    public long positionOfStart;

    public int adds;
    public int deletes;
    public int deleteByQuery;
    public int errors;

    public boolean failed;

    @Override
    public String toString() {
      return "RecoveryInfo{adds="+adds+" deletes="+deletes+ " deleteByQuery="+deleteByQuery+" errors="+errors + " positionOfStart="+positionOfStart+"}";
    }
  }

  long id = -1;
  protected State state = State.ACTIVE;
  protected int operationFlags;  // flags to write in the transaction log with operations (i.e. FLAG_GAP)

  protected TransactionLog tlog;
  protected TransactionLog prevTlog;
  protected Deque<TransactionLog> logs = new LinkedList<>();  // list of recent logs, newest first
  protected LinkedList<TransactionLog> newestLogsOnStartup = new LinkedList<>();
  protected int numOldRecords;  // number of records in the recent logs

  protected Map<BytesRef,LogPtr> map = new HashMap<>();
  protected Map<BytesRef,LogPtr> prevMap;  // used while committing/reopening is happening
  protected Map<BytesRef,LogPtr> prevMap2;  // used while committing/reopening is happening
  protected TransactionLog prevMapLog;  // the transaction log used to look up entries found in prevMap
  protected TransactionLog prevMapLog2;  // the transaction log used to look up entries found in prevMap

  protected final int numDeletesToKeep = 1000;
  protected final int numDeletesByQueryToKeep = 100;
  public final int numRecordsToKeep = 100;

  // keep track of deletes only... this is not updated on an add
  protected LinkedHashMap<BytesRef, LogPtr> oldDeletes = new LinkedHashMap<BytesRef, LogPtr>(numDeletesToKeep) {
    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > numDeletesToKeep;
    }
  };

  public class DBQ {
    public String q;     // the query string
    public long version; // positive version of the DBQ

    @Override
    public String toString() {
      return "DBQ{version=" + version + ",q="+q+"}";
    }
  }

  protected LinkedList<DBQ> deleteByQueries = new LinkedList<>();

  protected String[] tlogFiles;
  protected File tlogDir;
  protected Collection<String> globalStrings;

  protected String dataDir;
  protected String lastDataDir;

  protected VersionInfo versionInfo;

  protected SyncLevel defaultSyncLevel = SyncLevel.FLUSH;

  volatile UpdateHandler uhandler;    // a core reload can change this reference!
  protected volatile boolean cancelApplyBufferUpdate;
  List<Long> startingVersions;
  int startingOperation;  // last operation in the logs on startup

  public static class LogPtr {
    final long pointer;
    final long version;

    public LogPtr(long pointer, long version) {
      this.pointer = pointer;
      this.version = version;
    }

    @Override
    public String toString() {
      return "LogPtr(" + pointer + ")";
    }
  }

  public long getTotalLogsSize() {
    long size = 0;
    synchronized (this) {
      for (TransactionLog log : logs) {
        size += log.getLogSize();
      }
    }
    return size;
  }

  public long getTotalLogsNumber() {
    synchronized (this) {
      return logs.size();
    }
  }

  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  @Override
  public void init(PluginInfo info) {
    dataDir = (String)info.initArgs.get("dir");
    defaultSyncLevel = SyncLevel.getSyncLevel((String)info.initArgs.get("syncLevel"));
  }

  /* Note, when this is called, uhandler is not completely constructed.
   * This must be called when a new log is created, or
   * for an existing log whenever the core or update handler changes.
   */
  public void init(UpdateHandler uhandler, SolrCore core) {
    dataDir = core.getUlogDir();

    this.uhandler = uhandler;

    if (dataDir.equals(lastDataDir)) {
      if (debug) {
        log.debug("UpdateHandler init: tlogDir=" + tlogDir + ", next id=" + id, " this is a reopen... nothing else to do.");
      }

      versionInfo.reload();

      // on a normal reopen, we currently shouldn't have to do anything
      return;
    }
    lastDataDir = dataDir;
    tlogDir = new File(dataDir, TLOG_NAME);
    tlogDir.mkdirs();
    tlogFiles = getLogList(tlogDir);
    id = getLastLogId() + 1;   // add 1 since we will create a new log for the next update

    if (debug) {
      log.debug("UpdateHandler init: tlogDir=" + tlogDir + ", existing tlogs=" + Arrays.asList(tlogFiles) + ", next id=" + id);
    }
    
    TransactionLog oldLog = null;
    for (String oldLogName : tlogFiles) {
      File f = new File(tlogDir, oldLogName);
      try {
        oldLog = new TransactionLog( f, null, true );
        addOldLog(oldLog, false);  // don't remove old logs on startup since more than one may be uncapped.
      } catch (Exception e) {
        SolrException.log(log, "Failure to open existing log file (non fatal) " + f, e);
        deleteFile(f);
      }
    }

    // Record first two logs (oldest first) at startup for potential tlog recovery.
    // It's possible that at abnormal close both "tlog" and "prevTlog" were uncapped.
    for (TransactionLog ll : logs) {
      newestLogsOnStartup.addFirst(ll);
      if (newestLogsOnStartup.size() >= 2) break;
    }

    try {
      versionInfo = new VersionInfo(this, 256);
    } catch (SolrException e) {
      log.error("Unable to use updateLog: " + e.getMessage(), e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "Unable to use updateLog: " + e.getMessage(), e);
    }

    // TODO: these startingVersions assume that we successfully recover from all non-complete tlogs.
    UpdateLog.RecentUpdates startingUpdates = getRecentUpdates();
    try {
      startingVersions = startingUpdates.getVersions(numRecordsToKeep);
      startingOperation = startingUpdates.getLatestOperation();

      // populate recent deletes list (since we can't get that info from the index)
      for (int i=startingUpdates.deleteList.size()-1; i>=0; i--) {
        DeleteUpdate du = startingUpdates.deleteList.get(i);
        oldDeletes.put(new BytesRef(du.id), new LogPtr(-1,du.version));
      }

      // populate recent deleteByQuery commands
      for (int i=startingUpdates.deleteByQueryList.size()-1; i>=0; i--) {
        Update update = startingUpdates.deleteByQueryList.get(i);
        List<Object> dbq = (List<Object>) update.log.lookup(update.pointer);
        long version = (Long) dbq.get(1);
        String q = (String) dbq.get(2);
        trackDeleteByQuery(q, version);
      }

    } finally {
      startingUpdates.close();
    }

  }
  
  public String getLogDir() {
    return tlogDir.getAbsolutePath();
  }
  
  public List<Long> getStartingVersions() {
    return startingVersions;
  }

  public int getStartingOperation() {
    return startingOperation;
  }

  /* Takes over ownership of the log, keeping it until no longer needed
     and then decrementing it's reference and dropping it.
   */
  protected synchronized void addOldLog(TransactionLog oldLog, boolean removeOld) {
    if (oldLog == null) return;

    numOldRecords += oldLog.numRecords();

    int currRecords = numOldRecords;

    if (oldLog != tlog &&  tlog != null) {
      currRecords += tlog.numRecords();
    }

    while (removeOld && logs.size() > 0) {
      TransactionLog log = logs.peekLast();
      int nrec = log.numRecords();
      // remove oldest log if we don't need it to keep at least numRecordsToKeep, or if
      // we already have the limit of 10 log files.
      if (currRecords - nrec >= numRecordsToKeep || logs.size() >= 10) {
        currRecords -= nrec;
        numOldRecords -= nrec;
        logs.removeLast().decref();  // dereference so it will be deleted when no longer in use
        continue;
      }

      break;
    }

    // don't incref... we are taking ownership from the caller.
    logs.addFirst(oldLog);
  }


  public String[] getLogList(File directory) {
    final String prefix = TLOG_NAME+'.';
    String[] names = directory.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(prefix);
      }
    });
    if (names == null) {
      throw new RuntimeException(new FileNotFoundException(directory.getAbsolutePath()));
    }
    Arrays.sort(names);
    return names;
  }


  public long getLastLogId() {
    if (id != -1) return id;
    if (tlogFiles.length == 0) return -1;
    String last = tlogFiles[tlogFiles.length-1];
    return Long.parseLong(last.substring(TLOG_NAME.length()+1));
  }


  public void add(AddUpdateCommand cmd) {
    add(cmd, false);
  }


  public void add(AddUpdateCommand cmd, boolean clearCaches) {
    // don't log if we are replaying from another log
    // TODO: we currently need to log to maintain correct versioning, rtg, etc
    // if ((cmd.getFlags() & UpdateCommand.REPLAY) != 0) return;

    synchronized (this) {
      long pos = -1;

      // don't log if we are replaying from another log
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog();
        pos = tlog.write(cmd, operationFlags);
      }

      if (!clearCaches) {
        // TODO: in the future we could support a real position for a REPLAY update.
        // Only currently would be useful for RTG while in recovery mode though.
        LogPtr ptr = new LogPtr(pos, cmd.getVersion());

        // only update our map if we're not buffering
        if ((cmd.getFlags() & UpdateCommand.BUFFERING) == 0) {
          map.put(cmd.getIndexedId(), ptr);
        }

        if (trace) {
          log.trace("TLOG: added id " + cmd.getPrintableId() + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
        }

      } else {
        // replicate the deleteByQuery logic.  See deleteByQuery for comments.

        if (map != null) map.clear();
        if (prevMap != null) prevMap.clear();
        if (prevMap2 != null) prevMap2.clear();

        try {
          RefCounted<SolrIndexSearcher> holder = uhandler.core.openNewSearcher(true, true);
          holder.decref();
        } catch (Exception e) {
          SolrException.log(log, "Error opening realtime searcher for deleteByQuery", e);
        }

        if (trace) {
          log.trace("TLOG: added id " + cmd.getPrintableId() + " to " + tlog + " clearCaches=true");
        }

      }

    }
  }



  public void delete(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();

    synchronized (this) {
      long pos = -1;

      // don't log if we are replaying from another log
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog();
        pos = tlog.writeDelete(cmd, operationFlags);
      }

      LogPtr ptr = new LogPtr(pos, cmd.version);

      // only update our map if we're not buffering
      if ((cmd.getFlags() & UpdateCommand.BUFFERING) == 0) {
        map.put(br, ptr);

        oldDeletes.put(br, ptr);
      }

      if (trace) {
        log.trace("TLOG: added delete for id " + cmd.id + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
      }
    }
  }

  public void deleteByQuery(DeleteUpdateCommand cmd) {
    synchronized (this) {
      long pos = -1;
      // don't log if we are replaying from another log
      if ((cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        ensureLog();
        pos = tlog.writeDeleteByQuery(cmd, operationFlags);
      }

      // only change our caches if we are not buffering
      if ((cmd.getFlags() & UpdateCommand.BUFFERING) == 0) {
        // given that we just did a delete-by-query, we don't know what documents were
        // affected and hence we must purge our caches.
        if (map != null) map.clear();
        if (prevMap != null) prevMap.clear();
        if (prevMap2 != null) prevMap2.clear();

        trackDeleteByQuery(cmd.getQuery(), cmd.getVersion());

        // oldDeletes.clear();

        // We must cause a new IndexReader to be opened before anything looks at these caches again
        // so that a cache miss will read fresh data.
        //
        // TODO: FUTURE: open a new searcher lazily for better throughput with delete-by-query commands
        try {
          RefCounted<SolrIndexSearcher> holder = uhandler.core.openNewSearcher(true, true);
          holder.decref();
        } catch (Exception e) {
          SolrException.log(log, "Error opening realtime searcher for deleteByQuery", e);
        }

      }

      LogPtr ptr = new LogPtr(pos, cmd.getVersion());

      if (trace) {
        log.trace("TLOG: added deleteByQuery " + cmd.query + " to " + tlog + " " + ptr + " map=" + System.identityHashCode(map));
      }
    }
  }

  /** currently for testing only */
  public void deleteAll() {
    synchronized (this) {

      try {
        RefCounted<SolrIndexSearcher> holder = uhandler.core.openNewSearcher(true, true);
        holder.decref();
      } catch (Exception e) {
        SolrException.log(log, "Error opening realtime searcher for deleteByQuery", e);
      }

      if (map != null) map.clear();
      if (prevMap != null) prevMap.clear();
      if (prevMap2 != null) prevMap2.clear();

      oldDeletes.clear();
      deleteByQueries.clear();
    }
  }


  void trackDeleteByQuery(String q, long version) {
    version = Math.abs(version);
    DBQ dbq = new DBQ();
    dbq.q = q;
    dbq.version = version;

    synchronized (this) {
      if (deleteByQueries.isEmpty() || deleteByQueries.getFirst().version < version) {
        // common non-reordered case
        deleteByQueries.addFirst(dbq);
      } else {
        // find correct insertion point
        ListIterator<DBQ> iter = deleteByQueries.listIterator();
        iter.next();  // we already checked the first element in the previous "if" clause
        while (iter.hasNext()) {
          DBQ oldDBQ = iter.next();
          if (oldDBQ.version < version) {
            iter.previous();
            break;
          } else if (oldDBQ.version == version && oldDBQ.q.equals(q)) {
            // a duplicate
            return;
          }
        }
        iter.add(dbq);  // this also handles the case of adding at the end when hasNext() == false
      }

      if (deleteByQueries.size() > numDeletesByQueryToKeep) {
        deleteByQueries.removeLast();
      }
    }
  }

  public List<DBQ> getDBQNewer(long version) {
    synchronized (this) {
      if (deleteByQueries.isEmpty() || deleteByQueries.getFirst().version < version) {
        // fast common case
        return null;
      }

      List<DBQ> dbqList = new ArrayList<>();
      for (DBQ dbq : deleteByQueries) {
        if (dbq.version <= version) break;
        dbqList.add(dbq);
      }
      return dbqList;
    }
  }

  protected void newMap() {
    prevMap2 = prevMap;
    prevMapLog2 = prevMapLog;

    prevMap = map;
    prevMapLog = tlog;

    map = new HashMap<>();
  }

  private void clearOldMaps() {
    prevMap = null;
    prevMap2 = null;
  }

  public boolean hasUncommittedChanges() {
    return tlog != null;
  }
  
  public void preCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      if (debug) {
        log.debug("TLOG: preCommit");
      }

      if (getState() != State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
        // if we aren't in the active state, and this isn't a replay
        // from the recovery process, then we shouldn't mess with
        // the current transaction log.  This normally shouldn't happen
        // as DistributedUpdateProcessor will prevent this.  Commits
        // that don't use the processor are possible though.
        return;
      }

      // since we're changing the log, we must change the map.
      newMap();

      if (prevTlog != null) {
        globalStrings = prevTlog.getGlobalStrings();
      }

      // since document additions can happen concurrently with commit, create
      // a new transaction log first so that we know the old one is definitely
      // in the index.
      prevTlog = tlog;
      tlog = null;
      id++;
    }
  }

  public void postCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      if (debug) {
        log.debug("TLOG: postCommit");
      }
      if (prevTlog != null) {
        // if we made it through the commit, write a commit command to the log
        // TODO: check that this works to cap a tlog we were using to buffer so we don't replay on startup.
        prevTlog.writeCommit(cmd, operationFlags);

        addOldLog(prevTlog, true);
        // the old log list will decref when no longer needed
        // prevTlog.decref();
        prevTlog = null;
      }
    }
  }

  public void preSoftCommit(CommitUpdateCommand cmd) {
    debug = log.isDebugEnabled(); // refresh our view of debugging occasionally
    trace = log.isTraceEnabled();

    synchronized (this) {

      if (!cmd.softCommit) return;  // already handled this at the start of the hard commit
      newMap();

      // start adding documents to a new map since we won't know if
      // any added documents will make it into this commit or not.
      // But we do know that any updates already added will definitely
      // show up in the latest reader after the commit succeeds.
      map = new HashMap<>();

      if (debug) {
        log.debug("TLOG: preSoftCommit: prevMap="+ System.identityHashCode(prevMap) + " new map=" + System.identityHashCode(map));
      }
    }
  }

  public void postSoftCommit(CommitUpdateCommand cmd) {
    synchronized (this) {
      // We can clear out all old maps now that a new searcher has been opened.
      // This currently only works since DUH2 synchronizes around preCommit to avoid
      // it being called in the middle of a preSoftCommit, postSoftCommit sequence.
      // If this DUH2 synchronization were to be removed, preSoftCommit should
      // record what old maps were created and only remove those.

      if (debug) {
        SolrCore.verbose("TLOG: postSoftCommit: disposing of prevMap="+ System.identityHashCode(prevMap) + ", prevMap2=" + System.identityHashCode(prevMap2));
      }
      clearOldMaps();
    }
  }

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
      currLog.finish(syncLevel);
    } finally {
      currLog.decref();
    }
  }


  public Future<RecoveryInfo> recoverFromLog() {
    recoveryInfo = new RecoveryInfo();

    List<TransactionLog> recoverLogs = new ArrayList<>(1);
    for (TransactionLog ll : newestLogsOnStartup) {
      if (!ll.try_incref()) continue;

      try {
        if (ll.endsWithCommit()) {
          ll.decref();
          continue;
        }
      } catch (IOException e) {
        log.error("Error inspecting tlog " + ll, e);
        ll.decref();
        continue;
      }

      recoverLogs.add(ll);
    }

    if (recoverLogs.isEmpty()) return null;

    ExecutorCompletionService<RecoveryInfo> cs = new ExecutorCompletionService<>(recoveryExecutor);
    LogReplayer replayer = new LogReplayer(recoverLogs, false);

    versionInfo.blockUpdates();
    try {
      state = State.REPLAYING;
    } finally {
      versionInfo.unblockUpdates();
    }

    // At this point, we are guaranteed that any new updates coming in will see the state as "replaying"

    return cs.submit(replayer, recoveryInfo);
  }


  protected void ensureLog() {
    if (tlog == null) {
      String newLogName = String.format(Locale.ROOT, LOG_FILENAME_PATTERN, TLOG_NAME, id);
      tlog = new TransactionLog(new File(tlogDir, newLogName), globalStrings);
    }
  }


  private void doClose(TransactionLog theLog, boolean writeCommit) {
    if (theLog != null) {
      if (writeCommit) {
        // record a commit
        log.info("Recording current closed for " + uhandler.core + " log=" + theLog);
        CommitUpdateCommand cmd = new CommitUpdateCommand(new LocalSolrQueryRequest(uhandler.core, new ModifiableSolrParams((SolrParams)null)), false);
        theLog.writeCommit(cmd, operationFlags);
      }

      theLog.deleteOnClose = false;
      theLog.decref();
      theLog.forceClose();
    }
  }
  
  public void close(boolean committed) {
    close(committed, false);
  }
  
  public void close(boolean committed, boolean deleteOnClose) {
    synchronized (this) {
      try {
        ExecutorUtil.shutdownNowAndAwaitTermination(recoveryExecutor);
      } catch (Exception e) {
        SolrException.log(log, e);
      }

      // Don't delete the old tlogs, we want to be able to replay from them and retrieve old versions

      doClose(prevTlog, committed);
      doClose(tlog, committed);

      for (TransactionLog log : logs) {
        if (log == prevTlog || log == tlog) continue;
        log.deleteOnClose = false;
        log.decref();
        log.forceClose();
      }

    }
  }


  static class Update {
    TransactionLog log;
    long version;
    long pointer;
  }

  static class DeleteUpdate {
    long version;
    byte[] id;

    public DeleteUpdate(long version, byte[] id) {
      this.version = version;
      this.id = id;
    }
  }
  
  public class RecentUpdates {
    Deque<TransactionLog> logList;    // newest first
    List<List<Update>> updateList;
    HashMap<Long, Update> updates;
    List<Update> deleteByQueryList;
    List<DeleteUpdate> deleteList;
    int latestOperation;

    public List<Long> getVersions(int n) {
      List<Long> ret = new ArrayList(n);
      
      for (List<Update> singleList : updateList) {
        for (Update ptr : singleList) {
          ret.add(ptr.version);
          if (--n <= 0) return ret;
        }
      }
      
      return ret;
    }
    
    public Object lookup(long version) {
      Update update = updates.get(version);
      if (update == null) return null;

      return update.log.lookup(update.pointer);
    }

    /** Returns the list of deleteByQueries that happened after the given version */
    public List<Object> getDeleteByQuery(long afterVersion) {
      List<Object> result = new ArrayList<>(deleteByQueryList.size());
      for (Update update : deleteByQueryList) {
        if (Math.abs(update.version) > afterVersion) {
          Object dbq = update.log.lookup(update.pointer);
          result.add(dbq);
        }
      }
      return result;
    }

    public int getLatestOperation() {
      return latestOperation;
    }


    private void update() {
      int numUpdates = 0;
      updateList = new ArrayList<>(logList.size());
      deleteByQueryList = new ArrayList<>();
      deleteList = new ArrayList<>();
      updates = new HashMap<>(numRecordsToKeep);

      for (TransactionLog oldLog : logList) {
        List<Update> updatesForLog = new ArrayList<>();

        TransactionLog.ReverseReader reader = null;
        try {
          reader = oldLog.getReverseReader();

          while (numUpdates < numRecordsToKeep) {
            Object o = null;
            try {
              o = reader.next();
              if (o==null) break;
              
              // should currently be a List<Oper,Ver,Doc/Id>
              List entry = (List)o;

              // TODO: refactor this out so we get common error handling
              int opAndFlags = (Integer)entry.get(0);
              if (latestOperation == 0) {
                latestOperation = opAndFlags;
              }
              int oper = opAndFlags & UpdateLog.OPERATION_MASK;
              long version = (Long) entry.get(1);

              switch (oper) {
                case UpdateLog.ADD:
                case UpdateLog.DELETE:
                case UpdateLog.DELETE_BY_QUERY:
                  Update update = new Update();
                  update.log = oldLog;
                  update.pointer = reader.position();
                  update.version = version;

                  updatesForLog.add(update);
                  updates.put(version, update);
                  
                  if (oper == UpdateLog.DELETE_BY_QUERY) {
                    deleteByQueryList.add(update);
                  } else if (oper == UpdateLog.DELETE) {
                    deleteList.add(new DeleteUpdate(version, (byte[])entry.get(2)));
                  }
                  
                  break;

                case UpdateLog.COMMIT:
                  break;
                default:
                  throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
              }
            } catch (ClassCastException cl) {
              log.warn("Unexpected log entry or corrupt log.  Entry=" + o, cl);
              // would be caused by a corrupt transaction log
            } catch (Exception ex) {
              log.warn("Exception reverse reading log", ex);
              break;
            }

            numUpdates++;
          }

        } catch (IOException e) {
          // failure to read a log record isn't fatal
          log.error("Exception reading versions from log",e);
        } finally {
          if (reader != null) reader.close();
        }

        updateList.add(updatesForLog);
      }

    }
    
    public void close() {
      for (TransactionLog log : logList) {
        log.decref();
      }
    }
  }

  /** The RecentUpdates object returned must be closed after use */
  public RecentUpdates getRecentUpdates() {
    Deque<TransactionLog> logList;
    synchronized (this) {
      logList = new LinkedList<>(logs);
      for (TransactionLog log : logList) {
        log.incref();
      }
      if (prevTlog != null) {
        prevTlog.incref();
        logList.addFirst(prevTlog);
      }
      if (tlog != null) {
        tlog.incref();
        logList.addFirst(tlog);
      }
    }

    // TODO: what if I hand out a list of updates, then do an update, then hand out another list (and
    // one of the updates I originally handed out fell off the list).  Over-request?

    boolean success = false;
    RecentUpdates recentUpdates = null;
    try {
      recentUpdates = new RecentUpdates();
      recentUpdates.logList = logList;
      recentUpdates.update();
      success = true;
    } finally {
      // defensive: if some unknown exception is thrown,
      // make sure we close so that the tlogs are decref'd
      if (!success && recentUpdates != null) {
        recentUpdates.close();
      }
    }

    return recentUpdates;
  }

  public void bufferUpdates() {
    // recovery trips this assert under some race - even when
    // it checks the state first
    // assert state == State.ACTIVE;

    recoveryInfo = new RecoveryInfo();

    // block all updates to eliminate race conditions
    // reading state and acting on it in the update processor
    versionInfo.blockUpdates();
    try {
      if (state != State.ACTIVE) return;

      if (log.isInfoEnabled()) {
        log.info("Starting to buffer updates. " + this);
      }

      // since we blocked updates, this synchronization shouldn't strictly be necessary.
      synchronized (this) {
        recoveryInfo.positionOfStart = tlog == null ? 0 : tlog.snapshot();
      }

      state = State.BUFFERING;

      // currently, buffering is only called by recovery, meaning that there is most likely a gap in updates
      operationFlags |= FLAG_GAP;
    } finally {
      versionInfo.unblockUpdates();
    }
  }

  /** Returns true if we were able to drop buffered updates and return to the ACTIVE state */
  public boolean dropBufferedUpdates() {
    versionInfo.blockUpdates();
    try {
      if (state != State.BUFFERING) return false;

      if (log.isInfoEnabled()) {
        log.info("Dropping buffered updates " + this);
      }

      // since we blocked updates, this synchronization shouldn't strictly be necessary.
      synchronized (this) {
        if (tlog != null) {
          tlog.rollback(recoveryInfo.positionOfStart);
        }
      }

      state = State.ACTIVE;
      operationFlags &= ~FLAG_GAP;
    } catch (IOException e) {
      SolrException.log(log,"Error attempting to roll back log", e);
      return false;
    }
    finally {
      versionInfo.unblockUpdates();
    }
    return true;
  }


  /** Returns the Future to wait on, or null if no replay was needed */
  public Future<RecoveryInfo> applyBufferedUpdates() {
    // recovery trips this assert under some race - even when
    // it checks the state first
    // assert state == State.BUFFERING;

    // block all updates to eliminate race conditions
    // reading state and acting on it in the update processor
    versionInfo.blockUpdates();
    try {
      cancelApplyBufferUpdate = false;
      if (state != State.BUFFERING) return null;
      operationFlags &= ~FLAG_GAP;

      // handle case when no log was even created because no updates
      // were received.
      if (tlog == null) {
        state = State.ACTIVE;
        return null;
      }
      tlog.incref();
      state = State.APPLYING_BUFFERED;
    } finally {
      versionInfo.unblockUpdates();
    }

    if (recoveryExecutor.isShutdown()) {
      tlog.decref();
      throw new RuntimeException("executor is not running...");
    }
    ExecutorCompletionService<RecoveryInfo> cs = new ExecutorCompletionService<>(recoveryExecutor);
    LogReplayer replayer = new LogReplayer(Arrays.asList(new TransactionLog[]{tlog}), true);
    return cs.submit(replayer, recoveryInfo);
  }

  public State getState() {
    return state;
  }

  @Override
  public String toString() {
    return "FSUpdateLog{state="+getState()+", tlog="+tlog+"}";
  }


  public static Runnable testing_logReplayHook;  // called before each log read
  public static Runnable testing_logReplayFinishHook;  // called when log replay has finished



  protected RecoveryInfo recoveryInfo;

  class LogReplayer implements Runnable {
    private Logger loglog = log;  // set to something different?

    Deque<TransactionLog> translogs;
    TransactionLog.LogReader tlogReader;
    boolean activeLog;
    boolean finishing = false;  // state where we lock out other updates and finish those updates that snuck in before we locked
    boolean debug = loglog.isDebugEnabled();

    public LogReplayer(List<TransactionLog> translogs, boolean activeLog) {
      this.translogs = new LinkedList<>();
      this.translogs.addAll(translogs);
      this.activeLog = activeLog;
    }



    private SolrQueryRequest req;
    private SolrQueryResponse rsp;


    @Override
    public void run() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(DISTRIB_UPDATE_PARAM, FROMLEADER.toString());
      params.set(DistributedUpdateProcessor.LOG_REPLAY, "true");
      req = new LocalSolrQueryRequest(uhandler.core, params);
      rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));    // setting request info will help logging

      try {
        for(;;) {
          TransactionLog translog = translogs.pollFirst();
          if (translog == null) break;
          doReplay(translog);
        }
      } catch (SolrException e) {
        if (e.code() == ErrorCode.SERVICE_UNAVAILABLE.code) {
          SolrException.log(log, e);
          recoveryInfo.failed = true;
        } else {
          recoveryInfo.errors++;
          SolrException.log(log, e);
        }
      } catch (Exception e) {
        recoveryInfo.errors++;
        SolrException.log(log, e);
      } finally {
        // change the state while updates are still blocked to prevent races
        state = State.ACTIVE;
        if (finishing) {
          versionInfo.unblockUpdates();
        }

        // clean up in case we hit some unexpected exception and didn't get
        // to more transaction logs
        for (TransactionLog translog : translogs) {
          log.error("ERROR: didn't get to recover from tlog " + translog);
          translog.decref();
        }
      }

      loglog.warn("Log replay finished. recoveryInfo=" + recoveryInfo);

      if (testing_logReplayFinishHook != null) testing_logReplayFinishHook.run();

      SolrRequestInfo.clearRequestInfo();
    }


    public void doReplay(TransactionLog translog) {
      try {
        loglog.warn("Starting log replay " + translog + " active="+activeLog + " starting pos=" + recoveryInfo.positionOfStart);
        long lastStatusTime = System.nanoTime();
        tlogReader = translog.getReader(recoveryInfo.positionOfStart);

        // NOTE: we don't currently handle a core reload during recovery.  This would cause the core
        // to change underneath us.

        UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessingChain(null);
        UpdateRequestProcessor proc = processorChain.createProcessor(req, rsp);

        long commitVersion = 0;
        int operationAndFlags = 0;
        long nextCount = 0;

        for(;;) {
          Object o = null;
          if (cancelApplyBufferUpdate) break;
          try {
            if (testing_logReplayHook != null) testing_logReplayHook.run();
            if (nextCount++ % 1000 == 0) {
              long now = System.nanoTime();
              if (now - lastStatusTime > STATUS_TIME) {
                lastStatusTime = now;
                long cpos = tlogReader.currentPos();
                long csize = tlogReader.currentSize();
                loglog.info(
                        "log replay status {} active={} starting pos={} current pos={} current size={} % read={}",
                        translog, activeLog, recoveryInfo.positionOfStart, cpos, csize,
                        Math.round(cpos / (double) csize * 100.));
                
              }
            }
            
            o = null;
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
          } catch (Exception e) {
            SolrException.log(log,e);
          }

          if (o == null) break;

          try {

            // should currently be a List<Oper,Ver,Doc/Id>
            List entry = (List)o;

            operationAndFlags = (Integer)entry.get(0);
            int oper = operationAndFlags & OPERATION_MASK;
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
                if (debug) log.debug("add " +  cmd);

                proc.processAdd(cmd);
                break;
              }
              case UpdateLog.DELETE:
              {
                recoveryInfo.deletes++;
                byte[] idBytes = (byte[]) entry.get(2);
                DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
                cmd.setIndexedId(new BytesRef(idBytes));
                cmd.setVersion(version);
                cmd.setFlags(UpdateCommand.REPLAY | UpdateCommand.IGNORE_AUTOCOMMIT);
                if (debug) log.debug("delete " +  cmd);
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
                if (debug) log.debug("deleteByQuery " +  cmd);
                proc.processDelete(cmd);
                break;
              }

              case UpdateLog.COMMIT:
              {
                commitVersion = version;
                break;
              }

              default:
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,  "Unknown Operation! " + oper);
            }

            if (rsp.getException() != null) {
              loglog.error("REPLAY_ERR: Exception replaying log", rsp.getException());
              throw rsp.getException();
            }
          } catch (IOException ex) {
            recoveryInfo.errors++;
            loglog.warn("REYPLAY_ERR: IOException reading log", ex);
            // could be caused by an incomplete flush if recovering from log
          } catch (ClassCastException cl) {
            recoveryInfo.errors++;
            loglog.warn("REPLAY_ERR: Unexpected log entry or corrupt log.  Entry=" + o, cl);
            // would be caused by a corrupt transaction log
          }  catch (SolrException ex) {
            if (ex.code() == ErrorCode.SERVICE_UNAVAILABLE.code) {
              throw ex;
            }
            recoveryInfo.errors++;
            loglog.warn("REYPLAY_ERR: IOException reading log", ex);
            // could be caused by an incomplete flush if recovering from log
          } catch (Exception ex) {
            recoveryInfo.errors++;
            loglog.warn("REPLAY_ERR: Exception replaying log", ex);
            // something wrong with the request?
          }
        }

        CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
        cmd.setVersion(commitVersion);
        cmd.softCommit = false;
        cmd.waitSearcher = true;
        cmd.setFlags(UpdateCommand.REPLAY);
        try {
          if (debug) log.debug("commit " +  cmd);
          uhandler.commit(cmd);          // this should cause a commit to be added to the incomplete log and avoid it being replayed again after a restart.
        } catch (IOException ex) {
          recoveryInfo.errors++;
          loglog.error("Replay exception: final commit.", ex);
        }

        if (!activeLog) {
          // if we are replaying an old tlog file, we need to add a commit to the end
          // so we don't replay it again if we restart right after.

          // if the last operation we replayed had FLAG_GAP set, we want to use that again so we don't lose it
          // as the flag on the last operation.
          translog.writeCommit(cmd, operationFlags | (operationAndFlags & ~OPERATION_MASK));
        }

        try {
          proc.finish();
        } catch (IOException ex) {
          recoveryInfo.errors++;
          loglog.error("Replay exception: finish()", ex);
        }

      } finally {
        if (tlogReader != null) tlogReader.close();
        translog.decref();
      }
    }
  }

  public void cancelApplyBufferedUpdates() {
    this.cancelApplyBufferUpdate = true;
  }

  ThreadPoolExecutor recoveryExecutor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("recoveryExecutor"));


  public static void deleteFile(File file) {
    boolean success = false;
    try {
      Files.deleteIfExists(file.toPath());
      success = true;
    } catch (Exception e) {
      log.error("Error deleting file: " + file, e);
    }

    if (!success) {
      try {
        file.deleteOnExit();
      } catch (Exception e) {
        log.error("Error deleting file on exit: " + file, e);
      }
    }
  }
  
  protected String getTlogDir(SolrCore core, PluginInfo info) {
    String dataDir = (String) info.initArgs.get("dir");
    
    String ulogDir = core.getCoreDescriptor().getUlogDir();
    if (ulogDir != null) {
      dataDir = ulogDir;
    }
    
    if (dataDir == null || dataDir.length() == 0) {
      dataDir = core.getDataDir();
    }

    return dataDir + "/" + TLOG_NAME;
  }
  
  /**
   * Clears the logs on the file system. Only call before init.
   * 
   * @param core the SolrCore
   * @param ulogPluginInfo the init info for the UpdateHandler
   */
  public void clearLog(SolrCore core, PluginInfo ulogPluginInfo) {
    if (ulogPluginInfo == null) return;
    File tlogDir = new File(getTlogDir(core, ulogPluginInfo));
    if (tlogDir.exists()) {
      String[] files = getLogList(tlogDir);
      for (String file : files) {
        File f = new File(tlogDir, file);
        try {
          Files.delete(f.toPath());
        } catch (IOException cause) {
          // NOTE: still throws SecurityException as before.
          log.error("Could not remove tlog file:" + f, cause);
        }
      }
    }
  }
  
}


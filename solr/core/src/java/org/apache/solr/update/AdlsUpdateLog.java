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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.store.adls.AdlsProvider;
import org.apache.solr.util.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.experimental */
public class AdlsUpdateLog extends UpdateLog {

  private final Object fsLock = new Object();
  private volatile String tlogDir;
  private Integer tlogDfsReplication;
  private AdlsProvider provider;

  private boolean firstTime=true;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean debug = log.isDebugEnabled();

  // used internally by tests to track total count of failed tran log loads in init
  public static AtomicLong INIT_FAILED_LOGS_COUNT = new AtomicLong();

  public AdlsUpdateLog(AdlsProvider provider) {
    this.provider = provider;
  }

  @Override
  public void init(PluginInfo info) {
    super.init(info);
  }

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {

    // ulogDir from CoreDescriptor overrides
    String ulogDir = core.getCoreDescriptor().getUlogDir();

    this.uhandler = uhandler;

    synchronized (fsLock) {
      // just like dataDir, we do not allow
      // moving the tlog dir on reload
      if (firstTime) {
        if (ulogDir != null) {
          dataDir = ulogDir;
        }
        if (dataDir == null || dataDir.length() == 0) {
          dataDir = core.getDataDir();
        }

        if (!core.getDirectoryFactory().isAbsolute(dataDir)) {
          try {
            dataDir = core.getDirectoryFactory().getDataHome(core.getCoreDescriptor());
          } catch (IOException e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        }
        firstTime=false;
      } else {
        if (debug) {
          log.debug("UpdateHandler init: tlogDir=" + tlogDir + ", next id=" + id,
              " this is a reopen or double init ... nothing else to do.");
        }
        versionInfo.reload();
        return;
      }
    }

    tlogDir = dataDir+"/"+TLOG_NAME;
      try {
      if (!provider.checkExists(tlogDir)) {
        boolean success = provider.createDirectory(tlogDir);
        if (!success) {
          throw new RuntimeException("Could not create directory:" + tlogDir);
        }
      }
    } catch (IOException e){
      throw new RuntimeException(e);
    }

    List<String> oldBufferTlog = getBufferLogList(provider, tlogDir);
    if (oldBufferTlog != null && oldBufferTlog.size() != 0) {
      existOldBufferLog = true;
    }


    List<String> logList = getLogList(provider, tlogDir);
    tlogFiles = new String[logList.size()];
    logList.toArray(tlogFiles);
    id = getLastLogId() + 1; // add 1 since we will create a new log for the
    // next update

    if (debug) {
      log.debug("UpdateHandler init: tlogDir=" + tlogDir + ", existing tlogs="
          + Arrays.asList(tlogFiles) + ", next id=" + id);
    }

    TransactionLog oldLog = null;
    for (String oldLogName : tlogFiles) {
      String f = tlogDir+"/"+oldLogName;
      try {
        oldLog = new AdlsTransactionLog(provider, f, null, true);
        addOldLog(oldLog, false); // don't remove old logs on startup since more
        // than one may be uncapped.
      } catch (Exception e) {
        INIT_FAILED_LOGS_COUNT.incrementAndGet();
        SolrException.log(log, "Failure to open existing log file (non fatal) "
            + f, e);
        try {
          provider.delete(f);
        } catch (IOException e1) {
          throw new RuntimeException(e1);
        }
      }
    }

    // Record first two logs (oldest first) at startup for potential tlog
    // recovery.
    // It's possible that at abnormal close both "tlog" and "prevTlog" were
    // uncapped.
    for (TransactionLog ll : logs) {
      if (newestLogsOnStartup.size() < 2) {
        newestLogsOnStartup.addFirst(ll);
      } else {
        // We're never going to modify old non-recovery logs - no need to hold their output open
        log.info("Closing output for old non-recovery log " + ll);
        ll.closeOutput();
      }
    }

    try {
      versionInfo = new VersionInfo(this, numVersionBuckets);
    } catch (SolrException e) {
      log.error("Unable to use updateLog: " + e.getMessage(), e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unable to use updateLog: " + e.getMessage(), e);
    }

    // TODO: these startingVersions assume that we successfully recover from all
    // non-complete tlogs.
    try (RecentUpdates startingUpdates = getRecentUpdates()) {
      startingVersions = startingUpdates.getVersions(getNumRecordsToKeep());

      // populate recent deletes list (since we can't get that info from the
      // index)
      for (int i = startingUpdates.deleteList.size() - 1; i >= 0; i--) {
        DeleteUpdate du = startingUpdates.deleteList.get(i);
        oldDeletes.put(new BytesRef(du.id), new LogPtr(-1, du.version));
      }

      // populate recent deleteByQuery commands
      for (int i = startingUpdates.deleteByQueryList.size() - 1; i >= 0; i--) {
        Update update = startingUpdates.deleteByQueryList.get(i);
        List<Object> dbq = (List<Object>) update.log.lookup(update.pointer);
        long version = (Long) dbq.get(1);
        String q = (String) dbq.get(2);
        trackDeleteByQuery(q, version);
      }

    }

    // initialize metrics
    core.getCoreMetricManager().registerMetricProducer(SolrInfoBean.Category.TLOG.toString(), this);
  }

  @Override
  public String getLogDir() {
    return tlogDir;
  }

  public static List<String> getBufferLogList(AdlsProvider provider,String tlogDir)  {
    try {
      final String prefix = BUFFER_TLOG_NAME+'.';

      return provider.enumerateDirectory(tlogDir)
          .stream()
          .filter((entry)->entry.name.startsWith(prefix))
          .map((entry)->entry.name)
          .collect(Collectors.toList());

    } catch (IOException e){
      throw new RuntimeException(e);
    }

//    assert fs != null;
//    FileStatus[] fileStatuses;
//    try {
//      fileStatuses = fs.listStatus(tlogDir, path -> path.getName().startsWith(prefix));
//    } catch (IOException e) {
//      throw new SolrException(ErrorCode.SERVER_ERROR, "Failed on listing old buffer tlog", e);
//    }
//
//    String[] names = new String[fileStatuses.length];
//    for (int i = 0; i < fileStatuses.length; i++) {
//      names[i] = fileStatuses[i].getPath().getName();
//    }
//    return names;
  }

  public static List<String> getLogList(AdlsProvider provider, String tlogDir)  {
    try {
      final String prefix = TLOG_NAME + '.';

      return provider.enumerateDirectory(tlogDir)
          .stream()
          .filter((entry)->entry.name.startsWith(prefix))
          .map((entry)->entry.name)
          .collect(Collectors.toList());

    } catch (IOException e){
      throw new RuntimeException(e);
    }

//    final String prefix = TLOG_NAME + '.';
//    assert fs != null;
//    FileStatus[] fileStatuses;
//    try {
//      fileStatuses = fs.listStatus(tlogDir, new PathFilter() {
//
//        @Override
//        public boolean accept(Path path) {
//          return path.getName().startsWith(prefix);
//        }
//      });
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//    String[] names = new String[fileStatuses.length];
//    for (int i = 0; i < fileStatuses.length; i++) {
//      names[i] = fileStatuses[i].getPath().getName();
//    }
//    Arrays.sort(names);
//
//    return names;
  }

  @Override
  public void close(boolean committed) {
    close(committed, false);
  }

  @Override
  public void close(boolean committed, boolean deleteOnClose) {
    try {
      super.close(committed, deleteOnClose);
    } finally {
    }
  }

  @Override
  protected void ensureBufferTlog() {
    if (bufferTlog != null) return;
    String newLogName = String.format(Locale.ROOT, LOG_FILENAME_PATTERN, BUFFER_TLOG_NAME, System.nanoTime());
    bufferTlog = new AdlsTransactionLog(provider, (tlogDir+"/"+newLogName), globalStrings, false);

    bufferTlog.isBuffer = true;
  }

  @Override
  protected void deleteBufferLogs() {
    // Delete old buffer logs
    List<String> oldBufferTlog = getBufferLogList(provider, tlogDir);
    if (!CollectionUtils.isEmpty(oldBufferTlog)) {
      for (String oldBufferLogName : oldBufferTlog) {
        String f = tlogDir +"/"+oldBufferLogName;
        try {
          boolean s = provider.delete(f);
          if (!s) {
            log.error("Could not remove old buffer tlog file:" + f);
          }
        } catch (IOException e) {
          // No need to bubble up this exception, because it won't cause any problems on recovering
          log.error("Could not remove old buffer tlog file:" + f, e);
        }
      }
    }
  }

  @Override
  protected void ensureLog() {
    if (tlog == null) {
      String newLogName = String.format(Locale.ROOT, LOG_FILENAME_PATTERN,
          TLOG_NAME, id);

      AdlsTransactionLog ntlog = new AdlsTransactionLog(provider,(tlogDir+"/"+newLogName),globalStrings,false);

      tlog = ntlog;
    }
  }

  /**
   * Clears the logs on the file system. Only call before init.
   *
   * @param core the SolrCore
   * @param ulogPluginInfo the init info for the UpdateHandler
   */
  @Override
  public void clearLog(SolrCore core, PluginInfo ulogPluginInfo) {
    if (ulogPluginInfo == null) return;
    String tlogDir = getTlogDir(core, ulogPluginInfo);

    try {
      if (provider.checkExists(tlogDir)) {
        List<String> files = getLogList(provider,tlogDir);
        for (String file : files) {
          boolean s = provider.delete(tlogDir+"/"+file);
          if (!s) {
            log.error("Could not remove tlog file:" + tlogDir+"/"+file);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public void preSoftCommit(CommitUpdateCommand cmd) {
    debug = log.isDebugEnabled();
    super.preSoftCommit(cmd);
  }

//  public List<String> getLogList(String tlogDir) throws FileNotFoundException, IOException {
//    try {
//      final String prefix = TLOG_NAME+'.';
//
//      return provider.enumerateDirectory(tlogDir)
//          .stream()
//          .filter((entry)->entry.name.startsWith(prefix))
//          .map((entry)->entry.name)
//          .collect(Collectors.toList());
//
//    } catch (IOException e){
//      throw new RuntimeException(e);
//    }

//    FileStatus[] files = fs.listStatus(tlogDir, new PathFilter() {
//
//      @Override
//      public boolean accept(Path name) {
//        return name.getName().startsWith(prefix);
//      }
//    });
//    List<String> fileList = new ArrayList<>(files.length);
//    for (FileStatus file : files) {
//      fileList.add(file.getPath().getName());
//    }
//    return fileList.toArray(new String[0]);
//  }

  /**
   * Returns true if we were able to drop buffered updates and return to the
   * ACTIVE state
   */
  // public boolean dropBufferedUpdates() {
  // versionInfo.blockUpdates();
  // try {
  // if (state != State.BUFFERING) return false;
  //
  // if (log.isInfoEnabled()) {
  // log.info("Dropping buffered updates " + this);
  // }
  //
  // // since we blocked updates, this synchronization shouldn't strictly be
  // necessary.
  // synchronized (this) {
  // if (tlog != null) {
  // tlog.rollback(recoveryInfo.positionOfStart);
  // }
  // }
  //
  // state = State.ACTIVE;
  // operationFlags &= ~FLAG_GAP;
  // } catch (IOException e) {
  // SolrException.log(log,"Error attempting to roll back log", e);
  // return false;
  // }
  // finally {
  // versionInfo.unblockUpdates();
  // }
  // return true;
  // }

  public String toString() {
    return "AdlsUpdateLog{state=" + getState() + ", tlog=" + tlog + "}";
  }

}

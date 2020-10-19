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
import org.apache.solr.util.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.experimental
 * @deprecated since 8.6; see <a href="https://cwiki.apache.org/confluence/display/SOLR/Deprecations">Deprecations</a>
 */
public class HdfsUpdateLog extends UpdateLog {
  
  private final Object fsLock = new Object();
  private FileSystem fs;
  private volatile Path tlogDir;
  private final String confDir;
  private Integer tlogDfsReplication;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean debug = log.isDebugEnabled();
  
  // used internally by tests to track total count of failed tran log loads in init
  public static AtomicLong INIT_FAILED_LOGS_COUNT = new AtomicLong();

  public HdfsUpdateLog() {
    this(null);
  }
  
  public HdfsUpdateLog(String confDir) {
    this.confDir = confDir;
  }
  
  @Override
  public void init(PluginInfo info) {
    super.init(info);
    
    tlogDfsReplication = (Integer) info.initArgs.get( "tlogDfsReplication");
    if (tlogDfsReplication == null) tlogDfsReplication = 3;

    log.info("Initializing HdfsUpdateLog: tlogDfsReplication={}", tlogDfsReplication);
  }

  private Configuration getConf(Path path) {
    Configuration conf = new Configuration();
    if (confDir != null) {
      HdfsUtil.addHdfsResources(conf, confDir);
    }

    String fsScheme = path.toUri().getScheme();
    if(fsScheme != null) {
      conf.setBoolean("fs." + fsScheme + ".impl.disable.cache", true);
    }
    return conf;
  }
  
  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
    
    // ulogDir from CoreDescriptor overrides
    String ulogDir = core.getCoreDescriptor().getUlogDir();

    this.uhandler = uhandler;

    synchronized (fsLock) {
      // just like dataDir, we do not allow
      // moving the tlog dir on reload
      if (fs == null) {
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
        
        try {
          Path dataDirPath = new Path(dataDir);
          fs = FileSystem.get(dataDirPath.toUri(), getConf(dataDirPath));
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      } else {
        if (debug) {
          log.debug("UpdateHandler init: tlogDir={}, next id={}  this is a reopen or double init ... nothing else to do."
              , tlogDir, id);
        }
        versionInfo.reload();
        return;
      }
    }
    
    tlogDir = new Path(dataDir, TLOG_NAME);
    while (true) {
      try {
        if (!fs.exists(tlogDir)) {
          boolean success = fs.mkdirs(tlogDir);
          if (!success) {
            throw new RuntimeException("Could not create directory:" + tlogDir);
          }
        } else {
          fs.mkdirs(tlogDir); // To check for safe mode
        }
        break;
      } catch (RemoteException e) {
        if (e.getClassName().equals(
            "org.apache.hadoop.hdfs.server.namenode.SafeModeException")) {
          log.warn("The NameNode is in SafeMode - Solr will wait 5 seconds and try again.");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e1) {
            Thread.interrupted();
          }
          continue;
        }
        throw new RuntimeException(
            "Problem creating directory: " + tlogDir, e);
      } catch (IOException e) {
        throw new RuntimeException("Problem creating directory: " + tlogDir, e);
      }
    }

    String[] oldBufferTlog = getBufferLogList(fs, tlogDir);
    if (oldBufferTlog != null && oldBufferTlog.length != 0) {
      existOldBufferLog = true;
    }
    
    tlogFiles = getLogList(fs, tlogDir);
    id = getLastLogId() + 1; // add 1 since we will create a new log for the
                             // next update
    
    if (debug) {
      log.debug("UpdateHandler init: tlogDir={}, existing tlogs={}, next id={}"
          , tlogDir, Arrays.asList(tlogFiles), id);
    }
    
    TransactionLog oldLog = null;
    for (String oldLogName : tlogFiles) {
      Path f = new Path(tlogDir, oldLogName);
      try {
        oldLog = new HdfsTransactionLog(fs, f, null, true, tlogDfsReplication);
        addOldLog(oldLog, false); // don't remove old logs on startup since more
                                  // than one may be uncapped.
      } catch (Exception e) {
        INIT_FAILED_LOGS_COUNT.incrementAndGet();
        SolrException.log(log, "Failure to open existing log file (non fatal) "
            + f, e);
        try {
          fs.delete(f, false);
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
        log.info("Closing output for old non-recovery log {}", ll);
        ll.closeOutput();
      }
    }
    
    try {
      versionInfo = new VersionInfo(this, numVersionBuckets);
    } catch (SolrException e) {
      log.error("Unable to use updateLog: ", e);
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
        @SuppressWarnings({"unchecked"})
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
    return tlogDir.toUri().toString();
  }

  public static String[] getBufferLogList(FileSystem fs, Path tlogDir) {
    final String prefix = BUFFER_TLOG_NAME+'.';
    assert fs != null;
    FileStatus[] fileStatuses;
    try {
      fileStatuses = fs.listStatus(tlogDir, path -> path.getName().startsWith(prefix));
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Failed on listing old buffer tlog", e);
    }

    String[] names = new String[fileStatuses.length];
    for (int i = 0; i < fileStatuses.length; i++) {
      names[i] = fileStatuses[i].getPath().getName();
    }
    return names;
  }
  
  public static String[] getLogList(FileSystem fs, Path tlogDir) {
    final String prefix = TLOG_NAME + '.';
    assert fs != null;
    FileStatus[] fileStatuses;
    try {
      fileStatuses = fs.listStatus(tlogDir, new PathFilter() {
        
        @Override
        public boolean accept(Path path) {
          return path.getName().startsWith(prefix);
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String[] names = new String[fileStatuses.length];
    for (int i = 0; i < fileStatuses.length; i++) {
      names[i] = fileStatuses[i].getPath().getName();
    }
    Arrays.sort(names);

    return names;
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
      IOUtils.closeQuietly(fs);
    }
  }

  @Override
  protected void ensureBufferTlog() {
    if (bufferTlog != null) return;
    String newLogName = String.format(Locale.ROOT, LOG_FILENAME_PATTERN, BUFFER_TLOG_NAME, System.nanoTime());
    bufferTlog = new HdfsTransactionLog(fs, new Path(tlogDir, newLogName),
        globalStrings, tlogDfsReplication);
    bufferTlog.isBuffer = true;
  }

  @Override
  protected void deleteBufferLogs() {
    // Delete old buffer logs
    String[] oldBufferTlog = getBufferLogList(fs, tlogDir);
    if (oldBufferTlog != null && oldBufferTlog.length != 0) {
      for (String oldBufferLogName : oldBufferTlog) {
        Path f = new Path(tlogDir, oldBufferLogName);
        try {
          boolean s = fs.delete(f, false);
          if (!s) {
            log.error("Could not remove old buffer tlog file:{}", f);
          }
        } catch (IOException e) {
          // No need to bubble up this exception, because it won't cause any problems on recovering
          log.error("Could not remove old buffer tlog file:{}", f, e);
        }
      }
    }
  }

  @Override
  protected void ensureLog() {
    if (tlog == null) {
      String newLogName = String.format(Locale.ROOT, LOG_FILENAME_PATTERN,
          TLOG_NAME, id);
      HdfsTransactionLog ntlog = new HdfsTransactionLog(fs, new Path(tlogDir, newLogName),
          globalStrings, tlogDfsReplication);
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
    Path tlogDir = new Path(getTlogDir(core, ulogPluginInfo));
    try {
      if (fs != null && fs.exists(tlogDir)) {
        String[] files = getLogList(tlogDir);
        for (String file : files) {
          Path f = new Path(tlogDir, file);
          boolean s = fs.delete(f, false);
          if (!s) {
            log.error("Could not remove tlog file:{}", f);
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
  
  public String[] getLogList(Path tlogDir) throws FileNotFoundException, IOException {
    final String prefix = TLOG_NAME+'.';
    FileStatus[] files = fs.listStatus(tlogDir, new PathFilter() {
      
      @Override
      public boolean accept(Path name) {
        return name.getName().startsWith(prefix);
      }
    });
    List<String> fileList = new ArrayList<>(files.length);
    for (FileStatus file : files) {
      fileList.add(file.getPath().getName());
    }
    return fileList.toArray(new String[0]);
  }

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
    return "HDFSUpdateLog{state=" + getState() + ", tlog=" + tlog + "}";
  }
  
}

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.HdfsUtil;
import org.apache.solr.util.IOUtils;

/** @lucene.experimental */
public class HdfsUpdateLog extends UpdateLog {
  
  private volatile FileSystem fs;
  private volatile Path tlogDir;
  private final String confDir;

  public HdfsUpdateLog() {
    this.confDir = null;
  }
  
  public HdfsUpdateLog(String confDir) {
    this.confDir = confDir;
  }
  
  // HACK
  // while waiting for HDFS-3107, instead of quickly
  // dropping, we slowly apply
  // This is somewhat brittle, but current usage
  // allows for it
  @Override
  public boolean dropBufferedUpdates() {
    Future<RecoveryInfo> future = applyBufferedUpdates();
    if (future != null) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    return true;
  }
  
  @Override
  public void init(PluginInfo info) {
    dataDir = (String) info.initArgs.get("dir");
    
    defaultSyncLevel = SyncLevel.getSyncLevel((String) info.initArgs
        .get("syncLevel"));
    
  }

  private Configuration getConf() {
    Configuration conf = new Configuration();
    if (confDir != null) {
      HdfsUtil.addHdfsResources(conf, confDir);
    }
    
    return conf;
  }
  
  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
    
    // ulogDir from CoreDescriptor overrides
    String ulogDir = core.getCoreDescriptor().getUlogDir();

    if (ulogDir != null) {
      dataDir = ulogDir;
    }
    if (dataDir == null || dataDir.length()==0) {
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
      if (fs != null) {
        fs.close();
      }
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    
    try {
      fs = FileSystem.newInstance(new Path(dataDir).toUri(), getConf());
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    
    this.uhandler = uhandler;
    
    if (dataDir.equals(lastDataDir)) {
      if (debug) {
        log.debug("UpdateHandler init: tlogDir=" + tlogDir + ", next id=" + id,
            " this is a reopen... nothing else to do.");
      }
      
      versionInfo.reload();
      
      // on a normal reopen, we currently shouldn't have to do anything
      return;
    }
    lastDataDir = dataDir;
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
    
    tlogFiles = getLogList(fs, tlogDir);
    id = getLastLogId() + 1; // add 1 since we will create a new log for the
                             // next update
    
    if (debug) {
      log.debug("UpdateHandler init: tlogDir=" + tlogDir + ", existing tlogs="
          + Arrays.asList(tlogFiles) + ", next id=" + id);
    }
    
    TransactionLog oldLog = null;
    for (String oldLogName : tlogFiles) {
      Path f = new Path(tlogDir, oldLogName);
      try {
        oldLog = new HdfsTransactionLog(fs, f, null, true);
        addOldLog(oldLog, false); // don't remove old logs on startup since more
                                  // than one may be uncapped.
      } catch (Exception e) {
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
    // It's possible that at abnormal shutdown both "tlog" and "prevTlog" were
    // uncapped.
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
    
    // TODO: these startingVersions assume that we successfully recover from all
    // non-complete tlogs.
    HdfsUpdateLog.RecentUpdates startingUpdates = getRecentUpdates();
    try {
      startingVersions = startingUpdates.getVersions(numRecordsToKeep);
      startingOperation = startingUpdates.getLatestOperation();
      
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
      
    } finally {
      startingUpdates.close();
    }
    
  }
  
  @Override
  public String getLogDir() {
    return tlogDir.toUri().toString();
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
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
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
    synchronized (this) {
      super.close(committed);
      IOUtils.closeQuietly(fs);
    }
  }
  
  @Override
  protected void ensureLog() {
    if (tlog == null) {
      String newLogName = String.format(Locale.ROOT, LOG_FILENAME_PATTERN,
          TLOG_NAME, id);
      tlog = new HdfsTransactionLog(fs, new Path(tlogDir, newLogName),
          globalStrings);
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
      if (fs.exists(tlogDir)) {
        String[] files = getLogList(tlogDir);
        for (String file : files) {
          Path f = new Path(tlogDir, file);
          boolean s = fs.delete(f, false);
          if (!s) {
            log.error("Could not remove tlog file:" + f);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private String[] getLogList(Path tlogDir) throws FileNotFoundException, IOException {
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

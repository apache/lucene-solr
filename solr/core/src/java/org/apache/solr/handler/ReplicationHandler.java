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
package org.apache.solr.handler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.DeflaterOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrDeletionPolicy;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.PropertiesInputStream;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> A Handler which provides a REST API for replication and serves replication requests from Slaves. <p/> </p>
 * <p>When running on the master, it provides the following commands <ol> <li>Get the current replicable index version
 * (command=indexversion)</li> <li>Get the list of files for a given index version
 * (command=filelist&amp;indexversion=&lt;VERSION&gt;)</li> <li>Get full or a part (chunk) of a given index or a config
 * file (command=filecontent&amp;file=&lt;FILE_NAME&gt;) You can optionally specify an offset and length to get that
 * chunk of the file. You can request a configuration file by using "cf" parameter instead of the "file" parameter.</li>
 * <li>Get status/statistics (command=details)</li> </ol> </p> <p>When running on the slave, it provides the following
 * commands <ol> <li>Perform a snap pull now (command=snappull)</li> <li>Get status/statistics (command=details)</li>
 * <li>Abort a snap pull (command=abort)</li> <li>Enable/Disable polling the master for new versions (command=enablepoll
 * or command=disablepoll)</li> </ol> </p>
 *
 *
 * @since solr 1.4
 */
public class ReplicationHandler extends RequestHandlerBase implements SolrCoreAware {
  
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationHandler.class.getName());
  SolrCore core;

  private static final class CommitVersionInfo {
    public final long version;
    public final long generation;
    private CommitVersionInfo(long g, long v) {
      generation = g;
      version = v;
    }
    /**
     * builds a CommitVersionInfo data for the specified IndexCommit.  
     * Will never be null, ut version and generation may be zero if 
     * there are problems extracting them from the commit data
     */
    public static CommitVersionInfo build(IndexCommit commit) {
      long generation = commit.getGeneration();
      long version = 0;
      try {
        final Map<String,String> commitData = commit.getUserData();
        String commitTime = commitData.get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
        if (commitTime != null) {
          try {
            version = Long.parseLong(commitTime);
          } catch (NumberFormatException e) {
            LOG.warn("Version in commitData was not formated correctly: " + commitTime, e);
          }
        }
      } catch (IOException e) {
        LOG.warn("Unable to get version from commitData, commit: " + commit, e);
      }
      return new CommitVersionInfo(generation, version);
    }
  }

  private SnapPuller snapPuller;

  private ReentrantLock snapPullLock = new ReentrantLock();

  private String includeConfFiles;

  private NamedList<String> confFileNameAlias = new NamedList<>();

  private boolean isMaster = false;

  private boolean isSlave = false;

  private boolean replicateOnOptimize = false;

  private boolean replicateOnCommit = false;

  private boolean replicateOnStart = false;
  
  private int numberBackupsToKeep = 0; //zero: do not delete old backups

  private int numTimesReplicated = 0;

  private final Map<String, FileInfo> confFileInfoCache = new HashMap<>();

  private Integer reserveCommitDuration = SnapPuller.readInterval("00:00:10");

  volatile IndexCommit indexCommitPoint;

  volatile NamedList<Object> snapShootDetails;

  private AtomicBoolean replicationEnabled = new AtomicBoolean(true);

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.setHttpCaching(false);
    final SolrParams solrParams = req.getParams();
    String command = solrParams.get(COMMAND);
    if (command == null) {
      rsp.add(STATUS, OK_STATUS);
      rsp.add("message", "No command");
      return;
    }
    // This command does not give the current index version of the master
    // It gives the current 'replicateable' index version
    if (command.equals(CMD_INDEX_VERSION)) {
      IndexCommit commitPoint = indexCommitPoint;  // make a copy so it won't change
 
      if (commitPoint == null) {
        // if this handler is 'lazy', we may not have tracked the last commit
        // because our commit listener is registered on inform
        commitPoint = core.getDeletionPolicy().getLatestCommit();
      }
      
      if (commitPoint != null && replicationEnabled.get()) {
        //
        // There is a race condition here.  The commit point may be changed / deleted by the time
        // we get around to reserving it.  This is a very small window though, and should not result
        // in a catastrophic failure, but will result in the client getting an empty file list for
        // the CMD_GET_FILE_LIST command.
        //
        core.getDeletionPolicy().setReserveDuration(commitPoint.getGeneration(), reserveCommitDuration);
        rsp.add(CMD_INDEX_VERSION, IndexDeletionPolicyWrapper.getCommitTimestamp(commitPoint));
        rsp.add(GENERATION, commitPoint.getGeneration());
      } else {
        // This happens when replication is not configured to happen after startup and no commit/optimize
        // has happened yet.
        rsp.add(CMD_INDEX_VERSION, 0L);
        rsp.add(GENERATION, 0L);
      }
    } else if (command.equals(CMD_GET_FILE)) {
      getFileStream(solrParams, rsp);
    } else if (command.equals(CMD_GET_FILE_LIST)) {
      getFileList(solrParams, rsp);
    } else if (command.equalsIgnoreCase(CMD_BACKUP)) {
      doSnapShoot(new ModifiableSolrParams(solrParams), rsp, req);
      rsp.add(STATUS, OK_STATUS);
    } else if (command.equalsIgnoreCase(CMD_DELETE_BACKUP)) {
      deleteSnapshot(new ModifiableSolrParams(solrParams), rsp, req);
      rsp.add(STATUS, OK_STATUS);
    } else if (command.equalsIgnoreCase(CMD_FETCH_INDEX)) {
      String masterUrl = solrParams.get(MASTER_URL);
      if (!isSlave && masterUrl == null) {
        rsp.add(STATUS,ERR_STATUS);
        rsp.add("message","No slave configured or no 'masterUrl' Specified");
        return;
      }
      final SolrParams paramsCopy = new ModifiableSolrParams(solrParams);
      Thread puller = new Thread("explicit-fetchindex-cmd") {
        @Override
        public void run() {
          doFetch(paramsCopy, false);
        }
      };
      puller.start();
      if (solrParams.getBool(WAIT, false)) {
        puller.join();
      }
      rsp.add(STATUS, OK_STATUS);
    } else if (command.equalsIgnoreCase(CMD_DISABLE_POLL)) {
      if (snapPuller != null){
        snapPuller.disablePoll();
        rsp.add(STATUS, OK_STATUS);
      } else {
        rsp.add(STATUS, ERR_STATUS);
        rsp.add("message","No slave configured");
      }
    } else if (command.equalsIgnoreCase(CMD_ENABLE_POLL)) {
      if (snapPuller != null){
        snapPuller.enablePoll();
        rsp.add(STATUS, OK_STATUS);
      }else {
        rsp.add(STATUS,ERR_STATUS);
        rsp.add("message","No slave configured");
      }
    } else if (command.equalsIgnoreCase(CMD_ABORT_FETCH)) {
      SnapPuller temp = tempSnapPuller;
      if (temp != null){
        temp.abortPull();
        rsp.add(STATUS, OK_STATUS);
      } else {
        rsp.add(STATUS,ERR_STATUS);
        rsp.add("message","No slave configured");
      }
    } else if (command.equals(CMD_SHOW_COMMITS)) {
      rsp.add(CMD_SHOW_COMMITS, getCommits());
    } else if (command.equals(CMD_DETAILS)) {
      rsp.add(CMD_DETAILS, getReplicationDetails(solrParams.getBool("slave", true)));
    } else if (CMD_ENABLE_REPL.equalsIgnoreCase(command)) {
      replicationEnabled.set(true);
      rsp.add(STATUS, OK_STATUS);
    } else if (CMD_DISABLE_REPL.equalsIgnoreCase(command)) {
      replicationEnabled.set(false);
      rsp.add(STATUS, OK_STATUS);
    }
  }

  private void deleteSnapshot(ModifiableSolrParams params, SolrQueryResponse rsp, SolrQueryRequest req) {
    String name = params.get("name");
    if(name == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Missing mandatory param: name");
    }

    SnapShooter snapShooter = new SnapShooter(core, params.get("location"), params.get("name"));
    snapShooter.validateDeleteSnapshot();
    snapShooter.deleteSnapAsync(this);
  }

  private List<NamedList<Object>> getCommits() {
    Map<Long, IndexCommit> commits = core.getDeletionPolicy().getCommits();
    List<NamedList<Object>> l = new ArrayList<>();

    for (IndexCommit c : commits.values()) {
      try {
        NamedList<Object> nl = new NamedList<>();
        nl.add("indexVersion", IndexDeletionPolicyWrapper.getCommitTimestamp(c));
        nl.add(GENERATION, c.getGeneration());
        List<String> commitList = new ArrayList<>(c.getFileNames().size());
        commitList.addAll(c.getFileNames());
        Collections.sort(commitList);
        nl.add(CMD_GET_FILE_LIST, commitList);
        l.add(nl);
      } catch (IOException e) {
        LOG.warn("Exception while reading files for commit " + c, e);
      }
    }
    return l;
  }

  static Long getCheckSum(Checksum checksum, File f) {
    FileInputStream fis = null;
    checksum.reset();
    byte[] buffer = new byte[1024 * 1024];
    int bytesRead;
    try {
      fis = new FileInputStream(f);
      while ((bytesRead = fis.read(buffer)) >= 0)
        checksum.update(buffer, 0, bytesRead);
      return checksum.getValue();
    } catch (Exception e) {
      LOG.warn("Exception in finding checksum of " + f, e);
    } finally {
      IOUtils.closeQuietly(fis);
    }
    return null;
  }

  private volatile SnapPuller tempSnapPuller;

  public boolean doFetch(SolrParams solrParams, boolean forceReplication) {
    String masterUrl = solrParams == null ? null : solrParams.get(MASTER_URL);
    if (!snapPullLock.tryLock())
      return false;
    try {
      tempSnapPuller = snapPuller;
      if (masterUrl != null) {
        NamedList<Object> nl = solrParams.toNamedList();
        nl.remove(SnapPuller.POLL_INTERVAL);
        tempSnapPuller = new SnapPuller(nl, this, core);
      }
      return tempSnapPuller.fetchLatestIndex(core, forceReplication);
    } catch (Exception e) {
      SolrException.log(LOG, "SnapPull failed ", e);
    } finally {
      if (snapPuller != null) {
        tempSnapPuller = snapPuller;
      }
      snapPullLock.unlock();
    }
    return false;
  }

  boolean isReplicating() {
    return snapPullLock.isLocked();
  }

  private void doSnapShoot(SolrParams params, SolrQueryResponse rsp,
      SolrQueryRequest req) {
    try {
      int numberToKeep = params.getInt(NUMBER_BACKUPS_TO_KEEP_REQUEST_PARAM, 0);
      if (numberToKeep > 0 && numberBackupsToKeep > 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot use "
            + NUMBER_BACKUPS_TO_KEEP_REQUEST_PARAM + " if "
            + NUMBER_BACKUPS_TO_KEEP_INIT_PARAM
            + " was specified in the configuration.");
      }
      numberToKeep = Math.max(numberToKeep, numberBackupsToKeep);
      if (numberToKeep < 1) {
        numberToKeep = Integer.MAX_VALUE;
      }
      
      IndexDeletionPolicyWrapper delPolicy = core.getDeletionPolicy();
      IndexCommit indexCommit = delPolicy.getLatestCommit();
      
      if (indexCommit == null) {
        indexCommit = req.getSearcher().getIndexReader().getIndexCommit();
      }
      
      // small race here before the commit point is saved
      SnapShooter snapShooter = new SnapShooter(core, params.get("location"), params.get("name"));
      snapShooter.validateCreateSnapshot();
      snapShooter.createSnapAsync(indexCommit, numberToKeep, this);
      
    } catch (Exception e) {
      LOG.warn("Exception during creating a snapshot", e);
      rsp.add("exception", e);
    }
  }

  /**
   * This method adds an Object of FileStream to the response . The FileStream implements a custom protocol which is
   * understood by SnapPuller.FileFetcher
   *
   * @see org.apache.solr.handler.SnapPuller.LocalFsFileFetcher
   * @see org.apache.solr.handler.SnapPuller.DirectoryFileFetcher
   */
  private void getFileStream(SolrParams solrParams, SolrQueryResponse rsp) {
    ModifiableSolrParams rawParams = new ModifiableSolrParams(solrParams);
    rawParams.set(CommonParams.WT, FILE_STREAM);
    
    String cfileName = solrParams.get(CONF_FILE_SHORT);
    if (cfileName != null) {
      rsp.add(FILE_STREAM, new LocalFsFileStream(solrParams));
    } else {
      rsp.add(FILE_STREAM, new DirectoryFileStream(solrParams));
    }
  }

  @SuppressWarnings("unchecked")
  private void getFileList(SolrParams solrParams, SolrQueryResponse rsp) {
    String v = solrParams.get(GENERATION);
    if (v == null) {
      rsp.add("status", "no index generation specified");
      return;
    }
    long gen = Long.parseLong(v);
    IndexCommit commit = core.getDeletionPolicy().getCommitPoint(gen);
 
    //System.out.println("ask for files for gen:" + commit.getGeneration() + core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName());
    if (commit == null) {
      rsp.add("status", "invalid index generation");
      return;
    }
    // reserve the indexcommit for sometime
    core.getDeletionPolicy().setReserveDuration(gen, reserveCommitDuration);
    List<Map<String, Object>> result = new ArrayList<>();
    Directory dir = null;
    try {
      // get all the files in the commit
      // use a set to workaround possible Lucene bug which returns same file
      // name multiple times
      Collection<String> files = new HashSet<>(commit.getFileNames());
      dir = core.getDirectoryFactory().get(core.getNewIndexDir(), DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
      try {
        
        for (String fileName : files) {
          if (fileName.endsWith(".lock")) continue;
          Map<String,Object> fileMeta = new HashMap<>();
          fileMeta.put(NAME, fileName);
          fileMeta.put(SIZE, dir.fileLength(fileName));
          result.add(fileMeta);
        }
      } finally {
        core.getDirectoryFactory().release(dir);
      }
    } catch (IOException e) {
      rsp.add("status", "unable to get file names for given index generation");
      rsp.add("exception", e);
      LOG.error("Unable to get file names for indexCommit generation: " + gen, e);
    }
    rsp.add(CMD_GET_FILE_LIST, result);
    if (confFileNameAlias.size() < 1 || core.getCoreDescriptor().getCoreContainer().isZooKeeperAware())
      return;
    LOG.debug("Adding config files to list: " + includeConfFiles);
    //if configuration files need to be included get their details
    rsp.add(CONF_FILES, getConfFileInfoFromCache(confFileNameAlias, confFileInfoCache));
  }

  /**
   * For configuration files, checksum of the file is included because, unlike index files, they may have same content
   * but different timestamps.
   * <p/>
   * The local conf files information is cached so that everytime it does not have to compute the checksum. The cache is
   * refreshed only if the lastModified of the file changes
   */
  List<Map<String, Object>> getConfFileInfoFromCache(NamedList<String> nameAndAlias,
                                                     final Map<String, FileInfo> confFileInfoCache) {
    List<Map<String, Object>> confFiles = new ArrayList<>();
    synchronized (confFileInfoCache) {
      File confDir = new File(core.getResourceLoader().getConfigDir());
      Checksum checksum = null;
      for (int i = 0; i < nameAndAlias.size(); i++) {
        String cf = nameAndAlias.getName(i);
        File f = new File(confDir, cf);
        if (!f.exists() || f.isDirectory()) continue; //must not happen
        FileInfo info = confFileInfoCache.get(cf);
        if (info == null || info.lastmodified != f.lastModified() || info.size != f.length()) {
          if (checksum == null) checksum = new Adler32();
          info = new FileInfo(f.lastModified(), cf, f.length(), getCheckSum(checksum, f));
          confFileInfoCache.put(cf, info);
        }
        Map<String, Object> m = info.getAsMap();
        if (nameAndAlias.getVal(i) != null) m.put(ALIAS, nameAndAlias.getVal(i));
        confFiles.add(m);
      }
    }
    return confFiles;
  }

  static class FileInfo {
    long lastmodified;
    String name;
    long size;
    long checksum;

    public FileInfo(long lasmodified, String name, long size, long checksum) {
      this.lastmodified = lasmodified;
      this.name = name;
      this.size = size;
      this.checksum = checksum;
    }

    Map<String, Object> getAsMap() {
      Map<String, Object> map = new HashMap<>();
      map.put(NAME, name);
      map.put(SIZE, size);
      map.put(CHECKSUM, checksum);
      return map;
    }
  }

  void disablePoll() {
    if (isSlave)
      snapPuller.disablePoll();
  }

  void enablePoll() {
    if (isSlave)
      snapPuller.enablePoll();
  }

  boolean isPollingDisabled() {
    if (snapPuller == null) return true;
    return snapPuller.isPollingDisabled();
  }

  int getTimesReplicatedSinceStartup() {
    return numTimesReplicated;
  }

  void setTimesReplicatedSinceStartup() {
    numTimesReplicated++;
  }

  long getIndexSize() {
    Directory dir;
    long size = 0;
    try {
      dir = core.getDirectoryFactory().get(core.getIndexDir(), DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
      try {
        size = DirectoryFactory.sizeOfDirectory(dir);
      } finally {
        core.getDirectoryFactory().release(dir);
      }
    } catch (IOException e) {
      SolrException.log(LOG, "IO error while trying to get the size of the Directory", e);
    }
    return size;
  }

  @Override
  public String getDescription() {
    return "ReplicationHandler provides replication of index and configuration files from Master to Slaves";
  }

  /** 
   * returns the CommitVersionInfo for the current searcher, or null on error.
   */
  private CommitVersionInfo getIndexVersion() {
    CommitVersionInfo v = null;
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    try {
      v = CommitVersionInfo.build(searcher.get().getIndexReader().getIndexCommit());
    } catch (IOException e) {
      LOG.warn("Unable to get index commit: ", e);
    } finally {
      searcher.decref();
    }
    return v;
  }

  @Override
  @SuppressWarnings("unchecked")
  public NamedList getStatistics() {
    NamedList list = super.getStatistics();
    if (core != null) {
      list.add("indexSize", NumberUtils.readableSize(getIndexSize()));
      CommitVersionInfo vInfo = getIndexVersion();
      list.add("indexVersion", null == vInfo ? 0 : vInfo.version);
      list.add(GENERATION, null == vInfo ? 0 : vInfo.generation);

      list.add("indexPath", core.getIndexDir());
      list.add("isMaster", String.valueOf(isMaster));
      list.add("isSlave", String.valueOf(isSlave));

      SnapPuller snapPuller = tempSnapPuller;
      if (snapPuller != null) {
        list.add(MASTER_URL, snapPuller.getMasterUrl());
        if (snapPuller.getPollInterval() != null) {
          list.add(SnapPuller.POLL_INTERVAL, snapPuller.getPollInterval());
        }
        list.add("isPollingDisabled", String.valueOf(isPollingDisabled()));
        list.add("isReplicating", String.valueOf(isReplicating()));
        long elapsed = getTimeElapsed(snapPuller);
        long val = SnapPuller.getTotalBytesDownloaded(snapPuller);
        if (elapsed > 0) {
          list.add("timeElapsed", elapsed);
          list.add("bytesDownloaded", val);
          list.add("downloadSpeed", val / elapsed);
        }
        Properties props = loadReplicationProperties();
        addVal(list, SnapPuller.PREVIOUS_CYCLE_TIME_TAKEN, props, Long.class);
        addVal(list, SnapPuller.INDEX_REPLICATED_AT, props, Date.class);
        addVal(list, SnapPuller.CONF_FILES_REPLICATED_AT, props, Date.class);
        addVal(list, SnapPuller.REPLICATION_FAILED_AT, props, Date.class);
        addVal(list, SnapPuller.TIMES_FAILED, props, Integer.class);
        addVal(list, SnapPuller.TIMES_INDEX_REPLICATED, props, Integer.class);
        addVal(list, SnapPuller.LAST_CYCLE_BYTES_DOWNLOADED, props, Long.class);
        addVal(list, SnapPuller.TIMES_CONFIG_REPLICATED, props, Integer.class);
        addVal(list, SnapPuller.CONF_FILES_REPLICATED, props, String.class);
      }
      if (isMaster) {
        if (includeConfFiles != null) list.add("confFilesToReplicate", includeConfFiles);
        list.add(REPLICATE_AFTER, getReplicateAfterStrings());
        list.add("replicationEnabled", String.valueOf(replicationEnabled.get()));
      }
    }
    return list;
  }

  /**
   * Used for showing statistics and progress information.
   */
  private NamedList<Object> getReplicationDetails(boolean showSlaveDetails) {
    NamedList<Object> details = new SimpleOrderedMap<>();
    NamedList<Object> master = new SimpleOrderedMap<>();
    NamedList<Object> slave = new SimpleOrderedMap<>();

    details.add("indexSize", NumberUtils.readableSize(getIndexSize()));
    details.add("indexPath", core.getIndexDir());
    details.add(CMD_SHOW_COMMITS, getCommits());
    details.add("isMaster", String.valueOf(isMaster));
    details.add("isSlave", String.valueOf(isSlave));
    CommitVersionInfo vInfo = getIndexVersion();
    details.add("indexVersion", null == vInfo ? 0 : vInfo.version);
    details.add(GENERATION, null == vInfo ? 0 : vInfo.generation);
    
    IndexCommit commit = indexCommitPoint;  // make a copy so it won't change

    if (isMaster) {
      if (includeConfFiles != null) master.add(CONF_FILES, includeConfFiles);
      master.add(REPLICATE_AFTER, getReplicateAfterStrings());
      master.add("replicationEnabled", String.valueOf(replicationEnabled.get()));
    }

    if (isMaster && commit != null) {
      CommitVersionInfo repCommitInfo = CommitVersionInfo.build(commit);
      master.add("replicableVersion", repCommitInfo.version);
      master.add("replicableGeneration", repCommitInfo.generation);
    }

    SnapPuller snapPuller = tempSnapPuller;
    if (snapPuller != null) {
      Properties props = loadReplicationProperties();
      if (showSlaveDetails) {
        try {
          NamedList nl = snapPuller.getDetails();
          slave.add("masterDetails", nl.get(CMD_DETAILS));
        } catch (Exception e) {
          LOG.warn(
              "Exception while invoking 'details' method for replication on master ",
              e);
          slave.add(ERR_STATUS, "invalid_master");
        }
      }
      slave.add(MASTER_URL, snapPuller.getMasterUrl());
      if (snapPuller.getPollInterval() != null) {
        slave.add(SnapPuller.POLL_INTERVAL, snapPuller.getPollInterval());
      }
      if (snapPuller.getNextScheduledExecTime() != null && !isPollingDisabled()) {
        slave.add(NEXT_EXECUTION_AT, new Date(snapPuller.getNextScheduledExecTime()).toString());
      } else if (isPollingDisabled()) {
        slave.add(NEXT_EXECUTION_AT, "Polling disabled");
      }
      addVal(slave, SnapPuller.INDEX_REPLICATED_AT, props, Date.class);
      addVal(slave, SnapPuller.INDEX_REPLICATED_AT_LIST, props, List.class);
      addVal(slave, SnapPuller.REPLICATION_FAILED_AT_LIST, props, List.class);
      addVal(slave, SnapPuller.TIMES_INDEX_REPLICATED, props, Integer.class);
      addVal(slave, SnapPuller.CONF_FILES_REPLICATED, props, Integer.class);
      addVal(slave, SnapPuller.TIMES_CONFIG_REPLICATED, props, Integer.class);
      addVal(slave, SnapPuller.CONF_FILES_REPLICATED_AT, props, Integer.class);
      addVal(slave, SnapPuller.LAST_CYCLE_BYTES_DOWNLOADED, props, Long.class);
      addVal(slave, SnapPuller.TIMES_FAILED, props, Integer.class);
      addVal(slave, SnapPuller.REPLICATION_FAILED_AT, props, Date.class);
      addVal(slave, SnapPuller.PREVIOUS_CYCLE_TIME_TAKEN, props, Long.class);

      slave.add("currentDate", new Date().toString());
      slave.add("isPollingDisabled", String.valueOf(isPollingDisabled()));
      boolean isReplicating = isReplicating();
      slave.add("isReplicating", String.valueOf(isReplicating));
      if (isReplicating) {
        try {
          long bytesToDownload = 0;
          List<String> filesToDownload = new ArrayList<>();
          for (Map<String, Object> file : snapPuller.getFilesToDownload()) {
            filesToDownload.add((String) file.get(NAME));
            bytesToDownload += (Long) file.get(SIZE);
          }

          //get list of conf files to download
          for (Map<String, Object> file : snapPuller.getConfFilesToDownload()) {
            filesToDownload.add((String) file.get(NAME));
            bytesToDownload += (Long) file.get(SIZE);
          }

          slave.add("filesToDownload", filesToDownload);
          slave.add("numFilesToDownload", String.valueOf(filesToDownload.size()));
          slave.add("bytesToDownload", NumberUtils.readableSize(bytesToDownload));

          long bytesDownloaded = 0;
          List<String> filesDownloaded = new ArrayList<>();
          for (Map<String, Object> file : snapPuller.getFilesDownloaded()) {
            filesDownloaded.add((String) file.get(NAME));
            bytesDownloaded += (Long) file.get(SIZE);
          }

          //get list of conf files downloaded
          for (Map<String, Object> file : snapPuller.getConfFilesDownloaded()) {
            filesDownloaded.add((String) file.get(NAME));
            bytesDownloaded += (Long) file.get(SIZE);
          }

          Map<String, Object> currentFile = snapPuller.getCurrentFile();
          String currFile = null;
          long currFileSize = 0, currFileSizeDownloaded = 0;
          float percentDownloaded = 0;
          if (currentFile != null) {
            currFile = (String) currentFile.get(NAME);
            currFileSize = (Long) currentFile.get(SIZE);
            if (currentFile.containsKey("bytesDownloaded")) {
              currFileSizeDownloaded = (Long) currentFile.get("bytesDownloaded");
              bytesDownloaded += currFileSizeDownloaded;
              if (currFileSize > 0)
                percentDownloaded = (currFileSizeDownloaded * 100) / currFileSize;
            }
          }
          slave.add("filesDownloaded", filesDownloaded);
          slave.add("numFilesDownloaded", String.valueOf(filesDownloaded.size()));

          long estimatedTimeRemaining = 0;

          if (snapPuller.getReplicationStartTime() > 0) {
            slave.add("replicationStartTime", new Date(snapPuller.getReplicationStartTime()).toString());
          }
          long elapsed = getTimeElapsed(snapPuller);
          slave.add("timeElapsed", String.valueOf(elapsed) + "s");

          if (bytesDownloaded > 0)
            estimatedTimeRemaining = ((bytesToDownload - bytesDownloaded) * elapsed) / bytesDownloaded;
          float totalPercent = 0;
          long downloadSpeed = 0;
          if (bytesToDownload > 0)
            totalPercent = (bytesDownloaded * 100) / bytesToDownload;
          if (elapsed > 0)
            downloadSpeed = (bytesDownloaded / elapsed);
          if (currFile != null)
            slave.add("currentFile", currFile);
          slave.add("currentFileSize", NumberUtils.readableSize(currFileSize));
          slave.add("currentFileSizeDownloaded", NumberUtils.readableSize(currFileSizeDownloaded));
          slave.add("currentFileSizePercent", String.valueOf(percentDownloaded));
          slave.add("bytesDownloaded", NumberUtils.readableSize(bytesDownloaded));
          slave.add("totalPercent", String.valueOf(totalPercent));
          slave.add("timeRemaining", String.valueOf(estimatedTimeRemaining) + "s");
          slave.add("downloadSpeed", NumberUtils.readableSize(downloadSpeed));
        } catch (Exception e) {
          LOG.error("Exception while writing replication details: ", e);
        }
      }
    }

    if (isMaster)
      details.add("master", master);
    if (slave.size() > 0)
      details.add("slave", slave);
    
    NamedList snapshotStats = snapShootDetails;
    if (snapshotStats != null)
      details.add(CMD_BACKUP, snapshotStats);
    
    return details;
  }

  private void addVal(NamedList<Object> nl, String key, Properties props, Class clzz) {
    String s = props.getProperty(key);
    if (s == null || s.trim().length() == 0) return;
    if (clzz == Date.class) {
      try {
        Long l = Long.parseLong(s);
        nl.add(key, new Date(l).toString());
      } catch (NumberFormatException e) {/*no op*/ }
    } else if (clzz == List.class) {
      String ss[] = s.split(",");
      List<String> l = new ArrayList<>();
      for (int i = 0; i < ss.length; i++) {
        l.add(new Date(Long.valueOf(ss[i])).toString());
      }
      nl.add(key, l);
    } else {
      nl.add(key, s);
    }

  }

  private List<String> getReplicateAfterStrings() {
    List<String> replicateAfter = new ArrayList<>();
    if (replicateOnCommit)
      replicateAfter.add("commit");
    if (replicateOnOptimize)
      replicateAfter.add("optimize");
    if (replicateOnStart)
      replicateAfter.add("startup");
    return replicateAfter;
  }

  private long getTimeElapsed(SnapPuller snapPuller) {
    long timeElapsed = 0;
    if (snapPuller.getReplicationStartTime() > 0)
      timeElapsed = TimeUnit.SECONDS.convert(System.currentTimeMillis() - snapPuller.getReplicationStartTime(), TimeUnit.MILLISECONDS);
    return timeElapsed;
  }

  Properties loadReplicationProperties() {
    Directory dir = null;
    try {
      try {
        dir = core.getDirectoryFactory().get(core.getDataDir(),
            DirContext.META_DATA, core.getSolrConfig().indexConfig.lockType);
        IndexInput input;
        try {
          input = dir.openInput(
            SnapPuller.REPLICATION_PROPERTIES, IOContext.DEFAULT);
        } catch (FileNotFoundException | NoSuchFileException e) {
          return new Properties();
        }

        try {
          final InputStream is = new PropertiesInputStream(input);
          Properties props = new Properties();
          props.load(new InputStreamReader(is, StandardCharsets.UTF_8));
          return props;
        } finally {
          input.close();
        }
      } finally {
        if (dir != null) {
          core.getDirectoryFactory().release(dir);
        }
      }
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }


//  void refreshCommitpoint() {
//    IndexCommit commitPoint = core.getDeletionPolicy().getLatestCommit();
//    if(replicateOnCommit || (replicateOnOptimize && commitPoint.getSegmentCount() == 1)) {
//      indexCommitPoint = commitPoint;
//    }
//  }

  @Override
  @SuppressWarnings("unchecked")
  public void inform(SolrCore core) {
    this.core = core;
    registerFileStreamResponseWriter();
    registerCloseHook();
    Object nbtk = initArgs.get(NUMBER_BACKUPS_TO_KEEP_INIT_PARAM);
    if(nbtk!=null) {
      numberBackupsToKeep = Integer.parseInt(nbtk.toString());
    } else {
      numberBackupsToKeep = 0;
    }
    NamedList slave = (NamedList) initArgs.get("slave");
    boolean enableSlave = isEnabled( slave );
    if (enableSlave) {
      tempSnapPuller = snapPuller = new SnapPuller(slave, this, core);
      isSlave = true;
    }
    NamedList master = (NamedList) initArgs.get("master");
    boolean enableMaster = isEnabled( master );

    if (enableMaster || enableSlave) {
      if (core.getCoreDescriptor().getCoreContainer().getZkController() != null) {
        LOG.warn("SolrCloud is enabled for core " + core.getName() + " but so is old-style replication. Make sure you" +
            " intend this behavior, it usually indicates a mis-configuration. Master setting is " +
            Boolean.toString(enableMaster) + " and slave setting is " + Boolean.toString(enableSlave));
      }
    }
    
    if (!enableSlave && !enableMaster) {
      enableMaster = true;
      master = new NamedList<>();
    }
    
    if (enableMaster) {
      includeConfFiles = (String) master.get(CONF_FILES);
      if (includeConfFiles != null && includeConfFiles.trim().length() > 0) {
        List<String> files = Arrays.asList(includeConfFiles.split(","));
        for (String file : files) {
          if (file.trim().length() == 0) continue;
          String[] strs = file.trim().split(":");
          // if there is an alias add it or it is null
          confFileNameAlias.add(strs[0], strs.length > 1 ? strs[1] : null);
        }
        LOG.info("Replication enabled for following config files: " + includeConfFiles);
      }
      List backup = master.getAll("backupAfter");
      boolean backupOnCommit = backup.contains("commit");
      boolean backupOnOptimize = !backupOnCommit && backup.contains("optimize");
      List replicateAfter = master.getAll(REPLICATE_AFTER);
      replicateOnCommit = replicateAfter.contains("commit");
      replicateOnOptimize = !replicateOnCommit && replicateAfter.contains("optimize");

      if (!replicateOnCommit && ! replicateOnOptimize) {
        replicateOnCommit = true;
      }
      
      // if we only want to replicate on optimize, we need the deletion policy to
      // save the last optimized commit point.
      if (replicateOnOptimize) {
        IndexDeletionPolicyWrapper wrapper = core.getDeletionPolicy();
        IndexDeletionPolicy policy = wrapper == null ? null : wrapper.getWrappedDeletionPolicy();
        if (policy instanceof SolrDeletionPolicy) {
          SolrDeletionPolicy solrPolicy = (SolrDeletionPolicy)policy;
          if (solrPolicy.getMaxOptimizedCommitsToKeep() < 1) {
            solrPolicy.setMaxOptimizedCommitsToKeep(1);
          }
        } else {
          LOG.warn("Replication can't call setMaxOptimizedCommitsToKeep on " + policy);
        }
      }

      if (replicateOnOptimize || backupOnOptimize) {
        core.getUpdateHandler().registerOptimizeCallback(getEventListener(backupOnOptimize, replicateOnOptimize));
      }
      if (replicateOnCommit || backupOnCommit) {
        replicateOnCommit = true;
        core.getUpdateHandler().registerCommitCallback(getEventListener(backupOnCommit, replicateOnCommit));
      }
      if (replicateAfter.contains("startup")) {
        replicateOnStart = true;
        RefCounted<SolrIndexSearcher> s = core.getNewestSearcher(false);
        try {
          DirectoryReader reader = s==null ? null : s.get().getIndexReader();
          if (reader!=null && reader.getIndexCommit() != null && reader.getIndexCommit().getGeneration() != 1L) {
            try {
              if(replicateOnOptimize){
                Collection<IndexCommit> commits = DirectoryReader.listCommits(reader.directory());
                for (IndexCommit ic : commits) {
                  if(ic.getSegmentCount() == 1){
                    if(indexCommitPoint == null || indexCommitPoint.getGeneration() < ic.getGeneration()) indexCommitPoint = ic;
                  }
                }
              } else{
                indexCommitPoint = reader.getIndexCommit();
              }
            } finally {
              // We don't need to save commit points for replication, the SolrDeletionPolicy
              // always saves the last commit point (and the last optimized commit point, if needed)
              /***
              if(indexCommitPoint != null){
                core.getDeletionPolicy().saveCommitPoint(indexCommitPoint.getGeneration());
              }
              ***/
            }
          }

          // ensure the writer is init'd so that we have a list of commit points
          RefCounted<IndexWriter> iw = core.getUpdateHandler().getSolrCoreState().getIndexWriter(core);
          iw.decref();

        } catch (IOException e) {
          LOG.warn("Unable to get IndexCommit on startup", e);
        } finally {
          if (s!=null) s.decref();
        }
      }
      String reserve = (String) master.get(RESERVE);
      if (reserve != null && !reserve.trim().equals("")) {
        reserveCommitDuration = SnapPuller.readInterval(reserve);
      }
      LOG.info("Commits will be reserved for  " + reserveCommitDuration);
      isMaster = true;
    }
  }
  
  // check master or slave is enabled
  private boolean isEnabled( NamedList params ){
    if( params == null ) return false;
    Object enable = params.get( "enable" );
    if( enable == null ) return true;
    if( enable instanceof String )
      return StrUtils.parseBool( (String)enable );
    return Boolean.TRUE.equals( enable );
  }

  /**
   * register a closehook
   */
  private void registerCloseHook() {
    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        if (snapPuller != null) {
          snapPuller.destroy();
        }
      }

      @Override
      public void postClose(SolrCore core) {}
    });
  }

  /**
   * A ResponseWriter is registered automatically for wt=filestream This response writer is used to transfer index files
   * in a block-by-block manner within the same HTTP response.
   */
  private void registerFileStreamResponseWriter() {
    core.registerResponseWriter(FILE_STREAM, new BinaryQueryResponseWriter() {
      @Override
      public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse resp) throws IOException {
        DirectoryFileStream stream = (DirectoryFileStream) resp.getValues().get(FILE_STREAM);
        stream.write(out);
      }

      @Override
      public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) {
        throw new RuntimeException("This is a binary writer , Cannot write to a characterstream");
      }

      @Override
      public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
        return BinaryResponseParser.BINARY_CONTENT_TYPE;
      }

      @Override
      public void init(NamedList args) { /*no op*/ }
    });

  }

  /**
   * Register a listener for postcommit/optimize
   *
   * @param snapshoot do a snapshoot
   * @param getCommit get a commitpoint also
   *
   * @return an instance of the eventlistener
   */
  private SolrEventListener getEventListener(final boolean snapshoot, final boolean getCommit) {
    return new SolrEventListener() {
      @Override
      public void init(NamedList args) {/*no op*/ }

      /**
       * This refreshes the latest replicateable index commit and optionally can create Snapshots as well
       */
      @Override
      public void postCommit() {
        IndexCommit currentCommitPoint = core.getDeletionPolicy().getLatestCommit();

        if (getCommit) {
          // IndexCommit oldCommitPoint = indexCommitPoint;
          indexCommitPoint = currentCommitPoint;

          // We don't need to save commit points for replication, the SolrDeletionPolicy
          // always saves the last commit point (and the last optimized commit point, if needed)
          /***
          if (indexCommitPoint != null) {
            core.getDeletionPolicy().saveCommitPoint(indexCommitPoint.getGeneration());
          }
          if(oldCommitPoint != null){
            core.getDeletionPolicy().releaseCommitPointAndExtendReserve(oldCommitPoint.getGeneration());
          }
          ***/
        }
        if (snapshoot) {
          try {            
            int numberToKeep = numberBackupsToKeep;
            if (numberToKeep < 1) {
              numberToKeep = Integer.MAX_VALUE;
            }            
            SnapShooter snapShooter = new SnapShooter(core, null, null);
            snapShooter.createSnapAsync(currentCommitPoint, numberToKeep, ReplicationHandler.this);
          } catch (Exception e) {
            LOG.error("Exception while snapshooting", e);
          }
        }
      }

      @Override
      public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) { /*no op*/}

      @Override
      public void postSoftCommit() {

      }
    };
  }

  /**This class is used to read and send files in the lucene index
   *
   */
  private class DirectoryFileStream {
    protected SolrParams params;

    protected FastOutputStream fos;

    protected Long indexGen;
    protected IndexDeletionPolicyWrapper delPolicy;

    protected String fileName;
    protected String cfileName;
    protected String sOffset;
    protected String sLen;
    protected String compress;
    protected boolean useChecksum;

    protected long offset = -1;
    protected int len = -1;

    protected Checksum checksum;

    private RateLimiter rateLimiter;

    byte[] buf;

    public DirectoryFileStream(SolrParams solrParams) {
      params = solrParams;
      delPolicy = core.getDeletionPolicy();

      fileName = params.get(FILE);
      cfileName = params.get(CONF_FILE_SHORT);
      sOffset = params.get(OFFSET);
      sLen = params.get(LEN);
      compress = params.get(COMPRESSION);
      useChecksum = params.getBool(CHECKSUM, false);
      indexGen = params.getLong(GENERATION, null);
      if (useChecksum) {
        checksum = new Adler32();
      }
      //No throttle if MAX_WRITE_PER_SECOND is not specified
      double maxWriteMBPerSec = params.getDouble(MAX_WRITE_PER_SECOND, Double.MAX_VALUE);
      rateLimiter = new RateLimiter.SimpleRateLimiter(maxWriteMBPerSec);
    }

    protected void initWrite() throws IOException {
      if (sOffset != null) offset = Long.parseLong(sOffset);
      if (sLen != null) len = Integer.parseInt(sLen);
      if (fileName == null && cfileName == null) {
        // no filename do nothing
        writeNothingAndFlush();
      }
      buf = new byte[(len == -1 || len > PACKET_SZ) ? PACKET_SZ : len];

      //reserve commit point till write is complete
      if(indexGen != null) {
        delPolicy.saveCommitPoint(indexGen);
      }
    }

    protected void createOutputStream(OutputStream out) {
      if (Boolean.parseBoolean(compress)) {
        fos = new FastOutputStream(new DeflaterOutputStream(out));
      } else {
        fos = new FastOutputStream(out);
      }
    }

    protected void releaseCommitPointAndExtendReserve() {
      if(indexGen != null) {
        //release the commit point as the write is complete
        delPolicy.releaseCommitPoint(indexGen);

        //Reserve the commit point for another 10s for the next file to be to fetched.
        //We need to keep extending the commit reservation between requests so that the replica can fetch
        //all the files correctly.
        delPolicy.setReserveDuration(indexGen, reserveCommitDuration);
      }

    }
    public void write(OutputStream out) throws IOException {
      createOutputStream(out);

      IndexInput in = null;
      try {
        initWrite();

        RefCounted<SolrIndexSearcher> sref = core.getSearcher();
        Directory dir;
        try {
          SolrIndexSearcher searcher = sref.get();
          dir = searcher.getIndexReader().directory();
        } finally {
          sref.decref();
        }
        in = dir.openInput(fileName, IOContext.READONCE);
        // if offset is mentioned move the pointer to that point
        if (offset != -1) in.seek(offset);
        
        long filelen = dir.fileLength(fileName);
        long maxBytesBeforePause = 0;

        while (true) {
          offset = offset == -1 ? 0 : offset;
          int read = (int) Math.min(buf.length, filelen - offset);
          in.readBytes(buf, 0, read);
          fos.writeInt(read);
          if (useChecksum) {
            checksum.reset();
            checksum.update(buf, 0, read);
            fos.writeLong(checksum.getValue());
          }
          fos.write(buf, 0, read);
          fos.flush();

          //Pause if necessary
          maxBytesBeforePause += read;
          if (maxBytesBeforePause >= rateLimiter.getMinPauseCheckBytes()) {
            rateLimiter.pause(maxBytesBeforePause);
            maxBytesBeforePause = 0;
          }
          if (read != buf.length) {
            writeNothingAndFlush();
            fos.close();
            break;
          }
          offset += read;
          in.seek(offset);
        }
      } catch (IOException e) {
        LOG.warn("Exception while writing response for params: " + params, e);
      } finally {
        if (in != null) {
          in.close();
        }
        releaseCommitPointAndExtendReserve();
      }
    }


    /**
     * Used to write a marker for EOF
     */
    protected void writeNothingAndFlush() throws IOException {
      fos.writeInt(0);
      fos.flush();
    }
  }

  /**This is used to write files in the conf directory.
   */
  private class LocalFsFileStream extends DirectoryFileStream {

    public LocalFsFileStream(SolrParams solrParams) {
      super(solrParams);
    }

    @Override
    public void write(OutputStream out) throws IOException {
      createOutputStream(out);
      FileInputStream inputStream = null;
      try {
        initWrite();
  
        //if if is a conf file read from config diectory
        File file = new File(core.getResourceLoader().getConfigDir(), cfileName);

        if (file.exists() && file.canRead()) {
          inputStream = new FileInputStream(file);
          FileChannel channel = inputStream.getChannel();
          //if offset is mentioned move the pointer to that point
          if (offset != -1)
            channel.position(offset);
          ByteBuffer bb = ByteBuffer.wrap(buf);

          while (true) {
            bb.clear();
            long bytesRead = channel.read(bb);
            if (bytesRead <= 0) {
              writeNothingAndFlush();
              fos.close();
              break;
            }
            fos.writeInt((int) bytesRead);
            if (useChecksum) {
              checksum.reset();
              checksum.update(buf, 0, (int) bytesRead);
              fos.writeLong(checksum.getValue());
            }
            fos.write(buf, 0, (int) bytesRead);
            fos.flush();
          }
        } else {
          writeNothingAndFlush();
        }
      } catch (IOException e) {
        LOG.warn("Exception while writing response for params: " + params, e);
      } finally {
        IOUtils.closeQuietly(inputStream);
        releaseCommitPointAndExtendReserve();
      }
    }
  } 
  
  public static final String MASTER_URL = "masterUrl";

  public static final String STATUS = "status";

  public static final String COMMAND = "command";

  public static final String CMD_DETAILS = "details";

  public static final String CMD_BACKUP = "backup";

  public static final String CMD_FETCH_INDEX = "fetchindex";

  public static final String CMD_ABORT_FETCH = "abortfetch";

  public static final String CMD_GET_FILE_LIST = "filelist";

  public static final String CMD_GET_FILE = "filecontent";

  public static final String CMD_DISABLE_POLL = "disablepoll";

  public static final String CMD_DISABLE_REPL = "disablereplication";

  public static final String CMD_ENABLE_REPL = "enablereplication";

  public static final String CMD_ENABLE_POLL = "enablepoll";

  public static final String CMD_INDEX_VERSION = "indexversion";

  public static final String CMD_SHOW_COMMITS = "commits";

  public static final String CMD_DELETE_BACKUP = "deletebackup";

  public static final String GENERATION = "generation";

  public static final String OFFSET = "offset";

  public static final String LEN = "len";

  public static final String FILE = "file";

  public static final String NAME = "name";

  public static final String SIZE = "size";

  public static final String MAX_WRITE_PER_SECOND = "maxWriteMBPerSec";

  public static final String CONF_FILE_SHORT = "cf";

  public static final String CHECKSUM = "checksum";

  public static final String ALIAS = "alias";

  public static final String CONF_CHECKSUM = "confchecksum";

  public static final String CONF_FILES = "confFiles";

  public static final String REPLICATE_AFTER = "replicateAfter";

  public static final String FILE_STREAM = "filestream";

  public static final int PACKET_SZ = 1024 * 1024; // 1MB

  public static final String RESERVE = "commitReserveDuration";

  public static final String COMPRESSION = "compression";

  public static final String EXTERNAL = "external";

  public static final String INTERNAL = "internal";

  public static final String ERR_STATUS = "ERROR";

  public static final String OK_STATUS = "OK";

  public static final String NEXT_EXECUTION_AT = "nextExecutionAt";
  
  public static final String NUMBER_BACKUPS_TO_KEEP_REQUEST_PARAM = "numberToKeep";
  
  public static final String NUMBER_BACKUPS_TO_KEEP_INIT_PARAM = "maxNumberOfBackups";

  /** 
   * Boolean param for tests that can be specified when using 
   * {@link #CMD_FETCH_INDEX} to force the current request to block until 
   * the fetch is complete.  <b>NOTE:</b> This param is not advised for 
   * non-test code, since the the durration of the fetch for non-trivial
   * indexes will likeley cause the request to time out.
   *
   * @lucene.internal
   */
  public static final String WAIT = "wait";
}

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
package org.apache.solr.handler;

import org.apache.lucene.index.IndexCommit;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.request.BinaryQueryResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.DeflaterOutputStream;

/**
 * <p> A Handler which provides a REST API for replication and serves replication requests from Slaves. <p/> </p>
 * <p>When running on the master, it provides the following commands <ol> <li>Get the current replicatable index version
 * (command=indexversion)</li> <li>Get the list of files for a given index version
 * (command=filelist&amp;indexversion=&lt;VERSION&gt;)</li> <li>Get full or a part (chunk) of a given index or a config
 * file (command=filecontent&amp;file=&lt;FILE_NAME&gt;) You can optionally specify an offset and length to get that
 * chunk of the file. You can request a configuration file by using "cf" parameter instead of the "file" parameter.</li>
 * <li>Get status/statistics (command=details)</li> </ol> </p> <p>When running on the slave, it provides the following
 * commands <ol> <li>Perform a snap pull now (command=snappull)</li> <li>Get status/statistics (command=details)</li>
 * <li>Abort a snap pull (command=abort)</li> <li>Enable/Disable polling the master for new versions (command=enablepoll
 * or command=disablepoll)</li> </ol> </p>
 *
 * @version $Id$
 * @since solr 1.4
 */
public class ReplicationHandler extends RequestHandlerBase implements SolrCoreAware {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationHandler.class.getName());
  private SolrCore core;

  private SnapPuller snapPuller;

  private ReentrantLock snapPullLock = new ReentrantLock();

  private String includeConfFiles;

  private NamedList<String> confFileNameAlias = new NamedList<String>();

  private boolean isMaster = false;

  private boolean isSlave = false;

  private boolean replicateOnOptimize = false;

  private boolean replicateOnCommit = false;

  private int numTimesReplicated = 0;

  private final Map<String, FileInfo> confFileInfoCache = new HashMap<String, FileInfo>();

  private Integer reserveCommitDuration = SnapPuller.readInterval("00:00:10");

  private volatile IndexCommit indexCommitPoint;

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.setHttpCaching(false);
    final SolrParams solrParams = req.getParams();
    String command = solrParams.get(COMMAND);
    if (command == null) {
      rsp.add("status", "OK");
      return;
    }
    // This command does not give the current index version of the master
    // It gives the current 'replicateable' index version
    if (command.equals(CMD_INDEX_VERSION)) {
      IndexCommit commitPoint = indexCommitPoint;  // make a copy so it won't change
      if (commitPoint != null) {
        rsp.add(CMD_INDEX_VERSION, commitPoint.getVersion());
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
    } else if (command.equals(CMD_SNAP_SHOOT)) {
      doSnapShoot(rsp);
    } else if (command.equals(CMD_SNAP_PULL)) {
      new Thread() {
        public void run() {
          doSnapPull(solrParams);
        }
      }.start();
      rsp.add("status", "OK");
    } else if (command.equals(CMD_DISABLE_POLL)) {
      if (snapPuller != null)
        snapPuller.disablePoll();
    } else if (command.equals(CMD_ENABLE_POLL)) {
      if (snapPuller != null)
        snapPuller.enablePoll();
    } else if (command.equals(CMD_ABORT_SNAP_PULL)) {
      if (snapPuller != null)
        snapPuller.abortPull();
    } else if (command.equals(CMD_FILE_CHECKSUM)) {
      // this command is not used by anyone
      getFileChecksum(solrParams, rsp);
    } else if (command.equals(CMD_SHOW_COMMITS)) {
      rsp.add(CMD_SHOW_COMMITS, getCommits());
    } else if (command.equals(CMD_DETAILS)) {
      getReplicationDetails(rsp);
    }
  }

  private List<NamedList> getCommits() {
    Map<Long, IndexCommit> commits = core.getDeletionPolicy().getCommits();
    List<NamedList> l = new ArrayList<NamedList>();

    for (IndexCommit c : commits.values()) {
      try {
        NamedList nl = new NamedList();
        nl.add(CMD_INDEX_VERSION, c.getVersion());
        nl.add(GENERATION, c.getGeneration());
        nl.add(CMD_GET_FILE_LIST, c.getFileNames().toString());
        l.add(nl);
      } catch (IOException e) {
        LOG.warn("Exception while reading files for commit " + c, e);
      }
    }
    return l;
  }

  /**
   * Gets the checksum of a file
   */
  private void getFileChecksum(SolrParams solrParams, SolrQueryResponse rsp) {
    Checksum checksum = new Adler32();
    File dir = new File(core.getIndexDir());
    rsp.add(CHECKSUM, getCheckSums(solrParams.getParams(FILE), dir, checksum));
    dir = new File(core.getResourceLoader().getConfigDir());
    rsp.add(CONF_CHECKSUM, getCheckSums(solrParams.getParams(CONF_FILE_SHORT), dir, checksum));
  }

  private Map<String, Long> getCheckSums(String[] files, File dir, Checksum checksum) {
    Map<String, Long> checksumMap = new HashMap<String, Long>();
    if (files == null || files.length == 0)
      return checksumMap;
    for (String file : files) {
      File f = new File(dir, file);
      Long checkSumVal = getCheckSum(checksum, f);
      if (checkSumVal != null)
        checksumMap.put(file, checkSumVal);
    }
    return checksumMap;
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
      closeNoExp(fis);
    }
    return null;
  }

  private volatile SnapPuller tempSnapPuller;

  void doSnapPull(SolrParams solrParams) {
    String masterUrl = solrParams == null ? null : solrParams.get(MASTER_URL);
    if (!isSlave && masterUrl == null)
      return;
    if (!snapPullLock.tryLock())
      return;
    try {
      tempSnapPuller = snapPuller;
      if (masterUrl != null) {
        tempSnapPuller = new SnapPuller(solrParams.toNamedList(), this, core);
      }
      tempSnapPuller.fetchLatestIndex(core);
    } catch (Exception e) {
      LOG.error("SnapPull failed ", e);
    } finally {
      tempSnapPuller = snapPuller;
      snapPullLock.unlock();
    }
  }

  boolean isReplicating() {
    return snapPullLock.isLocked();
  }

  private void doSnapShoot(SolrQueryResponse rsp) {
    try {
      new SnapShooter(core).createSnapAsync(core.getDeletionPolicy().getLatestCommit().getFileNames());
    } catch (Exception e) {
      rsp.add("exception", e);
    }
  }

  /**
   * This method adds an Object of FileStream to the resposnse . The FileStream implements a custom protocol which is
   * understood by SnapPuller.FileFetcher
   *
   * @see org.apache.solr.handler.SnapPuller.FileFetcher
   */
  private void getFileStream(SolrParams solrParams, SolrQueryResponse rsp) {
    ModifiableSolrParams rawParams = new ModifiableSolrParams(solrParams);
    rawParams.set(CommonParams.WT, FILE_STREAM);
    rsp.add(FILE_STREAM, new FileStream(solrParams));
  }

  @SuppressWarnings("unchecked")
  private void getFileList(SolrParams solrParams, SolrQueryResponse rsp) {
    String v = solrParams.get(CMD_INDEX_VERSION);
    if (v == null) {
      rsp.add("status", "no indexversion specified");
      return;
    }
    long version = Long.parseLong(v);
    IndexCommit commit = core.getDeletionPolicy().getCommitPoint(version);
    if (commit == null) {
      rsp.add("status", "invalid indexversion");
      return;
    }
    // reserve the indexcommit for sometime
    core.getDeletionPolicy().setReserveDuration(version, reserveCommitDuration);
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    try {
      //get all the files in the commit
      Collection<String> files = commit.getFileNames();
      for (String fileName : files) {
        File file = new File(core.getIndexDir(), fileName);
        Map<String, Object> fileMeta = getFileInfo(file);
        result.add(fileMeta);
      }
    } catch (IOException e) {
      rsp.add("status", "unable to get file names for given indexversion");
      rsp.add("exception", e);
      LOG.warn("Unable to get file names for indexCommit version: "
              + version, e);
    }
    rsp.add(CMD_GET_FILE_LIST, result);
    if (confFileNameAlias.size() < 1)
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
    List<Map<String, Object>> confFiles = new ArrayList<Map<String, Object>>();
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
      Map<String, Object> map = new HashMap<String, Object>();
      map.put(NAME, name);
      map.put(SIZE, size);
      map.put(LAST_MODIFIED, lastmodified);
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
    return snapPuller.isPollingDisabled();
  }

  int getTimesReplicatedSinceStartup() {
    return numTimesReplicated;
  }

  void setTimesReplicatedSinceStartup() {
    numTimesReplicated++;
  }

  long getIndexSize() {
    return computeIndexSize(new File(core.getIndexDir()));
  }

  private long computeIndexSize(File f) {
    if (f.isFile())
      return f.length();
    File[] files = f.listFiles();
    long size = 0;
    if (files != null && files.length > 0) {
      for (File file : files) size += file.length();
    }
    return size;
  }

  /**
   * Collects the details such as name, size ,lastModified of a file
   */
  private Map<String, Object> getFileInfo(File file) {
    Map<String, Object> fileMeta = new HashMap<String, Object>();
    fileMeta.put(NAME, file.getName());
    fileMeta.put(SIZE, file.length());
    fileMeta.put(LAST_MODIFIED, file.lastModified());
    return fileMeta;
  }

  public String getDescription() {
    return "ReplicationHandler provides replication of index and configuration files from Master to Slaves";
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public String getVersion() {
    return "$Revision$";
  }

  String readableSize(long size) {
    NumberFormat formatter = NumberFormat.getNumberInstance();
    formatter.setMaximumFractionDigits(2);
    if (size / (1024 * 1024 * 1024) > 0) {
      return formatter.format(size * 1.0d / (1024 * 1024 * 1024)) + " GB";
    } else if (size / (1024 * 1024) > 0) {
      return formatter.format(size * 1.0d / (1024 * 1024)) + " MB";
    } else if (size / 1024 > 0) {
      return formatter.format(size * 1.0d / 1024) + " KB";
    } else {
      return String.valueOf(size) + " bytes";
    }
  }

  private long[] getIndexVersion() {
    long version[] = new long[2];
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    try {
      version[0] = searcher.get().getReader().getIndexCommit().getVersion();
      version[1] = searcher.get().getReader().getIndexCommit().getGeneration();
    } catch (IOException e) {
      LOG.warn("Unable to get index version : ", e);
    } finally {
      searcher.decref();
    }
    return version;
  }

  @Override
  @SuppressWarnings("unchecked")
  public NamedList getStatistics() {
    NamedList list = super.getStatistics();
    if (core != null) {
      list.add("indexSize", readableSize(getIndexSize()));
      long[] versionGen = getIndexVersion();
      list.add(CMD_INDEX_VERSION, versionGen[0]);
      list.add(GENERATION, versionGen[1]);

      list.add("indexPath", core.getIndexDir());
      list.add("isMaster", String.valueOf(isMaster));

      SnapPuller snapPuller = tempSnapPuller;
      if (snapPuller != null) {
        list.add(MASTER_URL, snapPuller.getMasterUrl());
        if (snapPuller.getPollInterval() != null) {
          list.add(SnapPuller.POLL_INTERVAL, snapPuller.getPollInterval());
        }
        list.add("isPollingDisabled", String.valueOf(isPollingDisabled()));
        list.add("isReplicating", String.valueOf(isReplicating()));
      }
      if (isMaster) {
        if (includeConfFiles != null)
          list.add("confFilesToReplicate", includeConfFiles);
        if (replicateOnCommit)
          list.add(REPLICATE_AFTER, "commit");
        if (replicateOnOptimize)
          list.add(REPLICATE_AFTER, "optimize");
      }
    }
    return list;
  }

  /**
   * Used for showing statistics and progress information.
   */
  void getReplicationDetails(SolrQueryResponse resp) {
    String timeLastReplicated = "", confFilesReplicated = "", confFilesReplicatedTime = "", timesIndexReplicated = "", timesConfigReplicated = "";
    NamedList<Object> details = new SimpleOrderedMap<Object>();
    FileInputStream inFile = null;

    details.add("indexSize", readableSize(getIndexSize()));
    details.add("indexPath", core.getIndexDir());
    details.add(CMD_SHOW_COMMITS, getCommits());
    details.add("isMaster", String.valueOf(isMaster));
    long[] versionAndGeneration = getIndexVersion();
    details.add(CMD_INDEX_VERSION, versionAndGeneration[0]);
    details.add(GENERATION, versionAndGeneration[1]);
    IndexCommit commit = indexCommitPoint;  // make a copy so it won't change
    if (isMaster && commit != null) {
      details.add("replicatable" + CMD_INDEX_VERSION, commit.getVersion());
      details.add("replicatable" + GENERATION, commit.getGeneration());
    }
    SnapPuller snapPuller = tempSnapPuller;
    if (snapPuller != null) {
      try {
        Properties props = new Properties();
        File f = new File(core.getDataDir(), SnapPuller.REPLICATION_PROPERTIES);
        if (f.exists()) {
          inFile = new FileInputStream(f);
          props.load(inFile);
          timeLastReplicated = props.getProperty("indexReplicatedAt");
          if (props.containsKey("timesIndexReplicated"))
            timesIndexReplicated = props.getProperty("timesIndexReplicated");
          if (props.containsKey("confFilesReplicated"))
            confFilesReplicated = props.getProperty("confFilesReplicated");
          if (props.containsKey("confFilesReplicatedAt"))
            confFilesReplicatedTime = props.getProperty("confFilesReplicatedAt");
          if (props.containsKey("timesConfigReplicated"))
            timesConfigReplicated = props.getProperty("timesConfigReplicated");
        }
      } catch (Exception e) {
        LOG.warn("Exception while reading " + SnapPuller.REPLICATION_PROPERTIES);
      } finally {
        closeNoExp(inFile);
      }
      try {
        NamedList nl = snapPuller.getCommandResponse(CMD_DETAILS);
        details.add("masterDetails", nl.get(CMD_DETAILS));
      } catch (IOException e) {
        LOG.warn("Exception while invoking a 'details' method on master ", e);
      }
      details.add(MASTER_URL, snapPuller.getMasterUrl());
      if (snapPuller.getPollInterval() != null) {
        details.add(SnapPuller.POLL_INTERVAL, snapPuller.getPollInterval());
      }
      if (snapPuller.getNextScheduledExecTime() != null && !isPollingDisabled()) {
        Date d = new Date(snapPuller.getNextScheduledExecTime());
        details.add("nextExecutionAt", d.toString());
      } else if (isPollingDisabled()) {
        details.add("nextExecutionAt", "Polling disabled");
      } else
        details.add("nextExecutionAt", "");

      if (timeLastReplicated != null && timeLastReplicated.length() > 0) {
        Date d = new Date(Long.valueOf(timeLastReplicated));
        details.add("indexReplicatedAt", d.toString());
      } else {
        details.add("indexReplicatedAt", "");
      }
      details.add("timesIndexReplicated", timesIndexReplicated);
      details.add("confFilesReplicated", confFilesReplicated);
      details.add("timesConfigReplicated", timesConfigReplicated);
      if (confFilesReplicatedTime != null && confFilesReplicatedTime.length() > 0) {
        Date d = new Date(Long.valueOf(confFilesReplicatedTime));
        details.add("confFilesReplicatedAt", d.toString());
      } else {
        details.add("confFilesReplicatedAt", confFilesReplicatedTime);
      }

      try {
        long bytesToDownload = 0;
        List<String> filesToDownload = new ArrayList<String>();
        if (snapPuller.getFilesToDownload() != null) {
          for (Map<String, Object> file : snapPuller.getFilesToDownload()) {
            filesToDownload.add((String) file.get(NAME));
            bytesToDownload += (Long) file.get(SIZE);
          }
        }

        //get list of conf files to download
        for (Map<String, Object> file : snapPuller.getConfFilesToDownload()) {
          filesToDownload.add((String) file.get(NAME));
          bytesToDownload += (Long) file.get(SIZE);
        }

        details.add("filesToDownload", filesToDownload.toString());
        details.add("numFilesToDownload", String.valueOf(filesToDownload.size()));
        details.add("bytesToDownload", readableSize(bytesToDownload));

        long bytesDownloaded = 0;
        List<String> filesDownloaded = new ArrayList<String>();
        for (Map<String, Object> file : snapPuller.getFilesDownloaded()) {
          filesDownloaded.add((String) file.get(NAME));
          bytesDownloaded += (Long) file.get(SIZE);
        }

        //get list of conf files downloaded
        for (Map<String, Object> file : snapPuller.getConfFilesDownloaded()) {
          filesDownloaded.add((String) file.get(NAME));
          bytesDownloaded += (Long) file.get(SIZE);
        }

        details.add("filesDownloaded", filesDownloaded.toString());
        details.add("numFilesDownloaded", String.valueOf(filesDownloaded.size()));

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

        long timeElapsed = 0, estimatedTimeRemaining = 0;
        Date replicationStartTime = null;
        if (snapPuller.getReplicationStartTime() > 0) {
          replicationStartTime = new Date(snapPuller.getReplicationStartTime());
          timeElapsed = (System.currentTimeMillis() - snapPuller.getReplicationStartTime()) / 1000;
        }
        if (replicationStartTime != null) {
          details.add("replicationStartTime", replicationStartTime.toString());
        }
        details.add("timeElapsed", String.valueOf(timeElapsed) + "s");

        if (bytesDownloaded > 0)
          estimatedTimeRemaining = ((bytesToDownload - bytesDownloaded) * timeElapsed) / bytesDownloaded;
        float totalPercent = 0;
        long downloadSpeed = 0;
        if (bytesToDownload > 0)
          totalPercent = (bytesDownloaded * 100) / bytesToDownload;
        if (timeElapsed > 0)
          downloadSpeed = (bytesDownloaded / timeElapsed);
        if (currFile != null)
          details.add("currentFile", currFile);
        details.add("currentFileSize", readableSize(currFileSize));
        details.add("currentFileSizeDownloaded", readableSize(currFileSizeDownloaded));
        details.add("currentFileSizePercent", String.valueOf(percentDownloaded));
        details.add("bytesDownloaded", readableSize(bytesDownloaded));
        details.add("totalPercent", String.valueOf(totalPercent));
        details.add("timeRemaining", String.valueOf(estimatedTimeRemaining) + "s");
        details.add("downloadSpeed", readableSize(downloadSpeed));
        details.add("isPollingDisabled", String.valueOf(isPollingDisabled()));
        details.add("isReplicating", String.valueOf(isReplicating()));
      } catch (Exception e) {
        LOG.error("Exception while writing details: ", e);
      }
    }

    if (isMaster) {
      if (includeConfFiles != null)
        details.add(CONF_FILES, includeConfFiles);
      if (replicateOnCommit)
        details.add(REPLICATE_AFTER, "commit");
      if (replicateOnOptimize)
        details.add(REPLICATE_AFTER, "optimize");
    }

    resp.add(CMD_DETAILS, details);
  }

  @SuppressWarnings("unchecked")
  public void inform(SolrCore core) {
    this.core = core;
    registerFileStreamResponseWriter();
    registerCloseHook();
    NamedList slave = (NamedList) initArgs.get("slave");
    if (slave != null) {
      tempSnapPuller = snapPuller = new SnapPuller(slave, this, core);
      isSlave = true;
    }
    NamedList master = (NamedList) initArgs.get("master");
    if (master != null) {
      includeConfFiles = (String) master.get(CONF_FILES);
      if (includeConfFiles != null && includeConfFiles.trim().length() > 0) {
        List<String> files = Arrays.asList(includeConfFiles.split(","));
        for (String file : files) {
          if (file.trim().length() == 0) continue;
          String[] strs = file.split(":");
          // if there is an alias add it or it is null
          confFileNameAlias.add(strs[0], strs.length > 1 ? strs[1] : null);
        }
        LOG.info("Replication enabled for following config files: " + includeConfFiles);
      }
      List snapshot = master.getAll("snapshot");
      boolean snapshotOnCommit = snapshot.contains("commit");
      boolean snapshotOnOptimize = snapshot.contains("optimize");
      List replicateAfter = master.getAll(REPLICATE_AFTER);
      replicateOnCommit = replicateAfter.contains("commit");
      replicateOnOptimize = replicateAfter.contains("optimize");

      if (replicateOnOptimize || snapshotOnOptimize) {
        core.getUpdateHandler().registerOptimizeCallback(getEventListener(snapshotOnOptimize, replicateOnOptimize));
      }
      if (replicateOnCommit || snapshotOnCommit) {
        replicateOnCommit = true;
        core.getUpdateHandler().registerCommitCallback(getEventListener(snapshotOnCommit, replicateOnCommit));
      }
      if (replicateAfter.contains("startup")) {
        RefCounted<SolrIndexSearcher> s = core.getNewestSearcher(false);
        try {
          indexCommitPoint = s.get().getReader().getIndexCommit();
        } catch (IOException e) {
          LOG.warn("Unable to get IndexCommit on startup", e);
        } finally {
          s.decref();
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

  /**
   * register a closehook
   */
  private void registerCloseHook() {
    core.addCloseHook(new CloseHook() {
      public void close(SolrCore core) {
        if (snapPuller != null) {
          snapPuller.destroy();
        }
      }
    });
  }

  /**
   * A ResponseWriter is registered automatically for wt=filestream This response writer is used to transfer index files
   * in a block-by-block manner within the same HTTP response.
   */
  private void registerFileStreamResponseWriter() {
    core.registerResponseWriter(FILE_STREAM, new BinaryQueryResponseWriter() {
      public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse resp) throws IOException {
        FileStream stream = (FileStream) resp.getValues().get(FILE_STREAM);
        stream.write(out);
      }

      public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
        throw new RuntimeException("This is a binary writer , Cannot write to a characterstream");
      }

      public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
        return "application/octet-stream";
      }

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
      public void init(NamedList args) {/*no op*/ }

      /**
       * This refreshes the latest replicateable index commit and optionally can create Snapshots as well
       */
      public void postCommit() {
        if (getCommit) {
          indexCommitPoint = core.getDeletionPolicy().getLatestCommit();
        }
        if (snapshoot) {
          try {
            SnapShooter snapShooter = new SnapShooter(core);
            snapShooter.createSnapAsync(core.getDeletionPolicy().getLatestCommit().getFileNames());
          } catch (Exception e) {
            LOG.error("Exception while snapshooting", e);
          }
        }
      }

      public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) { /*no op*/}
    };
  }

  static void closeNoExp(Closeable closeable) {
    try {
      if (closeable != null)
        closeable.close();
    } catch (Exception e) {/*no op*/ }
  }

  private class FileStream {
    private SolrParams params;

    private FastOutputStream fos;

    private Long indexVersion;
    private IndexDeletionPolicyWrapper delPolicy;

    public FileStream(SolrParams solrParams) {
      params = solrParams;
      delPolicy = core.getDeletionPolicy();
    }

    public void write(OutputStream out) throws IOException {
      String fileName = params.get(FILE);
      String cfileName = params.get(CONF_FILE_SHORT);
      String sOffset = params.get(OFFSET);
      String sLen = params.get(LEN);
      String compress = params.get(COMPRESSION);
      String sChecksum = params.get(CHECKSUM);
      String sindexVersion = params.get(CMD_INDEX_VERSION);
      if (sindexVersion != null) indexVersion = Long.parseLong(sindexVersion);
      if (Boolean.parseBoolean(compress)) {
        fos = new FastOutputStream(new DeflaterOutputStream(out));
      } else {
        fos = new FastOutputStream(out);
      }
      FileInputStream inputStream = null;
      int packetsWritten = 0;
      try {
        long offset = -1;
        int len = -1;
        //check if checksum is requested
        boolean useChecksum = Boolean.parseBoolean(sChecksum);
        if (sOffset != null)
          offset = Long.parseLong(sOffset);
        if (sLen != null)
          len = Integer.parseInt(sLen);
        if (fileName == null && cfileName == null) {
          //no filename do nothing
          writeNothing();
        }

        File file = null;
        if (cfileName != null) {
          //if if is a conf file read from config diectory
          file = new File(core.getResourceLoader().getConfigDir(), cfileName);
        } else {
          //else read from the indexdirectory
          file = new File(core.getIndexDir(), fileName);
        }
        if (file.exists() && file.canRead()) {
          inputStream = new FileInputStream(file);
          FileChannel channel = inputStream.getChannel();
          //if offset is mentioned move the pointer to that point
          if (offset != -1)
            channel.position(offset);
          byte[] buf = new byte[(len == -1 || len > PACKET_SZ) ? PACKET_SZ : len];
          Checksum checksum = null;
          if (useChecksum)
            checksum = new Adler32();
          ByteBuffer bb = ByteBuffer.wrap(buf);

          while (true) {
            bb.clear();
            long bytesRead = channel.read(bb);
            if (bytesRead <= 0) {
              writeNothing();
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
            if (indexVersion != null && (packetsWritten % 5 == 0)) {
              //after every 5 packets reserve the commitpoint for some time
              delPolicy.setReserveDuration(indexVersion, reserveCommitDuration);
            }
            packetsWritten++;
          }
        } else {
          writeNothing();
        }
      } catch (IOException e) {
        LOG.warn("Exception while writing response for params: " + params, e);
      } finally {
        closeNoExp(inputStream);
      }
    }


    /**
     * Used to write a marker for EOF
     */
    private void writeNothing() throws IOException {
      fos.writeInt(0);
      fos.flush();
    }
  }

  public static final String MASTER_URL = "masterUrl";

  public static final String COMMAND = "command";

  public static final String CMD_DETAILS = "details";

  public static final String CMD_SNAP_SHOOT = "snapshoot";

  public static final String CMD_SNAP_PULL = "snappull";

  public static final String CMD_ABORT_SNAP_PULL = "abortsnappull";

  public static final String CMD_GET_FILE_LIST = "filelist";

  public static final String CMD_GET_FILE = "filecontent";

  public static final String CMD_FILE_CHECKSUM = "filechecksum";

  public static final String CMD_DISABLE_POLL = "disablepoll";

  public static final String CMD_ENABLE_POLL = "enablepoll";

  public static final String CMD_INDEX_VERSION = "indexversion";

  public static final String CMD_SHOW_COMMITS = "commits";

  public static final String GENERATION = "generation";

  public static final String OFFSET = "offset";

  public static final String LEN = "len";

  public static final String FILE = "file";

  public static final String NAME = "name";

  public static final String SIZE = "size";

  public static final String LAST_MODIFIED = "lastmodified";

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

}

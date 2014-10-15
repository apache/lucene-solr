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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler.FileInfo;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.PropertiesInputStream;
import org.apache.solr.util.PropertiesOutputStream;
import org.apache.solr.util.RefCounted;
import org.eclipse.jetty.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.ReplicationHandler.ALIAS;
import static org.apache.solr.handler.ReplicationHandler.CHECKSUM;
import static org.apache.solr.handler.ReplicationHandler.CMD_DETAILS;
import static org.apache.solr.handler.ReplicationHandler.CMD_GET_FILE;
import static org.apache.solr.handler.ReplicationHandler.CMD_GET_FILE_LIST;
import static org.apache.solr.handler.ReplicationHandler.CMD_INDEX_VERSION;
import static org.apache.solr.handler.ReplicationHandler.COMMAND;
import static org.apache.solr.handler.ReplicationHandler.COMPRESSION;
import static org.apache.solr.handler.ReplicationHandler.CONF_FILES;
import static org.apache.solr.handler.ReplicationHandler.CONF_FILE_SHORT;
import static org.apache.solr.handler.ReplicationHandler.EXTERNAL;
import static org.apache.solr.handler.ReplicationHandler.FILE;
import static org.apache.solr.handler.ReplicationHandler.FILE_STREAM;
import static org.apache.solr.handler.ReplicationHandler.GENERATION;
import static org.apache.solr.handler.ReplicationHandler.INTERNAL;
import static org.apache.solr.handler.ReplicationHandler.MASTER_URL;
import static org.apache.solr.handler.ReplicationHandler.NAME;
import static org.apache.solr.handler.ReplicationHandler.OFFSET;
import static org.apache.solr.handler.ReplicationHandler.SIZE;

/**
 * <p/> Provides functionality of downloading changed index files as well as config files and a timer for scheduling fetches from the
 * master. </p>
 *
 *
 * @since solr 1.4
 */
public class SnapPuller {
  public static final String INDEX_PROPERTIES = "index.properties";

  private static final Logger LOG = LoggerFactory.getLogger(SnapPuller.class.getName());

  private final String masterUrl;

  private final ReplicationHandler replicationHandler;

  private final Integer pollInterval;

  private String pollIntervalStr;

  private ScheduledExecutorService executorService;

  private volatile long executorStartTime;

  private volatile long replicationStartTime;

  private final SolrCore solrCore;

  private volatile List<Map<String, Object>> filesToDownload;

  private volatile List<Map<String, Object>> confFilesToDownload;

  private volatile List<Map<String, Object>> filesDownloaded;

  private volatile List<Map<String, Object>> confFilesDownloaded;

  private volatile Map<String, Object> currentFile;

  private volatile DirectoryFileFetcher dirFileFetcher;
  
  private volatile LocalFsFileFetcher localFileFetcher;

  private volatile ExecutorService fsyncService;

  private volatile boolean stop = false;

  private boolean useInternal = false;

  private boolean useExternal = false;

  /**
   * Disable the timer task for polling
   */
  private AtomicBoolean pollDisabled = new AtomicBoolean(false);

  private final HttpClient myHttpClient;

  private static HttpClient createHttpClient(SolrCore core, String connTimeout, String readTimeout, String httpBasicAuthUser, String httpBasicAuthPassword, boolean useCompression) {
    final ModifiableSolrParams httpClientParams = new ModifiableSolrParams();
    httpClientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connTimeout != null ? connTimeout : "5000");
    httpClientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, readTimeout != null ? readTimeout : "20000");
    httpClientParams.set(HttpClientUtil.PROP_BASIC_AUTH_USER, httpBasicAuthUser);
    httpClientParams.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, httpBasicAuthPassword);
    httpClientParams.set(HttpClientUtil.PROP_ALLOW_COMPRESSION, useCompression);

    HttpClient httpClient = HttpClientUtil.createClient(httpClientParams, core.getCoreDescriptor().getCoreContainer().getUpdateShardHandler().getConnectionManager());

    return httpClient;
  }

  public SnapPuller(final NamedList initArgs, final ReplicationHandler handler, final SolrCore sc) {
    solrCore = sc;
    final SolrParams params = SolrParams.toSolrParams(initArgs);
    String masterUrl = (String) initArgs.get(MASTER_URL);
    if (masterUrl == null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "'masterUrl' is required for a slave");
    if (masterUrl.endsWith("/replication")) {
      masterUrl = masterUrl.substring(0, masterUrl.length()-12);
      LOG.warn("'masterUrl' must be specified without the /replication suffix");
    }
    this.masterUrl = masterUrl;
    
    this.replicationHandler = handler;
    pollIntervalStr = (String) initArgs.get(POLL_INTERVAL);
    pollInterval = readInterval(pollIntervalStr);
    String compress = (String) initArgs.get(COMPRESSION);
    useInternal = INTERNAL.equals(compress);
    useExternal = EXTERNAL.equals(compress);
    String connTimeout = (String) initArgs.get(HttpClientUtil.PROP_CONNECTION_TIMEOUT);
    String readTimeout = (String) initArgs.get(HttpClientUtil.PROP_SO_TIMEOUT);
    String httpBasicAuthUser = (String) initArgs.get(HttpClientUtil.PROP_BASIC_AUTH_USER);
    String httpBasicAuthPassword = (String) initArgs.get(HttpClientUtil.PROP_BASIC_AUTH_PASS);
    myHttpClient = createHttpClient(solrCore, connTimeout, readTimeout, httpBasicAuthUser, httpBasicAuthPassword, useExternal);
    if (pollInterval != null && pollInterval > 0) {
      startExecutorService();
    } else {
      LOG.info(" No value set for 'pollInterval'. Timer Task not started.");
    }
  }

  private void startExecutorService() {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        if (pollDisabled.get()) {
          LOG.info("Poll disabled");
          return;
        }
        try {
          LOG.debug("Polling for index modifications");
          executorStartTime = System.currentTimeMillis();
          replicationHandler.doFetch(null, false);
        } catch (Exception e) {
          LOG.error("Exception in fetching index", e);
        }
      }
    };
    executorService = Executors.newSingleThreadScheduledExecutor(
        new DefaultSolrThreadFactory("snapPuller"));
    long initialDelay = pollInterval - (System.currentTimeMillis() % pollInterval);
    executorService.scheduleAtFixedRate(task, initialDelay, pollInterval, TimeUnit.MILLISECONDS);
    LOG.info("Poll Scheduled at an interval of " + pollInterval + "ms");
  }

  /**
   * Gets the latest commit version and generation from the master
   */
  @SuppressWarnings("unchecked")
  NamedList getLatestVersion() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COMMAND, CMD_INDEX_VERSION);
    params.set(CommonParams.WT, "javabin");
    params.set(CommonParams.QT, "/replication");
    QueryRequest req = new QueryRequest(params);
    HttpSolrServer server = new HttpSolrServer(masterUrl, myHttpClient); //XXX modify to use shardhandler
    NamedList rsp;
    try {
      server.setSoTimeout(60000);
      server.setConnectionTimeout(15000);
      
      rsp = server.request(req);
    } catch (SolrServerException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e.getMessage(), e);
    } finally {
      server.shutdown();
    }
    return rsp;
  }

  /**
   * Fetches the list of files in a given index commit point and updates internal list of files to download.
   */
  private void fetchFileList(long gen) throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COMMAND,  CMD_GET_FILE_LIST);
    params.set(GENERATION, String.valueOf(gen));
    params.set(CommonParams.WT, "javabin");
    params.set(CommonParams.QT, "/replication");
    QueryRequest req = new QueryRequest(params);
    HttpSolrServer server = new HttpSolrServer(masterUrl, myHttpClient);  //XXX modify to use shardhandler
    try {
      server.setSoTimeout(60000);
      server.setConnectionTimeout(15000);
      NamedList response = server.request(req);

      List<Map<String, Object>> files = (List<Map<String,Object>>) response.get(CMD_GET_FILE_LIST);
      if (files != null)
        filesToDownload = Collections.synchronizedList(files);
      else {
        filesToDownload = Collections.emptyList();
        LOG.error("No files to download for index generation: "+ gen);
      }

      files = (List<Map<String,Object>>) response.get(CONF_FILES);
      if (files != null)
        confFilesToDownload = Collections.synchronizedList(files);

    } catch (SolrServerException e) {
      throw new IOException(e);
    } finally {
      server.shutdown();
    }
  }

  private boolean successfulInstall = false;

  /**
   * This command downloads all the necessary files from master to install a index commit point. Only changed files are
   * downloaded. It also downloads the conf files (if they are modified).
   *
   * @param core the SolrCore
   * @param forceReplication force a replication in all cases 
   * @return true on success, false if slave is already in sync
   * @throws IOException if an exception occurs
   */
  boolean fetchLatestIndex(final SolrCore core, boolean forceReplication) throws IOException, InterruptedException {
    successfulInstall = false;
    replicationStartTime = System.currentTimeMillis();
    Directory tmpIndexDir = null;
    String tmpIndex = null;
    Directory indexDir = null;
    String indexDirPath = null;
    boolean deleteTmpIdxDir = true;
    try {
      //get the current 'replicateable' index version in the master
      NamedList response = null;
      try {
        response = getLatestVersion();
      } catch (Exception e) {
        LOG.error("Master at: " + masterUrl + " is not available. Index fetch failed. Exception: " + e.getMessage());
        return false;
      }
      long latestVersion = (Long) response.get(CMD_INDEX_VERSION);
      long latestGeneration = (Long) response.get(GENERATION);

      // TODO: make sure that getLatestCommit only returns commit points for the main index (i.e. no side-car indexes)
      IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
      if (commit == null) {
        // Presumably the IndexWriter hasn't been opened yet, and hence the deletion policy hasn't been updated with commit points
        RefCounted<SolrIndexSearcher> searcherRefCounted = null;
        try {
          searcherRefCounted = core.getNewestSearcher(false);
          if (searcherRefCounted == null) {
            LOG.warn("No open searcher found - fetch aborted");
            return false;
          }
          commit = searcherRefCounted.get().getIndexReader().getIndexCommit();
        } finally {
          if (searcherRefCounted != null)
            searcherRefCounted.decref();
        }
      }


      if (latestVersion == 0L) {
        if (forceReplication && commit.getGeneration() != 0) {
          // since we won't get the files for an empty index,
          // we just clear ours and commit
          RefCounted<IndexWriter> iw = core.getUpdateHandler().getSolrCoreState().getIndexWriter(core);
          try {
            iw.get().deleteAll();
          } finally {
            iw.decref();
          }
          SolrQueryRequest req = new LocalSolrQueryRequest(core,
              new ModifiableSolrParams());
          core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
        }
        
        //there is nothing to be replicated
        successfulInstall = true;
        return true;
      }
      
      if (!forceReplication && IndexDeletionPolicyWrapper.getCommitTimestamp(commit) == latestVersion) {
        //master and slave are already in sync just return
        LOG.info("Slave in sync with master.");
        successfulInstall = true;
        return true;
      }
      LOG.info("Master's generation: " + latestGeneration);
      LOG.info("Slave's generation: " + commit.getGeneration());
      LOG.info("Starting replication process");
      // get the list of files first
      fetchFileList(latestGeneration);
      // this can happen if the commit point is deleted before we fetch the file list.
      if(filesToDownload.isEmpty()) return false;
      LOG.info("Number of files in latest index in master: " + filesToDownload.size());

      // Create the sync service
      fsyncService = Executors.newSingleThreadExecutor(new DefaultSolrThreadFactory("fsyncService"));
      // use a synchronized list because the list is read by other threads (to show details)
      filesDownloaded = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
      // if the generation of master is older than that of the slave , it means they are not compatible to be copied
      // then a new index directory to be created and all the files need to be copied
      boolean isFullCopyNeeded = IndexDeletionPolicyWrapper
          .getCommitTimestamp(commit) >= latestVersion
          || commit.getGeneration() >= latestGeneration || forceReplication;

      String tmpIdxDirName = "index." + new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date());
      tmpIndex = createTempindexDir(core, tmpIdxDirName);

      tmpIndexDir = core.getDirectoryFactory().get(tmpIndex, DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
      
      // cindex dir...
      indexDirPath = core.getIndexDir();
      indexDir = core.getDirectoryFactory().get(indexDirPath, DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);

      try {
        
        if (isIndexStale(indexDir)) {
          isFullCopyNeeded = true;
        }
        
        if (!isFullCopyNeeded) {
          // rollback - and do it before we download any files
          // so we don't remove files we thought we didn't need
          // to download later
          solrCore.getUpdateHandler().getSolrCoreState()
          .closeIndexWriter(core, true);
        }
        
        boolean reloadCore = false;
        
        try {
          LOG.info("Starting download to " + tmpIndexDir + " fullCopy="
              + isFullCopyNeeded);
          successfulInstall = false;
          
          downloadIndexFiles(isFullCopyNeeded, indexDir, tmpIndexDir,
              latestGeneration);
          LOG.info("Total time taken for download : "
              + ((System.currentTimeMillis() - replicationStartTime) / 1000)
              + " secs");
          Collection<Map<String,Object>> modifiedConfFiles = getModifiedConfFiles(confFilesToDownload);
          if (!modifiedConfFiles.isEmpty()) {
            downloadConfFiles(confFilesToDownload, latestGeneration);
            if (isFullCopyNeeded) {
              successfulInstall = modifyIndexProps(tmpIdxDirName);
              deleteTmpIdxDir = false;
            } else {
              successfulInstall = moveIndexFiles(tmpIndexDir, indexDir);
            }
            if (successfulInstall) {
              if (isFullCopyNeeded) {
                // let the system know we are changing dir's and the old one
                // may be closed
                if (indexDir != null) {
                  LOG.info("removing old index directory " + indexDir);
                  core.getDirectoryFactory().doneWithDirectory(indexDir);
                  core.getDirectoryFactory().remove(indexDir);
                }
              }
              
              LOG.info("Configuration files are modified, core will be reloaded");
              logReplicationTimeAndConfFiles(modifiedConfFiles,
                  successfulInstall);// write to a file time of replication and
                                     // conf files.
              reloadCore = true;
            }
          } else {
            terminateAndWaitFsyncService();
            if (isFullCopyNeeded) {
              successfulInstall = modifyIndexProps(tmpIdxDirName);
              deleteTmpIdxDir = false;
            } else {
              successfulInstall = moveIndexFiles(tmpIndexDir, indexDir);
            }
            if (successfulInstall) {
              logReplicationTimeAndConfFiles(modifiedConfFiles,
                  successfulInstall);
            }
          }
        } finally {
          if (!isFullCopyNeeded) {
            solrCore.getUpdateHandler().getSolrCoreState().openIndexWriter(core);
          }
        }
        
        // we must reload the core after we open the IW back up
        if (reloadCore) {
          reloadCore();
        }

        if (successfulInstall) {
          if (isFullCopyNeeded) {
            // let the system know we are changing dir's and the old one
            // may be closed
            if (indexDir != null) {
              LOG.info("removing old index directory " + indexDir);
              core.getDirectoryFactory().doneWithDirectory(indexDir);
              core.getDirectoryFactory().remove(indexDir);
            }
          }
          if (isFullCopyNeeded) {
            solrCore.getUpdateHandler().newIndexWriter(isFullCopyNeeded);
          }
          
          openNewSearcherAndUpdateCommitPoint(isFullCopyNeeded);
        }
        
        replicationStartTime = 0;
        return successfulInstall;
      } catch (ReplicationHandlerException e) {
        LOG.error("User aborted Replication");
        return false;
      } catch (SolrException e) {
        throw e;
      } catch (InterruptedException e) {
        throw new InterruptedException("Index fetch interrupted");
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Index fetch failed : ", e);
      }
    } finally {
      try {
        if (!successfulInstall) {
          logReplicationTimeAndConfFiles(null, successfulInstall);
        }
        filesToDownload = filesDownloaded = confFilesDownloaded = confFilesToDownload = null;
        replicationStartTime = 0;
        dirFileFetcher = null;
        localFileFetcher = null;
        if (fsyncService != null && !fsyncService.isShutdown()) fsyncService
            .shutdownNow();
        fsyncService = null;
        stop = false;
        fsyncException = null;
      } finally {
        if (deleteTmpIdxDir && tmpIndexDir != null) {
          try {
            core.getDirectoryFactory().doneWithDirectory(tmpIndexDir);
            core.getDirectoryFactory().remove(tmpIndexDir);
          } catch (IOException e) {
            SolrException.log(LOG, "Error removing directory " + tmpIndexDir, e);
          }
        }
        
        if (tmpIndexDir != null) {
          core.getDirectoryFactory().release(tmpIndexDir);
        }
        
        if (indexDir != null) {
          core.getDirectoryFactory().release(indexDir);
        }
      }
    }
  }

  private volatile Exception fsyncException;

  /**
   * terminate the fsync service and wait for all the tasks to complete. If it is already terminated
   */
  private void terminateAndWaitFsyncService() throws Exception {
    if (fsyncService.isTerminated()) return;
    fsyncService.shutdown();
     // give a long wait say 1 hr
    fsyncService.awaitTermination(3600, TimeUnit.SECONDS);
    // if any fsync failed, throw that exception back
    Exception fsyncExceptionCopy = fsyncException;
    if (fsyncExceptionCopy != null) throw fsyncExceptionCopy;
  }

  /**
   * Helper method to record the last replication's details so that we can show them on the statistics page across
   * restarts.
   * @throws IOException on IO error
   */
  private void logReplicationTimeAndConfFiles(Collection<Map<String, Object>> modifiedConfFiles, boolean successfulInstall) throws IOException {
    List<String> confFiles = new ArrayList<>();
    if (modifiedConfFiles != null && !modifiedConfFiles.isEmpty())
      for (Map<String, Object> map1 : modifiedConfFiles)
        confFiles.add((String) map1.get(NAME));

    Properties props = replicationHandler.loadReplicationProperties();
    long replicationTime = System.currentTimeMillis();
    long replicationTimeTaken = (replicationTime - getReplicationStartTime()) / 1000;
    Directory dir = null;
    try {
      dir = solrCore.getDirectoryFactory().get(solrCore.getDataDir(), DirContext.META_DATA, solrCore.getSolrConfig().indexConfig.lockType);
      
      int indexCount = 1, confFilesCount = 1;
      if (props.containsKey(TIMES_INDEX_REPLICATED)) {
        indexCount = Integer.valueOf(props.getProperty(TIMES_INDEX_REPLICATED)) + 1;
      }
      StringBuilder sb = readToStringBuilder(replicationTime, props.getProperty(INDEX_REPLICATED_AT_LIST));
      props.setProperty(INDEX_REPLICATED_AT_LIST, sb.toString());
      props.setProperty(INDEX_REPLICATED_AT, String.valueOf(replicationTime));
      props.setProperty(PREVIOUS_CYCLE_TIME_TAKEN, String.valueOf(replicationTimeTaken));
      props.setProperty(TIMES_INDEX_REPLICATED, String.valueOf(indexCount));
      if (modifiedConfFiles != null && !modifiedConfFiles.isEmpty()) {
        props.setProperty(CONF_FILES_REPLICATED, confFiles.toString());
        props.setProperty(CONF_FILES_REPLICATED_AT, String.valueOf(replicationTime));
        if (props.containsKey(TIMES_CONFIG_REPLICATED)) {
          confFilesCount = Integer.valueOf(props.getProperty(TIMES_CONFIG_REPLICATED)) + 1;
        }
        props.setProperty(TIMES_CONFIG_REPLICATED, String.valueOf(confFilesCount));
      }

      props.setProperty(LAST_CYCLE_BYTES_DOWNLOADED, String.valueOf(getTotalBytesDownloaded(this)));
      if (!successfulInstall) {
        int numFailures = 1;
        if (props.containsKey(TIMES_FAILED)) {
          numFailures = Integer.valueOf(props.getProperty(TIMES_FAILED)) + 1;
        }
        props.setProperty(TIMES_FAILED, String.valueOf(numFailures));
        props.setProperty(REPLICATION_FAILED_AT, String.valueOf(replicationTime));
        sb = readToStringBuilder(replicationTime, props.getProperty(REPLICATION_FAILED_AT_LIST));
        props.setProperty(REPLICATION_FAILED_AT_LIST, sb.toString());
      }

      final IndexOutput out = dir.createOutput(REPLICATION_PROPERTIES, DirectoryFactory.IOCONTEXT_NO_CACHE);
      Writer outFile = new OutputStreamWriter(new PropertiesOutputStream(out), StandardCharsets.UTF_8);
      try {
        props.store(outFile, "Replication details");
        dir.sync(Collections.singleton(REPLICATION_PROPERTIES));
      } finally {
        IOUtils.closeQuietly(outFile);
      }
    } catch (Exception e) {
      LOG.warn("Exception while updating statistics", e);
    } finally {
      if (dir != null) {
        solrCore.getDirectoryFactory().release(dir);
      }
    }
  }

  static long getTotalBytesDownloaded(SnapPuller snappuller) {
    long bytesDownloaded = 0;
    //get size from list of files to download
    for (Map<String, Object> file : snappuller.getFilesDownloaded()) {
      bytesDownloaded += (Long) file.get(SIZE);
    }

    //get size from list of conf files to download
    for (Map<String, Object> file : snappuller.getConfFilesDownloaded()) {
      bytesDownloaded += (Long) file.get(SIZE);
    }

    //get size from current file being downloaded
    Map<String, Object> currentFile = snappuller.getCurrentFile();
    if (currentFile != null) {
      if (currentFile.containsKey("bytesDownloaded")) {
        bytesDownloaded += (Long) currentFile.get("bytesDownloaded");
      }
    }
    return bytesDownloaded;
  }

  private StringBuilder readToStringBuilder(long replicationTime, String str) {
    StringBuilder sb = new StringBuilder();
    List<String> l = new ArrayList<>();
    if (str != null && str.length() != 0) {
      String[] ss = str.split(",");
      for (int i = 0; i < ss.length; i++) {
        l.add(ss[i]);
      }
    }
    sb.append(replicationTime);
    if (!l.isEmpty()) {
      for (int i = 0; i < l.size() || i < 9; i++) {
        if (i == l.size() || i == 9) break;
        String s = l.get(i);
        sb.append(",").append(s);
      }
    }
    return sb;
  }

  private void openNewSearcherAndUpdateCommitPoint(boolean isFullCopyNeeded) throws IOException {
    SolrQueryRequest req = new LocalSolrQueryRequest(solrCore,
        new ModifiableSolrParams());
    
    RefCounted<SolrIndexSearcher> searcher = null;
    IndexCommit commitPoint;
    try {
      Future[] waitSearcher = new Future[1];
      searcher = solrCore.getSearcher(true, true, waitSearcher, true);
      if (waitSearcher[0] != null) {
        try {
          waitSearcher[0].get();
        } catch (InterruptedException e) {
          SolrException.log(LOG, e);
        } catch (ExecutionException e) {
          SolrException.log(LOG, e);
        }
      }
      commitPoint = searcher.get().getIndexReader().getIndexCommit();
    } finally {
      req.close();
      if (searcher != null) {
        searcher.decref();
      }
    }

    // update the commit point in replication handler
    replicationHandler.indexCommitPoint = commitPoint;
    
  }

  /**
   * All the files are copied to a temp dir first
   */
  private String createTempindexDir(SolrCore core, String tmpIdxDirName) {
    // TODO: there should probably be a DirectoryFactory#concatPath(parent, name)
    // or something
    String tmpIdxDir = core.getDataDir() + tmpIdxDirName;
    return tmpIdxDir;
  }

  private void reloadCore() {
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override
      public void run() {
        try {
          solrCore.getCoreDescriptor().getCoreContainer().reload(solrCore.getName());
        } catch (Exception e) {
          LOG.error("Could not reload core ", e);
        } finally {
          latch.countDown();
        }
      }
    }.start();
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for core reload to finish", e);
    }
  }

  private void downloadConfFiles(List<Map<String, Object>> confFilesToDownload, long latestGeneration) throws Exception {
    LOG.info("Starting download of configuration files from master: " + confFilesToDownload);
    confFilesDownloaded = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
    File tmpconfDir = new File(solrCore.getResourceLoader().getConfigDir(), "conf." + getDateAsStr(new Date()));
    try {
      boolean status = tmpconfDir.mkdirs();
      if (!status) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Failed to create temporary config folder: " + tmpconfDir.getName());
      }
      for (Map<String, Object> file : confFilesToDownload) {
        String saveAs = (String) (file.get(ALIAS) == null ? file.get(NAME) : file.get(ALIAS));
        localFileFetcher = new LocalFsFileFetcher(tmpconfDir, file, saveAs, true, latestGeneration);
        currentFile = file;
        localFileFetcher.fetchFile();
        confFilesDownloaded.add(new HashMap<>(file));
      }
      // this is called before copying the files to the original conf dir
      // so that if there is an exception avoid corrupting the original files.
      terminateAndWaitFsyncService();
      copyTmpConfFiles2Conf(tmpconfDir);
    } finally {
      delTree(tmpconfDir);
    }
  }

  /**
   * Download the index files. If a new index is needed, download all the files.
   *
   * @param downloadCompleteIndex is it a fresh index copy
   * @param tmpIndexDir              the directory to which files need to be downloadeed to
   * @param indexDir                 the indexDir to be merged to
   * @param latestGeneration         the version number
   */
  private void downloadIndexFiles(boolean downloadCompleteIndex,
      Directory indexDir, Directory tmpIndexDir, long latestGeneration)
      throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Download files to dir: " + Arrays.asList(indexDir.listAll()));
    }
    for (Map<String,Object> file : filesToDownload) {
      if (!slowFileExists(indexDir, (String) file.get(NAME))
          || downloadCompleteIndex) {
        dirFileFetcher = new DirectoryFileFetcher(tmpIndexDir, file,
            (String) file.get(NAME), false, latestGeneration);
        currentFile = file;
        dirFileFetcher.fetchFile();
        filesDownloaded.add(new HashMap<>(file));
      } else {
        LOG.info("Skipping download for " + file.get(NAME)
            + " because it already exists");
      }
    }
  }

  /** Returns true if the file exists (can be opened), false
   *  if it cannot be opened, and (unlike Java's
   *  File.exists) throws IOException if there's some
   *  unexpected error. */
  private static boolean slowFileExists(Directory dir, String fileName) throws IOException {
    try {
      dir.openInput(fileName, IOContext.DEFAULT).close();
      return true;
    } catch (NoSuchFileException | FileNotFoundException e) {
      return false;
    }
  }  

  /**
   * All the files which are common between master and slave must have same size else we assume they are
   * not compatible (stale).
   *
   * @return true if the index stale and we need to download a fresh copy, false otherwise.
   * @throws IOException  if low level io error
   */
  private boolean isIndexStale(Directory dir) throws IOException {
    for (Map<String, Object> file : filesToDownload) {
      if (slowFileExists(dir, (String) file.get(NAME))
              && dir.fileLength((String) file.get(NAME)) != (Long) file.get(SIZE)) {
        LOG.warn("File " + file.get(NAME) + " expected to be " + file.get(SIZE)
            + " while it is " + dir.fileLength((String) file.get(NAME)));
        // file exists and size is different, therefore we must assume
        // corrupted index
        return true;
      }
    }
    return false;
  }

  /**
   * Copy a file by the File#renameTo() method. If it fails, it is considered a failure
   * <p/>
   */
  private boolean moveAFile(Directory tmpIdxDir, Directory indexDir, String fname, List<String> copiedfiles) {
    LOG.debug("Moving file: {}", fname);
    boolean success = false;
    try {
      if (slowFileExists(indexDir, fname)) {
        LOG.info("Skipping move file - it already exists:" + fname);
        return true;
      }
    } catch (IOException e) {
      SolrException.log(LOG, "could not check if a file exists", e);
      return false;
    }
    try {
      solrCore.getDirectoryFactory().move(tmpIdxDir, indexDir, fname, DirectoryFactory.IOCONTEXT_NO_CACHE);
      success = true;
    } catch (IOException e) {
      SolrException.log(LOG, "Could not move file", e);
    }
    return success;
  }

  /**
   * Copy all index files from the temp index dir to the actual index. The segments_N file is copied last.
   */
  private boolean moveIndexFiles(Directory tmpIdxDir, Directory indexDir) {
    if (LOG.isDebugEnabled()) {
      try {
        LOG.info("From dir files:" + Arrays.asList(tmpIdxDir.listAll()));
        LOG.info("To dir files:" + Arrays.asList(indexDir.listAll()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    String segmentsFile = null;
    List<String> movedfiles = new ArrayList<>();
    for (Map<String, Object> f : filesDownloaded) {
      String fname = (String) f.get(NAME);
      // the segments file must be copied last
      // or else if there is a failure in between the
      // index will be corrupted
      if (fname.startsWith("segments_")) {
        //The segments file must be copied in the end
        //Otherwise , if the copy fails index ends up corrupted
        segmentsFile = fname;
        continue;
      }
      if (!moveAFile(tmpIdxDir, indexDir, fname, movedfiles)) return false;
      movedfiles.add(fname);
    }
    //copy the segments file last
    if (segmentsFile != null) {
      if (!moveAFile(tmpIdxDir, indexDir, segmentsFile, movedfiles)) return false;
    }
    return true;
  }

  /**
   * Make file list 
   */
  private List<File> makeTmpConfDirFileList(File dir, List<File> fileList) {
    File[] files = dir.listFiles();
    for (File file : files) {
      if (file.isFile()) {
        fileList.add(file);
      } else if (file.isDirectory()) {
        fileList = makeTmpConfDirFileList(file, fileList);
      }
    }
    return fileList;
  }
  
  /**
   * The conf files are copied to the tmp dir to the conf dir. A backup of the old file is maintained
   */
  private void copyTmpConfFiles2Conf(File tmpconfDir) {
    boolean status = false;
    File confDir = new File(solrCore.getResourceLoader().getConfigDir());
    for (File file : makeTmpConfDirFileList(tmpconfDir, new ArrayList<File>())) {
      File oldFile = new File(confDir, file.getPath().substring(tmpconfDir.getPath().length(), file.getPath().length()));
      if (!oldFile.getParentFile().exists()) {
        status = oldFile.getParentFile().mkdirs();
        if (!status) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
                  "Unable to mkdirs: " + oldFile.getParentFile());
        }
      }
      if (oldFile.exists()) {
        File backupFile = new File(oldFile.getPath() + "." + getDateAsStr(new Date(oldFile.lastModified())));
        if (!backupFile.getParentFile().exists()) {
          status = backupFile.getParentFile().mkdirs();
          if (!status) {
            throw new SolrException(ErrorCode.SERVER_ERROR,
                    "Unable to mkdirs: " + backupFile.getParentFile());
          }
        }
        status = oldFile.renameTo(backupFile);
        if (!status) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  "Unable to rename: " + oldFile + " to: " + backupFile);
        }
      }
      status = file.renameTo(oldFile);
      if (!status) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
                "Unable to rename: " + file + " to: " + oldFile);
      }
    }
  }

  private String getDateAsStr(Date d) {
    return new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(d);
  }

  /**
   * If the index is stale by any chance, load index from a different dir in the data dir.
   */
  private boolean modifyIndexProps(String tmpIdxDirName) {
    LOG.info("New index installed. Updating index properties... index="+tmpIdxDirName);
    Properties p = new Properties();
    Directory dir = null;
    try {
      dir = solrCore.getDirectoryFactory().get(solrCore.getDataDir(), DirContext.META_DATA, solrCore.getSolrConfig().indexConfig.lockType);
      if (slowFileExists(dir, SnapPuller.INDEX_PROPERTIES)){
        final IndexInput input = dir.openInput(SnapPuller.INDEX_PROPERTIES, DirectoryFactory.IOCONTEXT_NO_CACHE);
  
        final InputStream is = new PropertiesInputStream(input);
        try {
          p.load(new InputStreamReader(is, StandardCharsets.UTF_8));
        } catch (Exception e) {
          LOG.error("Unable to load " + SnapPuller.INDEX_PROPERTIES, e);
        } finally {
          IOUtils.closeQuietly(is);
        }
      }
      try {
        dir.deleteFile(SnapPuller.INDEX_PROPERTIES);
      } catch (IOException e) {
        // no problem
      }
      final IndexOutput out = dir.createOutput(SnapPuller.INDEX_PROPERTIES, DirectoryFactory.IOCONTEXT_NO_CACHE);
      p.put("index", tmpIdxDirName);
      Writer os = null;
      try {
        os = new OutputStreamWriter(new PropertiesOutputStream(out), StandardCharsets.UTF_8);
        p.store(os, SnapPuller.INDEX_PROPERTIES);
        dir.sync(Collections.singleton(INDEX_PROPERTIES));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unable to write " + SnapPuller.INDEX_PROPERTIES, e);
      } finally {
        IOUtils.closeQuietly(os);
      }
      return true;

    } catch (IOException e1) {
      throw new RuntimeException(e1);
    } finally {
      if (dir != null) {
        try {
          solrCore.getDirectoryFactory().release(dir);
        } catch (IOException e) {
          SolrException.log(LOG, "", e);
        }
      }
    }
    
  }

  private final Map<String, FileInfo> confFileInfoCache = new HashMap<>();

  /**
   * The local conf files are compared with the conf files in the master. If they are same (by checksum) do not copy.
   *
   * @param confFilesToDownload The list of files obtained from master
   *
   * @return a list of configuration files which have changed on the master and need to be downloaded.
   */
  private Collection<Map<String, Object>> getModifiedConfFiles(List<Map<String, Object>> confFilesToDownload) {
    if (confFilesToDownload == null || confFilesToDownload.isEmpty())
      return Collections.EMPTY_LIST;
    //build a map with alias/name as the key
    Map<String, Map<String, Object>> nameVsFile = new HashMap<>();
    NamedList names = new NamedList();
    for (Map<String, Object> map : confFilesToDownload) {
      //if alias is present that is the name the file may have in the slave
      String name = (String) (map.get(ALIAS) == null ? map.get(NAME) : map.get(ALIAS));
      nameVsFile.put(name, map);
      names.add(name, null);
    }
    //get the details of the local conf files with the same alias/name
    List<Map<String, Object>> localFilesInfo = replicationHandler.getConfFileInfoFromCache(names, confFileInfoCache);
    //compare their size/checksum to see if
    for (Map<String, Object> fileInfo : localFilesInfo) {
      String name = (String) fileInfo.get(NAME);
      Map<String, Object> m = nameVsFile.get(name);
      if (m == null) continue; // the file is not even present locally (so must be downloaded)
      if (m.get(CHECKSUM).equals(fileInfo.get(CHECKSUM))) {
        nameVsFile.remove(name); //checksums are same so the file need not be downloaded
      }
    }
    return nameVsFile.isEmpty() ? Collections.EMPTY_LIST : nameVsFile.values();
  }
  
  /** 
   * This simulates File.delete exception-wise, since this class has some strange behavior with it.
   * The only difference is it returns null on success, throws SecurityException on SecurityException, 
   * otherwise returns Throwable preventing deletion (instead of false), for additional information.
   */
  static Throwable delete(File file) {
    try {
      Files.delete(file.toPath());
      return null;
    } catch (SecurityException e) {
      throw e;
    } catch (Throwable other) {
      return other;
    }
  }
  
  static boolean delTree(File dir) {
    try {
      org.apache.lucene.util.IOUtils.rm(dir.toPath());
      return true;
    } catch (IOException e) {
      LOG.warn("Unable to delete directory : " + dir, e);
      return false;
    }
  }

  /**
   * Disable periodic polling
   */
  void disablePoll() {
    pollDisabled.set(true);
    LOG.info("inside disable poll, value of pollDisabled = " + pollDisabled);
  }

  /**
   * Enable periodic polling
   */
  void enablePoll() {
    pollDisabled.set(false);
    LOG.info("inside enable poll, value of pollDisabled = " + pollDisabled);
  }

  /**
   * Stops the ongoing pull
   */
  void abortPull() {
    stop = true;
  }

  long getReplicationStartTime() {
    return replicationStartTime;
  }

  List<Map<String, Object>> getConfFilesToDownload() {
    //make a copy first because it can be null later
    List<Map<String, Object>> tmp = confFilesToDownload;
    //create a new instance. or else iterator may fail
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<>(tmp);
  }

  List<Map<String, Object>> getConfFilesDownloaded() {
    //make a copy first because it can be null later
    List<Map<String, Object>> tmp = confFilesDownloaded;
    // NOTE: it's safe to make a copy of a SynchronizedCollection(ArrayList)
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<>(tmp);
  }

  List<Map<String, Object>> getFilesToDownload() {
    //make a copy first because it can be null later
    List<Map<String, Object>> tmp = filesToDownload;
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<>(tmp);
  }

  List<Map<String, Object>> getFilesDownloaded() {
    List<Map<String, Object>> tmp = filesDownloaded;
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<>(tmp);
  }

  // TODO: currently does not reflect conf files
  Map<String, Object> getCurrentFile() {
    Map<String, Object> tmp = currentFile;
    DirectoryFileFetcher tmpFileFetcher = dirFileFetcher;
    if (tmp == null)
      return null;
    tmp = new HashMap<>(tmp);
    if (tmpFileFetcher != null)
      tmp.put("bytesDownloaded", tmpFileFetcher.bytesDownloaded);
    return tmp;
  }

  boolean isPollingDisabled() {
    return pollDisabled.get();
  }

  Long getNextScheduledExecTime() {
    Long nextTime = null;
    if (executorStartTime > 0)
      nextTime = executorStartTime + pollInterval;
    return nextTime;
  }

  private static class ReplicationHandlerException extends InterruptedException {
    public ReplicationHandlerException(String message) {
      super(message);
    }
  }

  /**
   * The class acts as a client for ReplicationHandler.FileStream. It understands the protocol of wt=filestream
   *
   * @see org.apache.solr.handler.ReplicationHandler.DirectoryFileStream
   */
  private class DirectoryFileFetcher {
    boolean includeChecksum = true;

    Directory copy2Dir;

    String fileName;

    String saveAs;

    long size;

    long bytesDownloaded = 0;

    byte[] buf = new byte[1024 * 1024];

    Checksum checksum;

    int errorCount = 0;

    private boolean isConf;

    private boolean aborted = false;

    private Long indexGen;

    private IndexOutput outStream;

    DirectoryFileFetcher(Directory tmpIndexDir, Map<String, Object> fileDetails, String saveAs,
                boolean isConf, long latestGen) throws IOException {
      this.copy2Dir = tmpIndexDir;
      this.fileName = (String) fileDetails.get(NAME);
      this.size = (Long) fileDetails.get(SIZE);
      this.isConf = isConf;
      this.saveAs = saveAs;

      indexGen = latestGen;
      
      outStream = copy2Dir.createOutput(saveAs, DirectoryFactory.IOCONTEXT_NO_CACHE);

      if (includeChecksum)
        checksum = new Adler32();
    }

    /**
     * The main method which downloads file
     */
    void fetchFile() throws Exception {
      try {
        while (true) {
          final FastInputStream is = getStream();
          int result;
          try {
            //fetch packets one by one in a single request
            result = fetchPackets(is);
            if (result == 0 || result == NO_CONTENT) {

              return;
            }
            //if there is an error continue. But continue from the point where it got broken
          } finally {
            IOUtils.closeQuietly(is);
          }
        }
      } finally {
        cleanup();
        //if cleanup suceeds . The file is downloaded fully. do an fsync
        fsyncService.submit(new Runnable(){
          @Override
          public void run() {
            try {
              copy2Dir.sync(Collections.singleton(saveAs));
            } catch (IOException e) {
              fsyncException = e;
            }
          }
        });
      }
    }

    private int fetchPackets(FastInputStream fis) throws Exception {
      byte[] intbytes = new byte[4];
      byte[] longbytes = new byte[8];
      try {
        while (true) {
          if (stop) {
            stop = false;
            aborted = true;
            throw new ReplicationHandlerException("User aborted replication");
          }
          long checkSumServer = -1;
          fis.readFully(intbytes);
          //read the size of the packet
          int packetSize = readInt(intbytes);
          if (packetSize <= 0) {
            LOG.warn("No content received for file: " + currentFile);
            return NO_CONTENT;
          }
          if (buf.length < packetSize)
            buf = new byte[packetSize];
          if (checksum != null) {
            //read the checksum
            fis.readFully(longbytes);
            checkSumServer = readLong(longbytes);
          }
          //then read the packet of bytes
          fis.readFully(buf, 0, packetSize);
          //compare the checksum as sent from the master
          if (includeChecksum) {
            checksum.reset();
            checksum.update(buf, 0, packetSize);
            long checkSumClient = checksum.getValue();
            if (checkSumClient != checkSumServer) {
              LOG.error("Checksum not matched between client and server for: " + currentFile);
              //if checksum is wrong it is a problem return for retry
              return 1;
            }
          }
          //if everything is fine, write down the packet to the file
          writeBytes(packetSize);
          bytesDownloaded += packetSize;
          if (bytesDownloaded >= size)
            return 0;
          //errorcount is always set to zero after a successful packet
          errorCount = 0;
        }
      } catch (ReplicationHandlerException e) {
        throw e;
      } catch (Exception e) {
        LOG.warn("Error in fetching packets ", e);
        //for any failure , increment the error count
        errorCount++;
        //if it fails for the same pacaket for   MAX_RETRIES fail and come out
        if (errorCount > MAX_RETRIES) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  "Fetch failed for file:" + fileName, e);
        }
        return ERR;
      }
    }

    protected void writeBytes(int packetSize) throws IOException {
      outStream.writeBytes(buf, 0, packetSize);
    }

    /**
     * The webcontainer flushes the data only after it fills the buffer size. So, all data has to be read as readFully()
     * other wise it fails. So read everything as bytes and then extract an integer out of it
     */
    private int readInt(byte[] b) {
      return (((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16)
              | ((b[2] & 0xff) << 8) | (b[3] & 0xff));

    }

    /**
     * Same as above but to read longs from a byte array
     */
    private long readLong(byte[] b) {
      return (((long) (b[0] & 0xff)) << 56) | (((long) (b[1] & 0xff)) << 48)
              | (((long) (b[2] & 0xff)) << 40) | (((long) (b[3] & 0xff)) << 32)
              | (((long) (b[4] & 0xff)) << 24) | ((b[5] & 0xff) << 16)
              | ((b[6] & 0xff) << 8) | ((b[7] & 0xff));

    }

    /**
     * cleanup everything
     */
    private void cleanup() {
      try {
        outStream.close();
      } catch (Exception e) {/* noop */
          LOG.error("Error closing the file stream: "+ this.saveAs ,e);
      }
      if (bytesDownloaded != size) {
        //if the download is not complete then
        //delete the file being downloaded
        try {
          copy2Dir.deleteFile(saveAs);
        } catch (Exception e) {
          LOG.error("Error deleting file in cleanup" + e.getMessage());
        }
        //if the failure is due to a user abort it is returned nomally else an exception is thrown
        if (!aborted)
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  "Unable to download " + fileName + " completely. Downloaded "
                          + bytesDownloaded + "!=" + size);
      }
    }

    /**
     * Open a new stream using HttpClient
     */
    FastInputStream getStream() throws IOException {

      ModifiableSolrParams params = new ModifiableSolrParams();

//    //the method is command=filecontent
      params.set(COMMAND, CMD_GET_FILE);
      params.set(GENERATION, Long.toString(indexGen));
      params.set(CommonParams.QT, "/replication");
      //add the version to download. This is used to reserve the download
      if (isConf) {
        //set cf instead of file for config file
        params.set(CONF_FILE_SHORT, fileName);
      } else {
        params.set(FILE, fileName);
      }
      if (useInternal) {
        params.set(COMPRESSION, "true"); 
      }
      //use checksum
      if (this.includeChecksum) {
        params.set(CHECKSUM, true);
      }
      //wt=filestream this is a custom protocol
      params.set(CommonParams.WT, FILE_STREAM);
        // This happen if there is a failure there is a retry. the offset=<sizedownloaded> ensures that
        // the server starts from the offset
      if (bytesDownloaded > 0) {
        params.set(OFFSET, Long.toString(bytesDownloaded));
      }
      

      NamedList response;
      InputStream is = null;
      
      HttpSolrServer s = new HttpSolrServer(masterUrl, myHttpClient, null);  //XXX use shardhandler
      try {
        s.setSoTimeout(60000);
        s.setConnectionTimeout(15000);
        QueryRequest req = new QueryRequest(params);
        response = s.request(req);
        is = (InputStream) response.get("stream");
        if(useInternal) {
          is = new InflaterInputStream(is);
        }
        return new FastInputStream(is);
      } catch (Exception e) {
        //close stream on error
        IOUtils.closeQuietly(is);
        throw new IOException("Could not download file '" + fileName + "'", e);
      } finally {
        s.shutdown();
      }
    }
  }
  
  /**
   * The class acts as a client for ReplicationHandler.FileStream. It understands the protocol of wt=filestream
   *
   * @see org.apache.solr.handler.ReplicationHandler.LocalFsFileStream
   */
  private class LocalFsFileFetcher {
    boolean includeChecksum = true;

    private File copy2Dir;

    String fileName;

    String saveAs;

    long size;

    long bytesDownloaded = 0;

    FileChannel fileChannel;
    
    private FileOutputStream fileOutputStream;

    byte[] buf = new byte[1024 * 1024];

    Checksum checksum;

    File file;

    int errorCount = 0;

    private boolean isConf;

    private boolean aborted = false;

    private Long indexGen;

    // TODO: could do more code sharing with DirectoryFileFetcher
    LocalFsFileFetcher(File dir, Map<String, Object> fileDetails, String saveAs,
                boolean isConf, long latestGen) throws IOException {
      this.copy2Dir = dir;
      this.fileName = (String) fileDetails.get(NAME);
      this.size = (Long) fileDetails.get(SIZE);
      this.isConf = isConf;
      this.saveAs = saveAs;

      indexGen = latestGen;

      this.file = new File(copy2Dir, saveAs);
      
      File parentDir = this.file.getParentFile();
      if( ! parentDir.exists() ){
        if ( ! parentDir.mkdirs() ) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                  "Failed to create (sub)directory for file: " + saveAs);
        }
      }
      
      this.fileOutputStream = new FileOutputStream(file);
      this.fileChannel = this.fileOutputStream.getChannel();

      if (includeChecksum)
        checksum = new Adler32();
    }

    /**
     * The main method which downloads file
     */
    void fetchFile() throws Exception {
      try {
        while (true) {
          final FastInputStream is = getStream();
          int result;
          try {
            //fetch packets one by one in a single request
            result = fetchPackets(is);
            if (result == 0 || result == NO_CONTENT) {
              return;
            }
            //if there is an error continue. But continue from the point where it got broken
          } finally {
            IOUtils.closeQuietly(is);
          }
        }
      } finally {
        cleanup();
        //if cleanup suceeds . The file is downloaded fully. do an fsync
        fsyncService.submit(new Runnable(){
          @Override
          public void run() {
            try {
              FileUtils.sync(file);
            } catch (IOException e) {
              fsyncException = e;
            }
          }
        });
      }
    }

    private int fetchPackets(FastInputStream fis) throws Exception {
      byte[] intbytes = new byte[4];
      byte[] longbytes = new byte[8];
      try {
        while (true) {
          if (stop) {
            stop = false;
            aborted = true;
            throw new ReplicationHandlerException("User aborted replication");
          }
          long checkSumServer = -1;
          fis.readFully(intbytes);
          //read the size of the packet
          int packetSize = readInt(intbytes);
          if (packetSize <= 0) {
            LOG.warn("No content received for file: " + currentFile);
            return NO_CONTENT;
          }
          if (buf.length < packetSize)
            buf = new byte[packetSize];
          if (checksum != null) {
            //read the checksum
            fis.readFully(longbytes);
            checkSumServer = readLong(longbytes);
          }
          //then read the packet of bytes
          fis.readFully(buf, 0, packetSize);
          //compare the checksum as sent from the master
          if (includeChecksum) {
            checksum.reset();
            checksum.update(buf, 0, packetSize);
            long checkSumClient = checksum.getValue();
            if (checkSumClient != checkSumServer) {
              LOG.error("Checksum not matched between client and server for: " + currentFile);
              //if checksum is wrong it is a problem return for retry
              return 1;
            }
          }
          //if everything is fine, write down the packet to the file
          fileChannel.write(ByteBuffer.wrap(buf, 0, packetSize));
          bytesDownloaded += packetSize;
          if (bytesDownloaded >= size)
            return 0;
          //errorcount is always set to zero after a successful packet
          errorCount = 0;
        }
      } catch (ReplicationHandlerException e) {
        throw e;
      } catch (Exception e) {
        LOG.warn("Error in fetching packets ", e);
        //for any failure , increment the error count
        errorCount++;
        //if it fails for the same pacaket for   MAX_RETRIES fail and come out
        if (errorCount > MAX_RETRIES) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  "Fetch failed for file:" + fileName, e);
        }
        return ERR;
      }
    }

    /**
     * The webcontainer flushes the data only after it fills the buffer size. So, all data has to be read as readFully()
     * other wise it fails. So read everything as bytes and then extract an integer out of it
     */
    private int readInt(byte[] b) {
      return (((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16)
              | ((b[2] & 0xff) << 8) | (b[3] & 0xff));

    }

    /**
     * Same as above but to read longs from a byte array
     */
    private long readLong(byte[] b) {
      return (((long) (b[0] & 0xff)) << 56) | (((long) (b[1] & 0xff)) << 48)
              | (((long) (b[2] & 0xff)) << 40) | (((long) (b[3] & 0xff)) << 32)
              | (((long) (b[4] & 0xff)) << 24) | ((b[5] & 0xff) << 16)
              | ((b[6] & 0xff) << 8) | ((b[7] & 0xff));

    }

    /**
     * cleanup everything
     */
    private void cleanup() {
      try {
        //close the FileOutputStream (which also closes the Channel)
        fileOutputStream.close();
      } catch (Exception e) {/* noop */
          LOG.error("Error closing the file stream: "+ this.saveAs ,e);
      }
      if (bytesDownloaded != size) {
        //if the download is not complete then
        //delete the file being downloaded
        try {
          Files.delete(file.toPath());
        } catch (SecurityException e) {
          LOG.error("Error deleting file in cleanup" + e.getMessage());
        } catch (Throwable other) {
          // TODO: should this class care if a file couldnt be deleted?
          // this just emulates previous behavior, where only SecurityException would be handled.
        }
        //if the failure is due to a user abort it is returned nomally else an exception is thrown
        if (!aborted)
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  "Unable to download " + fileName + " completely. Downloaded "
                          + bytesDownloaded + "!=" + size);
      }
    }

    /**
     * Open a new stream using HttpClient
     */
    FastInputStream getStream() throws IOException {

      ModifiableSolrParams params = new ModifiableSolrParams();

//    //the method is command=filecontent
      params.set(COMMAND, CMD_GET_FILE);
      params.set(GENERATION, Long.toString(indexGen));
      params.set(CommonParams.QT, "/replication");
      //add the version to download. This is used to reserve the download
      if (isConf) {
        //set cf instead of file for config file
        params.set(CONF_FILE_SHORT, fileName);
      } else {
        params.set(FILE, fileName);
      }
      if (useInternal) {
        params.set(COMPRESSION, "true"); 
      }
      //use checksum
      if (this.includeChecksum) {
        params.set(CHECKSUM, true);
      }
      //wt=filestream this is a custom protocol
      params.set(CommonParams.WT, FILE_STREAM);
        // This happen if there is a failure there is a retry. the offset=<sizedownloaded> ensures that
        // the server starts from the offset
      if (bytesDownloaded > 0) {
        params.set(OFFSET, Long.toString(bytesDownloaded));
      }
      

      NamedList response;
      InputStream is = null;
      HttpSolrServer s = new HttpSolrServer(masterUrl, myHttpClient, null);  //XXX use shardhandler
      try {
        s.setSoTimeout(60000);
        s.setConnectionTimeout(15000);
        QueryRequest req = new QueryRequest(params);
        response = s.request(req);
        is = (InputStream) response.get("stream");
        if(useInternal) {
          is = new InflaterInputStream(is);
        }
        return new FastInputStream(is);
      } catch (Exception e) {
        //close stream on error
        IOUtils.closeQuietly(is);
        throw new IOException("Could not download file '" + fileName + "'", e);
      } finally {
        s.shutdown();
      }
    }
  }
  
  NamedList getDetails() throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COMMAND, CMD_DETAILS);
    params.set("slave", false);
    params.set(CommonParams.QT, "/replication");
    HttpSolrServer server = new HttpSolrServer(masterUrl, myHttpClient); //XXX use shardhandler
    NamedList rsp;
    try {
      server.setSoTimeout(60000);
      server.setConnectionTimeout(15000);
      QueryRequest request = new QueryRequest(params);
      rsp = server.request(request);
    } finally {
      server.shutdown();
    }
    return rsp;
  }

  static Integer readInterval(String interval) {
    if (interval == null)
      return null;
    int result = 0;
    if (interval != null) {
      Matcher m = INTERVAL_PATTERN.matcher(interval.trim());
      if (m.find()) {
        String hr = m.group(1);
        String min = m.group(2);
        String sec = m.group(3);
        result = 0;
        try {
          if (sec != null && sec.length() > 0)
            result += Integer.parseInt(sec);
          if (min != null && min.length() > 0)
            result += (60 * Integer.parseInt(min));
          if (hr != null && hr.length() > 0)
            result += (60 * 60 * Integer.parseInt(hr));
          result *= 1000;
        } catch (NumberFormatException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  INTERVAL_ERR_MSG);
        }
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                INTERVAL_ERR_MSG);
      }

    }
    return result;
  }

  public void destroy() {
    try {
      if (executorService != null) executorService.shutdown();
    } finally {
      try {
        abortPull();
      } finally {
        if (executorService != null) ExecutorUtil
            .shutdownNowAndAwaitTermination(executorService);
      }
    }
  }

  String getMasterUrl() {
    return masterUrl;
  }

  String getPollInterval() {
    return pollIntervalStr;
  }

  private static final int MAX_RETRIES = 5;

  private static final int NO_CONTENT = 1;

  private static final int ERR = 2;

  public static final String REPLICATION_PROPERTIES = "replication.properties";

  public static final String POLL_INTERVAL = "pollInterval";

  public static final String INTERVAL_ERR_MSG = "The " + POLL_INTERVAL + " must be in this format 'HH:mm:ss'";

  private static final Pattern INTERVAL_PATTERN = Pattern.compile("(\\d*?):(\\d*?):(\\d*)");

  static final String INDEX_REPLICATED_AT = "indexReplicatedAt";

  static final String TIMES_INDEX_REPLICATED = "timesIndexReplicated";

  static final String CONF_FILES_REPLICATED = "confFilesReplicated";

  static final String CONF_FILES_REPLICATED_AT = "confFilesReplicatedAt";

  static final String TIMES_CONFIG_REPLICATED = "timesConfigReplicated";

  static final String LAST_CYCLE_BYTES_DOWNLOADED = "lastCycleBytesDownloaded";

  static final String TIMES_FAILED = "timesFailed";

  static final String REPLICATION_FAILED_AT = "replicationFailedAt";

  static final String PREVIOUS_CYCLE_TIME_TAKEN = "previousCycleTimeInSeconds";

  static final String INDEX_REPLICATED_AT_LIST = "indexReplicatedAtList";

  static final String REPLICATION_FAILED_AT_LIST = "replicationFailedAtList";
}

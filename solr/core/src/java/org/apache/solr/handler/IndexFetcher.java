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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.JAVABIN;
import static org.apache.solr.common.params.CommonParams.NAME;
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
import static org.apache.solr.handler.ReplicationHandler.OFFSET;
import static org.apache.solr.handler.ReplicationHandler.SIZE;

/**
 * <p> Provides functionality of downloading changed index files as well as config files and a timer for scheduling fetches from the
 * master. </p>
 *
 *
 * @since solr 1.4
 */
public class IndexFetcher {
  private static final int _100K = 100000;

  public static final String INDEX_PROPERTIES = "index.properties";

  private static final Logger LOG = LoggerFactory.getLogger(IndexFetcher.class.getName());

  private final String masterUrl;

  private final ReplicationHandler replicationHandler;

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

  private final HttpClient myHttpClient;

  private static HttpClient createHttpClient(SolrCore core, String connTimeout, String readTimeout, String httpBasicAuthUser, String httpBasicAuthPassword, boolean useCompression) {
    final ModifiableSolrParams httpClientParams = new ModifiableSolrParams();
    httpClientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connTimeout != null ? connTimeout : "5000");
    httpClientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, readTimeout != null ? readTimeout : "20000");
    httpClientParams.set(HttpClientUtil.PROP_BASIC_AUTH_USER, httpBasicAuthUser);
    httpClientParams.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, httpBasicAuthPassword);
    httpClientParams.set(HttpClientUtil.PROP_ALLOW_COMPRESSION, useCompression);

    return HttpClientUtil.createClient(httpClientParams, core.getCoreDescriptor().getCoreContainer().getUpdateShardHandler().getConnectionManager());
  }

  public IndexFetcher(final NamedList initArgs, final ReplicationHandler handler, final SolrCore sc) {
    solrCore = sc;
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
    String compress = (String) initArgs.get(COMPRESSION);
    useInternal = INTERNAL.equals(compress);
    useExternal = EXTERNAL.equals(compress);
    String connTimeout = (String) initArgs.get(HttpClientUtil.PROP_CONNECTION_TIMEOUT);
    String readTimeout = (String) initArgs.get(HttpClientUtil.PROP_SO_TIMEOUT);
    String httpBasicAuthUser = (String) initArgs.get(HttpClientUtil.PROP_BASIC_AUTH_USER);
    String httpBasicAuthPassword = (String) initArgs.get(HttpClientUtil.PROP_BASIC_AUTH_PASS);
    myHttpClient = createHttpClient(solrCore, connTimeout, readTimeout, httpBasicAuthUser, httpBasicAuthPassword, useExternal);
  }

  /**
   * Gets the latest commit version and generation from the master
   */
  @SuppressWarnings("unchecked")
  NamedList getLatestVersion() throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COMMAND, CMD_INDEX_VERSION);
    params.set(CommonParams.WT, JAVABIN);
    params.set(CommonParams.QT, "/replication");
    QueryRequest req = new QueryRequest(params);

    // TODO modify to use shardhandler
    try (HttpSolrClient client = new HttpSolrClient(masterUrl, myHttpClient)) {
      client.setSoTimeout(60000);
      client.setConnectionTimeout(15000);
      
      return client.request(req);
    } catch (SolrServerException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Fetches the list of files in a given index commit point and updates internal list of files to download.
   */
  private void fetchFileList(long gen) throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COMMAND,  CMD_GET_FILE_LIST);
    params.set(GENERATION, String.valueOf(gen));
    params.set(CommonParams.WT, JAVABIN);
    params.set(CommonParams.QT, "/replication");
    QueryRequest req = new QueryRequest(params);

    // TODO modify to use shardhandler
    try (HttpSolrClient client = new HttpSolrClient(masterUrl, myHttpClient)) {
      client.setSoTimeout(60000);
      client.setConnectionTimeout(15000);
      NamedList response = client.request(req);

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
    }
  }

  boolean fetchLatestIndex(boolean forceReplication) throws IOException, InterruptedException {
    return fetchLatestIndex(forceReplication, false);
  }
  
  /**
   * This command downloads all the necessary files from master to install a index commit point. Only changed files are
   * downloaded. It also downloads the conf files (if they are modified).
   *
   * @param forceReplication force a replication in all cases 
   * @param forceCoreReload force a core reload in all cases
   * @return true on success, false if slave is already in sync
   * @throws IOException if an exception occurs
   */
  boolean fetchLatestIndex(boolean forceReplication, boolean forceCoreReload) throws IOException, InterruptedException {
    
    boolean cleanupDone = false;
    boolean successfulInstall = false;
    replicationStartTime = System.currentTimeMillis();
    Directory tmpIndexDir = null;
    String tmpIndex;
    Directory indexDir = null;
    String indexDirPath;
    boolean deleteTmpIdxDir = true;
    
    if (!solrCore.getSolrCoreState().getLastReplicateIndexSuccess()) {
      // if the last replication was not a success, we force a full replication
      // when we are a bit more confident we may want to try a partial replication
      // if the error is connection related or something, but we have to be careful
      forceReplication = true;
    }
    
    try {
      //get the current 'replicateable' index version in the master
      NamedList response;
      try {
        response = getLatestVersion();
      } catch (Exception e) {
        LOG.error("Master at: " + masterUrl + " is not available. Index fetch failed. Exception: " + e.getMessage());
        return false;
      }
      long latestVersion = (Long) response.get(CMD_INDEX_VERSION);
      long latestGeneration = (Long) response.get(GENERATION);

      // TODO: make sure that getLatestCommit only returns commit points for the main index (i.e. no side-car indexes)
      IndexCommit commit = solrCore.getDeletionPolicy().getLatestCommit();
      if (commit == null) {
        // Presumably the IndexWriter hasn't been opened yet, and hence the deletion policy hasn't been updated with commit points
        RefCounted<SolrIndexSearcher> searcherRefCounted = null;
        try {
          searcherRefCounted = solrCore.getNewestSearcher(false);
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
          RefCounted<IndexWriter> iw = solrCore.getUpdateHandler().getSolrCoreState().getIndexWriter(solrCore);
          try {
            iw.get().deleteAll();
          } finally {
            iw.decref();
          }
          SolrQueryRequest req = new LocalSolrQueryRequest(solrCore, new ModifiableSolrParams());
          solrCore.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
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
      if (filesToDownload.isEmpty()) {
        return false;
      }
      LOG.info("Number of files in latest index in master: " + filesToDownload.size());

      // Create the sync service
      fsyncService = ExecutorUtil.newMDCAwareSingleThreadExecutor(new DefaultSolrThreadFactory("fsyncService"));
      // use a synchronized list because the list is read by other threads (to show details)
      filesDownloaded = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
      // if the generation of master is older than that of the slave , it means they are not compatible to be copied
      // then a new index directory to be created and all the files need to be copied
      boolean isFullCopyNeeded = IndexDeletionPolicyWrapper
          .getCommitTimestamp(commit) >= latestVersion
          || commit.getGeneration() >= latestGeneration || forceReplication;

      String tmpIdxDirName = "index." + new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date());
      tmpIndex = solrCore.getDataDir() + tmpIdxDirName;

      tmpIndexDir = solrCore.getDirectoryFactory().get(tmpIndex, DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
      
      // cindex dir...
      indexDirPath = solrCore.getIndexDir();
      indexDir = solrCore.getDirectoryFactory().get(indexDirPath, DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);

      try {

        SegmentInfos infos = SegmentInfos.readCommit(indexDir, commit.getSegmentsFileName());
        Version oldestVersion = IndexFetcher.checkOldestVersion(infos);

        //We will compare all the index files from the master vs the index files on disk to see if there is a mismatch
        //in the metadata. If there is a mismatch for the same index file then we download the entire index again.
        if (!isFullCopyNeeded && isIndexStale(indexDir, oldestVersion)) {
          isFullCopyNeeded = true;
        }
        
        if (!isFullCopyNeeded) {
          // a searcher might be using some flushed but not committed segments
          // because of soft commits (which open a searcher on IW's data)
          // so we need to close the existing searcher on the last commit
          // and wait until we are able to clean up all unused lucene files
          if (solrCore.getCoreDescriptor().getCoreContainer().isZooKeeperAware()) {
            solrCore.closeSearcher();
          }

          // rollback and reopen index writer and wait until all unused files
          // are successfully deleted
          solrCore.getUpdateHandler().newIndexWriter(true);
          RefCounted<IndexWriter> writer = solrCore.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
          try {
            IndexWriter indexWriter = writer.get();
            int c = 0;
            indexWriter.deleteUnusedFiles();
            while (hasUnusedFiles(indexDir, commit)) {
              indexWriter.deleteUnusedFiles();
              LOG.info("Sleeping for 1000ms to wait for unused lucene index files to be delete-able");
              Thread.sleep(1000);
              c++;
              if (c >= 30)  {
                LOG.warn("IndexFetcher unable to cleanup unused lucene index files so we must do a full copy instead");
                isFullCopyNeeded = true;
                break;
              }
            }
            if (c > 0)  {
              LOG.info("IndexFetcher slept for " + (c * 1000) + "ms for unused lucene index files to be delete-able");
            }
          } finally {
            writer.decref();
          }
          solrCore.getUpdateHandler().getSolrCoreState().closeIndexWriter(solrCore, true);
        }
        boolean reloadCore = false;
        
        try {
          LOG.info("Starting download to " + tmpIndexDir + " fullCopy="
              + isFullCopyNeeded);
          successfulInstall = false;
          
          downloadIndexFiles(isFullCopyNeeded, indexDir, oldestVersion, tmpIndexDir, latestGeneration);
          LOG.info("Total time taken for download : "
              + ((System.currentTimeMillis() - replicationStartTime) / 1000)
              + " secs");
          Collection<Map<String,Object>> modifiedConfFiles = getModifiedConfFiles(confFilesToDownload);
          if (!modifiedConfFiles.isEmpty()) {
            reloadCore = true;
            downloadConfFiles(confFilesToDownload, latestGeneration);
            if (isFullCopyNeeded) {
              successfulInstall = IndexFetcher.modifyIndexProps(solrCore, tmpIdxDirName);
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
                  solrCore.getDirectoryFactory().doneWithDirectory(indexDir);
                  solrCore.getDirectoryFactory().remove(indexDir);
                }
              }
              
              LOG.info("Configuration files are modified, core will be reloaded");
              logReplicationTimeAndConfFiles(modifiedConfFiles,
                  successfulInstall);// write to a file time of replication and
                                     // conf files.
            }
          } else {
            terminateAndWaitFsyncService();
            if (isFullCopyNeeded) {
              successfulInstall = IndexFetcher.modifyIndexProps(solrCore, tmpIdxDirName);
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
            solrCore.getUpdateHandler().getSolrCoreState().openIndexWriter(solrCore);
          }
        }
        
        // we must reload the core after we open the IW back up
       if (successfulInstall && (reloadCore || forceCoreReload)) {
          LOG.info("Reloading SolrCore {}", solrCore.getName());
          reloadCore();
        }

        if (successfulInstall) {
          if (isFullCopyNeeded) {
            // let the system know we are changing dir's and the old one
            // may be closed
            if (indexDir != null) {
              LOG.info("removing old index directory " + indexDir);
              solrCore.getDirectoryFactory().doneWithDirectory(indexDir);
              solrCore.getDirectoryFactory().remove(indexDir);
            }
          }
          if (isFullCopyNeeded) {
            solrCore.getUpdateHandler().newIndexWriter(isFullCopyNeeded);
          }
          
          openNewSearcherAndUpdateCommitPoint();
        }
        
        if (!isFullCopyNeeded && !forceReplication && !successfulInstall) {
          cleanup(solrCore, tmpIndexDir, indexDir, deleteTmpIdxDir, successfulInstall);
          cleanupDone = true;
          // we try with a full copy of the index
          LOG.warn(
              "Replication attempt was not successful - trying a full index replication reloadCore={}",
              reloadCore);
          successfulInstall = fetchLatestIndex(true, reloadCore);
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
        throw new SolrException(ErrorCode.SERVER_ERROR, "Index fetch failed : ", e);
      }
    } finally {
      if (!cleanupDone) {
        cleanup(solrCore, tmpIndexDir, indexDir, deleteTmpIdxDir, successfulInstall);
      }
    }
  }

  private void cleanup(final SolrCore core, Directory tmpIndexDir,
      Directory indexDir, boolean deleteTmpIdxDir, boolean successfulInstall) throws IOException {
    try {
      if (!successfulInstall) {
        try {
          logReplicationTimeAndConfFiles(null, successfulInstall);
        } catch (Exception e) {
          LOG.error("caught", e);
        }
      }

      core.getUpdateHandler().getSolrCoreState().setLastReplicateIndexSuccess(successfulInstall);

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

  private boolean hasUnusedFiles(Directory indexDir, IndexCommit commit) throws IOException {
    String segmentsFileName = commit.getSegmentsFileName();
    SegmentInfos infos = SegmentInfos.readCommit(indexDir, segmentsFileName);
    Set<String> currentFiles = new HashSet<>(infos.files(true));
    String[] allFiles = indexDir.listAll();
    for (String file : allFiles) {
      if (!file.equals(segmentsFileName) && !currentFiles.contains(file) && !file.endsWith(".lock")) {
        LOG.info("Found unused file: " + file);
        return true;
      }
    }
    return false;
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

      props.setProperty(LAST_CYCLE_BYTES_DOWNLOADED, String.valueOf(getTotalBytesDownloaded()));
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

  long getTotalBytesDownloaded() {
    long bytesDownloaded = 0;
    //get size from list of files to download
    for (Map<String, Object> file : getFilesDownloaded()) {
      bytesDownloaded += (Long) file.get(SIZE);
    }

    //get size from list of conf files to download
    for (Map<String, Object> file : getConfFilesDownloaded()) {
      bytesDownloaded += (Long) file.get(SIZE);
    }

    //get size from current file being downloaded
    Map<String, Object> currentFile = getCurrentFile();
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
      Collections.addAll(l, ss);
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

  private void openNewSearcherAndUpdateCommitPoint() throws IOException {
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
        } catch (InterruptedException | ExecutionException e) {
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
   * @param indexDir                 the indexDir to be merged to
   * @param version                  Version of index
   * @param tmpIndexDir              the directory to which files need to be downloadeed to
   * @param latestGeneration         the version number
   */
  private void downloadIndexFiles(boolean downloadCompleteIndex, Directory indexDir, Version version, Directory tmpIndexDir, long latestGeneration)
      throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Download files to dir: " + Arrays.asList(indexDir.listAll()));
    }
    for (Map<String,Object> file : filesToDownload) {
      String filename = (String) file.get(NAME);
      long size = (Long) file.get(SIZE);
      CompareResult compareResult = compareFile(indexDir, version, filename, size, (Long) file.get(CHECKSUM));
      if (!compareResult.equal || downloadCompleteIndex
          || filesToAlwaysDownloadIfNoChecksums(filename, size, compareResult)) {
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

  private boolean filesToAlwaysDownloadIfNoChecksums(String filename,
      long size, CompareResult compareResult) {
    // without checksums to compare, we always download .si, .liv, segments_N,
    // and any very small files
    return !compareResult.checkSummed && (filename.endsWith(".si") || filename.endsWith(".liv")
    || filename.startsWith("segments_") || size < _100K);
  }

  protected static Version checkOldestVersion(SegmentInfos infos) {
    // we treat these files as if they all the oldest version we see
    Version oldestVersion = Version.LUCENE_CURRENT;
    for (SegmentCommitInfo commitInfo : infos) {
      Version version = commitInfo.info.getVersion();
      if (oldestVersion.onOrAfter(version)) {
        oldestVersion = version;
      }
    }
    return oldestVersion;
  }

  protected static class CompareResult {
    boolean equal = false;
    boolean checkSummed = false;
  }

  protected static CompareResult compareFile(Directory indexDir, Version version, String filename, Long backupIndexFileLen, Long backupIndexFileChecksum) {
    CompareResult compareResult = new CompareResult();
    try {
      try (final IndexInput indexInput = indexDir.openInput(filename, IOContext.READONCE)) {
        long indexFileLen = indexInput.length();
        long indexFileChecksum = 0;

        if (backupIndexFileChecksum != null && version.onOrAfter(Version.LUCENE_4_8_0)) {
          try {
            indexFileChecksum = CodecUtil.retrieveChecksum(indexInput);
            compareResult.checkSummed = true;
          } catch (Exception e) {
            LOG.warn("Could not retrieve checksum from file.", e);
          }
        }
        
        if (!compareResult.checkSummed) {
          // we don't have checksums to compare
          
          if (indexFileLen == backupIndexFileLen) {
            compareResult.equal = true;
            return compareResult;
          } else {
            LOG.warn(
                "File {} did not match. expected length is {} and actual length is {}", filename, backupIndexFileLen, indexFileLen);
            compareResult.equal = false;
            return compareResult;
          }
        }
        
        // we have checksums to compare
        
        if (indexFileLen == backupIndexFileLen && indexFileChecksum == backupIndexFileChecksum) {
          compareResult.equal = true;
          return compareResult;
        } else {
          LOG.warn("File {} did not match. expected checksum is {} and actual is checksum {}. " +
              "expected length is {} and actual length is {}", filename, backupIndexFileChecksum, indexFileChecksum,
              backupIndexFileLen, indexFileLen);
          compareResult.equal = false;
          return compareResult;
        }
      }
    } catch (NoSuchFileException | FileNotFoundException e) {
      compareResult.equal = false;
      return compareResult;
    } catch (IOException e) {
      LOG.error("Could not read file " + filename + ". Downloading it again", e);
      compareResult.equal = false;
      return compareResult;
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
   * All the files which are common between master and slave must have same size and same checksum else we assume
   * they are not compatible (stale).
   *
   * @return true if the index stale and we need to download a fresh copy, false otherwise.
   * @throws IOException  if low level io error
   */
  private boolean isIndexStale(Directory dir, Version indexVersion) throws IOException {
    for (Map<String, Object> file : filesToDownload) {
      String filename = (String) file.get(NAME);
      Long length = (Long) file.get(SIZE);
      Long checksum = (Long) file.get(CHECKSUM);
      if (slowFileExists(dir, filename)) {
        if (checksum != null) {
          if (!(compareFile(dir, indexVersion, filename, length, checksum).equal)) {
            // file exists and size or checksum is different, therefore we must download it again
            return true;
          }
        } else {
          if (length != dir.fileLength(filename)) {
            LOG.warn("File {} did not match. expected length is {} and actual length is {}",
                filename, length, dir.fileLength(filename));
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Copy a file by the File#renameTo() method. If it fails, it is considered a failure
   * <p/>
   */
  private boolean moveAFile(Directory tmpIdxDir, Directory indexDir, String fname) {
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
      if (!moveAFile(tmpIdxDir, indexDir, fname)) return false;
    }
    //copy the segments file last
    if (segmentsFile != null) {
      if (!moveAFile(tmpIdxDir, indexDir, segmentsFile)) return false;
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
  protected static boolean modifyIndexProps(SolrCore solrCore, String tmpIdxDirName) {
    LOG.info("New index installed. Updating index properties... index="+tmpIdxDirName);
    Properties p = new Properties();
    Directory dir = null;
    try {
      dir = solrCore.getDirectoryFactory().get(solrCore.getDataDir(), DirContext.META_DATA, solrCore.getSolrConfig().indexConfig.lockType);
      if (slowFileExists(dir, IndexFetcher.INDEX_PROPERTIES)){
        final IndexInput input = dir.openInput(IndexFetcher.INDEX_PROPERTIES, DirectoryFactory.IOCONTEXT_NO_CACHE);

        final InputStream is = new PropertiesInputStream(input);
        try {
          p.load(new InputStreamReader(is, StandardCharsets.UTF_8));
        } catch (Exception e) {
          LOG.error("Unable to load " + IndexFetcher.INDEX_PROPERTIES, e);
        } finally {
          IOUtils.closeQuietly(is);
        }
      }
      try {
        dir.deleteFile(IndexFetcher.INDEX_PROPERTIES);
      } catch (IOException e) {
        // no problem
      }
      final IndexOutput out = dir.createOutput(IndexFetcher.INDEX_PROPERTIES, DirectoryFactory.IOCONTEXT_NO_CACHE);
      p.put("index", tmpIdxDirName);
      Writer os = null;
      try {
        os = new OutputStreamWriter(new PropertiesOutputStream(out), StandardCharsets.UTF_8);
        p.store(os, IndexFetcher.INDEX_PROPERTIES);
        dir.sync(Collections.singleton(INDEX_PROPERTIES));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unable to write " + IndexFetcher.INDEX_PROPERTIES, e);
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
   * Stops the ongoing fetch
   */
  void abortFetch() {
    stop = true;
  }

  long getReplicationStartTime() {
    return replicationStartTime;
  }

  long getReplicationTimeElapsed() {
    long timeElapsed = 0;
    if (getReplicationStartTime() > 0)
      timeElapsed = TimeUnit.SECONDS.convert(System.currentTimeMillis() - getReplicationStartTime(), TimeUnit.MILLISECONDS);
    return timeElapsed;
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
      tmp.put("bytesDownloaded", tmpFileFetcher.getBytesDownloaded());
    return tmp;
  }

  private static class ReplicationHandlerException extends InterruptedException {
    public ReplicationHandlerException(String message) {
      super(message);
    }
  }

  private interface FileInterface {
    public void sync() throws IOException;
    public void write(byte[] buf, int packetSize) throws IOException;
    public void close() throws Exception;
    public void delete() throws Exception;
  }

  /**
   * The class acts as a client for ReplicationHandler.FileStream. It understands the protocol of wt=filestream
   *
   * @see org.apache.solr.handler.ReplicationHandler.DirectoryFileStream
   */
  private class FileFetcher {
    private final FileInterface file;
    private boolean includeChecksum = true;
    private String fileName;
    private String saveAs;
    private boolean isConf;
    private Long indexGen;

    private long size;
    private long bytesDownloaded = 0;
    private byte[] buf = new byte[1024 * 1024];
    private Checksum checksum;
    private int errorCount = 0;
    private boolean aborted = false;

    FileFetcher(FileInterface file, Map<String, Object> fileDetails, String saveAs,
                boolean isConf, long latestGen) throws IOException {
      this.file = file;
      this.fileName = (String) fileDetails.get(NAME);
      this.size = (Long) fileDetails.get(SIZE);
      this.isConf = isConf;
      this.saveAs = saveAs;
      indexGen = latestGen;
      if (includeChecksum)
        checksum = new Adler32();
    }

    public long getBytesDownloaded() {
      return bytesDownloaded;
    }

    /**
     * The main method which downloads file
     */
    public void fetchFile() throws Exception {
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
        //if cleanup succeeds . The file is downloaded fully. do an fsync
        fsyncService.submit(new Runnable(){
          @Override
          public void run() {
            try {
              file.sync();
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
            LOG.warn("No content received for file: {}", fileName);
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
              LOG.error("Checksum not matched between client and server for file: {}", fileName);
              //if checksum is wrong it is a problem return for retry
              return 1;
            }
          }
          //if everything is fine, write down the packet to the file
          file.write(buf, packetSize);
          bytesDownloaded += packetSize;
          LOG.debug("Fetched and wrote {} bytes of file: {}", bytesDownloaded, fileName);
          if (bytesDownloaded >= size)
            return 0;
          //errorCount is always set to zero after a successful packet
          errorCount = 0;
        }
      } catch (ReplicationHandlerException e) {
        throw e;
      } catch (Exception e) {
        LOG.warn("Error in fetching file: {} (downloaded {} of {} bytes)",
            fileName, bytesDownloaded, size, e);
        //for any failure, increment the error count
        errorCount++;
        //if it fails for the same packet for MAX_RETRIES fail and come out
        if (errorCount > MAX_RETRIES) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Failed to fetch file: " + fileName +
                  " (downloaded " + bytesDownloaded + " of " + size + " bytes" +
                  ", error count: " + errorCount + " > " + MAX_RETRIES + ")", e);
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
        file.close();
      } catch (Exception e) {/* no-op */
        LOG.error("Error closing file: {}", this.saveAs, e);
      }
      if (bytesDownloaded != size) {
        //if the download is not complete then
        //delete the file being downloaded
        try {
          file.delete();
        } catch (Exception e) {
          LOG.error("Error deleting file: {}", this.saveAs, e);
        }
        //if the failure is due to a user abort it is returned normally else an exception is thrown
        if (!aborted)
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unable to download " + fileName + " completely. Downloaded "
                  + bytesDownloaded + "!=" + size);
      }
    }

    /**
     * Open a new stream using HttpClient
     */
    private FastInputStream getStream() throws IOException {

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

      // TODO use shardhandler
      try (HttpSolrClient client = new HttpSolrClient(masterUrl, myHttpClient, null)) {
        client.setSoTimeout(60000);
        client.setConnectionTimeout(15000);
        QueryRequest req = new QueryRequest(params);
        response = client.request(req);
        is = (InputStream) response.get("stream");
        if(useInternal) {
          is = new InflaterInputStream(is);
        }
        return new FastInputStream(is);
      } catch (Exception e) {
        //close stream on error
        IOUtils.closeQuietly(is);
        throw new IOException("Could not download file '" + fileName + "'", e);
      }
    }
  }

  private class DirectoryFile implements FileInterface {
    private final String saveAs;
    private Directory copy2Dir;
    private IndexOutput outStream;

    DirectoryFile(Directory tmpIndexDir, String saveAs) throws IOException {
      this.saveAs = saveAs;
      this.copy2Dir = tmpIndexDir;
      outStream = copy2Dir.createOutput(this.saveAs, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }

    public void sync() throws IOException {
      copy2Dir.sync(Collections.singleton(saveAs));
    }

    public void write(byte[] buf, int packetSize) throws IOException {
      outStream.writeBytes(buf, 0, packetSize);
    }

    public void close() throws Exception {
      outStream.close();
    }

    public void delete() throws Exception {
      copy2Dir.deleteFile(saveAs);
    }
  }

  private class DirectoryFileFetcher extends FileFetcher {
    DirectoryFileFetcher(Directory tmpIndexDir, Map<String, Object> fileDetails, String saveAs,
                boolean isConf, long latestGen) throws IOException {
      super(new DirectoryFile(tmpIndexDir, saveAs), fileDetails, saveAs, isConf, latestGen);
    }
  }

  private class LocalFsFile implements FileInterface {
    private File copy2Dir;

    FileChannel fileChannel;
    private FileOutputStream fileOutputStream;
    File file;

    LocalFsFile(File dir, String saveAs) throws IOException {
      this.copy2Dir = dir;

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
    }

    public void sync() throws IOException {
      FileUtils.sync(file);
    }

    public void write(byte[] buf, int packetSize) throws IOException {
      fileChannel.write(ByteBuffer.wrap(buf, 0, packetSize));
    }

    public void close() throws Exception {
      //close the FileOutputStream (which also closes the Channel)
      fileOutputStream.close();
    }

    public void delete() throws Exception {
      Files.delete(file.toPath());
    }
  }

  private class LocalFsFileFetcher extends FileFetcher {
    LocalFsFileFetcher(File dir, Map<String, Object> fileDetails, String saveAs,
                boolean isConf, long latestGen) throws IOException {
      super(new LocalFsFile(dir, saveAs), fileDetails, saveAs, isConf, latestGen);
    }
  }

  NamedList getDetails() throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COMMAND, CMD_DETAILS);
    params.set("slave", false);
    params.set(CommonParams.QT, "/replication");

    // TODO use shardhandler
    try (HttpSolrClient client = new HttpSolrClient(masterUrl, myHttpClient)) {
      client.setSoTimeout(60000);
      client.setConnectionTimeout(15000);
      QueryRequest request = new QueryRequest(params);
      return client.request(request);
    }
  }

  public void destroy() {
    abortFetch();
  }

  String getMasterUrl() {
    return masterUrl;
  }

  private static final int MAX_RETRIES = 5;

  private static final int NO_CONTENT = 1;

  private static final int ERR = 2;

  public static final String REPLICATION_PROPERTIES = "replication.properties";

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

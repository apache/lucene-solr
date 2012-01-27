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

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FileUtils;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import static org.apache.solr.handler.ReplicationHandler.*;

import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

/**
 * <p/> Provides functionality of downloading changed index files as well as config files and a timer for scheduling fetches from the
 * master. </p>
 *
 *
 * @since solr 1.4
 */
public class SnapPuller {
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

  private volatile FileFetcher fileFetcher;

  private volatile ExecutorService fsyncService;

  private volatile boolean stop = false;

  private boolean useInternal = false;

  private boolean useExternal = false;

  /**
   * Disable the timer task for polling
   */
  private AtomicBoolean pollDisabled = new AtomicBoolean(false);

  // HttpClient shared by all cores (used if timeout is not specified for a core)
  private static HttpClient client;
  // HttpClient for this instance if connectionTimeout or readTimeout has been specified
  private final HttpClient myHttpClient;

  private static synchronized HttpClient createHttpClient(String connTimeout, String readTimeout) {
    if (connTimeout == null && readTimeout == null && client != null)  return client;
    MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
    // Keeping a very high number so that if you have a large number of cores
    // no requests are kept waiting for an idle connection.
    mgr.getParams().setDefaultMaxConnectionsPerHost(10000);
    mgr.getParams().setMaxTotalConnections(10000);
    mgr.getParams().setSoTimeout(readTimeout == null ? 20000 : Integer.parseInt(readTimeout)); //20 secs
    mgr.getParams().setConnectionTimeout(connTimeout == null ? 5000 : Integer.parseInt(connTimeout)); //5 secs
    HttpClient httpClient = new HttpClient(mgr);
    if (client == null && connTimeout == null && readTimeout == null) client = httpClient;
    return httpClient;
  }

  public SnapPuller(NamedList initArgs, ReplicationHandler handler, SolrCore sc) {
    solrCore = sc;
    masterUrl = (String) initArgs.get(MASTER_URL);
    if (masterUrl == null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "'masterUrl' is required for a slave");
    this.replicationHandler = handler;
    pollIntervalStr = (String) initArgs.get(POLL_INTERVAL);
    pollInterval = readInterval(pollIntervalStr);
    String compress = (String) initArgs.get(COMPRESSION);
    useInternal = INTERNAL.equals(compress);
    useExternal = EXTERNAL.equals(compress);
    String connTimeout = (String) initArgs.get(HTTP_CONN_TIMEOUT);
    String readTimeout = (String) initArgs.get(HTTP_READ_TIMEOUT);
    String httpBasicAuthUser = (String) initArgs.get(HTTP_BASIC_AUTH_USER);
    String httpBasicAuthPassword = (String) initArgs.get(HTTP_BASIC_AUTH_PASSWORD);
    myHttpClient = createHttpClient(connTimeout, readTimeout);
    if (httpBasicAuthUser != null && httpBasicAuthPassword != null) {
      myHttpClient.getState().setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(httpBasicAuthUser, httpBasicAuthPassword));
    }
    if (pollInterval != null && pollInterval > 0) {
      startExecutorService();
    } else {
      LOG.info(" No value set for 'pollInterval'. Timer Task not started.");
    }
  }

  private void startExecutorService() {
    Runnable task = new Runnable() {
      public void run() {
        if (pollDisabled.get()) {
          LOG.info("Poll disabled");
          return;
        }
        try {
          executorStartTime = System.currentTimeMillis();
          replicationHandler.doFetch(null, false);
        } catch (Exception e) {
          LOG.error("Exception in fetching index", e);
        }
      }
    };
    executorService = Executors.newSingleThreadScheduledExecutor();
    long initialDelay = pollInterval - (System.currentTimeMillis() % pollInterval);
    executorService.scheduleAtFixedRate(task, initialDelay, pollInterval, TimeUnit.MILLISECONDS);
    LOG.info("Poll Scheduled at an interval of " + pollInterval + "ms");
  }

  /**
   * Gets the latest commit version and generation from the master
   */
  @SuppressWarnings("unchecked")
  NamedList getLatestVersion() throws IOException {
    PostMethod post = new PostMethod(masterUrl);
    post.addParameter(COMMAND, CMD_INDEX_VERSION);
    post.addParameter("wt", "javabin");
    return getNamedListResponse(post);
  }

  NamedList getCommandResponse(NamedList<String> commands) throws IOException {
    PostMethod post = new PostMethod(masterUrl);
    for (Map.Entry<String, String> c : commands) {
      post.addParameter(c.getKey(),c.getValue());
    }
    post.addParameter("wt", "javabin");
    return getNamedListResponse(post);
  }

  private NamedList<?> getNamedListResponse(PostMethod method) throws IOException {
    try {
      int status = myHttpClient.executeMethod(method);
      if (status != HttpStatus.SC_OK) {
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                "Request failed for the url " + method);
      }
      return (NamedList<?>) new JavaBinCodec().unmarshal(method.getResponseBodyAsStream());
    } finally {
      try {
        method.releaseConnection();
      } catch (Exception e) {
      }
    }
  }

  /**
   * Fetches the list of files in a given index commit point
   */
  void fetchFileList(long version) throws IOException {
    PostMethod post = new PostMethod(masterUrl);
    post.addParameter(COMMAND, CMD_GET_FILE_LIST);
    post.addParameter(CMD_INDEX_VERSION, String.valueOf(version));
    post.addParameter("wt", "javabin");

    @SuppressWarnings("unchecked")
    NamedList<List<Map<String, Object>>> nl 
      = (NamedList<List<Map<String, Object>>>) getNamedListResponse(post);

    List<Map<String, Object>> f = nl.get(CMD_GET_FILE_LIST);
    if (f != null)
      filesToDownload = Collections.synchronizedList(f);
    else {
      filesToDownload = Collections.emptyList();
      LOG.error("No files to download for indexversion: "+ version);
    }

    f = nl.get(CONF_FILES);
    if (f != null)
      confFilesToDownload = Collections.synchronizedList(f);
  }

  /**
   * This command downloads all the necessary files from master to install a index commit point. Only changed files are
   * downloaded. It also downloads the conf files (if they are modified).
   *
   * @param core the SolrCore
   * @return true on success, false if slave is already in sync
   * @throws IOException if an exception occurs
   */
  @SuppressWarnings("unchecked")
  boolean successfulInstall = false;

  boolean fetchLatestIndex(SolrCore core, boolean force) throws IOException, InterruptedException {
    successfulInstall = false;
    replicationStartTime = System.currentTimeMillis();
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

      IndexCommit commit;
      RefCounted<SolrIndexSearcher> searcherRefCounted = null;
      try {
        searcherRefCounted = core.getNewestSearcher(false);
        if (searcherRefCounted == null) {
          SolrException.log(LOG, "No open searcher found - fetch aborted");
          return false;
        }
        commit = searcherRefCounted.get().getIndexReader().getIndexCommit();
      } finally {
        if (searcherRefCounted != null)
          searcherRefCounted.decref();
      }
      
      if (latestVersion == 0L) {
        if (force && commit.getVersion() != 0) {
          // since we won't get the files for an empty index,
          // we just clear ours and commit
          core.getUpdateHandler().getSolrCoreState().getIndexWriter(core).deleteAll();
          SolrQueryRequest req = new LocalSolrQueryRequest(core,
              new ModifiableSolrParams());
          core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
        }
        
        //there is nothing to be replicated
        successfulInstall = true;
        return true;
      }
      
      if (!force && commit.getVersion() == latestVersion && commit.getGeneration() == latestGeneration) {
        //master and slave are already in sync just return
        LOG.info("Slave in sync with master.");
        successfulInstall = true;
        return true;
      }
      LOG.info("Master's version: " + latestVersion + ", generation: " + latestGeneration);
      LOG.info("Slave's version: " + commit.getVersion() + ", generation: " + commit.getGeneration());
      LOG.info("Starting replication process");
      // get the list of files first
      fetchFileList(latestVersion);
      // this can happen if the commit point is deleted before we fetch the file list.
      if(filesToDownload.isEmpty()) return false;
      LOG.info("Number of files in latest index in master: " + filesToDownload.size());

      // Create the sync service
      fsyncService = Executors.newSingleThreadExecutor();
      // use a synchronized list because the list is read by other threads (to show details)
      filesDownloaded = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
      // if the generateion of master is older than that of the slave , it means they are not compatible to be copied
      // then a new index direcory to be created and all the files need to be copied
      boolean isFullCopyNeeded = commit.getVersion() >= latestVersion || force;
      File tmpIndexDir = createTempindexDir(core);
      if (isIndexStale())
        isFullCopyNeeded = true;
      successfulInstall = false;
      boolean deleteTmpIdxDir = true;
      File indexDir = null ;
      try {
        indexDir = new File(core.getIndexDir());
        downloadIndexFiles(isFullCopyNeeded, tmpIndexDir, latestVersion);
        LOG.info("Total time taken for download : " + ((System.currentTimeMillis() - replicationStartTime) / 1000) + " secs");
        Collection<Map<String, Object>> modifiedConfFiles = getModifiedConfFiles(confFilesToDownload);
        if (!modifiedConfFiles.isEmpty()) {
          downloadConfFiles(confFilesToDownload, latestVersion);
          if (isFullCopyNeeded) {
            successfulInstall = modifyIndexProps(tmpIndexDir.getName());
            deleteTmpIdxDir =  false;
          } else {
            successfulInstall = copyIndexFiles(tmpIndexDir, indexDir);
          }
          if (successfulInstall) {
            LOG.info("Configuration files are modified, core will be reloaded");
            logReplicationTimeAndConfFiles(modifiedConfFiles, successfulInstall);//write to a file time of replication and conf files.
            reloadCore();
          }
        } else {
          terminateAndWaitFsyncService();
          if (isFullCopyNeeded) {
            successfulInstall = modifyIndexProps(tmpIndexDir.getName());
            deleteTmpIdxDir =  false;
          } else {
            successfulInstall = copyIndexFiles(tmpIndexDir, indexDir);
          }
          if (successfulInstall) {
            logReplicationTimeAndConfFiles(modifiedConfFiles, successfulInstall);
            doCommit();
          }
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
      } finally {
        if (deleteTmpIdxDir) delTree(tmpIndexDir);
        else delTree(indexDir);
      }
    } finally {
      if (!successfulInstall) {
        logReplicationTimeAndConfFiles(null, successfulInstall);
      }
      filesToDownload = filesDownloaded = confFilesDownloaded = confFilesToDownload = null;
      replicationStartTime = 0;
      fileFetcher = null;
      if (fsyncService != null && !fsyncService.isShutdown()) fsyncService.shutdownNow();
      fsyncService = null;
      stop = false;
      fsyncException = null;
    }
  }

  private volatile Exception fsyncException;

  /**
   * terminate the fsync service and wait for all the tasks to complete. If it is already terminated
   *
   * @throws Exception
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
   */
  private void logReplicationTimeAndConfFiles(Collection<Map<String, Object>> modifiedConfFiles, boolean successfulInstall) {
    FileOutputStream outFile = null;
    List<String> confFiles = new ArrayList<String>();
    if (modifiedConfFiles != null && !modifiedConfFiles.isEmpty())
      for (Map<String, Object> map1 : modifiedConfFiles)
        confFiles.add((String) map1.get(NAME));

    Properties props = replicationHandler.loadReplicationProperties();
    long replicationTime = System.currentTimeMillis();
    long replicationTimeTaken = (replicationTime - getReplicationStartTime()) / 1000;
    try {
      int indexCount = 1, confFilesCount = 1;
      if (props.containsKey(TIMES_INDEX_REPLICATED)) {
        indexCount = Integer.valueOf(props.getProperty(TIMES_INDEX_REPLICATED)) + 1;
      }
      StringBuffer sb = readToStringBuffer(replicationTime, props.getProperty(INDEX_REPLICATED_AT_LIST));
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
        sb = readToStringBuffer(replicationTime, props.getProperty(REPLICATION_FAILED_AT_LIST));
        props.setProperty(REPLICATION_FAILED_AT_LIST, sb.toString());
      }
      File f = new File(solrCore.getDataDir(), REPLICATION_PROPERTIES);
      outFile = new FileOutputStream(f);
      props.store(outFile, "Replication details");
      outFile.close();
    } catch (Exception e) {
      LOG.warn("Exception while updating statistics", e);
    }
    finally {
      IOUtils.closeQuietly(outFile);
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

  private StringBuffer readToStringBuffer(long replicationTime, String str) {
    StringBuffer sb = new StringBuffer();
    List<String> l = new ArrayList<String>();
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

  private void doCommit() throws IOException {
    SolrQueryRequest req = new LocalSolrQueryRequest(solrCore,
        new ModifiableSolrParams());
    try {
      
      // reboot the writer on the new index and get a new searcher
      solrCore.getUpdateHandler().newIndexWriter();
      // update our commit point to the right dir
      solrCore.getUpdateHandler().commit(new CommitUpdateCommand(req, false));

    } finally {
      req.close();
    }
  }


  /**
   * All the files are copied to a temp dir first
   */
  private File createTempindexDir(SolrCore core) {
    String tmpIdxDirName = "index." + new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.US).format(new Date());
    File tmpIdxDir = new File(core.getDataDir(), tmpIdxDirName);
    tmpIdxDir.mkdirs();
    return tmpIdxDir;
  }

  private void reloadCore() {
    new Thread() {
      @Override
      public void run() {
        try {
          solrCore.getCoreDescriptor().getCoreContainer().reload(solrCore.getName());
        } catch (Exception e) {
          LOG.error("Could not restart core ", e);
        }
      }
    }.start();
  }

  private void downloadConfFiles(List<Map<String, Object>> confFilesToDownload, long latestVersion) throws Exception {
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
        fileFetcher = new FileFetcher(tmpconfDir, file, saveAs, true, latestVersion);
        currentFile = file;
        fileFetcher.fetchFile();
        confFilesDownloaded.add(new HashMap<String, Object>(file));
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
   * @param tmpIdxDir               the directory to which files need to be downloadeed to
   * @param latestVersion         the version number
   */
  private void downloadIndexFiles(boolean downloadCompleteIndex, File tmpIdxDir, long latestVersion) throws Exception {
    for (Map<String, Object> file : filesToDownload) {
      File localIndexFile = new File(solrCore.getIndexDir(), (String) file.get(NAME));
      if (!localIndexFile.exists() || downloadCompleteIndex) {
        fileFetcher = new FileFetcher(tmpIdxDir, file, (String) file.get(NAME), false, latestVersion);
        currentFile = file;
        fileFetcher.fetchFile();
        filesDownloaded.add(new HashMap<String, Object>(file));
      } else {
        LOG.info("Skipping download for " + localIndexFile);
      }
    }
  }

  /**
   * All the files which are common between master and slave must have same timestamp and size else we assume they are
   * not compatible (stale).
   *
   * @return true if the index stale and we need to download a fresh copy, false otherwise.
   */
  private boolean isIndexStale() {
    for (Map<String, Object> file : filesToDownload) {
      File localIndexFile = new File(solrCore.getIndexDir(), (String) file
              .get(NAME));
      if (localIndexFile.exists()
              && localIndexFile.length() != (Long) file.get(SIZE)) {
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
  private boolean copyAFile(File tmpIdxDir, File indexDir, String fname, List<String> copiedfiles) {
    File indexFileInTmpDir = new File(tmpIdxDir, fname);
    File indexFileInIndex = new File(indexDir, fname);
    boolean success = indexFileInTmpDir.renameTo(indexFileInIndex);
    if(!success){
      try {
        LOG.error("Unable to move index file from: " + indexFileInTmpDir
              + " to: " + indexFileInIndex + "Trying to do a copy");
        FileUtils.copyFile(indexFileInTmpDir,indexFileInIndex);
        success = true;
      } catch (IOException e) {
        LOG.error("Unable to copy index file from: " + indexFileInTmpDir
              + " to: " + indexFileInIndex , e);
      }
    }

    if (!success) {
      for (String f : copiedfiles) {
        File indexFile = new File(indexDir, f);
        if (indexFile.exists())
          indexFile.delete();
      }
      delTree(tmpIdxDir);
      return false;
    }
    return true;
  }

  /**
   * Copy all index files from the temp index dir to the actual index. The segments_N file is copied last.
   */
  private boolean copyIndexFiles(File tmpIdxDir, File indexDir) throws IOException {
    String segmentsFile = null;
    List<String> copiedfiles = new ArrayList<String>();
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
      if (!copyAFile(tmpIdxDir, indexDir, fname, copiedfiles)) return false;
      copiedfiles.add(fname);
    }
    //copy the segments file last
    if (segmentsFile != null) {
      if (!copyAFile(tmpIdxDir, indexDir, segmentsFile, copiedfiles)) return false;
    }
    return true;
  }

  /**
   * The conf files are copied to the tmp dir to the conf dir. A backup of the old file is maintained
   */
  private void copyTmpConfFiles2Conf(File tmpconfDir) throws IOException {
    File confDir = new File(solrCore.getResourceLoader().getConfigDir());
    for (File file : tmpconfDir.listFiles()) {
      File oldFile = new File(confDir, file.getName());
      if (oldFile.exists()) {
        File backupFile = new File(confDir, oldFile.getName() + "." + getDateAsStr(new Date(oldFile.lastModified())));
        boolean status = oldFile.renameTo(backupFile);
        if (!status) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  "Unable to rename: " + oldFile + " to: " + backupFile);
        }
      }
      boolean status = file.renameTo(oldFile);
      if (status) {
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Unable to rename: " + file + " to: " + oldFile);
      }
    }
  }

  private String getDateAsStr(Date d) {
    return new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.US).format(d);
  }

  /**
   * If the index is stale by any chance, load index from a different dir in the data dir.
   */
  private boolean modifyIndexProps(String tmpIdxDirName) {
    LOG.info("New index installed. Updating index properties...");
    File idxprops = new File(solrCore.getDataDir() + "index.properties");
    Properties p = new Properties();
    if (idxprops.exists()) {
      InputStream is = null;
      try {
        is = new FileInputStream(idxprops);
        p.load(is);
      } catch (Exception e) {
        LOG.error("Unable to load index.properties");
      } finally {
        IOUtils.closeQuietly(is);
      }
    }
    p.put("index", tmpIdxDirName);
    FileOutputStream os = null;
    try {
      os = new FileOutputStream(idxprops);
      p.store(os, "index properties");
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Unable to write index.properties", e);
    } finally {
      IOUtils.closeQuietly(os);
    }
      return true;
  }

  private final Map<String, FileInfo> confFileInfoCache = new HashMap<String, FileInfo>();

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
    Map<String, Map<String, Object>> nameVsFile = new HashMap<String, Map<String, Object>>();
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
   * Delete the directory tree recursively
   */
  static boolean delTree(File dir) {
    if (dir == null || !dir.exists())
      return false;
    boolean isSuccess = true;
    File contents[] = dir.listFiles();
    if (contents != null) {
      for (File file : contents) {
        if (file.isDirectory()) {
          boolean success = delTree(file);
          if (!success) {
            LOG.warn("Unable to delete directory : " + file);
            isSuccess = false;
          }
        } else {
          boolean success = file.delete();
          if (!success) {
            LOG.warn("Unable to delete file : " + file);
            isSuccess = false;
            return false;
          }
        }
      }
    }
    return isSuccess && dir.delete();
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
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<Map<String, Object>>(tmp);
  }

  List<Map<String, Object>> getConfFilesDownloaded() {
    //make a copy first because it can be null later
    List<Map<String, Object>> tmp = confFilesDownloaded;
    // NOTE: it's safe to make a copy of a SynchronizedCollection(ArrayList)
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<Map<String, Object>>(tmp);
  }

  List<Map<String, Object>> getFilesToDownload() {
    //make a copy first because it can be null later
    List<Map<String, Object>> tmp = filesToDownload;
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<Map<String, Object>>(tmp);
  }

  List<Map<String, Object>> getFilesDownloaded() {
    List<Map<String, Object>> tmp = filesDownloaded;
    return tmp == null ? Collections.EMPTY_LIST : new ArrayList<Map<String, Object>>(tmp);
  }

  Map<String, Object> getCurrentFile() {
    Map<String, Object> tmp = currentFile;
    FileFetcher tmpFileFetcher = fileFetcher;
    if (tmp == null)
      return null;
    tmp = new HashMap<String, Object>(tmp);
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
   * @see org.apache.solr.handler.ReplicationHandler.FileStream
   */
  private class FileFetcher {
    boolean includeChecksum = true;

    private File copy2Dir;

    String fileName;

    String saveAs;

    long size, lastmodified;

    long bytesDownloaded = 0;

    FileChannel fileChannel;
    
    private FileOutputStream fileOutputStream;

    byte[] buf = new byte[1024 * 1024];

    Checksum checksum;

    File file;

    int errorCount = 0;

    private boolean isConf;

    private PostMethod post;

    private boolean aborted = false;

    private Long indexVersion;

    FileFetcher(File dir, Map<String, Object> fileDetails, String saveAs,
                boolean isConf, long latestVersion) throws IOException {
      this.copy2Dir = dir;
      this.fileName = (String) fileDetails.get(NAME);
      this.size = (Long) fileDetails.get(SIZE);
      this.isConf = isConf;
      this.saveAs = saveAs;
      if(fileDetails.get(LAST_MODIFIED) != null){
        lastmodified = (Long)fileDetails.get(LAST_MODIFIED);
      }
      indexVersion = latestVersion;

      this.file = new File(copy2Dir, saveAs);
      
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
              // if the file is downloaded properly set the
              //  timestamp same as that in the server
              if (file.exists() && lastmodified > 0)
                file.setLastModified(lastmodified);
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
            LOG.warn("No content recieved for file: " + currentFile);
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
      try {
        post.releaseConnection();
      } catch (Exception e) {
      }
      if (bytesDownloaded != size) {
        //if the download is not complete then
        //delete the file being downloaded
        try {
          file.delete();
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
      post = new PostMethod(masterUrl);
      //the method is command=filecontent
      post.addParameter(COMMAND, CMD_GET_FILE);
      //add the version to download. This is used to reserve the download
      post.addParameter(CMD_INDEX_VERSION, indexVersion.toString());
      if (isConf) {
        //set cf instead of file for config file
        post.addParameter(CONF_FILE_SHORT, fileName);
      } else {
        post.addParameter(FILE, fileName);
      }
      if (useInternal) {
        post.addParameter(COMPRESSION, "true");
      }
      if (useExternal) {
        post.setRequestHeader(new Header("Accept-Encoding", "gzip,deflate"));
      }
      //use checksum
      if (this.includeChecksum)
        post.addParameter(CHECKSUM, "true");
      //wt=filestream this is a custom protocol
      post.addParameter("wt", FILE_STREAM);
      // This happen if there is a failure there is a retry. the offset=<sizedownloaded> ensures that
      // the server starts from the offset
      if (bytesDownloaded > 0) {
        post.addParameter(OFFSET, "" + bytesDownloaded);
      }
      myHttpClient.executeMethod(post);
      InputStream is = post.getResponseBodyAsStream();
      //wrap it using FastInputStream
      if (useInternal) {
        is = new InflaterInputStream(is);
      } else if (useExternal) {
        is = checkCompressed(post, is);
      }
      return new FastInputStream(is);
    }
  }

  /*
   * This is copied from CommonsHttpSolrServer
   */
  private InputStream checkCompressed(HttpMethod method, InputStream respBody) throws IOException {
    Header contentEncodingHeader = method.getResponseHeader("Content-Encoding");
    if (contentEncodingHeader != null) {
      String contentEncoding = contentEncodingHeader.getValue();
      if (contentEncoding.contains("gzip")) {
        respBody = new GZIPInputStream(respBody);
      } else if (contentEncoding.contains("deflate")) {
        respBody = new InflaterInputStream(respBody);
      }
    } else {
      Header contentTypeHeader = method.getResponseHeader("Content-Type");
      if (contentTypeHeader != null) {
        String contentType = contentTypeHeader.getValue();
        if (contentType != null) {
          if (contentType.startsWith("application/x-gzip-compressed")) {
            respBody = new GZIPInputStream(respBody);
          } else if (contentType.startsWith("application/x-deflate")) {
            respBody = new InflaterInputStream(respBody);
          }
        }
      }
    }
    return respBody;
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
    if (executorService != null) executorService.shutdown();
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

  private static final String HTTP_CONN_TIMEOUT = "httpConnTimeout";

  private static final String HTTP_READ_TIMEOUT = "httpReadTimeout";

  private static final String HTTP_BASIC_AUTH_USER = "httpBasicAuthUser";

  private static final String HTTP_BASIC_AUTH_PASSWORD = "httpBasicAuthPassword";

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

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
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.DeflaterOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrDeletionPolicy;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.handler.IndexFetcher.IndexFetchResult;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.PropertiesInputStream;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.core.XmlConfigFile.assertWarnOrFail;

/**
 * <p> A Handler which provides a REST API for replication and serves replication requests from Followers. </p>
 * <p>When running on the leader, it provides the following commands <ol> <li>Get the current replicable index version
 * (command=indexversion)</li> <li>Get the list of files for a given index version
 * (command=filelist&amp;indexversion=&lt;VERSION&gt;)</li> <li>Get full or a part (chunk) of a given index or a config
 * file (command=filecontent&amp;file=&lt;FILE_NAME&gt;) You can optionally specify an offset and length to get that
 * chunk of the file. You can request a configuration file by using "cf" parameter instead of the "file" parameter.</li>
 * <li>Get status/statistics (command=details)</li> </ol> <p>When running on the follower, it provides the following
 * commands <ol> <li>Perform an index fetch now (command=snappull)</li> <li>Get status/statistics (command=details)</li>
 * <li>Abort an index fetch (command=abort)</li> <li>Enable/Disable polling the leader for new versions (command=enablepoll
 * or command=disablepoll)</li> </ol>
 *
 *
 * @since solr 1.4
 */
public class ReplicationHandler extends RequestHandlerBase implements SolrCoreAware {

  public static final String PATH = "/replication";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  SolrCore core;
  
  private volatile boolean closed = false;

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
            log.warn("Version in commitData was not formatted correctly: {}", commitTime, e);
          }
        }
      } catch (IOException e) {
        log.warn("Unable to get version from commitData, commit: {}", commit, e);
      }
      return new CommitVersionInfo(generation, version);
    }

    public String toString() {
      return "generation=" + generation + ",version=" + version;
    }
  }

  private IndexFetcher pollingIndexFetcher;

  private ReentrantLock indexFetchLock = new ReentrantLock();

  private ExecutorService restoreExecutor = ExecutorUtil.newMDCAwareSingleThreadExecutor(
      new SolrNamedThreadFactory("restoreExecutor"));

  private volatile Future<Boolean> restoreFuture;

  private volatile String currentRestoreName;

  private String includeConfFiles;

  private NamedList<String> confFileNameAlias = new NamedList<>();

  private boolean isLeader = false;

  private boolean isFollower = false;

  private boolean replicateOnOptimize = false;

  private boolean replicateOnCommit = false;

  private boolean replicateOnStart = false;

  private volatile ScheduledExecutorService executorService;

  private volatile long executorStartTime;

  private int numberBackupsToKeep = 0; //zero: do not delete old backups

  private int numTimesReplicated = 0;

  private final Map<String, FileInfo> confFileInfoCache = new HashMap<>();

  private Long reserveCommitDuration = readIntervalMs("00:00:10");

  volatile IndexCommit indexCommitPoint;

  volatile NamedList<?> snapShootDetails;

  private AtomicBoolean replicationEnabled = new AtomicBoolean(true);

  private Long pollIntervalNs;
  private String pollIntervalStr;

  private PollListener pollListener;
  public interface PollListener {
    void onComplete(SolrCore solrCore, IndexFetchResult fetchResult) throws IOException;
  }

  /**
   * Disable the timer task for polling
   */
  private AtomicBoolean pollDisabled = new AtomicBoolean(false);

  String getPollInterval() {
    return pollIntervalStr;
  }

  public void setPollListener(PollListener pollListener) {
    this.pollListener = pollListener;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.setHttpCaching(false);
    final SolrParams solrParams = req.getParams();
    String command = solrParams.required().get(COMMAND);

    // This command does not give the current index version of the leader
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
        rsp.add(STATUS, OK_STATUS);
      } else {
        // This happens when replication is not configured to happen after startup and no commit/optimize
        // has happened yet.
        rsp.add(CMD_INDEX_VERSION, 0L);
        rsp.add(GENERATION, 0L);
        rsp.add(STATUS, OK_STATUS);
      }
    } else if (command.equals(CMD_GET_FILE)) {
      getFileStream(solrParams, rsp);
    } else if (command.equals(CMD_GET_FILE_LIST)) {
      getFileList(solrParams, rsp);
    } else if (command.equalsIgnoreCase(CMD_BACKUP)) {
      doSnapShoot(new ModifiableSolrParams(solrParams), rsp, req);
    } else if (command.equalsIgnoreCase(CMD_RESTORE)) {
      restore(new ModifiableSolrParams(solrParams), rsp, req);
    } else if (command.equalsIgnoreCase(CMD_RESTORE_STATUS)) {
      populateRestoreStatus(rsp);
    } else if (command.equalsIgnoreCase(CMD_DELETE_BACKUP)) {
      deleteSnapshot(new ModifiableSolrParams(solrParams), rsp);
    } else if (command.equalsIgnoreCase(CMD_FETCH_INDEX)) {
      fetchIndex(solrParams, rsp);
    } else if (command.equalsIgnoreCase(CMD_DISABLE_POLL)) {
      disablePoll(rsp);
    } else if (command.equalsIgnoreCase(CMD_ENABLE_POLL)) {
      enablePoll(rsp);
    } else if (command.equalsIgnoreCase(CMD_ABORT_FETCH)) {
      if (abortFetch()) {
        rsp.add(STATUS, OK_STATUS);
      } else {
        reportErrorOnResponse(rsp, "No follower configured", null);
      }
    } else if (command.equals(CMD_SHOW_COMMITS)) {
      populateCommitInfo(rsp);
    } else if (command.equals(CMD_DETAILS)) {
      getReplicationDetails(rsp, getBoolWithBackwardCompatibility(solrParams, "follower", "slave", true));
    } else if (CMD_ENABLE_REPL.equalsIgnoreCase(command)) {
      replicationEnabled.set(true);
      rsp.add(STATUS, OK_STATUS);
    } else if (CMD_DISABLE_REPL.equalsIgnoreCase(command)) {
      replicationEnabled.set(false);
      rsp.add(STATUS, OK_STATUS);
    }
  }
  
  static boolean getBoolWithBackwardCompatibility(SolrParams params, String preferredKey, String alternativeKey, boolean defaultValue) {
    Boolean value = params.getBool(preferredKey);
    if (value != null) {
      return value;
    }
    return params.getBool(alternativeKey, defaultValue);
  }
  
  @SuppressWarnings("unchecked")
  static <T> T getObjectWithBackwardCompatibility(SolrParams params, String preferredKey, String alternativeKey, T defaultValue) {
    Object value = params.get(preferredKey);
    if (value != null) {
      return (T) value;
    }
    value = params.get(alternativeKey);
    if (value != null) {
      return (T) value;
    }
    return defaultValue;
  }
  
  @SuppressWarnings("unchecked")
  static <T> T getObjectWithBackwardCompatibility(NamedList<?> params, String preferredKey, String alternativeKey) {
    Object value = params.get(preferredKey);
    if (value != null) {
      return (T) value;
    }
    return (T) params.get(alternativeKey);
  }

  private void reportErrorOnResponse(SolrQueryResponse response, String message, Exception e) {
    response.add(STATUS, ERR_STATUS);
    response.add(MESSAGE, message);
    if (e != null) {
      response.add(EXCEPTION, e);
    }
  }

  public boolean abortFetch() {
    IndexFetcher fetcher = currentIndexFetcher;
    if (fetcher != null){
      fetcher.abortFetch();
      return true;
    } else {
      return false;
    }
  }

  @SuppressWarnings("deprecation")
  private void deleteSnapshot(ModifiableSolrParams params, SolrQueryResponse rsp) {
    params.required().get(NAME);

    String location = params.get(CoreAdminParams.BACKUP_LOCATION);
    core.getCoreContainer().assertPathAllowed(location == null ? null : Paths.get(location));
    SnapShooter snapShooter = new SnapShooter(core, location, params.get(NAME));
    snapShooter.validateDeleteSnapshot();
    snapShooter.deleteSnapAsync(this);
    rsp.add(STATUS, OK_STATUS);
  }

  private void fetchIndex(SolrParams solrParams, SolrQueryResponse rsp) throws InterruptedException {
    String leaderUrl = getObjectWithBackwardCompatibility(solrParams, LEADER_URL, LEGACY_LEADER_URL, null);
    if (!isFollower && leaderUrl == null) {
      reportErrorOnResponse(rsp, "No follower configured or no 'leaderUrl' specified", null);
      return;
    }
    final SolrParams paramsCopy = new ModifiableSolrParams(solrParams);
    final IndexFetchResult[] results = new IndexFetchResult[1];
    Thread fetchThread = new Thread(() -> {
      IndexFetchResult result = doFetch(paramsCopy, false);
      results[0] = result;
    }, "explicit-fetchindex-cmd") ;
    fetchThread.setDaemon(false);
    fetchThread.start();
    if (solrParams.getBool(WAIT, false)) {
      fetchThread.join();
      if (results[0] == null) {
        reportErrorOnResponse(rsp, "Unable to determine result of synchronous index fetch", null);
      } else if (results[0].getSuccessful()) {
        rsp.add(STATUS, OK_STATUS);
      } else {
        reportErrorOnResponse(rsp, results[0].getMessage(), null);
      }
    } else {
      rsp.add(STATUS, OK_STATUS);
    }
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
        log.warn("Exception while reading files for commit {}", c, e);
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
      log.warn("Exception in finding checksum of {}", f, e);
    } finally {
      IOUtils.closeQuietly(fis);
    }
    return null;
  }

  private volatile IndexFetcher currentIndexFetcher;

  public IndexFetchResult doFetch(SolrParams solrParams, boolean forceReplication) {
    String leaderUrl = solrParams == null ? null : ReplicationHandler.getObjectWithBackwardCompatibility(solrParams, LEADER_URL, LEGACY_LEADER_URL, null);
    if (!indexFetchLock.tryLock())
      return IndexFetchResult.LOCK_OBTAIN_FAILED;
    if (core.getCoreContainer().isShutDown()) {
      log.warn("I was asked to replicate but CoreContainer is shutting down");
      return IndexFetchResult.CONTAINER_IS_SHUTTING_DOWN; 
    }
    try {
      if (leaderUrl != null) {
        if (currentIndexFetcher != null && currentIndexFetcher != pollingIndexFetcher) {
          currentIndexFetcher.destroy();
        }
        currentIndexFetcher = new IndexFetcher(solrParams.toNamedList(), this, core);
      } else {
        currentIndexFetcher = pollingIndexFetcher;
      }
      return currentIndexFetcher.fetchLatestIndex(forceReplication);
    } catch (Exception e) {
      SolrException.log(log, "Index fetch failed ", e);
      if (currentIndexFetcher != pollingIndexFetcher) {
        currentIndexFetcher.destroy();
      }
      return new IndexFetchResult(IndexFetchResult.FAILED_BY_EXCEPTION_MESSAGE, false, e);
    } finally {
      if (pollingIndexFetcher != null) {
       if( currentIndexFetcher != pollingIndexFetcher) {
         currentIndexFetcher.destroy();
       }
        currentIndexFetcher = pollingIndexFetcher;
      }
      indexFetchLock.unlock();
    }
  }

  boolean isReplicating() {
    return indexFetchLock.isLocked();
  }

  private void restore(SolrParams params, SolrQueryResponse rsp, SolrQueryRequest req) throws IOException {
    if (restoreFuture != null && !restoreFuture.isDone()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Restore in progress. Cannot run multiple restore operations" +
          "for the same core");
    }
    String name = params.get(NAME);
    String location = params.get(CoreAdminParams.BACKUP_LOCATION);
    String repoName = params.get(CoreAdminParams.BACKUP_REPOSITORY);
    CoreContainer cc = core.getCoreContainer();
    BackupRepository repo = null;
    if (repoName != null) {
      repo = cc.newBackupRepository(Optional.of(repoName));
      location = repo.getBackupLocation(location);
      if (location == null) {
        throw new IllegalArgumentException("location is required");
      }
    } else {
      repo = new LocalFileSystemRepository();
      //If location is not provided then assume that the restore index is present inside the data directory.
      if (location == null) {
        location = core.getDataDir();
      }
    }
    if ("file".equals(repo.createURI("x").getScheme())) {
      core.getCoreContainer().assertPathAllowed(Paths.get(location));
    }

    URI locationUri = repo.createDirectoryURI(location);

    //If name is not provided then look for the last unnamed( the ones with the snapshot.timestamp format)
    //snapshot folder since we allow snapshots to be taken without providing a name. Pick the latest timestamp.
    if (name == null) {
      String[] filePaths = repo.listAll(locationUri);
      List<OldBackupDirectory> dirs = new ArrayList<>();
      for (String f : filePaths) {
        OldBackupDirectory obd = new OldBackupDirectory(locationUri, f);
        if (obd.getTimestamp().isPresent()) {
          dirs.add(obd);
        }
      }
      Collections.sort(dirs);
      if (dirs.size() == 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No backup name specified and none found in " + core.getDataDir());
      }
      name = dirs.get(0).getDirName();
    } else {
      //"snapshot." is prefixed by snapshooter
      name = "snapshot." + name;
    }

    RestoreCore restoreCore = RestoreCore.create(repo, core, locationUri, name);
    try {
      MDC.put("RestoreCore.core", core.getName());
      MDC.put("RestoreCore.backupLocation", location);
      MDC.put("RestoreCore.backupName", name);
      restoreFuture = restoreExecutor.submit(restoreCore);
      currentRestoreName = name;
      rsp.add(STATUS, OK_STATUS);
    } finally {
      MDC.remove("RestoreCore.core");
      MDC.remove("RestoreCore.backupLocation");
      MDC.remove("RestoreCore.backupName");
    }
  }

  private void populateRestoreStatus(SolrQueryResponse rsp) {
    NamedList<Object> restoreStatus = new SimpleOrderedMap<>();
    if (restoreFuture == null) {
      restoreStatus.add(STATUS, "No restore actions in progress");
      rsp.add(CMD_RESTORE_STATUS, restoreStatus);
      rsp.add(STATUS, OK_STATUS);
      return;
    }

    restoreStatus.add("snapshotName", currentRestoreName);
    if (restoreFuture.isDone()) {
      try {
        boolean success = restoreFuture.get();
        if (success) {
          restoreStatus.add(STATUS, SUCCESS);
        } else {
          restoreStatus.add(STATUS, FAILED);
        }
      } catch (Exception e) {
        restoreStatus.add(STATUS, FAILED);
        restoreStatus.add(EXCEPTION, e.getMessage());
        rsp.add(CMD_RESTORE_STATUS, restoreStatus);
        reportErrorOnResponse(rsp, "Unable to read restorestatus", e);
        return;
      }
    } else {
      restoreStatus.add(STATUS, "In Progress");
    }

    rsp.add(CMD_RESTORE_STATUS, restoreStatus);
    rsp.add(STATUS, OK_STATUS);
  }

  private void populateCommitInfo(SolrQueryResponse rsp) {
    rsp.add(CMD_SHOW_COMMITS, getCommits());
    rsp.add(STATUS, OK_STATUS);
  }

  private void doSnapShoot(SolrParams params, SolrQueryResponse rsp, SolrQueryRequest req) {
    try {
      int numberToKeep = params.getInt(NUMBER_BACKUPS_TO_KEEP_REQUEST_PARAM, 0);
      if (numberToKeep > 0 && numberBackupsToKeep > 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot use " + NUMBER_BACKUPS_TO_KEEP_REQUEST_PARAM +
            " if " + NUMBER_BACKUPS_TO_KEEP_INIT_PARAM + " was specified in the configuration.");
      }
      numberToKeep = Math.max(numberToKeep, numberBackupsToKeep);
      if (numberToKeep < 1) {
        numberToKeep = Integer.MAX_VALUE;
      }

      String location = params.get(CoreAdminParams.BACKUP_LOCATION);
      String repoName = params.get(CoreAdminParams.BACKUP_REPOSITORY);
      CoreContainer cc = core.getCoreContainer();
      BackupRepository repo = null;
      if (repoName != null) {
        repo = cc.newBackupRepository(Optional.of(repoName));
        location = repo.getBackupLocation(location);
        if (location == null) {
          throw new IllegalArgumentException("location is required");
        }
      } else {
        repo = new LocalFileSystemRepository();
        if (location == null) {
          location = core.getDataDir();
        } else {
          location = core.getCoreDescriptor().getInstanceDir().resolve(location).normalize().toString();
        }
      }
      if ("file".equals(repo.createURI("x").getScheme())) {
        core.getCoreContainer().assertPathAllowed(Paths.get(location));
      }

        // small race here before the commit point is saved
      URI locationUri = repo.createDirectoryURI(location);
      String commitName = params.get(CoreAdminParams.COMMIT_NAME);
      SnapShooter snapShooter = new SnapShooter(repo, core, locationUri, params.get(NAME), commitName);
      snapShooter.validateCreateSnapshot();
      snapShooter.createSnapAsync(numberToKeep, (nl) -> snapShootDetails = nl);
      rsp.add(STATUS, OK_STATUS);
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      log.error("Exception while creating a snapshot", e);
      reportErrorOnResponse(rsp, "Error encountered while creating a snapshot: " + e.getMessage(), e);
    }
  }

  /**
   * This method adds an Object of FileStream to the response . The FileStream implements a custom protocol which is
   * understood by IndexFetcher.FileFetcher
   *
   * @see IndexFetcher.LocalFsFileFetcher
   * @see IndexFetcher.DirectoryFileFetcher
   */
  private void getFileStream(SolrParams solrParams, SolrQueryResponse rsp) {
    ModifiableSolrParams rawParams = new ModifiableSolrParams(solrParams);
    rawParams.set(CommonParams.WT, FILE_STREAM);

    String cfileName = solrParams.get(CONF_FILE_SHORT);
    String tlogFileName = solrParams.get(TLOG_FILE);
    if (cfileName != null) {
      rsp.add(FILE_STREAM, new LocalFsConfFileStream(solrParams));
    } else if (tlogFileName != null) {
      rsp.add(FILE_STREAM, new LocalFsTlogFileStream(solrParams));
    } else {
      rsp.add(FILE_STREAM, new DirectoryFileStream(solrParams));
    }
    rsp.add(STATUS, OK_STATUS);
  }

  private void getFileList(SolrParams solrParams, SolrQueryResponse rsp) {
    final IndexDeletionPolicyWrapper delPol = core.getDeletionPolicy();
    final long gen = Long.parseLong(solrParams.required().get(GENERATION));
    
    IndexCommit commit = null;
    try {
      if (gen == -1) {
        commit = delPol.getAndSaveLatestCommit();
        if (null == commit) {
          rsp.add(CMD_GET_FILE_LIST, Collections.emptyList());
          return;
        }
      } else {
        try {
          commit = delPol.getAndSaveCommitPoint(gen);
        } catch (IllegalStateException ignored) {
          /* handle this below the same way we handle a return value of null... */
        }
        if (null == commit) {
          // The gen they asked for either doesn't exist or has already been deleted
          reportErrorOnResponse(rsp, "invalid index generation", null);
          return;
        }
      }
      assert null != commit;
      
      List<Map<String, Object>> result = new ArrayList<>();
      Directory dir = null;
      try {
        dir = core.getDirectoryFactory().get(core.getNewIndexDir(), DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
        SegmentInfos infos = SegmentInfos.readCommit(dir, commit.getSegmentsFileName());
        for (SegmentCommitInfo commitInfo : infos) {
          for (String file : commitInfo.files()) {
            Map<String, Object> fileMeta = new HashMap<>();
            fileMeta.put(NAME, file);
            fileMeta.put(SIZE, dir.fileLength(file));
            
            try (final IndexInput in = dir.openInput(file, IOContext.READONCE)) {
              try {
                long checksum = CodecUtil.retrieveChecksum(in);
                fileMeta.put(CHECKSUM, checksum);
              } catch (Exception e) {
                //TODO Should this trigger a larger error?
                log.warn("Could not read checksum from index file: {}", file, e);
              }
            }
            
            result.add(fileMeta);
          }
        }
        
        // add the segments_N file
        
        Map<String, Object> fileMeta = new HashMap<>();
        fileMeta.put(NAME, infos.getSegmentsFileName());
        fileMeta.put(SIZE, dir.fileLength(infos.getSegmentsFileName()));
        if (infos.getId() != null) {
          try (final IndexInput in = dir.openInput(infos.getSegmentsFileName(), IOContext.READONCE)) {
            try {
              fileMeta.put(CHECKSUM, CodecUtil.retrieveChecksum(in));
            } catch (Exception e) {
              //TODO Should this trigger a larger error?
              log.warn("Could not read checksum from index file: {}", infos.getSegmentsFileName(), e);
            }
          }
        }
        result.add(fileMeta);
      } catch (IOException e) {
        log.error("Unable to get file names for indexCommit generation: {}", commit.getGeneration(), e);
        reportErrorOnResponse(rsp, "unable to get file names for given index generation", e);
        return;
      } finally {
        if (dir != null) {
          try {
            core.getDirectoryFactory().release(dir);
          } catch (IOException e) {
            SolrException.log(log, "Could not release directory after fetching file list", e);
          }
        }
      }
      rsp.add(CMD_GET_FILE_LIST, result);
      
      if (solrParams.getBool(TLOG_FILES, false)) {
        try {
          List<Map<String, Object>> tlogfiles = getTlogFileList(commit);
          log.info("Adding tlog files to list: {}", tlogfiles);
          rsp.add(TLOG_FILES, tlogfiles);
        }
        catch (IOException e) {
          log.error("Unable to get tlog file names for indexCommit generation: {}", commit.getGeneration(), e);
          reportErrorOnResponse(rsp, "unable to get tlog file names for given index generation", e);
          return;
        }
      }
      
      if (confFileNameAlias.size() < 1 || core.getCoreContainer().isZooKeeperAware())
        return;
      log.debug("Adding config files to list: {}", includeConfFiles);
      //if configuration files need to be included get their details
      rsp.add(CONF_FILES, getConfFileInfoFromCache(confFileNameAlias, confFileInfoCache));
      rsp.add(STATUS, OK_STATUS);
      
    } finally {
      if (null != commit) {
        // before releasing the save on our commit point, set a short reserve duration since
        // the main reason remote nodes will ask for the file list is because they are preparing to
        // replicate from us...
        delPol.setReserveDuration(commit.getGeneration(), reserveCommitDuration);
        delPol.releaseCommitPoint(commit);
      }
    }
  }

  /**
   * Retrieves the list of tlog files associated to a commit point.
   * NOTE: The commit <b>MUST</b> be reserved before calling this method
   */
  List<Map<String, Object>> getTlogFileList(IndexCommit commit) throws IOException {
    long maxVersion = this.getMaxVersion(commit);
    CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();
    String[] logList = ulog.getLogList(new File(ulog.getLogDir()));
    List<Map<String, Object>> tlogFiles = new ArrayList<>();
    for (String fileName : logList) {
      // filter out tlogs that are older than the current index commit generation, so that the list of tlog files is
      // in synch with the latest index commit point
      long startVersion = Math.abs(Long.parseLong(fileName.substring(fileName.lastIndexOf('.') + 1)));
      if (startVersion < maxVersion) {
        Map<String, Object> fileMeta = new HashMap<>();
        fileMeta.put(NAME, fileName);
        fileMeta.put(SIZE, new File(ulog.getLogDir(), fileName).length());
        tlogFiles.add(fileMeta);
      }
    }
    return tlogFiles;
  }

  /**
   * Retrieves the maximum version number from an index commit.
   * NOTE: The commit <b>MUST</b> be reserved before calling this method
   */
  private long getMaxVersion(IndexCommit commit) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(commit)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      VersionInfo vinfo = core.getUpdateHandler().getUpdateLog().getVersionInfo();
      return Math.abs(vinfo.getMaxVersionFromIndex(searcher));
    }
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

  private void disablePoll(SolrQueryResponse rsp) {
    if (pollingIndexFetcher != null){
      pollDisabled.set(true);
      log.info("inside disable poll, value of pollDisabled = {}", pollDisabled);
      rsp.add(STATUS, OK_STATUS);
    } else {
      reportErrorOnResponse(rsp, "No follower configured", null);
    }
  }

  private void enablePoll(SolrQueryResponse rsp) {
    if (pollingIndexFetcher != null){
      pollDisabled.set(false);
      log.info("inside enable poll, value of pollDisabled = {}", pollDisabled);
      rsp.add(STATUS, OK_STATUS);
    } else {
      reportErrorOnResponse(rsp, "No follower configured", null);
    }
  }

  boolean isPollingDisabled() {
    return pollDisabled.get();
  }

  @SuppressForbidden(reason = "Need currentTimeMillis, to output next execution time in replication details")
  private void markScheduledExecutionStart() {
    executorStartTime = System.currentTimeMillis();
  }

  private Date getNextScheduledExecTime() {
    Date nextTime = null;
    if (executorStartTime > 0)
      nextTime = new Date(executorStartTime + TimeUnit.MILLISECONDS.convert(pollIntervalNs, TimeUnit.NANOSECONDS));
    return nextTime;
  }

  int getTimesReplicatedSinceStartup() {
    return numTimesReplicated;
  }

  void setTimesReplicatedSinceStartup() {
    numTimesReplicated++;
  }

  @Override
  public Category getCategory() {
    return Category.REPLICATION;
  }

  @Override
  public String getDescription() {
    return "ReplicationHandler provides replication of index and configuration files from Leader to Followers";
  }

  /**
   * returns the CommitVersionInfo for the current searcher, or null on error.
   */
  private CommitVersionInfo getIndexVersion() {
    try {
      return core.withSearcher(searcher -> CommitVersionInfo.build(searcher.getIndexReader().getIndexCommit()));
    } catch (IOException e) {
      log.warn("Unable to get index commit: ", e);
      return null;
    }
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);
    solrMetricsContext.gauge(this,  () -> (core != null && !core.isClosed() ? NumberUtils.readableSize(core.getIndexSize()) : parentContext.nullString()),
        true, "indexSize", getCategory().toString(), scope);
    solrMetricsContext.gauge(this, () -> (core != null && !core.isClosed() ? getIndexVersion().toString() : parentContext.nullString()),
         true, "indexVersion", getCategory().toString(), scope);
    solrMetricsContext.gauge(this, () -> (core != null && !core.isClosed() ? getIndexVersion().generation : parentContext.nullNumber()),
        true, GENERATION, getCategory().toString(), scope);
    solrMetricsContext.gauge(this, () -> (core != null && !core.isClosed() ? core.getIndexDir() : parentContext.nullString()),
        true, "indexPath", getCategory().toString(), scope);
    solrMetricsContext.gauge(this, () -> isLeader,
         true, "isLeader", getCategory().toString(), scope);
    solrMetricsContext.gauge(this, () -> isFollower,
         true, "isFollower", getCategory().toString(), scope);
    final MetricsMap fetcherMap = new MetricsMap(map -> {
      IndexFetcher fetcher = currentIndexFetcher;
      if (fetcher != null) {
        map.put(LEGACY_LEADER_URL, fetcher.getLeaderUrl());
        if (getPollInterval() != null) {
          map.put(POLL_INTERVAL, getPollInterval());
        }
        map.put("isPollingDisabled", isPollingDisabled());
        map.put("isReplicating", isReplicating());
        long elapsed = fetcher.getReplicationTimeElapsed();
        long val = fetcher.getTotalBytesDownloaded();
        if (elapsed > 0) {
          map.put("timeElapsed", elapsed);
          map.put("bytesDownloaded", val);
          map.put("downloadSpeed", val / elapsed);
        }
        Properties props = loadReplicationProperties();
        addReplicationProperties(map::putNoEx, props);
      }
    });
    solrMetricsContext.gauge(this , fetcherMap, true, "fetcher", getCategory().toString(), scope);
    solrMetricsContext.gauge(this, () -> isLeader && includeConfFiles != null ? includeConfFiles : "",
         true, "confFilesToReplicate", getCategory().toString(), scope);
    solrMetricsContext.gauge(this, () -> isLeader ? getReplicateAfterStrings() : Collections.<String>emptyList(),
        true, REPLICATE_AFTER, getCategory().toString(), scope);
    solrMetricsContext.gauge(this,  () -> isLeader && replicationEnabled.get(),
        true, "replicationEnabled", getCategory().toString(), scope);
  }

  //TODO Should a failure retrieving any piece of info mark the overall request as a failure?  Is there a core set of values that are required to make a response here useful?
  /**
   * Used for showing statistics and progress information.
   */
  private NamedList<Object> getReplicationDetails(SolrQueryResponse rsp, boolean showFollowerDetails) {
    NamedList<Object> details = new SimpleOrderedMap<>();
    NamedList<Object> leader = new SimpleOrderedMap<>();
    NamedList<Object> follower = new SimpleOrderedMap<>();

    details.add("indexSize", NumberUtils.readableSize(core.getIndexSize()));
    details.add("indexPath", core.getIndexDir());
    details.add(CMD_SHOW_COMMITS, getCommits());
    details.add("isMaster", String.valueOf(isLeader));
    details.add("isSlave", String.valueOf(isFollower));
    CommitVersionInfo vInfo = getIndexVersion();
    details.add("indexVersion", null == vInfo ? 0 : vInfo.version);
    details.add(GENERATION, null == vInfo ? 0 : vInfo.generation);

    IndexCommit commit = indexCommitPoint;  // make a copy so it won't change

    if (isLeader) {
      if (includeConfFiles != null) leader.add(CONF_FILES, includeConfFiles);
      leader.add(REPLICATE_AFTER, getReplicateAfterStrings());
      leader.add("replicationEnabled", String.valueOf(replicationEnabled.get()));
    }

    if (isLeader && commit != null) {
      CommitVersionInfo repCommitInfo = CommitVersionInfo.build(commit);
      leader.add("replicableVersion", repCommitInfo.version);
      leader.add("replicableGeneration", repCommitInfo.generation);
    }

    IndexFetcher fetcher = currentIndexFetcher;
    if (fetcher != null) {
      Properties props = loadReplicationProperties();
      if (showFollowerDetails) {
        try {
          @SuppressWarnings({"rawtypes"})
          NamedList nl = fetcher.getDetails();
          follower.add("masterDetails", nl.get(CMD_DETAILS));
        } catch (Exception e) {
          log.warn(
              "Exception while invoking 'details' method for replication on leader ",
              e);
          follower.add(ERR_STATUS, "invalid_leader");
        }
      }
      follower.add(LEGACY_LEADER_URL, fetcher.getLeaderUrl());
      if (getPollInterval() != null) {
        follower.add(POLL_INTERVAL, getPollInterval());
      }
      Date nextScheduled = getNextScheduledExecTime();
      if (nextScheduled != null && !isPollingDisabled()) {
        follower.add(NEXT_EXECUTION_AT, nextScheduled.toString());
      } else if (isPollingDisabled()) {
        follower.add(NEXT_EXECUTION_AT, "Polling disabled");
      }
      addReplicationProperties(follower::add, props);

      follower.add("currentDate", new Date().toString());
      follower.add("isPollingDisabled", String.valueOf(isPollingDisabled()));
      boolean isReplicating = isReplicating();
      follower.add("isReplicating", String.valueOf(isReplicating));
      if (isReplicating) {
        try {
          long bytesToDownload = 0;
          List<String> filesToDownload = new ArrayList<>();
          for (Map<String, Object> file : fetcher.getFilesToDownload()) {
            filesToDownload.add((String) file.get(NAME));
            bytesToDownload += (Long) file.get(SIZE);
          }

          //get list of conf files to download
          for (Map<String, Object> file : fetcher.getConfFilesToDownload()) {
            filesToDownload.add((String) file.get(NAME));
            bytesToDownload += (Long) file.get(SIZE);
          }

          follower.add("filesToDownload", filesToDownload);
          follower.add("numFilesToDownload", String.valueOf(filesToDownload.size()));
          follower.add("bytesToDownload", NumberUtils.readableSize(bytesToDownload));

          long bytesDownloaded = 0;
          List<String> filesDownloaded = new ArrayList<>();
          for (Map<String, Object> file : fetcher.getFilesDownloaded()) {
            filesDownloaded.add((String) file.get(NAME));
            bytesDownloaded += (Long) file.get(SIZE);
          }

          //get list of conf files downloaded
          for (Map<String, Object> file : fetcher.getConfFilesDownloaded()) {
            filesDownloaded.add((String) file.get(NAME));
            bytesDownloaded += (Long) file.get(SIZE);
          }

          Map<String, Object> currentFile = fetcher.getCurrentFile();
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
          follower.add("filesDownloaded", filesDownloaded);
          follower.add("numFilesDownloaded", String.valueOf(filesDownloaded.size()));

          long estimatedTimeRemaining = 0;

          Date replicationStartTimeStamp = fetcher.getReplicationStartTimeStamp();
          if (replicationStartTimeStamp != null) {
            follower.add("replicationStartTime", replicationStartTimeStamp.toString());
          }
          long elapsed = fetcher.getReplicationTimeElapsed();
          follower.add("timeElapsed", String.valueOf(elapsed) + "s");

          if (bytesDownloaded > 0)
            estimatedTimeRemaining = ((bytesToDownload - bytesDownloaded) * elapsed) / bytesDownloaded;
          float totalPercent = 0;
          long downloadSpeed = 0;
          if (bytesToDownload > 0)
            totalPercent = (bytesDownloaded * 100) / bytesToDownload;
          if (elapsed > 0)
            downloadSpeed = (bytesDownloaded / elapsed);
          if (currFile != null)
            follower.add("currentFile", currFile);
          follower.add("currentFileSize", NumberUtils.readableSize(currFileSize));
          follower.add("currentFileSizeDownloaded", NumberUtils.readableSize(currFileSizeDownloaded));
          follower.add("currentFileSizePercent", String.valueOf(percentDownloaded));
          follower.add("bytesDownloaded", NumberUtils.readableSize(bytesDownloaded));
          follower.add("totalPercent", String.valueOf(totalPercent));
          follower.add("timeRemaining", String.valueOf(estimatedTimeRemaining) + "s");
          follower.add("downloadSpeed", NumberUtils.readableSize(downloadSpeed));
        } catch (Exception e) {
          log.error("Exception while writing replication details: ", e);
        }
      }
    }

    if (isLeader)
      details.add("master", leader);
    if (follower.size() > 0)
      details.add("slave", follower);

    @SuppressWarnings({"rawtypes"})
    NamedList snapshotStats = snapShootDetails;
    if (snapshotStats != null)
      details.add(CMD_BACKUP, snapshotStats);

    if (rsp.getValues().get(STATUS) == null) {
      rsp.add(STATUS, OK_STATUS);
    }
    rsp.add(CMD_DETAILS, details);
    return details;
  }

  private void addReplicationProperties(BiConsumer<String, Object> consumer, Properties props) {
    addVal(consumer, IndexFetcher.INDEX_REPLICATED_AT, props, Date.class);
    addVal(consumer, IndexFetcher.INDEX_REPLICATED_AT_LIST, props, List.class);
    addVal(consumer, IndexFetcher.REPLICATION_FAILED_AT_LIST, props, List.class);
    addVal(consumer, IndexFetcher.TIMES_INDEX_REPLICATED, props, Integer.class);
    addVal(consumer, IndexFetcher.CONF_FILES_REPLICATED, props, String.class);
    addVal(consumer, IndexFetcher.TIMES_CONFIG_REPLICATED, props, Integer.class);
    addVal(consumer, IndexFetcher.CONF_FILES_REPLICATED_AT, props, Date.class);
    addVal(consumer, IndexFetcher.LAST_CYCLE_BYTES_DOWNLOADED, props, Long.class);
    addVal(consumer, IndexFetcher.TIMES_FAILED, props, Integer.class);
    addVal(consumer, IndexFetcher.REPLICATION_FAILED_AT, props, Date.class);
    addVal(consumer, IndexFetcher.PREVIOUS_CYCLE_TIME_TAKEN, props, Long.class);
    addVal(consumer, IndexFetcher.CLEARED_LOCAL_IDX, props, Boolean.class);
  }

  private void addVal(BiConsumer<String, Object> consumer, String key, Properties props, @SuppressWarnings({"rawtypes"})Class clzz) {
    Object val = formatVal(key, props, clzz);
    if (val != null) {
      consumer.accept(key, val);
    }
  }

  private Object formatVal(String key, Properties props, @SuppressWarnings({"rawtypes"})Class clzz) {
    String s = props.getProperty(key);
    if (s == null || s.trim().length() == 0) return null;
    if (clzz == Date.class) {
      try {
        Long l = Long.parseLong(s);
        return new Date(l).toString();
      } catch (NumberFormatException e) {
        return null;
      }
    } else if (clzz == List.class) {
      String ss[] = s.split(",");
      List<String> l = new ArrayList<>();
      for (String s1 : ss) {
        l.add(new Date(Long.parseLong(s1)).toString());
      }
      return l;
    } else if (clzz == Long.class) {
      try {
        Long l = Long.parseLong(s);
        return l;
      } catch (NumberFormatException e) {
        return null;
      }
    } else if (clzz == Integer.class) {
      try {
        Integer i = Integer.parseInt(s);
        return i;
      } catch (NumberFormatException e) {
        return null;
      }
    } else if (clzz == Boolean.class) {
      return Boolean.parseBoolean(s);
    } else {
      return s;
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

  Properties loadReplicationProperties() {
    Directory dir = null;
    try {
      try {
        dir = core.getDirectoryFactory().get(core.getDataDir(),
            DirContext.META_DATA, core.getSolrConfig().indexConfig.lockType);
        IndexInput input;
        try {
          input = dir.openInput(
            IndexFetcher.REPLICATION_PROPERTIES, IOContext.DEFAULT);
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

  private void setupPolling(String intervalStr) {
    pollIntervalStr = intervalStr;
    pollIntervalNs = readIntervalNs(pollIntervalStr);
    if (pollIntervalNs == null || pollIntervalNs <= 0) {
      log.info(" No value set for 'pollInterval'. Timer Task not started.");
      return;
    }

    Runnable task = () -> {
      if (pollDisabled.get()) {
        log.info("Poll disabled");
        return;
      }
      ExecutorUtil.setServerThreadFlag(true); // so PKI auth works
      try {
        log.debug("Polling for index modifications");
        markScheduledExecutionStart();
        IndexFetchResult fetchResult = doFetch(null, false);
        if (pollListener != null) pollListener.onComplete(core, fetchResult);
      } catch (Exception e) {
        log.error("Exception in fetching index", e);
      } finally {
        ExecutorUtil.setServerThreadFlag(null);
      }
    };
    executorService = Executors.newSingleThreadScheduledExecutor(
        new SolrNamedThreadFactory("indexFetcher"));
    // Randomize initial delay, with a minimum of 1ms
    long initialDelayNs = new Random().nextLong() % pollIntervalNs
        + TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);
    executorService.scheduleAtFixedRate(task, initialDelayNs, pollIntervalNs, TimeUnit.NANOSECONDS);
    log.info("Poll scheduled at an interval of {}ms",
        TimeUnit.MILLISECONDS.convert(pollIntervalNs, TimeUnit.NANOSECONDS));
  }

  @Override
  @SuppressWarnings({"resource"})
  public void inform(SolrCore core) {
    this.core = core;
    registerCloseHook();
    Long deprecatedReserveCommitDuration = null;
    Object nbtk = initArgs.get(NUMBER_BACKUPS_TO_KEEP_INIT_PARAM);
    if(nbtk!=null) {
      numberBackupsToKeep = Integer.parseInt(nbtk.toString());
    } else {
      numberBackupsToKeep = 0;
    }
    @SuppressWarnings({"rawtypes"})
    NamedList follower = getObjectWithBackwardCompatibility(initArgs,  "follower",  "slave");
    boolean enableFollower = isEnabled( follower );
    if (enableFollower) {
      currentIndexFetcher = pollingIndexFetcher = new IndexFetcher(follower, this, core);
      setupPolling((String) follower.get(POLL_INTERVAL));
      isFollower = true;
    }
    @SuppressWarnings({"rawtypes"})
    NamedList leader = getObjectWithBackwardCompatibility(initArgs, "leader", "master");
    boolean enableLeader = isEnabled( leader );

    if (enableLeader || (enableFollower && !currentIndexFetcher.fetchFromLeader)) {
      if (core.getCoreContainer().getZkController() != null) {
        log.warn("SolrCloud is enabled for core {} but so is old-style replication. "
                + "Make sure you intend this behavior, it usually indicates a mis-configuration. "
                + "Leader setting is {} and follower setting is {}"
        , core.getName(), enableLeader, enableFollower);
      }
    }

    if (!enableFollower && !enableLeader) {
      enableLeader = true;
      leader = new NamedList<>();
    }

    if (enableLeader) {
      includeConfFiles = (String) leader.get(CONF_FILES);
      if (includeConfFiles != null && includeConfFiles.trim().length() > 0) {
        List<String> files = Arrays.asList(includeConfFiles.split(","));
        for (String file : files) {
          if (file.trim().length() == 0) continue;
          String[] strs = file.trim().split(":");
          // if there is an alias add it or it is null
          confFileNameAlias.add(strs[0], strs.length > 1 ? strs[1] : null);
        }
        log.info("Replication enabled for following config files: {}", includeConfFiles);
      }
      @SuppressWarnings({"rawtypes"})
      List backup = leader.getAll("backupAfter");
      boolean backupOnCommit = backup.contains("commit");
      boolean backupOnOptimize = !backupOnCommit && backup.contains("optimize");
      @SuppressWarnings({"rawtypes"})
      List replicateAfter = leader.getAll(REPLICATE_AFTER);
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
          log.warn("Replication can't call setMaxOptimizedCommitsToKeep on {}", policy);
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
          DirectoryReader reader = (s == null) ? null : s.get().getIndexReader();
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
          log.warn("Unable to get IndexCommit on startup", e);
        } finally {
          if (s!=null) s.decref();
        }
      }
      String reserve = (String) leader.get(RESERVE);
      if (reserve != null && !reserve.trim().equals("")) {
        reserveCommitDuration = readIntervalMs(reserve);
        deprecatedReserveCommitDuration = reserveCommitDuration;
        // remove this error check & backcompat logic when Version.LUCENE_7_1_0 is removed
        assertWarnOrFail(
          "Beginning with Solr 7.1, master."+RESERVE + " is deprecated and should now be configured directly on the ReplicationHandler.",
          (null == reserve),
          core.getSolrConfig().luceneMatchVersion.onOrAfter(Version.LUCENE_7_1_0));
      }
      isLeader = true;
    }

    {
      final String reserve = (String) initArgs.get(RESERVE);
      if (reserve != null && !reserve.trim().equals("")) {
        reserveCommitDuration = readIntervalMs(reserve);
        if (deprecatedReserveCommitDuration != null) {
          throw new IllegalArgumentException("'master."+RESERVE+"' and '"+RESERVE+"' are mutually exclusive.");
        }
      }
    }
    log.info("Commits will be reserved for {} ms", reserveCommitDuration);
  }

  // check leader or follower is enabled
  private boolean isEnabled( @SuppressWarnings({"rawtypes"})NamedList params ){
    if( params == null ) return false;
    Object enable = params.get( "enable" );
    if( enable == null ) return true;
    if( enable instanceof String )
      return StrUtils.parseBool( (String)enable );
    return Boolean.TRUE.equals( enable );
  }

  private final CloseHook startShutdownHook = new CloseHook() {
    @Override
    public void preClose(SolrCore core) {
      if (executorService != null)
        executorService.shutdown(); // we don't wait for shutdown - this can deadlock core reload
    }

    @Override
    public void postClose(SolrCore core) {
      if (pollingIndexFetcher != null) {
        pollingIndexFetcher.destroy();
      }
      if (currentIndexFetcher != null && currentIndexFetcher != pollingIndexFetcher) {
        currentIndexFetcher.destroy();
      }
    }
  };
  private final CloseHook finishShutdownHook = new CloseHook() {
    @Override
    public void preClose(SolrCore core) {
      ExecutorUtil.shutdownAndAwaitTermination(restoreExecutor);
      if (restoreFuture != null) {
        restoreFuture.cancel(false);
      }
    }

    @Override
    public void postClose(SolrCore core) {
    }
  };

  /**
   * register a closehook
   */
  private void registerCloseHook() {
    core.addCloseHook(startShutdownHook);
    core.addCloseHook(finishShutdownHook);
  }

  public void shutdown() {
    startShutdownHook.preClose(core);
    startShutdownHook.postClose(core);
    finishShutdownHook.preClose(core);
    finishShutdownHook.postClose(core);

    ExecutorUtil.shutdownAndAwaitTermination(executorService);

    core.removeCloseHook(startShutdownHook);
    core.removeCloseHook(finishShutdownHook);
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
      public void init(@SuppressWarnings({"rawtypes"})NamedList args) {/*no op*/ }

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
            snapShooter.validateCreateSnapshot();
            snapShooter.createSnapAsync(numberToKeep, (nl) -> snapShootDetails = nl);
          } catch (Exception e) {
            log.error("Exception while snapshooting", e);
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
  private class DirectoryFileStream implements SolrCore.RawWriter {
    protected SolrParams params;

    protected FastOutputStream fos;

    protected Long indexGen;
    protected IndexDeletionPolicyWrapper delPolicy;

    protected String fileName;
    protected String cfileName;
    protected String tlogFileName;
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

      fileName = validateFilenameOrError(params.get(FILE));
      cfileName = validateFilenameOrError(params.get(CONF_FILE_SHORT));
      tlogFileName = validateFilenameOrError(params.get(TLOG_FILE));
      
      sOffset = params.get(OFFSET);
      sLen = params.get(LEN);
      compress = params.get(COMPRESSION);
      useChecksum = params.getBool(CHECKSUM, false);
      indexGen = params.getLong(GENERATION);
      if (useChecksum) {
        checksum = new Adler32();
      }
      //No throttle if MAX_WRITE_PER_SECOND is not specified
      double maxWriteMBPerSec = params.getDouble(MAX_WRITE_PER_SECOND, Double.MAX_VALUE);
      rateLimiter = new RateLimiter.SimpleRateLimiter(maxWriteMBPerSec);
    }

    // Throw exception on directory traversal attempts 
    protected String validateFilenameOrError(String filename) {
      if (filename != null) {
        Path filePath = Paths.get(filename);
        filePath.forEach(subpath -> {
          if ("..".equals(subpath.toString())) {
            throw new SolrException(ErrorCode.FORBIDDEN, "File name cannot contain ..");
          }
        });
        if (filePath.isAbsolute()) {
          throw new SolrException(ErrorCode.FORBIDDEN, "File name must be relative");
        }
        return filename;
      } else return null;
    }

    protected void initWrite() throws IOException {
      if (sOffset != null) offset = Long.parseLong(sOffset);
      if (sLen != null) len = Integer.parseInt(sLen);
      if (fileName == null && cfileName == null && tlogFileName == null) {
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
      out = new CloseShieldOutputStream(out); // DeflaterOutputStream requires a close call, but don't close the request outputstream
      if (Boolean.parseBoolean(compress)) {
        fos = new FastOutputStream(new DeflaterOutputStream(out));
      } else {
        fos = new FastOutputStream(out);
      }
    }

    protected void extendReserveAndReleaseCommitPoint() {
      if(indexGen != null) {
        //Reserve the commit point for another 10s for the next file to be to fetched.
        //We need to keep extending the commit reservation between requests so that the replica can fetch
        //all the files correctly.
        delPolicy.setReserveDuration(indexGen, reserveCommitDuration);

        //release the commit point as the write is complete
        delPolicy.releaseCommitPoint(indexGen);
      }

    }
    public void write(OutputStream out) throws IOException {
      createOutputStream(out);

      IndexInput in = null;
      try {
        initWrite();

        Directory dir = core.withSearcher(searcher -> searcher.getIndexReader().directory());
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
          log.debug("Wrote {} bytes for file {}", offset + read, fileName); // nowarn

          //Pause if necessary
          maxBytesBeforePause += read;
          if (maxBytesBeforePause >= rateLimiter.getMinPauseCheckBytes()) {
            rateLimiter.pause(maxBytesBeforePause);
            maxBytesBeforePause = 0;
          }
          if (read != buf.length) {
            writeNothingAndFlush();
            fos.close(); // we close because DeflaterOutputStream requires a close call, but but the request outputstream is protected
            break;
          }
          offset += read;
          in.seek(offset);
        }
      } catch (IOException e) {
        log.warn("Exception while writing response for params: {}", params, e);
      } finally {
        if (in != null) {
          in.close();
        }
        extendReserveAndReleaseCommitPoint();
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
  private abstract class LocalFsFileStream extends DirectoryFileStream {

    private File file;

    public LocalFsFileStream(SolrParams solrParams) {
      super(solrParams);
      this.file = this.initFile();
    }

    protected abstract File initFile();

    @Override
    public void write(OutputStream out) throws IOException {
      createOutputStream(out);
      FileInputStream inputStream = null;
      try {
        initWrite();

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
              fos.close(); // we close because DeflaterOutputStream requires a close call, but but the request outputstream is protected
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
        log.warn("Exception while writing response for params: {}", params, e);
      } finally {
        IOUtils.closeQuietly(inputStream);
        extendReserveAndReleaseCommitPoint();
      }
    }
  }

  private class LocalFsTlogFileStream extends LocalFsFileStream {

    public LocalFsTlogFileStream(SolrParams solrParams) {
      super(solrParams);
    }

    protected File initFile() {
      //if it is a tlog file read from tlog directory
      return new File(core.getUpdateHandler().getUpdateLog().getLogDir(), tlogFileName);
    }

  }

  private class LocalFsConfFileStream extends LocalFsFileStream {

    public LocalFsConfFileStream(SolrParams solrParams) {
      super(solrParams);
    }

    protected File initFile() {
      //if it is a conf file read from config directory
      return new File(core.getResourceLoader().getConfigDir(), cfileName);
    }

  }

  private static Long readIntervalMs(String interval) {
    return TimeUnit.MILLISECONDS.convert(readIntervalNs(interval), TimeUnit.NANOSECONDS);
  }

  private static Long readIntervalNs(String interval) {
    if (interval == null)
      return null;
    int result = 0;
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
        return TimeUnit.NANOSECONDS.convert(result, TimeUnit.SECONDS);
      } catch (NumberFormatException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, INTERVAL_ERR_MSG);
      }
    } else {
      throw new SolrException(ErrorCode.SERVER_ERROR, INTERVAL_ERR_MSG);
    }
  }

  private static final String SUCCESS = "success";

  private static final String FAILED = "failed";

  private static final String EXCEPTION = "exception";

  public static final String LEADER_URL = "leaderUrl";
  @Deprecated
  /** @deprecated: Only used for backwards compatibility. Use {@link #LEADER_URL} */
  public static final String LEGACY_LEADER_URL = "masterUrl";

  public static final String FETCH_FROM_LEADER = "fetchFromLeader";

  // in case of TLOG replica, if leaderVersion = zero, don't do commit
  // otherwise updates from current tlog won't copied over properly to the new tlog, leading to data loss
  public static final String SKIP_COMMIT_ON_LEADER_VERSION_ZERO = "skipCommitOnLeaderVersionZero";
  @Deprecated
  /** @deprecated: Only used for backwards compatibility. Use {@link #SKIP_COMMIT_ON_LEADER_VERSION_ZERO} */
  public static final String LEGACY_SKIP_COMMIT_ON_LEADER_VERSION_ZERO = "skipCommitOnMasterVersionZero";

  public static final String STATUS = "status";

  public static final String MESSAGE = "message";

  public static final String COMMAND = "command";

  public static final String CMD_DETAILS = "details";

  public static final String CMD_BACKUP = "backup";

  public static final String CMD_RESTORE = "restore";

  public static final String CMD_RESTORE_STATUS = "restorestatus";

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

  public static final String SIZE = "size";

  public static final String MAX_WRITE_PER_SECOND = "maxWriteMBPerSec";

  public static final String CONF_FILE_SHORT = "cf";

  public static final String TLOG_FILE = "tlogFile";

  public static final String CHECKSUM = "checksum";

  public static final String ALIAS = "alias";

  public static final String CONF_CHECKSUM = "confchecksum";

  public static final String CONF_FILES = "confFiles";

  public static final String TLOG_FILES = "tlogFiles";

  public static final String REPLICATE_AFTER = "replicateAfter";

  public static final String FILE_STREAM = "filestream";

  public static final String POLL_INTERVAL = "pollInterval";

  public static final String INTERVAL_ERR_MSG = "The " + POLL_INTERVAL + " must be in this format 'HH:mm:ss'";

  private static final Pattern INTERVAL_PATTERN = Pattern.compile("(\\d*?):(\\d*?):(\\d*)");

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
   * non-test code, since the the duration of the fetch for non-trivial
   * indexes will likeley cause the request to time out.
   *
   * @lucene.internal
   */
  public static final String WAIT = "wait";
}

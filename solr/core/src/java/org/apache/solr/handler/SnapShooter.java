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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepository.PathType;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> Provides functionality equivalent to the snapshooter script </p>
 * This is no longer used in standard replication.
 *
 *
 * @since solr 1.4
 */
public class SnapShooter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private SolrCore solrCore;
  private String snapshotName = null;
  private String directoryName = null;
  private URI baseSnapDirPath = null;
  private URI snapshotDirPath = null;
  private BackupRepository backupRepo = null;
  private String commitName; // can be null

  @Deprecated
  public SnapShooter(SolrCore core, String location, String snapshotName) {
    String snapDirStr = null;
    // Note - This logic is only applicable to the usecase where a shared file-system is exposed via
    // local file-system interface (primarily for backwards compatibility). For other use-cases, users
    // will be required to specify "location" where the backup should be stored.
    if (location == null) {
      snapDirStr = core.getDataDir();
    } else {
      snapDirStr = core.getCoreDescriptor().getInstanceDir().resolve(location).normalize().toString();
    }
    initialize(new LocalFileSystemRepository(), core, Paths.get(snapDirStr).toUri(), snapshotName, null);
  }

  public SnapShooter(BackupRepository backupRepo, SolrCore core, URI location, String snapshotName, String commitName) {
    initialize(backupRepo, core, location, snapshotName, commitName);
  }

  private void initialize(BackupRepository backupRepo, SolrCore core, URI location, String snapshotName, String commitName) {
    this.solrCore = Objects.requireNonNull(core);
    this.backupRepo = Objects.requireNonNull(backupRepo);
    this.baseSnapDirPath = location;
    this.snapshotName = snapshotName;
    if ("file".equals(location.getScheme())) {
      solrCore.getCoreContainer().assertPathAllowed(Paths.get(location));
    }
    if (snapshotName != null) {
      directoryName = "snapshot." + snapshotName;
    } else {
      SimpleDateFormat fmt = new SimpleDateFormat(DATE_FMT, Locale.ROOT);
      directoryName = "snapshot." + fmt.format(new Date());
    }
    this.snapshotDirPath = backupRepo.resolveDirectory(location, directoryName);
    this.commitName = commitName;
  }

  public BackupRepository getBackupRepository() {
    return backupRepo;
  }

  /**
   * Gets the parent directory of the snapshots. This is the {@code location}
   * given in the constructor.
   */
  public URI getLocation() {
    return this.baseSnapDirPath;
  }

  public void validateDeleteSnapshot() {
    Objects.requireNonNull(this.snapshotName);

    boolean dirFound = false;
    String[] paths;
    try {
      paths = backupRepo.listAll(baseSnapDirPath);
      for (String path : paths) {
        if (path.equals(this.directoryName)
            && backupRepo.getPathType(backupRepo.resolveDirectory(baseSnapDirPath, path)) == PathType.DIRECTORY) {
          dirFound = true;
          break;
        }
      }
      if(dirFound == false) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Snapshot " + snapshotName + " cannot be found in directory: " + baseSnapDirPath);
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to find snapshot " + snapshotName + " in directory: " + baseSnapDirPath, e);
    }
  }

  protected void deleteSnapAsync(final ReplicationHandler replicationHandler) {
    new Thread(() -> deleteNamedSnapshot(replicationHandler)).start();
  }

  public void validateCreateSnapshot() throws IOException {
    // Note - Removed the current behavior of creating the directory hierarchy.
    // Do we really need to provide this support?
    if (!backupRepo.exists(baseSnapDirPath)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          " Directory does not exist: " + snapshotDirPath);
    }

    if (backupRepo.exists(snapshotDirPath)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Snapshot directory already exists: " + snapshotDirPath);
    }
  }

  @SuppressWarnings({"rawtypes"})
  public NamedList createSnapshot() throws Exception {
    final IndexCommit indexCommit = getAndSaveIndexCommit();
    try {
      return createSnapshot(indexCommit);
    } finally {
      solrCore.getDeletionPolicy().releaseCommitPoint(indexCommit.getGeneration());
    }
  }

  /**
   * If {@link #commitName} is non-null, then fetches the generation from the 
   * {@link SolrSnapshotMetaDataManager} and then returns 
   * {@link IndexDeletionPolicyWrapper#getAndSaveCommitPoint}, otherwise it returns 
   * {@link IndexDeletionPolicyWrapper#getAndSaveLatestCommit}.
   * <p>
   * Either way:
   * <ul>
   *  <li>This method does error handling for all cases where the commit can't be found 
   *       and wraps them in {@link SolrException}
   *  </li>
   *  <li>If this method returns, the result will be non null, and the caller <em>MUST</em> 
   *      call {@link IndexDeletionPolicyWrapper#releaseCommitPoint} when finished
   *  </li>
   * </ul>
   */
  private IndexCommit getAndSaveIndexCommit() throws IOException {
    final IndexDeletionPolicyWrapper delPolicy = solrCore.getDeletionPolicy();
    if (null != commitName) {
      return getAndSaveNamedIndexCommit(solrCore, commitName);
    }
    // else: not a named commit...
    final IndexCommit commit = delPolicy.getAndSaveLatestCommit();
    if (null == commit) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Index does not yet have any commits for core " +
                              solrCore.getName());
    }
    if (log.isDebugEnabled()) {
      log.debug("Using latest commit: generation={}", commit.getGeneration());
    }
    return commit;
  }

  public static IndexCommit getAndSaveNamedIndexCommit(SolrCore solrCore, String commitName) throws IOException {
    final IndexDeletionPolicyWrapper delPolicy = solrCore.getDeletionPolicy();
    final SolrSnapshotMetaDataManager snapshotMgr = solrCore.getSnapshotMetaDataManager();
    // We're going to tell the delPolicy to "save" this commit -- even though it's a named snapshot
    // that will already be protected -- just in case another thread deletes the name.
    // Because of this, we want to sync on the delPolicy to ensure there is no window of time after
    // snapshotMgr confirms commitName exists, but before we have a chance to 'save' it, when
    // the commitName might be deleted *and* the IndexWriter might call onCommit()
    synchronized (delPolicy) {
      final Optional<IndexCommit> namedCommit = snapshotMgr.getIndexCommitByName(commitName);
      if (namedCommit.isPresent()) {
        final IndexCommit commit = namedCommit.get();
        if (log.isDebugEnabled()) {
          log.debug("Using named commit: name={}, generation={}", commitName, commit.getGeneration());
        }
        delPolicy.saveCommitPoint(commit.getGeneration());
        return commit;
      }
    } // else...
    throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to find an index commit with name " +
            commitName + " for core " + solrCore.getName());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void createSnapAsync(final int numberToKeep, Consumer<NamedList> result) throws IOException {
    //TODO should use Solr's ExecutorUtil
    new Thread(() -> {
      NamedList snapShootDetails;
      try {
        snapShootDetails = createSnapshot();
      } catch (Exception e) {
        log.error("Exception while creating snapshot", e);
        snapShootDetails = new NamedList<>();
        snapShootDetails.add("exception", e.getMessage());
      }
      if (snapshotName == null) {
        try {
          deleteOldBackups(numberToKeep);
        } catch (IOException e) {
          log.warn("Unable to delete old snapshots ", e);
        }
      }
      if (null != snapShootDetails) result.accept(snapShootDetails);
    }).start();

  }

  /**
   * Handles the logic of creating a snapshot
   * <p>
   * <b>NOTE:</b> The caller <em>MUST</em> ensure that the {@link IndexCommit} is saved prior to 
   * calling this method, and released after calling this method, or there is no no garuntee that the 
   * method will function correctly.
   * </p>
   *
   * @see IndexDeletionPolicyWrapper#saveCommitPoint
   * @see IndexDeletionPolicyWrapper#releaseCommitPoint
   */
  @SuppressWarnings({"rawtypes"})
  protected NamedList createSnapshot(final IndexCommit indexCommit) throws Exception {
    assert indexCommit != null;
    if (log.isInfoEnabled()) {
      log.info("Creating backup snapshot {} at {}", (snapshotName == null ? "<not named>" : snapshotName), baseSnapDirPath);
    }
    boolean success = false;
    try {
      NamedList<Object> details = new NamedList<>();
      details.add("startTime", new Date().toString());//bad; should be Instant.now().toString()

      Collection<String> files = indexCommit.getFileNames();
      Directory dir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
      try {
        for(String fileName : files) {
          log.debug("Copying fileName={} from dir={} to snapshot={}", fileName, dir, snapshotDirPath);
          backupRepo.copyFileFrom(dir, fileName, snapshotDirPath);
        }
      } finally {
        solrCore.getDirectoryFactory().release(dir);
      }

      details.add("fileCount", files.size());
      details.add("status", "success");
      details.add("snapshotCompletedAt", new Date().toString());//bad; should be Instant.now().toString()
      details.add("snapshotName", snapshotName);
      details.add("directoryName", directoryName);
      if (log.isInfoEnabled()) {
        log.info("Done creating backup snapshot: {} into {}", (snapshotName == null ? "<not named>" : snapshotName), snapshotDirPath);
      }
      success = true;
      return details;
    } finally {
      if (!success) {
        try {
          backupRepo.deleteDirectory(snapshotDirPath);
        } catch (Exception excDuringDelete) {
          log.warn("Failed to delete {} after snapshot creation failed due to: {}", snapshotDirPath, excDuringDelete);
        }
      }
    }
  }

  private void deleteOldBackups(int numberToKeep) throws IOException {
    String[] paths = backupRepo.listAll(baseSnapDirPath);
    List<OldBackupDirectory> dirs = new ArrayList<>();
    for (String f : paths) {
      if (backupRepo.getPathType(baseSnapDirPath.resolve(f)) == PathType.DIRECTORY) {
        OldBackupDirectory obd = new OldBackupDirectory(baseSnapDirPath, f);
        if (obd.getTimestamp().isPresent()) {
          dirs.add(obd);
        }
      }
    }
    if (numberToKeep > dirs.size() -1) {
      return;
    }
    Collections.sort(dirs);
    int i=1;
    for (OldBackupDirectory dir : dirs) {
      if (i++ > numberToKeep) {
        backupRepo.deleteDirectory(dir.getPath());
      }
    }
  }

  protected void deleteNamedSnapshot(ReplicationHandler replicationHandler) {
    log.info("Deleting snapshot: {}", snapshotName);

    NamedList<Object> details = new NamedList<>();
    details.add("snapshotName", snapshotName);
      
    try {
      URI path = baseSnapDirPath.resolve("snapshot." + snapshotName);
      backupRepo.deleteDirectory(path);

      details.add("status", "success");
      details.add("snapshotDeletedAt", new Date().toString());

    } catch (IOException e) {
      details.add("status", "Unable to delete snapshot: " + snapshotName);
      log.warn("Unable to delete snapshot: {}", snapshotName, e);
    }

    replicationHandler.snapShootDetails = details;
  }

  public static final String DATE_FMT = "yyyyMMddHHmmssSSS";

}

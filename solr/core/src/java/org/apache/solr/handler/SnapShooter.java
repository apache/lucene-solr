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
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
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
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
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
    if (snapshotName != null) {
      directoryName = "snapshot." + snapshotName;
    } else {
      SimpleDateFormat fmt = new SimpleDateFormat(DATE_FMT, Locale.ROOT);
      directoryName = "snapshot." + fmt.format(new Date());
    }
    this.snapshotDirPath = backupRepo.resolve(location, directoryName);
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
            && backupRepo.getPathType(baseSnapDirPath.resolve(path)) == PathType.DIRECTORY) {
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

  public NamedList createSnapshot() throws Exception {
    RefCounted<SolrIndexSearcher> searcher = solrCore.getSearcher();
    try {
      if (commitName != null) {
        SolrSnapshotMetaDataManager snapshotMgr = solrCore.getSnapshotMetaDataManager();
        Optional<IndexCommit> commit = snapshotMgr.getIndexCommitByName(commitName);
        if(commit.isPresent()) {
          return createSnapshot(commit.get());
        }
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to find an index commit with name " + commitName +
            " for core " + solrCore.getName());
      } else {
        //TODO should we try solrCore.getDeletionPolicy().getLatestCommit() first?
        IndexDeletionPolicyWrapper deletionPolicy = solrCore.getDeletionPolicy();
        IndexCommit indexCommit = searcher.get().getIndexReader().getIndexCommit();
        deletionPolicy.saveCommitPoint(indexCommit.getGeneration());
        try {
          return createSnapshot(indexCommit);
        } finally {
          deletionPolicy.releaseCommitPoint(indexCommit.getGeneration());
        }
      }
    } finally {
      searcher.decref();
    }
  }

  public void createSnapAsync(final IndexCommit indexCommit, final int numberToKeep, Consumer<NamedList> result) {
    solrCore.getDeletionPolicy().saveCommitPoint(indexCommit.getGeneration());

    //TODO should use Solr's ExecutorUtil
    new Thread(() -> {
      try {
        result.accept(createSnapshot(indexCommit));
      } catch (Exception e) {
        LOG.error("Exception while creating snapshot", e);
        NamedList snapShootDetails = new NamedList<>();
        snapShootDetails.add("snapShootException", e.getMessage());
        result.accept(snapShootDetails);
      } finally {
        solrCore.getDeletionPolicy().releaseCommitPoint(indexCommit.getGeneration());
      }
      if (snapshotName == null) {
        try {
          deleteOldBackups(numberToKeep);
        } catch (IOException e) {
          LOG.warn("Unable to delete old snapshots ", e);
        }
      }
    }).start();

  }

  // note: remember to reserve the indexCommit first so it won't get deleted concurrently
  protected NamedList createSnapshot(final IndexCommit indexCommit) throws Exception {
    LOG.info("Creating backup snapshot " + (snapshotName == null ? "<not named>" : snapshotName) + " at " + baseSnapDirPath);
    boolean success = false;
    try {
      NamedList<Object> details = new NamedList<>();
      details.add("startTime", new Date().toString());//bad; should be Instant.now().toString()

      Collection<String> files = indexCommit.getFileNames();
      Directory dir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
      try {
        for(String fileName : files) {
          backupRepo.copyFileFrom(dir, fileName, snapshotDirPath);
        }
      } finally {
        solrCore.getDirectoryFactory().release(dir);
      }

      details.add("fileCount", files.size());
      details.add("status", "success");
      details.add("snapshotCompletedAt", new Date().toString());//bad; should be Instant.now().toString()
      details.add("snapshotName", snapshotName);
      LOG.info("Done creating backup snapshot: " + (snapshotName == null ? "<not named>" : snapshotName) +
          " at " + baseSnapDirPath);
      success = true;
      return details;
    } finally {
      if (!success) {
        backupRepo.deleteDirectory(snapshotDirPath);
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
    LOG.info("Deleting snapshot: " + snapshotName);

    NamedList<Object> details = new NamedList<>();

    try {
      URI path = baseSnapDirPath.resolve("snapshot." + snapshotName);
      backupRepo.deleteDirectory(path);

      details.add("status", "success");
      details.add("snapshotDeletedAt", new Date().toString());

    } catch (IOException e) {
      details.add("status", "Unable to delete snapshot: " + snapshotName);
      LOG.warn("Unable to delete snapshot: " + snapshotName, e);
    }

    replicationHandler.snapShootDetails = details;
  }

  public static final String DATE_FMT = "yyyyMMddHHmmssSSS";

}

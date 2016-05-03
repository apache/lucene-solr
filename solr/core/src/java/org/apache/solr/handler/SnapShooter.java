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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
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
  private String snapDir = null;
  private SolrCore solrCore;
  private String snapshotName = null;
  private String directoryName = null;
  private File snapShotDir = null;
  //TODO update to NIO Path API

  public SnapShooter(SolrCore core, String location, String snapshotName) {
    solrCore = core;
    if (location == null) {
      snapDir = core.getDataDir();
    }
    else  {
      snapDir = core.getCoreDescriptor().getInstanceDir().resolve(location).normalize().toString();
    }
    this.snapshotName = snapshotName;

    if(snapshotName != null) {
      directoryName = "snapshot." + snapshotName;
    } else {
      SimpleDateFormat fmt = new SimpleDateFormat(DATE_FMT, Locale.ROOT);
      directoryName = "snapshot." + fmt.format(new Date());
    }
  }

  /** Gets the parent directory of the snapshots.  This is the {@code location} given in the constructor after
   * being resolved against the core instance dir. */
  public Path getLocation() {
    return Paths.get(snapDir);
  }

  public void validateDeleteSnapshot() {
    boolean dirFound = false;
    File[] files = new File(snapDir).listFiles();
    for(File f : files) {
      if (f.getName().equals("snapshot." + snapshotName)) {
        dirFound = true;
        break;
      }
    }
    if(dirFound == false) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Snapshot cannot be found in directory: " + snapDir);
    }
  }

  protected void deleteSnapAsync(final ReplicationHandler replicationHandler) {
    new Thread() {
      @Override
      public void run() {
        deleteNamedSnapshot(replicationHandler);
      }
    }.start();
  }

  public void validateCreateSnapshot() throws IOException {
    snapShotDir = new File(snapDir, directoryName);
    if (snapShotDir.exists()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Snapshot directory already exists: " + snapShotDir.getAbsolutePath());
    }
    if (!snapShotDir.mkdirs()) { // note: TODO reconsider mkdirs vs mkdir
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Unable to create snapshot directory: " + snapShotDir.getAbsolutePath());
    }
  }

  public NamedList createSnapshot() throws Exception {
    IndexDeletionPolicyWrapper deletionPolicy = solrCore.getDeletionPolicy();
    RefCounted<SolrIndexSearcher> searcher = solrCore.getSearcher();
    try {
      //TODO should we try solrCore.getDeletionPolicy().getLatestCommit() first?
      IndexCommit indexCommit = searcher.get().getIndexReader().getIndexCommit();
      deletionPolicy.saveCommitPoint(indexCommit.getGeneration());
      try {
        return createSnapshot(indexCommit);
      } finally {
        deletionPolicy.releaseCommitPoint(indexCommit.getGeneration());
      }
    } finally {
      searcher.decref();
    }
  }

  public void createSnapAsync(final IndexCommit indexCommit, final int numberToKeep, Consumer<NamedList> result) {
    solrCore.getDeletionPolicy().saveCommitPoint(indexCommit.getGeneration());

    new Thread() { //TODO should use Solr's ExecutorUtil
      @Override
      public void run() {
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
          deleteOldBackups(numberToKeep);
        }
      }
    }.start();
  }

  // note: remember to reserve the indexCommit first so it won't get deleted concurrently
  protected NamedList createSnapshot(final IndexCommit indexCommit) throws Exception {
    LOG.info("Creating backup snapshot " + (snapshotName == null ? "<not named>" : snapshotName) + " at " + snapDir);
    boolean success = false;
    try {
      NamedList<Object> details = new NamedList<>();
      details.add("startTime", new Date().toString());//bad; should be Instant.now().toString()

      Collection<String> files = indexCommit.getFileNames();
      Directory dir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
      try {
        copyFiles(dir, files, snapShotDir);
      } finally {
        solrCore.getDirectoryFactory().release(dir);
      }

      details.add("fileCount", files.size());
      details.add("status", "success");
      details.add("snapshotCompletedAt", new Date().toString());//bad; should be Instant.now().toString()
      details.add("snapshotName", snapshotName);
      LOG.info("Done creating backup snapshot: " + (snapshotName == null ? "<not named>" : snapshotName) +
          " at " + snapDir);
      success = true;
      return details;
    } finally {
      if (!success) {
        IndexFetcher.delTree(snapShotDir);
      }
    }
  }

  private void deleteOldBackups(int numberToKeep) {
    File[] files = new File(snapDir).listFiles();
    List<OldBackupDirectory> dirs = new ArrayList<>();
    for (File f : files) {
      OldBackupDirectory obd = new OldBackupDirectory(f);
      if(obd.dir != null) {
        dirs.add(obd);
      }
    }
    if (numberToKeep > dirs.size() -1) {
      return;
    }

    Collections.sort(dirs);
    int i=1;
    for (OldBackupDirectory dir : dirs) {
      if (i++ > numberToKeep) {
        IndexFetcher.delTree(dir.dir);
      }
    }
  }

  protected void deleteNamedSnapshot(ReplicationHandler replicationHandler) {
    LOG.info("Deleting snapshot: " + snapshotName);

    NamedList<Object> details = new NamedList<>();
    boolean isSuccess;
    File f = new File(snapDir, "snapshot." + snapshotName);
    isSuccess = IndexFetcher.delTree(f);

    if(isSuccess) {
      details.add("status", "success");
      details.add("snapshotDeletedAt", new Date().toString());
    } else {
      details.add("status", "Unable to delete snapshot: " + snapshotName);
      LOG.warn("Unable to delete snapshot: " + snapshotName);
    }
    replicationHandler.snapShootDetails = details;
  }

  public static final String DATE_FMT = "yyyyMMddHHmmssSSS";


  private static void copyFiles(Directory sourceDir, Collection<String> files, File destDir) throws IOException {
    try (FSDirectory dir = new SimpleFSDirectory(destDir.toPath(), NoLockFactory.INSTANCE)) {
      for (String indexFile : files) {
        dir.copyFrom(sourceDir, indexFile, indexFile, DirectoryFactory.IOCONTEXT_NO_CACHE);
      }
    }
  }

}

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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
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
  private static final Logger LOG = LoggerFactory.getLogger(SnapShooter.class.getName());
  private String snapDir = null;
  private SolrCore solrCore;
  private String snapshotName = null;
  private String directoryName = null;
  private File snapShotDir = null;

  public SnapShooter(SolrCore core, String location, String snapshotName) {
    solrCore = core;
    if (location == null) {
      snapDir = core.getDataDir();
    }
    else  {
      File base = new File(core.getCoreDescriptor().getInstanceDir());
      snapDir = org.apache.solr.util.FileUtils.resolvePath(base, location).getAbsolutePath();
    }
    this.snapshotName = snapshotName;

    if(snapshotName != null) {
      directoryName = "snapshot." + snapshotName;
    } else {
      SimpleDateFormat fmt = new SimpleDateFormat(DATE_FMT, Locale.ROOT);
      directoryName = "snapshot." + fmt.format(new Date());
    }
  }

  void createSnapAsync(final IndexCommit indexCommit, final int numberToKeep, final ReplicationHandler replicationHandler) {
    replicationHandler.core.getDeletionPolicy().saveCommitPoint(indexCommit.getGeneration());

    new Thread() {
      @Override
      public void run() {
        if(snapshotName != null) {
          createSnapshot(indexCommit, replicationHandler);
        } else {
          createSnapshot(indexCommit, replicationHandler);
          deleteOldBackups(numberToKeep);
        }
      }
    }.start();
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

  void validateCreateSnapshot() throws IOException {
    snapShotDir = new File(snapDir, directoryName);
    if (snapShotDir.exists()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Snapshot directory already exists: " + snapShotDir.getAbsolutePath());
    }
    if (!snapShotDir.mkdirs()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Unable to create snapshot directory: " + snapShotDir.getAbsolutePath());
    }
  }

  void createSnapshot(final IndexCommit indexCommit, ReplicationHandler replicationHandler) {
    LOG.info("Creating backup snapshot " + (snapshotName == null ? "<not named>" : snapshotName) + " at " + snapDir);
    NamedList<Object> details = new NamedList<>();
    details.add("startTime", new Date().toString());
    try {
      Collection<String> files = indexCommit.getFileNames();

      Directory dir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
      try {
        copyFiles(dir, files, snapShotDir);
      } finally {
        solrCore.getDirectoryFactory().release(dir);
      }

      details.add("fileCount", files.size());
      details.add("status", "success");
      details.add("snapshotCompletedAt", new Date().toString());
      details.add("snapshotName", snapshotName);
      LOG.info("Done creating backup snapshot: " + (snapshotName == null ? "<not named>" : snapshotName) +
          " at " + snapDir);
    } catch (Exception e) {
      IndexFetcher.delTree(snapShotDir);
      LOG.error("Exception while creating snapshot", e);
      details.add("snapShootException", e.getMessage());
    } finally {
      replicationHandler.core.getDeletionPolicy().releaseCommitPoint(indexCommit.getGeneration());
      replicationHandler.snapShootDetails = details;
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

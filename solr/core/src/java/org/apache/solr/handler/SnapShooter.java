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
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p/> Provides functionality equivalent to the snapshooter script </p>
 * This is no longer used in standard replication.
 *
 *
 * @since solr 1.4
 */
public class SnapShooter {
  private static final Logger LOG = LoggerFactory.getLogger(SnapShooter.class.getName());
  private String snapDir = null;
  private SolrCore solrCore;
  private SimpleFSLockFactory lockFactory;
  private String snapshotName = null;
  private String directoryName = null;
  private File snapShotDir = null;
  private Lock lock = null;

  public SnapShooter(SolrCore core, String location, String snapshotName) {
    solrCore = core;
    if (location == null) snapDir = core.getDataDir();
    else  {
      File base = new File(core.getCoreDescriptor().getRawInstanceDir());
      snapDir = org.apache.solr.util.FileUtils.resolvePath(base, location).getAbsolutePath();
      File dir = new File(snapDir);
      if (!dir.exists())  dir.mkdirs();
    }
    lockFactory = new SimpleFSLockFactory(snapDir);
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
          deleteOldBackups(numberToKeep);
          createSnapshot(indexCommit, replicationHandler);
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

    Lock lock = lockFactory.makeLock(directoryName + ".lock");
    if (lock.isLocked()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Unable to acquire lock for snapshot directory: " + snapShotDir.getAbsolutePath());
    }
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
    LOG.info("Creating backup snapshot...");
    NamedList<Object> details = new NamedList<>();
    details.add("startTime", new Date().toString());
    String directoryName = null;

    try {
      Collection<String> files = indexCommit.getFileNames();
      FileCopier fileCopier = new FileCopier();

      Directory dir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
      try {
        fileCopier.copyFiles(dir, files, snapShotDir);
      } finally {
        solrCore.getDirectoryFactory().release(dir);
      }

      details.add("fileCount", files.size());
      details.add("status", "success");
      details.add("snapshotCompletedAt", new Date().toString());
    } catch (Exception e) {
      SnapPuller.delTree(snapShotDir);
      LOG.error("Exception while creating snapshot", e);
      details.add("snapShootException", e.getMessage());
    } finally {
      replicationHandler.core.getDeletionPolicy().releaseCommitPoint(indexCommit.getGeneration());
      replicationHandler.snapShootDetails = details;
      if (lock != null) {
        try {
          lock.close();
        } catch (IOException e) {
          LOG.error("Unable to release snapshoot lock: " + directoryName + ".lock");
        }
      }
    }
  }

  private void deleteOldBackups(int numberToKeep) {
    File[] files = new File(snapDir).listFiles();
    List<OldBackupDirectory> dirs = new ArrayList<>();
    for(File f : files) {
      OldBackupDirectory obd = new OldBackupDirectory(f);
      if(obd.dir != null) {
        dirs.add(obd);
      }
    }
    if(numberToKeep > dirs.size()) {
      return;
    }

    Collections.sort(dirs);
    int i=1;
    for(OldBackupDirectory dir : dirs) {
      if( i++ > numberToKeep-1 ) {
        SnapPuller.delTree(dir.dir);
      }
    }   
  }

  protected void deleteNamedSnapshot(ReplicationHandler replicationHandler) {
    LOG.info("Deleting snapshot: " + snapshotName);

    NamedList<Object> details = new NamedList<>();
    boolean isSuccess = false;
    File f = new File(snapDir, "snapshot." + snapshotName);
    isSuccess = SnapPuller.delTree(f);

    if(isSuccess) {
      details.add("status", "success");
    } else {
      details.add("status", "Unable to delete snapshot: " + snapshotName);
      LOG.warn("Unable to delete snapshot: " + snapshotName);
    }
    replicationHandler.snapShootDetails = details;
  }

  private class OldBackupDirectory implements Comparable<OldBackupDirectory>{
    File dir;
    Date timestamp;
    final Pattern dirNamePattern = Pattern.compile("^snapshot[.](.*)$");
    
    OldBackupDirectory(File dir) {
      if(dir.isDirectory()) {
        Matcher m = dirNamePattern.matcher(dir.getName());
        if(m.find()) {
          try {
            this.dir = dir;
            this.timestamp = new SimpleDateFormat(DATE_FMT, Locale.ROOT).parse(m.group(1));
          } catch(Exception e) {
            this.dir = null;
            this.timestamp = null;
          }
        }
      }
    }
    @Override
    public int compareTo(OldBackupDirectory that) {
      return that.timestamp.compareTo(this.timestamp);
    }
  }

  public static final String SNAP_DIR = "snapDir";
  public static final String DATE_FMT = "yyyyMMddHHmmssSSS";
  

  private class FileCopier {
    
    public void copyFiles(Directory sourceDir, Collection<String> files,
        File destDir) throws IOException {
      // does destinations directory exist ?
      if (destDir != null && !destDir.exists()) {
        destDir.mkdirs();
      }
      
      FSDirectory dir = FSDirectory.open(destDir);
      try {
        for (String indexFile : files) {
          copyFile(sourceDir, indexFile, new File(destDir, indexFile), dir);
        }
      } finally {
        dir.close();
      }
    }
    
    public void copyFile(Directory sourceDir, String indexFile, File destination, Directory destDir)
      throws IOException {

      // make sure we can write to destination
      if (destination.exists() && !destination.canWrite()) {
        String message = "Unable to open file " + destination + " for writing.";
        throw new IOException(message);
      }

      sourceDir.copy(destDir, indexFile, indexFile, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }
  }
  

}

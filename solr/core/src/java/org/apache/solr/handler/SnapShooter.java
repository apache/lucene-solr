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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p/> Provides functionality equivalent to the snapshooter script </p>
 *
 *
 * @since solr 1.4
 */
public class SnapShooter {
  private static final Logger LOG = LoggerFactory.getLogger(SnapShooter.class.getName());
  private String snapDir = null;
  private SolrCore solrCore;
  private SimpleFSLockFactory lockFactory;
  
  public SnapShooter(SolrCore core, String location) throws IOException {
    solrCore = core;
    if (location == null) snapDir = core.getDataDir();
    else  {
      File base = new File(core.getCoreDescriptor().getInstanceDir());
      snapDir = org.apache.solr.common.util.FileUtils.resolvePath(base, location).getAbsolutePath();
      File dir = new File(snapDir);
      if (!dir.exists())  dir.mkdirs();
    }
    lockFactory = new SimpleFSLockFactory(snapDir);
  }
  
  void createSnapAsync(final IndexCommit indexCommit, final ReplicationHandler replicationHandler) {
    createSnapAsync(indexCommit, Integer.MAX_VALUE, replicationHandler);
  }

  void createSnapAsync(final IndexCommit indexCommit, final int numberToKeep, final ReplicationHandler replicationHandler) {
    replicationHandler.core.getDeletionPolicy().saveCommitPoint(indexCommit.getVersion());

    new Thread() {
      @Override
      public void run() {
        createSnapshot(indexCommit, numberToKeep, replicationHandler);
      }
    }.start();
  }

  void createSnapshot(final IndexCommit indexCommit, int numberToKeep, ReplicationHandler replicationHandler) {

    NamedList<Object> details = new NamedList<Object>();
    details.add("startTime", new Date().toString());
    File snapShotDir = null;
    String directoryName = null;
    Lock lock = null;
    try {
      if(numberToKeep<Integer.MAX_VALUE) {
        deleteOldBackups(numberToKeep);
      }
      SimpleDateFormat fmt = new SimpleDateFormat(DATE_FMT, Locale.US);
      directoryName = "snapshot." + fmt.format(new Date());
      lock = lockFactory.makeLock(directoryName + ".lock");
      if (lock.isLocked()) return;
      snapShotDir = new File(snapDir, directoryName);
      if (!snapShotDir.mkdir()) {
        LOG.warn("Unable to create snapshot directory: " + snapShotDir.getAbsolutePath());
        return;
      }
      Collection<String> files = indexCommit.getFileNames();
      FileCopier fileCopier = new FileCopier(solrCore.getDeletionPolicy(), indexCommit);
      fileCopier.copyFiles(files, snapShotDir);

      details.add("fileCount", files.size());
      details.add("status", "success");
      details.add("snapshotCompletedAt", new Date().toString());
    } catch (Exception e) {
      SnapPuller.delTree(snapShotDir);
      LOG.error("Exception while creating snapshot", e);
      details.add("snapShootException", e.getMessage());
    } finally {
      replicationHandler.core.getDeletionPolicy().releaseCommitPoint(indexCommit.getVersion());   
      replicationHandler.snapShootDetails = details;
      if (lock != null) {
        try {
          lock.release();
        } catch (IOException e) {
          LOG.error("Unable to release snapshoot lock: " + directoryName + ".lock");
        }
      }
    }
  }
  private void deleteOldBackups(int numberToKeep) {
    File[] files = new File(snapDir).listFiles();
    List<OldBackupDirectory> dirs = new ArrayList<OldBackupDirectory>();
    for(File f : files) {
      OldBackupDirectory obd = new OldBackupDirectory(f);
      if(obd.dir != null) {
        dirs.add(obd);
      }
    }
    Collections.sort(dirs);
    int i=1;
    for(OldBackupDirectory dir : dirs) {
      if( i > numberToKeep-1 ) {
        SnapPuller.delTree(dir.dir);
      }
    }   
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
            this.timestamp = new SimpleDateFormat(DATE_FMT).parse(m.group(1));
          } catch(Exception e) {
            this.dir = null;
            this.timestamp = null;
          }
        }
      }
    }
    public int compareTo(OldBackupDirectory that) {
      return that.timestamp.compareTo(this.timestamp);
    }
  }

  public static final String SNAP_DIR = "snapDir";
  public static final String DATE_FMT = "yyyyMMddHHmmss";
  

  private class FileCopier {
    private static final int DEFAULT_BUFFER_SIZE = 32768;
    private byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    private IndexCommit indexCommit;
    private IndexDeletionPolicyWrapper delPolicy;

    public FileCopier(IndexDeletionPolicyWrapper delPolicy, IndexCommit commit) {
      this.delPolicy = delPolicy;
      this.indexCommit = commit;
    }
    
    public void copyFiles(Collection<String> files, File destDir) throws IOException {
      for (String indexFile : files) {
        File source = new File(solrCore.getIndexDir(), indexFile);
        copyFile(source, new File(destDir, source.getName()), true);
      }
    }
    
    public void copyFile(File source, File destination, boolean preserveFileDate)
      throws IOException {
      // check source exists
      if (!source.exists()) {
        String message = "File " + source + " does not exist";
        throw new FileNotFoundException(message);
      }

      // does destinations directory exist ?
      if (destination.getParentFile() != null
          && !destination.getParentFile().exists()) {
        destination.getParentFile().mkdirs();
      }

      // make sure we can write to destination
      if (destination.exists() && !destination.canWrite()) {
        String message = "Unable to open file " + destination + " for writing.";
        throw new IOException(message);
      }

      FileInputStream input = null;
      FileOutputStream output = null;
      try {
        input = new FileInputStream(source);
        output = new FileOutputStream(destination);
 
        int count = 0;
        int n = 0;
        int rcnt = 0;
        while (-1 != (n = input.read(buffer))) {
          output.write(buffer, 0, n);
          count += n;
          rcnt++;
          /***
          // reserve every 4.6875 MB
          if (rcnt == 150) {
            rcnt = 0;
            delPolicy.setReserveDuration(indexCommit.getVersion(), reserveTime);
          }
           ***/
        }
      } finally {
        try {
          IOUtils.closeQuietly(input);
        } finally {
          IOUtils.closeQuietly(output);
        }
      }

      if (source.length() != destination.length()) {
        String message = "Failed to copy full contents from " + source + " to "
          + destination;
        throw new IOException(message);
      }

      if (preserveFileDate) {
        // file copy should preserve file date
        destination.setLastModified(source.lastModified());
      }
    }
  }
  

}

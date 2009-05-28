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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.ArrayList;

/**
 * <p/> Provides functionality equivalent to the snapshooter script </p>
 *
 * @version $Id$
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

  void createSnapAsync(final Collection<String> files, final ReplicationHandler replicationHandler) {
    new Thread() {
      public void run() {
        createSnapshot(files, replicationHandler);
      }
    }.start();
  }

  void createSnapshot(Collection<String> files, ReplicationHandler replicationHandler) {
    NamedList details = new NamedList();
    details.add("startTime", new Date().toString());
    File snapShotDir = null;
    String directoryName = null;
    Lock lock = null;
    try {
      SimpleDateFormat fmt = new SimpleDateFormat(DATE_FMT);
      directoryName = "snapshot." + fmt.format(new Date());
      lock = lockFactory.makeLock(directoryName + ".lock");
      if (lock.isLocked()) return;
      snapShotDir = new File(snapDir, directoryName);
      if (!snapShotDir.mkdir()) {
        LOG.warn("Unable to create snapshot directory: " + snapShotDir.getAbsolutePath());
        return;
      }
      for (String indexFile : files) {
        FileUtils.copyFileToDirectory(new File(solrCore.getIndexDir(), indexFile), snapShotDir, true);
      }
      details.add("fileCount", files.size());
      details.add("status", "success");
      details.add("snapshotCompletedAt", new Date().toString());
    } catch (Exception e) {
      SnapPuller.delTree(snapShotDir);
      LOG.error("Exception while creating snapshot", e);
      details.add("snapShootException", e.getMessage());
    } finally {
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

  public static final String SNAP_DIR = "snapDir";
  public static final String DATE_FMT = "yyyyMMddhhmmss";
}

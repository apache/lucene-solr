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

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.WeakHashMap;

/**
 * <p/>
 * Provides functionality equivalent to the snapshooter script
 * </p>
 *
 * @version $Id$
 * @since solr 1.4
 */
public class SnapShooter {
  private String snapDir = null;
  private SolrCore solrCore;

  public SnapShooter(SolrCore core) {
    solrCore = core;
  }

  void createSnapAsync(final Collection<String> files) {
    new Thread() {
      public void run() {
        createSnapshot(files);
      }
    }.start();
  }

  void createSnapshot(Collection<String> files) {
    File lockFile = null;
    File snapShotDir = null;
    String directoryName = null;
    try {
      lockFile = new File(snapDir, directoryName + ".lock");
      if (lockFile.exists()) {
        return;
      }
      SimpleDateFormat fmt = new SimpleDateFormat(DATE_FMT);
      directoryName = "snapshot." + fmt.format(new Date());
      snapShotDir = new File(snapDir, directoryName);
      lockFile.createNewFile();
      snapShotDir.mkdir();
      for (String indexFile : files) {
        copyFile2Dir(new File(solrCore.getIndexDir(), indexFile), snapShotDir);
      }
    } catch (Exception e) {
      SnapPuller.delTree(snapShotDir);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      if (lockFile != null) {
        lockFile.delete();
      }
    }
  }

  static void copyFile2Dir(File file, File toDir) throws IOException {
    FileInputStream fis = null;
    FileOutputStream fos = null;
    try {
      fis = new FileInputStream(file);
      File destFile = new File(toDir, file.getName());
      fos = new FileOutputStream(destFile);
      fis.getChannel().transferTo(0, fis.available(), fos.getChannel());
      destFile.setLastModified(file.lastModified());
    } finally {
      ReplicationHandler.closeNoExp(fis);
      ReplicationHandler.closeNoExp(fos);
    }
  }

  public static final String SNAP_DIR = "snapDir";
  public static final String DATE_FMT = "yyyyMMddhhmmss";
  private static WeakHashMap<SolrCore, SnapShooter> SNAP_DIRS = new WeakHashMap<SolrCore, SnapShooter>();
}

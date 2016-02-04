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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestoreCore implements Callable<Boolean> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String backupName;
  private final String backupLocation;
  private final SolrCore core;

  public RestoreCore(SolrCore core, String location, String name) {
    this.core = core;
    this.backupLocation = location;
    this.backupName = name;
  }

  @Override
  public Boolean call() throws Exception {
    return doRestore();
  }

  private boolean doRestore() throws Exception {

    Path backupPath = Paths.get(backupLocation).resolve(backupName);
    String restoreIndexName = "restore." + backupName;
    String restoreIndexPath = core.getDataDir() + restoreIndexName;

    Directory restoreIndexDir = null;
    Directory indexDir = null;
    try (Directory backupDir = FSDirectory.open(backupPath)) {

      final Version version = IndexFetcher.checkOldestVersion(SegmentInfos.readLatestCommit(backupDir));

      restoreIndexDir = core.getDirectoryFactory().get(restoreIndexPath,
          DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);

      //Prefer local copy.
      indexDir = core.getDirectoryFactory().get(core.getIndexDir(),
          DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);

      //Move all files from backupDir to restoreIndexDir
      for (String filename : backupDir.listAll()) {
        checkInterrupted();
        log.info("Copying file {} to restore directory ", filename);
        try (IndexInput indexInput = backupDir.openInput(filename, IOContext.READONCE)) {
          Long checksum = null;
          try {
            checksum = CodecUtil.retrieveChecksum(indexInput);
          } catch (Exception e) {
            log.warn("Could not read checksum from index file: " + filename, e);
          }
          long length = indexInput.length();
          IndexFetcher.CompareResult compareResult = IndexFetcher.compareFile(indexDir, version, filename, length, checksum);
          if (!compareResult.equal || (!compareResult.checkSummed && (filename.endsWith(".si")
              || filename.endsWith(".liv") || filename.startsWith("segments_")))) {
            restoreIndexDir.copyFrom(backupDir, filename, filename, IOContext.READONCE);
          } else {
            //prefer local copy
            restoreIndexDir.copyFrom(indexDir, filename, filename, IOContext.READONCE);
          }
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Exception while restoring the backup index", e);
        }
      }
      log.debug("Switching directories");
      IndexFetcher.modifyIndexProps(core, restoreIndexName);

      boolean success;
      try {
        core.getUpdateHandler().newIndexWriter(false);
        openNewSearcher();
        success = true;
        log.info("Successfully restored to the backup index");
      } catch (Exception e) {
        //Rollback to the old index directory. Delete the restore index directory and mark the restore as failed.
        log.warn("Could not switch to restored index. Rolling back to the current index");
        Directory dir = null;
        try {
          dir = core.getDirectoryFactory().get(core.getDataDir(), DirectoryFactory.DirContext.META_DATA,
              core.getSolrConfig().indexConfig.lockType);
          dir.deleteFile(IndexFetcher.INDEX_PROPERTIES);
        } finally {
          if (dir != null) {
            core.getDirectoryFactory().release(dir);
          }
        }

        core.getDirectoryFactory().doneWithDirectory(restoreIndexDir);
        core.getDirectoryFactory().remove(restoreIndexDir);
        core.getUpdateHandler().newIndexWriter(false);
        openNewSearcher();
        throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Exception while restoring the backup index", e);
      }
      if (success) {
        core.getDirectoryFactory().doneWithDirectory(indexDir);
        core.getDirectoryFactory().remove(indexDir);
      }

      return true;
    } finally {
      if (restoreIndexDir != null) {
        core.getDirectoryFactory().release(restoreIndexDir);
      }
      if (indexDir != null) {
        core.getDirectoryFactory().release(indexDir);
      }
    }
  }

  private void checkInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("Stopping restore process. Thread was interrupted.");
    }
  }

  private void openNewSearcher() throws Exception {
    Future[] waitSearcher = new Future[1];
    core.getSearcher(true, false, waitSearcher, true);
    if (waitSearcher[0] != null) {
      waitSearcher[0].get();
    }
  }
}

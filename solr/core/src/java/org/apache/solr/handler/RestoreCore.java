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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestoreCore implements Callable<Boolean> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCore core;
  private RestoreRepository repository;

  private RestoreCore(SolrCore core, RestoreRepository repository) {
    this.core = core;
    this.repository = repository;
  }

  public static RestoreCore create(BackupRepository backupRepo, SolrCore core, URI location, String backupname) {
    RestoreRepository repository = new BasicRestoreRepository(backupRepo.resolveDirectory(location, backupname), backupRepo);
    return new RestoreCore(core, repository);
  }

  public static RestoreCore createWithMetaFile(BackupRepository repo, SolrCore core, URI location, ShardBackupId shardBackupId) throws IOException {
    BackupFilePaths incBackupFiles = new BackupFilePaths(repo, location);
    URI shardBackupMetadataDir = incBackupFiles.getShardBackupMetadataDir();
    ShardBackupIdRestoreRepository resolver = new ShardBackupIdRestoreRepository(location, incBackupFiles.getIndexDir(),
            repo, ShardBackupMetadata.from(repo, shardBackupMetadataDir, shardBackupId));
    return new RestoreCore(core, resolver);
  }

  @Override
  public Boolean call() throws Exception {
    return doRestore();
  }

  public boolean doRestore() throws Exception {
    SimpleDateFormat dateFormat = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT);
    String restoreIndexName = "restore." + dateFormat.format(new Date());
    String restoreIndexPath = core.getDataDir() + restoreIndexName;

    String indexDirPath = core.getIndexDir();
    Directory restoreIndexDir = null;
    Directory indexDir = null;
    try {

      restoreIndexDir = core.getDirectoryFactory().get(restoreIndexPath,
              DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);

      //Prefer local copy.
      indexDir = core.getDirectoryFactory().get(indexDirPath,
              DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
      Set<String> indexDirFiles = new HashSet<>(Arrays.asList(indexDir.listAll()));
      //Move all files from backupDir to restoreIndexDir
      for (String filename : repository.listAllFiles()) {
        checkInterrupted();
        try {
          if (indexDirFiles.contains(filename)) {
            Checksum cs = repository.checksum(filename);
            IndexFetcher.CompareResult compareResult;
            if (cs == null) {
              compareResult = new IndexFetcher.CompareResult();
              compareResult.equal = false;
            } else {
              compareResult = IndexFetcher.compareFile(indexDir, filename, cs.size, cs.checksum);
            }
            if (!compareResult.equal ||
                    (IndexFetcher.filesToAlwaysDownloadIfNoChecksums(filename, cs.size, compareResult))) {
              repository.repoCopy(filename, restoreIndexDir);
            } else {
              //prefer local copy
              repository.localCopy(indexDir, filename, restoreIndexDir);
            }
          } else {
            repository.repoCopy(filename, restoreIndexDir);
          }
        } catch (Exception e) {
          log.warn("Exception while restoring the backup index ", e);
          throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Exception while restoring the backup index", e);
        }
      }
      log.debug("Switching directories");
      core.modifyIndexProps(restoreIndexName);

      boolean success;
      try {
        core.getUpdateHandler().newIndexWriter(false);
        openNewSearcher();
        success = true;
        log.info("Successfully restored to the backup index");
      } catch (Exception e) {
        //Rollback to the old index directory. Delete the restore index directory and mark the restore as failed.
        log.warn("Could not switch to restored index. Rolling back to the current index", e);
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
        // Cleanup all index files not associated with any *named* snapshot.
        core.deleteNonSnapshotIndexFiles(indexDirPath);
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
    @SuppressWarnings({"rawtypes"})
    Future[] waitSearcher = new Future[1];
    core.getSearcher(true, false, waitSearcher, true);
    if (waitSearcher[0] != null) {
      waitSearcher[0].get();
    }
  }

  /**
   * A minimal version of {@link BackupRepository} used for restoring
   */
  private interface RestoreRepository {
    String[] listAllFiles() throws IOException;
    IndexInput openInput(String filename) throws IOException;
    void repoCopy(String filename, Directory dest) throws IOException;
    void localCopy(Directory src, String filename, Directory dest) throws IOException;
    Checksum checksum(String filename) throws IOException;
  }

  /**
   * A basic {@link RestoreRepository} simply delegates all calls to {@link BackupRepository}
   */
  private static class BasicRestoreRepository implements RestoreRepository {
    protected final URI backupPath;
    protected final BackupRepository repository;

    public BasicRestoreRepository(URI backupPath, BackupRepository repository) {
      this.backupPath = backupPath;
      this.repository = repository;
    }

    public String[] listAllFiles() throws IOException {
      return repository.listAll(backupPath);
    }

    public IndexInput openInput(String filename) throws IOException {
      return repository.openInput(backupPath, filename, IOContext.READONCE);
    }

    public void repoCopy(String filename, Directory dest) throws IOException {
      repository.copyFileTo(backupPath, filename, dest);
    }

    public void localCopy(Directory src, String filename, Directory dest) throws IOException {
      dest.copyFrom(src, filename, filename, IOContext.READONCE);
    }

    public Checksum checksum(String filename) throws IOException {
      try (IndexInput indexInput = repository.openInput(backupPath, filename, IOContext.READONCE)) {
        try {
          long checksum = CodecUtil.retrieveChecksum(indexInput);
          long length = indexInput.length();
          return new Checksum(checksum, length);
        } catch (Exception e) {
          log.warn("Could not read checksum from index file: {}", filename, e);
        }
      }
      return null;
    }
  }

  /**
   * A {@link RestoreRepository} based on information stored in {@link ShardBackupMetadata}
   */
  private static class ShardBackupIdRestoreRepository implements RestoreRepository {

    private final ShardBackupMetadata shardBackupMetadata;
    private final URI indexURI;
    protected final URI backupPath;
    protected final BackupRepository repository;

    public ShardBackupIdRestoreRepository(URI backupPath, URI indexURI, BackupRepository repository, ShardBackupMetadata shardBackupMetadata) {
      this.shardBackupMetadata = shardBackupMetadata;
      this.indexURI = indexURI;
      this.backupPath = backupPath;
      this.repository = repository;
    }

    @Override
    public String[] listAllFiles() {
      return shardBackupMetadata.listOriginalFileNames().toArray(new String[0]);
    }

    @Override
    public IndexInput openInput(String filename) throws IOException {
      String storedFileName = getStoredFilename(filename);
      return repository.openInput(indexURI, storedFileName, IOContext.READONCE);
    }

    @Override
    public void repoCopy(String filename, Directory dest) throws IOException {
      String storedFileName = getStoredFilename(filename);
      repository.copyIndexFileTo(this.indexURI, storedFileName, dest, filename);
    }

    @Override
    public void localCopy(Directory src, String filename, Directory dest) throws IOException {
      dest.copyFrom(src, filename, filename, IOContext.READONCE);
    }

    public Checksum checksum(String filename) {
      Optional<ShardBackupMetadata.BackedFile> backedFile = shardBackupMetadata.getFile(filename);
      return backedFile.map(bf -> bf.fileChecksum).orElse(null);
    }

    private String getStoredFilename(String filename) {
      return shardBackupMetadata.getFile(filename).get().uniqueFileName;
    }
  }
}

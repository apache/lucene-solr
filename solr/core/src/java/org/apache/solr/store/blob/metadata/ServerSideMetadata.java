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

package org.apache.solr.store.blob.metadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableCollection.Builder;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object capturing the metadata of a shard index on a Solr node. 
 *
 * This works in conjunction with {@link BlobCoreMetadata} to find the differences between 
 * local (Solr node) and remote (Blob store) commit point for a core.<p>
 *
 * This object is somewhere between {@link org.apache.lucene.index.IndexCommit} and {@link org.apache.lucene.index.SegmentInfos}
 * and by implementing it separately we can add additional metadata to it as needed.
 */
public class ServerSideMetadata {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * When an instance of this class is created for pushing data to the Blob store, reserve the commit point for a short
   * while to give the caller time to save it while it works on it. Can't save it here directly as it would be awkward to
   * try to release it on all execution paths.
   */
  private static final long RESERVE_COMMIT_DURATION = TimeUnit.SECONDS.toMillis(1L);

  private static final int MAX_ATTEMPTS_TO_CAPTURE_COMMIT_POINT = 5;
  /**
   * Files composing the core. They are are referenced from the core's current commit point's segments_N file
   * which is ALSO included in this collection.
   */
  private final ImmutableCollection<CoreFileData> latestCommitFiles;

  /**
   * Names of all files (all of them, no exception) in the local index directory.
   * These files do not matter when pushing contents to blob but they do matter if blob content being pulled conflicts with them.
   */
  private final ImmutableSet<String> allFiles;

  /**
   * Hash of the directory content used to make sure the content doesn't change as we proceed to pull new files from Blob
   * (if we need to pull new files from Blob)
   */
  private final String directoryHash;

  /**
   * Generation number of the local index.
   * This generation number is only meant to identify a scenario where local index generation number is higher than
   * what we have in blob. In that scenario we would switch index to a new directory when pulling contents from blob. 
   * Because in the presence of higher generation number locally, blob contents cannot establish their legitimacy.
   * It is also used for saving (reserving) local commit while doing pushes.
   */
  private final long generation;
  private final SolrCore core;
  private final String coreName;
  private final CoreContainer container;

  /**
   * Given a core name, builds the local metadata
   *
   * @param reserveCommit if <code>true</code>, latest commit is reserved for a short while ({@link #RESERVE_COMMIT_DURATION})
   *                      to (reasonably) allow the caller to save it while it pushes files to Blob.
   *
   * @throws Exception if core corresponding to <code>coreName</code> can't be found.
   */
  public ServerSideMetadata(String coreName, CoreContainer coreContainer, boolean reserveCommit) throws Exception {
    this.coreName = coreName;
    this.container = coreContainer;
    this.core = coreContainer.getCore(coreName);

    if (core == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't find core " + coreName);
    }

    try {
      Directory coreDir = core.getDirectoryFactory().get(core.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
      try {

        ImmutableCollection.Builder<CoreFileData> latestCommitBuilder;
        IndexCommit latestCommit;
        int attempt = 1;
        // we don't have an atomic way of capturing a commit point i.e. there is a slight chance of losing files between 
        // getting a latest commit and reserving it. Therefore, we try to capture commit point in a loop with maximum 
        // number of attempts. 
        while (true) {
          try {
            // Work around possible bug returning same file multiple times by using a set here
            // See org.apache.solr.handler.ReplicationHandler.getFileList()
            latestCommitBuilder = new ImmutableSet.Builder<>();
            latestCommit = tryCapturingLatestCommit(coreDir, latestCommitBuilder, reserveCommit);
            break;
          } catch (FileNotFoundException | NoSuchFileException ex) {
            attempt++;
            if (attempt > MAX_ATTEMPTS_TO_CAPTURE_COMMIT_POINT) {
              throw ex;
            }
            log.info(String.format(Locale.ROOT, "Failed to capture commit point: core=%s attempt=%s reason=%s",
                coreName, attempt, ex.getMessage()));
          }
        }

        generation = latestCommit.getGeneration();
        latestCommitFiles = latestCommitBuilder.build();

        // Capture now the hash and verify again after files have been pulled and before the directory is updated (or before
        // the index is switched to use a new directory) to make sure there are no local changes at the same time that might
        // lead to a corruption in case of interaction with the download or might be a sign of other problems (it is not
        // expected that indexing can happen on a local directory of a SHARED replica if that replica is not up to date with
        // the Blob store version).
        directoryHash = getSolrDirectoryHash(coreDir);

        // Need to inventory all local files in case files that need to be pulled from Blob conflict with them.
        allFiles = ImmutableSet.copyOf(coreDir.listAll());
      } finally {
        core.getDirectoryFactory().release(coreDir);
      }
    } finally {
      core.close();
    }
  }

  private IndexCommit tryCapturingLatestCommit(Directory coreDir, Builder<CoreFileData> latestCommitBuilder, boolean reserveCommit) throws BlobException, IOException {
    IndexDeletionPolicyWrapper deletionPolicy = core.getDeletionPolicy();
    IndexCommit latestCommit = deletionPolicy.getLatestCommit();
    if (latestCommit == null) {
      throw new BlobException("Core " + core.getName() + " has no available commit point");
    }

    if (reserveCommit) {
      // Caller will save the commit point shortly. See CorePushPull.pushToBlobStore()
      deletionPolicy.setReserveDuration(latestCommit.getGeneration(), RESERVE_COMMIT_DURATION);
    }

    for (String fileName : latestCommit.getFileNames()) {
      // Note we add here all segment related files as well as the commit point's segments_N file
      // Commit points do not contain lock (write.lock) files.
      try (final IndexInput indexInput = coreDir.openInput(fileName, IOContext.READONCE)) {
        long length = indexInput.length();
        long checksum = CodecUtil.retrieveChecksum(indexInput);
        latestCommitBuilder.add(new CoreFileData(fileName, length, checksum));
      }
    }
    return latestCommit;
  }


  public String getCoreName() {
    return this.coreName;
  }

  public CoreContainer getCoreContainer() {
    return container;
  }

  public long getGeneration() {
    return this.generation;
  }

  public String getDirectoryHash() {
    return this.directoryHash;
  }

  public ImmutableCollection<CoreFileData> getLatestCommitFiles(){
    return this.latestCommitFiles;
  }

  public ImmutableSet<String> getAllFiles() {
    return this.allFiles;
  }

  /**
   * Returns <code>true</code> if the contents of the directory passed into this method is identical to the contents of
   * the directory of the Solr core of this instance, taken at instance creation time.<p>
   *
   * Passing in the Directory (expected to be the directory of the same core used during construction) because it seems
   * safer than trying to get it again here...
   */
  public boolean isSameDirectoryContent(Directory coreDir) throws NoSuchAlgorithmException, IOException {
    return directoryHash.equals(getSolrDirectoryHash(coreDir));
  }

  /**
   * Computes a hash of a Solr Directory in order to make sure the directory doesn't change as we pull content into it (if we need to
   * pull content into it)
   */
  private String getSolrDirectoryHash(Directory coreDir) throws NoSuchAlgorithmException, IOException {
    MessageDigest digest = MessageDigest.getInstance("sha1"); // not sure MD5 is available in Solr jars

    String[] filesNames = coreDir.listAll();
    // Computing the hash requires items to be submitted in the same order...
    Arrays.sort(filesNames);

    for (String fileName : filesNames) {
      // .lock files come and go. Ignore them (we're closing the Index Writer before adding any pulled files to the Core)
      if (!fileName.endsWith(".lock")) {
        // Hash the file name and file size so we can tell if any file has changed (or files appeared or vanished)
        digest.update(fileName.getBytes(StandardCharsets.UTF_8));
        try {
          digest.update(Long.toString(coreDir.fileLength(fileName)).getBytes(StandardCharsets.UTF_8));
        } catch (FileNotFoundException | NoSuchFileException fnf) {
          // The file was deleted between the listAll() and the check, use an impossible size to not match a digest
          // for which the file is completely present or completely absent (which will cause this hash to never match that directory again).
          digest.update(Long.toString(-42).getBytes(StandardCharsets.UTF_8));
        }
      }
    }

    final String hash = new String(Hex.encodeHex(digest.digest()));
    return hash;
  }

  @Override
  public String toString() {
    return "collectionName=" + core.getCoreDescriptor().getCollectionName() +
        " shardName=" + core.getCoreDescriptor().getCloudDescriptor().getShardId() +
        " coreName=" + core.getName() +
        " generation=" + generation;
  }

  /**
   * Information we capture per local core file (segments_N file *included*)
   */
  public static class CoreFileData {
    /** Local file name, no path */
    private final String fileName;
    /** Size in bytes */
    private final long fileSize;
    private final long checksum;

    CoreFileData(String fileName, long fileSize, long checksum) {
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.checksum = checksum;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CoreFileData other = (CoreFileData) o;

      return Objects.equals(fileName, other.fileName) &&
          Objects.equals(fileSize, other.fileSize) &&
          Objects.equals(checksum, other.checksum);
    }

    public String getFileName() {
      return fileName;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getChecksum() {
      return checksum;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileName, fileSize, checksum);
    }
  }
}

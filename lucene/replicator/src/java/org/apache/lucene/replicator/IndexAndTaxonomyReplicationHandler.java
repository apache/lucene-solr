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
package org.apache.lucene.replicator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.replicator.ReplicationClient.ReplicationHandler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;

/**
 * A {@link ReplicationHandler} for replication of an index and taxonomy pair. See {@link
 * IndexReplicationHandler} for more detail. This handler ensures that the search and taxonomy
 * indexes are replicated in a consistent way.
 *
 * <p><b>NOTE:</b> if you intend to recreate a taxonomy index, you should make sure to reopen an
 * IndexSearcher and TaxonomyReader pair via the provided callback, to guarantee that both indexes
 * are in sync. This handler does not prevent replicating such index and taxonomy pairs, and if they
 * are reopened by a different thread, unexpected errors can occur, as well as inconsistency between
 * the taxonomy and index readers.
 *
 * @see IndexReplicationHandler
 * @lucene.experimental
 */
public class IndexAndTaxonomyReplicationHandler implements ReplicationHandler {

  /**
   * The component used to log messages to the {@link InfoStream#getDefault() default} {@link
   * InfoStream}.
   */
  public static final String INFO_STREAM_COMPONENT = "IndexAndTaxonomyReplicationHandler";

  private final Directory indexDir;
  private final Directory taxoDir;
  private final Callable<Boolean> callback;

  private volatile Map<String, List<RevisionFile>> currentRevisionFiles;
  private volatile String currentVersion;
  private volatile InfoStream infoStream = InfoStream.getDefault();

  /**
   * Constructor with the given index directory and callback to notify when the indexes were
   * updated.
   */
  public IndexAndTaxonomyReplicationHandler(
      Directory indexDir, Directory taxoDir, Callable<Boolean> callback) throws IOException {
    this.callback = callback;
    this.indexDir = indexDir;
    this.taxoDir = taxoDir;
    currentRevisionFiles = null;
    currentVersion = null;
    final boolean indexExists = DirectoryReader.indexExists(indexDir);
    final boolean taxoExists = DirectoryReader.indexExists(taxoDir);
    if (indexExists != taxoExists) {
      throw new IllegalStateException(
          "search and taxonomy indexes must either both exist or not: index="
              + indexExists
              + " taxo="
              + taxoExists);
    }
    if (indexExists) { // both indexes exist
      final IndexCommit indexCommit = IndexReplicationHandler.getLastCommit(indexDir);
      final IndexCommit taxoCommit = IndexReplicationHandler.getLastCommit(taxoDir);
      currentRevisionFiles = IndexAndTaxonomyRevision.revisionFiles(indexCommit, taxoCommit);
      currentVersion = IndexAndTaxonomyRevision.revisionVersion(indexCommit, taxoCommit);
      final InfoStream infoStream = InfoStream.getDefault();
      if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
        infoStream.message(
            INFO_STREAM_COMPONENT,
            "constructor(): currentVersion="
                + currentVersion
                + " currentRevisionFiles="
                + currentRevisionFiles);
        infoStream.message(
            INFO_STREAM_COMPONENT,
            "constructor(): indexCommit=" + indexCommit + " taxoCommit=" + taxoCommit);
      }
    }
  }

  @Override
  public String currentVersion() {
    return currentVersion;
  }

  @Override
  public Map<String, List<RevisionFile>> currentRevisionFiles() {
    return currentRevisionFiles;
  }

  @Override
  public void revisionReady(
      String version,
      Map<String, List<RevisionFile>> revisionFiles,
      Map<String, List<String>> copiedFiles,
      Map<String, Directory> sourceDirectory)
      throws IOException {
    Directory taxoClientDir = sourceDirectory.get(IndexAndTaxonomyRevision.TAXONOMY_SOURCE);
    Directory indexClientDir = sourceDirectory.get(IndexAndTaxonomyRevision.INDEX_SOURCE);
    List<String> taxoFiles = copiedFiles.get(IndexAndTaxonomyRevision.TAXONOMY_SOURCE);
    List<String> indexFiles = copiedFiles.get(IndexAndTaxonomyRevision.INDEX_SOURCE);
    String taxoSegmentsFile = IndexReplicationHandler.getSegmentsFile(taxoFiles, true);
    String indexSegmentsFile = IndexReplicationHandler.getSegmentsFile(indexFiles, false);
    String taxoPendingFile = taxoSegmentsFile == null ? null : "pending_" + taxoSegmentsFile;
    String indexPendingFile = "pending_" + indexSegmentsFile;

    boolean success = false;
    try {
      // copy taxonomy files before index files
      IndexReplicationHandler.copyFiles(taxoClientDir, taxoDir, taxoFiles);
      IndexReplicationHandler.copyFiles(indexClientDir, indexDir, indexFiles);

      // fsync all copied files (except segmentsFile)
      if (!taxoFiles.isEmpty()) {
        taxoDir.sync(taxoFiles);
      }
      indexDir.sync(indexFiles);

      // now copy, fsync, and rename segmentsFile, taxonomy first because it is ok if a
      // reader sees a more advanced taxonomy than the index.

      if (taxoSegmentsFile != null) {
        taxoDir.copyFrom(taxoClientDir, taxoSegmentsFile, taxoPendingFile, IOContext.READONCE);
      }
      indexDir.copyFrom(indexClientDir, indexSegmentsFile, indexPendingFile, IOContext.READONCE);

      if (taxoSegmentsFile != null) {
        taxoDir.sync(Collections.singletonList(taxoPendingFile));
      }
      indexDir.sync(Collections.singletonList(indexPendingFile));

      if (taxoSegmentsFile != null) {
        taxoDir.rename(taxoPendingFile, taxoSegmentsFile);
        taxoDir.syncMetaData();
      }

      indexDir.rename(indexPendingFile, indexSegmentsFile);
      indexDir.syncMetaData();

      success = true;
    } finally {
      if (!success) {
        if (taxoSegmentsFile != null) {
          taxoFiles.add(taxoSegmentsFile); // add it back so it gets deleted too
          taxoFiles.add(taxoPendingFile);
        }
        IndexReplicationHandler.cleanupFilesOnFailure(taxoDir, taxoFiles);
        indexFiles.add(indexSegmentsFile); // add it back so it gets deleted too
        indexFiles.add(indexPendingFile);
        IndexReplicationHandler.cleanupFilesOnFailure(indexDir, indexFiles);
      }
    }

    // all files have been successfully copied + sync'd. update the handler's state
    currentRevisionFiles = revisionFiles;
    currentVersion = version;

    if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
      infoStream.message(
          INFO_STREAM_COMPONENT,
          "revisionReady(): currentVersion="
              + currentVersion
              + " currentRevisionFiles="
              + currentRevisionFiles);
    }

    // Cleanup the index directory from old and unused index files.
    // NOTE: we don't use IndexWriter.deleteUnusedFiles here since it may have
    // side-effects, e.g. if it hits sudden IO errors while opening the index
    // (and can end up deleting the entire index). It is not our job to protect
    // against those errors, app will probably hit them elsewhere.
    IndexReplicationHandler.cleanupOldIndexFiles(indexDir, indexSegmentsFile, infoStream);
    IndexReplicationHandler.cleanupOldIndexFiles(taxoDir, taxoSegmentsFile, infoStream);

    // successfully updated the index, notify the callback that the index is
    // ready.
    if (callback != null) {
      try {
        callback.call();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /** Sets the {@link InfoStream} to use for logging messages. */
  public void setInfoStream(InfoStream infoStream) {
    if (infoStream == null) {
      infoStream = InfoStream.NO_OUTPUT;
    }
    this.infoStream = infoStream;
  }
}

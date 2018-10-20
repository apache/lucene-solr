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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.replicator.ReplicationClient.ReplicationHandler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/**
 * A {@link ReplicationHandler} for replication of an index. Implements
 * {@link #revisionReady} by copying the files pointed by the client resolver to
 * the index {@link Directory} and then touches the index with
 * {@link IndexWriter} to make sure any unused files are deleted.
 * <p>
 * <b>NOTE:</b> this handler assumes that {@link IndexWriter} is not opened by
 * another process on the index directory. In fact, opening an
 * {@link IndexWriter} on the same directory to which files are copied can lead
 * to undefined behavior, where some or all the files will be deleted, override
 * other files or simply create a mess. When you replicate an index, it is best
 * if the index is never modified by {@link IndexWriter}, except the one that is
 * open on the source index, from which you replicate.
 * <p>
 * This handler notifies the application via a provided {@link Callable} when an
 * updated index commit was made available for it.
 * 
 * @lucene.experimental
 */
public class IndexReplicationHandler implements ReplicationHandler {
  
  /**
   * The component used to log messages to the {@link InfoStream#getDefault()
   * default} {@link InfoStream}.
   */
  public static final String INFO_STREAM_COMPONENT = "IndexReplicationHandler";
  
  private final Directory indexDir;
  private final Callable<Boolean> callback;
  
  private volatile Map<String,List<RevisionFile>> currentRevisionFiles;
  private volatile String currentVersion;
  private volatile InfoStream infoStream = InfoStream.getDefault();
  
  /**
   * Returns the last {@link IndexCommit} found in the {@link Directory}, or
   * {@code null} if there are no commits.
   */
  public static IndexCommit getLastCommit(Directory dir) throws IOException {
    try {
      if (DirectoryReader.indexExists(dir)) {
        List<IndexCommit> commits = DirectoryReader.listCommits(dir);
        // listCommits guarantees that we get at least one commit back, or
        // IndexNotFoundException which we handle below
        return commits.get(commits.size() - 1);
      }
    } catch (IndexNotFoundException e) {
      // ignore the exception and return null
    }
    return null;
  }
  
  /**
   * Verifies that the last file is segments_N and fails otherwise. It also
   * removes and returns the file from the list, because it needs to be handled
   * last, after all files. This is important in order to guarantee that if a
   * reader sees the new segments_N, all other segment files are already on
   * stable storage.
   * <p>
   * The reason why the code fails instead of putting segments_N file last is
   * that this indicates an error in the Revision implementation.
   */
  public static String getSegmentsFile(List<String> files, boolean allowEmpty) {
    if (files.isEmpty()) {
      if (allowEmpty) {
        return null;
      } else {
        throw new IllegalStateException("empty list of files not allowed");
      }
    }
    
    String segmentsFile = files.remove(files.size() - 1);
    if (!segmentsFile.startsWith(IndexFileNames.SEGMENTS) || segmentsFile.equals(IndexFileNames.OLD_SEGMENTS_GEN)) {
      throw new IllegalStateException("last file to copy+sync must be segments_N but got " + segmentsFile
          + "; check your Revision implementation!");
    }
    return segmentsFile;
  }

  /**
   * Cleanup the index directory by deleting all given files. Called when file
   * copy or sync failed.
   */
  public static void cleanupFilesOnFailure(Directory dir, List<String> files) {
    for (String file : files) {
      // suppress any exception because if we're here, it means copy
      // failed, and we must cleanup after ourselves.
      IOUtils.deleteFilesIgnoringExceptions(dir, file);
    }
  }
  
  /**
   * Cleans up the index directory from old index files. This method uses the
   * last commit found by {@link #getLastCommit(Directory)}. If it matches the
   * expected segmentsFile, then all files not referenced by this commit point
   * are deleted.
   * <p>
   * <b>NOTE:</b> this method does a best effort attempt to clean the index
   * directory. It suppresses any exceptions that occur, as this can be retried
   * the next time.
   */
  public static void cleanupOldIndexFiles(Directory dir, String segmentsFile, InfoStream infoStream) {
    try {
      IndexCommit commit = getLastCommit(dir);
      // commit == null means weird IO errors occurred, ignore them
      // if there were any IO errors reading the expected commit point (i.e.
      // segments files mismatch), then ignore that commit either.
      if (commit != null && commit.getSegmentsFileName().equals(segmentsFile)) {
        Set<String> commitFiles = new HashSet<>();
        commitFiles.addAll(commit.getFileNames());
        Matcher matcher = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
        for (String file : dir.listAll()) {
          if (!commitFiles.contains(file)
              && (matcher.reset(file).matches() || file.startsWith(IndexFileNames.SEGMENTS))) {
            // suppress exceptions, it's just a best effort
            IOUtils.deleteFilesIgnoringExceptions(dir, file);
          }
        }
      }
    } catch (Throwable t) {
      // ignore any errors that happen during this state and only log it. this
      // cleanup will have a chance to succeed the next time we get a new
      // revision.
      if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
        infoStream.message(INFO_STREAM_COMPONENT, "cleanupOldIndexFiles(): failed on error " + t.getMessage());
      }
    }
  }
  
  /**
   * Copies the files from the source directory to the target one, if they are
   * not the same.
   */
  public static void copyFiles(Directory source, Directory target, List<String> files) throws IOException {
    if (!source.equals(target)) {
      for (String file : files) {
        target.copyFrom(source, file, file, IOContext.READONCE);
      }
    }
  }

  /**
   * Constructor with the given index directory and callback to notify when the
   * indexes were updated.
   */
  public IndexReplicationHandler(Directory indexDir, Callable<Boolean> callback) throws IOException {
    this.callback = callback;
    this.indexDir = indexDir;
    currentRevisionFiles = null;
    currentVersion = null;
    if (DirectoryReader.indexExists(indexDir)) {
      final List<IndexCommit> commits = DirectoryReader.listCommits(indexDir);
      final IndexCommit commit = commits.get(commits.size() - 1);
      currentRevisionFiles = IndexRevision.revisionFiles(commit);
      currentVersion = IndexRevision.revisionVersion(commit);
      final InfoStream infoStream = InfoStream.getDefault();
      if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
        infoStream.message(INFO_STREAM_COMPONENT, "constructor(): currentVersion=" + currentVersion
            + " currentRevisionFiles=" + currentRevisionFiles);
        infoStream.message(INFO_STREAM_COMPONENT, "constructor(): commit=" + commit);
      }
    }
  }
  
  @Override
  public String currentVersion() {
    return currentVersion;
  }
  
  @Override
  public Map<String,List<RevisionFile>> currentRevisionFiles() {
    return currentRevisionFiles;
  }
  
  @Override
  public void revisionReady(String version, Map<String,List<RevisionFile>> revisionFiles,
      Map<String,List<String>> copiedFiles, Map<String,Directory> sourceDirectory) throws IOException {
    if (revisionFiles.size() > 1) {
      throw new IllegalArgumentException("this handler handles only a single source; got " + revisionFiles.keySet());
    }
    
    Directory clientDir = sourceDirectory.values().iterator().next();
    List<String> files = copiedFiles.values().iterator().next();
    String segmentsFile = getSegmentsFile(files, false);
    String pendingSegmentsFile = "pending_" + segmentsFile;
    
    boolean success = false;
    try {
      // copy files from the client to index directory
      copyFiles(clientDir, indexDir, files);
      
      // fsync all copied files (except segmentsFile)
      indexDir.sync(files);
      
      // now copy and fsync segmentsFile as pending, then rename (simulating lucene commit)
      indexDir.copyFrom(clientDir, segmentsFile, pendingSegmentsFile, IOContext.READONCE);
      indexDir.sync(Collections.singletonList(pendingSegmentsFile));
      indexDir.rename(pendingSegmentsFile, segmentsFile);
      indexDir.syncMetaData();
      
      success = true;
    } finally {
      if (!success) {
        files.add(segmentsFile); // add it back so it gets deleted too
        files.add(pendingSegmentsFile);
        cleanupFilesOnFailure(indexDir, files);
      }
    }

    // all files have been successfully copied + sync'd. update the handler's state
    currentRevisionFiles = revisionFiles;
    currentVersion = version;
    
    if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
      infoStream.message(INFO_STREAM_COMPONENT, "revisionReady(): currentVersion=" + currentVersion
          + " currentRevisionFiles=" + currentRevisionFiles);
    }
    
    // Cleanup the index directory from old and unused index files.
    // NOTE: we don't use IndexWriter.deleteUnusedFiles here since it may have
    // side-effects, e.g. if it hits sudden IO errors while opening the index
    // (and can end up deleting the entire index). It is not our job to protect
    // against those errors, app will probably hit them elsewhere.
    cleanupOldIndexFiles(indexDir, segmentsFile, infoStream);

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

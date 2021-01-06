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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * A {@link Revision} of a single index files which comprises the list of files that are part of the
 * current {@link IndexCommit}. To ensure the files are not deleted by {@link IndexWriter} for as
 * long as this revision stays alive (i.e. until {@link #release()}), the current commit point is
 * snapshotted, using {@link SnapshotDeletionPolicy} (this means that the given writer's {@link
 * IndexWriterConfig#getIndexDeletionPolicy() config} should return {@link SnapshotDeletionPolicy}).
 *
 * <p>When this revision is {@link #release() released}, it releases the obtained snapshot as well
 * as calls {@link IndexWriter#deleteUnusedFiles()} so that the snapshotted files are deleted (if
 * they are no longer needed).
 *
 * @lucene.experimental
 */
public class IndexRevision implements Revision {

  private static final int RADIX = 16;
  private static final String SOURCE = "index";

  private final IndexWriter writer;
  private final IndexCommit commit;
  private final SnapshotDeletionPolicy sdp;
  private final String version;
  private final Map<String, List<RevisionFile>> sourceFiles;

  // returns a RevisionFile with some metadata
  private static RevisionFile newRevisionFile(String file, Directory dir) throws IOException {
    RevisionFile revFile = new RevisionFile(file);
    revFile.size = dir.fileLength(file);
    return revFile;
  }

  /** Returns a singleton map of the revision files from the given {@link IndexCommit}. */
  public static Map<String, List<RevisionFile>> revisionFiles(IndexCommit commit)
      throws IOException {
    Collection<String> commitFiles = commit.getFileNames();
    List<RevisionFile> revisionFiles = new ArrayList<>(commitFiles.size());
    String segmentsFile = commit.getSegmentsFileName();
    Directory dir = commit.getDirectory();

    for (String file : commitFiles) {
      if (!file.equals(segmentsFile)) {
        revisionFiles.add(newRevisionFile(file, dir));
      }
    }
    revisionFiles.add(newRevisionFile(segmentsFile, dir)); // segments_N must be last
    return Collections.singletonMap(SOURCE, revisionFiles);
  }

  /** Returns a String representation of a revision's version from the given {@link IndexCommit}. */
  public static String revisionVersion(IndexCommit commit) {
    return Long.toString(commit.getGeneration(), RADIX);
  }

  /**
   * Constructor over the given {@link IndexWriter}. Uses the last {@link IndexCommit} found in the
   * {@link Directory} managed by the given writer.
   */
  public IndexRevision(IndexWriter writer) throws IOException {
    IndexDeletionPolicy delPolicy = writer.getConfig().getIndexDeletionPolicy();
    if (!(delPolicy instanceof SnapshotDeletionPolicy)) {
      throw new IllegalArgumentException("IndexWriter must be created with SnapshotDeletionPolicy");
    }
    this.writer = writer;
    this.sdp = (SnapshotDeletionPolicy) delPolicy;
    this.commit = sdp.snapshot();
    this.version = revisionVersion(commit);
    this.sourceFiles = revisionFiles(commit);
  }

  @Override
  public int compareTo(String version) {
    long gen = Long.parseLong(version, RADIX);
    long commitGen = commit.getGeneration();
    return commitGen < gen ? -1 : (commitGen > gen ? 1 : 0);
  }

  @Override
  public int compareTo(Revision o) {
    IndexRevision other = (IndexRevision) o;
    return commit.compareTo(other.commit);
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public Map<String, List<RevisionFile>> getSourceFiles() {
    return sourceFiles;
  }

  @Override
  public InputStream open(String source, String fileName) throws IOException {
    assert source.equals(SOURCE) : "invalid source; expected=" + SOURCE + " got=" + source;
    return new IndexInputInputStream(commit.getDirectory().openInput(fileName, IOContext.READONCE));
  }

  @Override
  public void release() throws IOException {
    sdp.release(commit);
    writer.deleteUnusedFiles();
  }

  @Override
  public String toString() {
    return "IndexRevision version=" + version + " files=" + sourceFiles;
  }
}

package org.apache.lucene.index;

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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoSuchDirectoryException;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.InfoStream;

/*
 * This class keeps track of each SegmentInfos instance that
 * is still "live", either because it corresponds to a
 * segments_N file in the Directory (a "commit", i.e. a
 * committed SegmentInfos) or because it's an in-memory
 * SegmentInfos that a writer is actively updating but has
 * not yet committed.  This class uses simple reference
 * counting to map the live SegmentInfos instances to
 * individual files in the Directory.
 *
 * The same directory file may be referenced by more than
 * one IndexCommit, i.e. more than one SegmentInfos.
 * Therefore we count how many commits reference each file.
 * When all the commits referencing a certain file have been
 * deleted, the refcount for that file becomes zero, and the
 * file is deleted.
 *
 * A separate deletion policy interface
 * (IndexDeletionPolicy) is consulted on creation (onInit)
 * and once per commit (onCommit), to decide when a commit
 * should be removed.
 *
 * It is the business of the IndexDeletionPolicy to choose
 * when to delete commit points.  The actual mechanics of
 * file deletion, retrying, etc, derived from the deletion
 * of commit points is the business of the IndexFileDeleter.
 *
 * The current default deletion policy is {@link
 * KeepOnlyLastCommitDeletionPolicy}, which removes all
 * prior commits when a new commit has completed.  This
 * matches the behavior before 2.2.
 *
 * Note that you must hold the write.lock before
 * instantiating this class.  It opens segments_N file(s)
 * directly with no retry logic.
 */

final class IndexFileDeleter implements Closeable {

  /* Files that we tried to delete but failed (likely
   * because they are open and we are running on Windows),
   * so we will retry them again later: */
  private List<String> deletable;

  /* Reference count for all files in the index.
   * Counts how many existing commits reference a file.
   **/
  private Map<String, RefCount> refCounts = new HashMap<String, RefCount>();

  /* Holds all commits (segments_N) currently in the index.
   * This will have just 1 commit if you are using the
   * default delete policy (KeepOnlyLastCommitDeletionPolicy).
   * Other policies may leave commit points live for longer
   * in which case this list would be longer than 1: */
  private List<CommitPoint> commits = new ArrayList<CommitPoint>();

  /* Holds files we had incref'd from the previous
   * non-commit checkpoint: */
  private List<Collection<String>> lastFiles = new ArrayList<Collection<String>>();

  /* Commits that the IndexDeletionPolicy have decided to delete: */
  private List<CommitPoint> commitsToDelete = new ArrayList<CommitPoint>();

  private final InfoStream infoStream;
  private Directory directory;
  private IndexDeletionPolicy policy;

  final boolean startingCommitDeleted;
  private SegmentInfos lastSegmentInfos;

  /** Change to true to see details of reference counts when
   *  infoStream is enabled */
  public static boolean VERBOSE_REF_COUNTS = false;

  // Used only for assert
  private final IndexWriter writer;

  // called only from assert
  private boolean locked() {
    return writer == null || Thread.holdsLock(writer);
  }

  /**
   * Initialize the deleter: find all previous commits in
   * the Directory, incref the files they reference, call
   * the policy to let it delete commits.  This will remove
   * any files not referenced by any of the commits.
   * @throws IOException if there is a low-level IO error
   */
  public IndexFileDeleter(Directory directory, IndexDeletionPolicy policy, SegmentInfos segmentInfos,
                          InfoStream infoStream, IndexWriter writer, boolean initialIndexExists) throws IOException {
    this.infoStream = infoStream;
    this.writer = writer;

    final String currentSegmentsFile = segmentInfos.getSegmentsFileName();

    if (infoStream.isEnabled("IFD")) {
      infoStream.message("IFD", "init: current segments file is \"" + currentSegmentsFile + "\"; deletionPolicy=" + policy);
    }

    this.policy = policy;
    this.directory = directory;

    // First pass: walk the files and initialize our ref
    // counts:
    long currentGen = segmentInfos.getGeneration();

    CommitPoint currentCommitPoint = null;
    String[] files = null;
    try {
      files = directory.listAll();
    } catch (NoSuchDirectoryException e) {
      // it means the directory is empty, so ignore it.
      files = new String[0];
    }
    
    if (currentSegmentsFile != null) {
      Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
      for (String fileName : files) {
        m.reset(fileName);
        if (!fileName.endsWith("write.lock") && !fileName.equals(IndexFileNames.SEGMENTS_GEN)
            && (m.matches() || fileName.startsWith(IndexFileNames.SEGMENTS))) {
          
          // Add this file to refCounts with initial count 0:
          getRefCount(fileName);
          
          if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
            
            // This is a commit (segments or segments_N), and
            // it's valid (<= the max gen).  Load it, then
            // incref all files it refers to:
            if (infoStream.isEnabled("IFD")) {
              infoStream.message("IFD", "init: load commit \"" + fileName + "\"");
            }
            SegmentInfos sis = new SegmentInfos();
            try {
              sis.read(directory, fileName);
            } catch (FileNotFoundException e) {
              // LUCENE-948: on NFS (and maybe others), if
              // you have writers switching back and forth
              // between machines, it's very likely that the
              // dir listing will be stale and will claim a
              // file segments_X exists when in fact it
              // doesn't.  So, we catch this and handle it
              // as if the file does not exist
              if (infoStream.isEnabled("IFD")) {
                infoStream.message("IFD", "init: hit FileNotFoundException when loading commit \"" + fileName + "\"; skipping this commit point");
              }
              sis = null;
            } catch (IOException e) {
              if (SegmentInfos.generationFromSegmentsFileName(fileName) <= currentGen && directory.fileLength(fileName) > 0) {
                throw e;
              } else {
                // Most likely we are opening an index that
                // has an aborted "future" commit, so suppress
                // exc in this case
                sis = null;
              }
            }
            if (sis != null) {
              final CommitPoint commitPoint = new CommitPoint(commitsToDelete, directory, sis);
              if (sis.getGeneration() == segmentInfos.getGeneration()) {
                currentCommitPoint = commitPoint;
              }
              commits.add(commitPoint);
              incRef(sis, true);
              
              if (lastSegmentInfos == null || sis.getGeneration() > lastSegmentInfos.getGeneration()) {
                lastSegmentInfos = sis;
              }
            }
          }
        }
      }
    }

    if (currentCommitPoint == null && currentSegmentsFile != null && initialIndexExists) {
      // We did not in fact see the segments_N file
      // corresponding to the segmentInfos that was passed
      // in.  Yet, it must exist, because our caller holds
      // the write lock.  This can happen when the directory
      // listing was stale (eg when index accessed via NFS
      // client with stale directory listing cache).  So we
      // try now to explicitly open this commit point:
      SegmentInfos sis = new SegmentInfos();
      try {
        sis.read(directory, currentSegmentsFile);
      } catch (IOException e) {
        throw new CorruptIndexException("failed to locate current segments_N file \"" + currentSegmentsFile + "\"");
      }
      if (infoStream.isEnabled("IFD")) {
        infoStream.message("IFD", "forced open of current segments file " + segmentInfos.getSegmentsFileName());
      }
      currentCommitPoint = new CommitPoint(commitsToDelete, directory, sis);
      commits.add(currentCommitPoint);
      incRef(sis, true);
    }

    // We keep commits list in sorted order (oldest to newest):
    CollectionUtil.mergeSort(commits);

    // Now delete anything with ref count at 0.  These are
    // presumably abandoned files eg due to crash of
    // IndexWriter.
    for(Map.Entry<String, RefCount> entry : refCounts.entrySet() ) {
      RefCount rc = entry.getValue();
      final String fileName = entry.getKey();
      if (0 == rc.count) {
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "init: removing unreferenced file \"" + fileName + "\"");
        }
        deleteFile(fileName);
      }
    }

    // Finally, give policy a chance to remove things on
    // startup:
    if (currentSegmentsFile != null) {
      policy.onInit(commits);
    }

    // Always protect the incoming segmentInfos since
    // sometime it may not be the most recent commit
    checkpoint(segmentInfos, false);

    startingCommitDeleted = currentCommitPoint == null ? false : currentCommitPoint.isDeleted();

    deleteCommits();
  }

  public SegmentInfos getLastSegmentInfos() {
    return lastSegmentInfos;
  }

  /**
   * Remove the CommitPoints in the commitsToDelete List by
   * DecRef'ing all files from each SegmentInfos.
   */
  private void deleteCommits() throws IOException {

    int size = commitsToDelete.size();

    if (size > 0) {

      // First decref all files that had been referred to by
      // the now-deleted commits:
      for(int i=0;i<size;i++) {
        CommitPoint commit = commitsToDelete.get(i);
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "deleteCommits: now decRef commit \"" + commit.getSegmentsFileName() + "\"");
        }
        for (final String file : commit.files) {
          decRef(file);
        }
      }
      commitsToDelete.clear();

      // Now compact commits to remove deleted ones (preserving the sort):
      size = commits.size();
      int readFrom = 0;
      int writeTo = 0;
      while(readFrom < size) {
        CommitPoint commit = commits.get(readFrom);
        if (!commit.deleted) {
          if (writeTo != readFrom) {
            commits.set(writeTo, commits.get(readFrom));
          }
          writeTo++;
        }
        readFrom++;
      }

      while(size > writeTo) {
        commits.remove(size-1);
        size--;
      }
    }
  }

  /**
   * Writer calls this when it has hit an error and had to
   * roll back, to tell us that there may now be
   * unreferenced files in the filesystem.  So we re-list
   * the filesystem and delete such files.  If segmentName
   * is non-null, we will only delete files corresponding to
   * that segment.
   */
  public void refresh(String segmentName) throws IOException {
    assert locked();

    String[] files = directory.listAll();
    String segmentPrefix1;
    String segmentPrefix2;
    if (segmentName != null) {
      segmentPrefix1 = segmentName + ".";
      segmentPrefix2 = segmentName + "_";
    } else {
      segmentPrefix1 = null;
      segmentPrefix2 = null;
    }

    Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");

    for(int i=0;i<files.length;i++) {
      String fileName = files[i];
      m.reset(fileName);
      if ((segmentName == null || fileName.startsWith(segmentPrefix1) || fileName.startsWith(segmentPrefix2)) &&
          !fileName.endsWith("write.lock") &&
          !refCounts.containsKey(fileName) &&
          !fileName.equals(IndexFileNames.SEGMENTS_GEN) &&
          (m.matches() || fileName.startsWith(IndexFileNames.SEGMENTS))) {
        // Unreferenced file, so remove it
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "refresh [prefix=" + segmentName + "]: removing newly created unreferenced file \"" + fileName + "\"");
        }
        deleteFile(fileName);
      }
    }
  }

  public void refresh() throws IOException {
    // Set to null so that we regenerate the list of pending
    // files; else we can accumulate same file more than
    // once
    assert locked();
    deletable = null;
    refresh(null);
  }

  public void close() throws IOException {
    // DecRef old files from the last checkpoint, if any:
    assert locked();
    int size = lastFiles.size();
    if (size > 0) {
      for(int i=0;i<size;i++) {
        decRef(lastFiles.get(i));
      }
      lastFiles.clear();
    }

    deletePendingFiles();
  }

  /**
   * Revisits the {@link IndexDeletionPolicy} by calling its
   * {@link IndexDeletionPolicy#onCommit(List)} again with the known commits.
   * This is useful in cases where a deletion policy which holds onto index
   * commits is used. The application may know that some commits are not held by
   * the deletion policy anymore and call
   * {@link IndexWriter#deleteUnusedFiles()}, which will attempt to delete the
   * unused commits again.
   */
  void revisitPolicy() throws IOException {
    assert locked();
    if (infoStream.isEnabled("IFD")) {
      infoStream.message("IFD", "now revisitPolicy");
    }

    if (commits.size() > 0) {
      policy.onCommit(commits);
      deleteCommits();
    }
  }

  public void deletePendingFiles() throws IOException {
    assert locked();
    if (deletable != null) {
      List<String> oldDeletable = deletable;
      deletable = null;
      int size = oldDeletable.size();
      for(int i=0;i<size;i++) {
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "delete pending file " + oldDeletable.get(i));
        }
        deleteFile(oldDeletable.get(i));
      }
    }
  }

  /**
   * For definition of "check point" see IndexWriter comments:
   * "Clarification: Check Points (and commits)".
   *
   * Writer calls this when it has made a "consistent
   * change" to the index, meaning new files are written to
   * the index and the in-memory SegmentInfos have been
   * modified to point to those files.
   *
   * This may or may not be a commit (segments_N may or may
   * not have been written).
   *
   * We simply incref the files referenced by the new
   * SegmentInfos and decref the files we had previously
   * seen (if any).
   *
   * If this is a commit, we also call the policy to give it
   * a chance to remove other commits.  If any commits are
   * removed, we decref their files as well.
   */
  public void checkpoint(SegmentInfos segmentInfos, boolean isCommit) throws IOException {
    assert locked();

    assert Thread.holdsLock(writer);
    long t0 = 0;
    if (infoStream.isEnabled("IFD")) {
      t0 = System.nanoTime();
      infoStream.message("IFD", "now checkpoint \"" + writer.segString(writer.toLiveInfos(segmentInfos)) + "\" [" + segmentInfos.size() + " segments " + "; isCommit = " + isCommit + "]");
    }

    // Try again now to delete any previously un-deletable
    // files (because they were in use, on Windows):
    deletePendingFiles();

    // Incref the files:
    incRef(segmentInfos, isCommit);

    if (isCommit) {
      // Append to our commits list:
      commits.add(new CommitPoint(commitsToDelete, directory, segmentInfos));

      // Tell policy so it can remove commits:
      policy.onCommit(commits);

      // Decref files for commits that were deleted by the policy:
      deleteCommits();
    } else {
      // DecRef old files from the last checkpoint, if any:
      for (Collection<String> lastFile : lastFiles) {
        decRef(lastFile);
      }
      lastFiles.clear();

      // Save files so we can decr on next checkpoint/commit:
      lastFiles.add(segmentInfos.files(directory, false));
    }
    if (infoStream.isEnabled("IFD")) {
      long t1 = System.nanoTime();
      infoStream.message("IFD", ((t1-t0)/1000000) + " msec to checkpoint");
    }
  }

  void incRef(SegmentInfos segmentInfos, boolean isCommit) throws IOException {
    assert locked();
    // If this is a commit point, also incRef the
    // segments_N file:
    for(final String fileName: segmentInfos.files(directory, isCommit)) {
      incRef(fileName);
    }
  }

  void incRef(Collection<String> files) {
    assert locked();
    for(final String file : files) {
      incRef(file);
    }
  }

  void incRef(String fileName) {
    assert locked();
    RefCount rc = getRefCount(fileName);
    if (infoStream.isEnabled("IFD")) {
      if (VERBOSE_REF_COUNTS) {
        infoStream.message("IFD", "  IncRef \"" + fileName + "\": pre-incr count is " + rc.count);
      }
    }
    rc.IncRef();
  }

  void decRef(Collection<String> files) throws IOException {
    assert locked();
    for(final String file : files) {
      decRef(file);
    }
  }

  void decRef(String fileName) throws IOException {
    assert locked();
    RefCount rc = getRefCount(fileName);
    if (infoStream.isEnabled("IFD")) {
      if (VERBOSE_REF_COUNTS) {
        infoStream.message("IFD", "  DecRef \"" + fileName + "\": pre-decr count is " + rc.count);
      }
    }
    if (0 == rc.DecRef()) {
      // This file is no longer referenced by any past
      // commit points nor by the in-memory SegmentInfos:
      deleteFile(fileName);
      refCounts.remove(fileName);
    }
  }

  void decRef(SegmentInfos segmentInfos) throws IOException {
    assert locked();
    for (final String file : segmentInfos.files(directory, false)) {
      decRef(file);
    }
  }

  public boolean exists(String fileName) {
    assert locked();
    if (!refCounts.containsKey(fileName)) {
      return false;
    } else {
      return getRefCount(fileName).count > 0;
    }
  }

  private RefCount getRefCount(String fileName) {
    assert locked();
    RefCount rc;
    if (!refCounts.containsKey(fileName)) {
      rc = new RefCount(fileName);
      refCounts.put(fileName, rc);
    } else {
      rc = refCounts.get(fileName);
    }
    return rc;
  }

  void deleteFiles(List<String> files) throws IOException {
    assert locked();
    for(final String file: files) {
      deleteFile(file);
    }
  }

  /** Deletes the specified files, but only if they are new
   *  (have not yet been incref'd). */
  void deleteNewFiles(Collection<String> files) throws IOException {
    assert locked();
    for (final String fileName: files) {
      // NOTE: it's very unusual yet possible for the
      // refCount to be present and 0: it can happen if you
      // open IW on a crashed index, and it removes a bunch
      // of unref'd files, and then you add new docs / do
      // merging, and it reuses that segment name.
      // TestCrash.testCrashAfterReopen can hit this:
      if (!refCounts.containsKey(fileName) || refCounts.get(fileName).count == 0) {
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "delete new file \"" + fileName + "\"");
        }
        deleteFile(fileName);
      }
    }
  }

  void deleteFile(String fileName)
       throws IOException {
    assert locked();
    try {
      if (infoStream.isEnabled("IFD")) {
        infoStream.message("IFD", "delete \"" + fileName + "\"");
      }
      directory.deleteFile(fileName);
    } catch (IOException e) {  // if delete fails
      if (directory.fileExists(fileName)) {

        // Some operating systems (e.g. Windows) don't
        // permit a file to be deleted while it is opened
        // for read (e.g. by another process or thread). So
        // we assume that when a delete fails it is because
        // the file is open in another process, and queue
        // the file for subsequent deletion.

        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "unable to remove file \"" + fileName + "\": " + e.toString() + "; Will re-try later.");
        }
        if (deletable == null) {
          deletable = new ArrayList<String>();
        }
        deletable.add(fileName);                  // add to deletable
      }
    }
  }

  /**
   * Tracks the reference count for a single index file:
   */
  final private static class RefCount {

    // fileName used only for better assert error messages
    final String fileName;
    boolean initDone;
    RefCount(String fileName) {
      this.fileName = fileName;
    }

    int count;

    public int IncRef() {
      if (!initDone) {
        initDone = true;
      } else {
        assert count > 0: Thread.currentThread().getName() + ": RefCount is 0 pre-increment for file \"" + fileName + "\"";
      }
      return ++count;
    }

    public int DecRef() {
      assert count > 0: Thread.currentThread().getName() + ": RefCount is 0 pre-decrement for file \"" + fileName + "\"";
      return --count;
    }
  }

  /**
   * Holds details for each commit point.  This class is
   * also passed to the deletion policy.  Note: this class
   * has a natural ordering that is inconsistent with
   * equals.
   */

  final private static class CommitPoint extends IndexCommit {

    Collection<String> files;
    String segmentsFileName;
    boolean deleted;
    Directory directory;
    Collection<CommitPoint> commitsToDelete;
    long generation;
    final Map<String,String> userData;
    private final int segmentCount;

    public CommitPoint(Collection<CommitPoint> commitsToDelete, Directory directory, SegmentInfos segmentInfos) throws IOException {
      this.directory = directory;
      this.commitsToDelete = commitsToDelete;
      userData = segmentInfos.getUserData();
      segmentsFileName = segmentInfos.getSegmentsFileName();
      generation = segmentInfos.getGeneration();
      files = Collections.unmodifiableCollection(segmentInfos.files(directory, true));
      segmentCount = segmentInfos.size();
    }

    @Override
    public String toString() {
      return "IndexFileDeleter.CommitPoint(" + segmentsFileName + ")";
    }

    @Override
    public int getSegmentCount() {
      return segmentCount;
    }

    @Override
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    @Override
    public Collection<String> getFileNames() {
      return files;
    }

    @Override
    public Directory getDirectory() {
      return directory;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public Map<String,String> getUserData() {
      return userData;
    }

    /**
     * Called only be the deletion policy, to remove this
     * commit point from the index.
     */
    @Override
    public void delete() {
      if (!deleted) {
        deleted = true;
        commitsToDelete.add(this);
      }
    }

    @Override
    public boolean isDeleted() {
      return deleted;
    }
  }
}

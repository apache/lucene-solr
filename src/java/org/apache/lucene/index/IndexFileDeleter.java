package org.apache.lucene.index;

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

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/*
 * This class keeps track of each SegmentInfos instance that
 * is still "live", either because it corresponds to a 
 * segments_N file in the Directory (a "commit", i.e. a 
 * committed SegmentInfos) or because it's the in-memory SegmentInfos 
 * that a writer is actively updating but has not yet committed 
 * (currently this only applies when autoCommit=false in IndexWriter).
 * This class uses simple reference counting to map the live
 * SegmentInfos instances to individual files in the Directory. 
 * 
 * The same directory file may be referenced by more than
 * one IndexCommitPoints, i.e. more than one SegmentInfos.
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

final class IndexFileDeleter {

  /* Files that we tried to delete but failed (likely
   * because they are open and we are running on Windows),
   * so we will retry them again later: */
  private List deletable;

  /* Reference count for all files in the index.  
   * Counts how many existing commits reference a file.
   * Maps String to RefCount (class below) instances: */
  private Map refCounts = new HashMap();

  /* Holds all commits (segments_N) currently in the index.
   * This will have just 1 commit if you are using the
   * default delete policy (KeepOnlyLastCommitDeletionPolicy).
   * Other policies may leave commit points live for longer
   * in which case this list would be longer than 1: */
  private List commits = new ArrayList();

  /* Holds files we had incref'd from the previous
   * non-commit checkpoint: */
  private List lastFiles = new ArrayList();

  /* Commits that the IndexDeletionPolicy have decided to delete: */ 
  private List commitsToDelete = new ArrayList();

  private PrintStream infoStream;
  private Directory directory;
  private IndexDeletionPolicy policy;
  private DocumentsWriter docWriter;

  void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }
  
  private void message(String message) {
    infoStream.println(this + " " + Thread.currentThread().getName() + ": " + message);
  }

  /**
   * Initialize the deleter: find all previous commits in
   * the Directory, incref the files they reference, call
   * the policy to let it delete commits.  The incoming
   * segmentInfos must have been loaded from a commit point
   * and not yet modified.  This will remove any files not
   * referenced by any of the commits.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public IndexFileDeleter(Directory directory, IndexDeletionPolicy policy, SegmentInfos segmentInfos, PrintStream infoStream, DocumentsWriter docWriter)
    throws CorruptIndexException, IOException {

    this.docWriter = docWriter;
    this.infoStream = infoStream;

    this.policy = policy;
    this.directory = directory;

    // First pass: walk the files and initialize our ref
    // counts:
    long currentGen = segmentInfos.getGeneration();
    IndexFileNameFilter filter = IndexFileNameFilter.getFilter();

    String[] files = directory.list();
    if (files == null)
      throw new IOException("cannot read directory " + directory + ": list() returned null");

    CommitPoint currentCommitPoint = null;

    for(int i=0;i<files.length;i++) {

      String fileName = files[i];

      if (filter.accept(null, fileName) && !fileName.equals(IndexFileNames.SEGMENTS_GEN)) {

        // Add this file to refCounts with initial count 0:
        getRefCount(fileName);

        if (fileName.startsWith(IndexFileNames.SEGMENTS)) {

          // This is a commit (segments or segments_N), and
          // it's valid (<= the max gen).  Load it, then
          // incref all files it refers to:
          if (SegmentInfos.generationFromSegmentsFileName(fileName) <= currentGen) {
            if (infoStream != null) {
              message("init: load commit \"" + fileName + "\"");
            }
            SegmentInfos sis = new SegmentInfos();
            sis.read(directory, fileName);
            CommitPoint commitPoint = new CommitPoint(sis);
            if (sis.getGeneration() == segmentInfos.getGeneration()) {
              currentCommitPoint = commitPoint;
            }
            commits.add(commitPoint);
            incRef(sis, true);
          }
        }
      }
    }

    if (currentCommitPoint == null) {
      throw new CorruptIndexException("failed to locate current segments_N file");
    }

    // We keep commits list in sorted order (oldest to newest):
    Collections.sort(commits);

    // Now delete anything with ref count at 0.  These are
    // presumably abandoned files eg due to crash of
    // IndexWriter.
    Iterator it = refCounts.keySet().iterator();
    while(it.hasNext()) {
      String fileName = (String) it.next();
      RefCount rc = (RefCount) refCounts.get(fileName);
      if (0 == rc.count) {
        if (infoStream != null) {
          message("init: removing unreferenced file \"" + fileName + "\"");
        }
        deleteFile(fileName);
      }
    }

    // Finally, give policy a chance to remove things on
    // startup:
    policy.onInit(commits);

    // It's OK for the onInit to remove the current commit
    // point; we just have to checkpoint our in-memory
    // SegmentInfos to protect those files that it uses:
    if (currentCommitPoint.deleted) {
      checkpoint(segmentInfos, false);
    }
    
    deleteCommits();
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
        CommitPoint commit = (CommitPoint) commitsToDelete.get(i);
        if (infoStream != null) {
          message("deleteCommits: now remove commit \"" + commit.getSegmentsFileName() + "\"");
        }
        int size2 = commit.files.size();
        for(int j=0;j<size2;j++) {
          decRef((List) commit.files.get(j));
        }
        decRef(commit.getSegmentsFileName());
      }
      commitsToDelete.clear();

      // Now compact commits to remove deleted ones (preserving the sort):
      size = commits.size();
      int readFrom = 0;
      int writeTo = 0;
      while(readFrom < size) {
        CommitPoint commit = (CommitPoint) commits.get(readFrom);
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
   * the filesystem and delete such files:
   */
  public void refresh() throws IOException {
    String[] files = directory.list();
    if (files == null)
      throw new IOException("cannot read directory " + directory + ": list() returned null");
    IndexFileNameFilter filter = IndexFileNameFilter.getFilter();
    for(int i=0;i<files.length;i++) {
      String fileName = files[i];
      if (filter.accept(null, fileName) && !refCounts.containsKey(fileName) && !fileName.equals(IndexFileNames.SEGMENTS_GEN)) {
        // Unreferenced file, so remove it
        if (infoStream != null) {
          message("refresh: removing newly created unreferenced file \"" + fileName + "\"");
        }
        deleteFile(fileName);
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

    if (infoStream != null) {
      message("now checkpoint \"" + segmentInfos.getCurrentSegmentFileName() + "\" [" + segmentInfos.size() + " segments " + "; isCommit = " + isCommit + "]");
    }

    // Try again now to delete any previously un-deletable
    // files (because they were in use, on Windows):
    if (deletable != null) {
      List oldDeletable = deletable;
      deletable = null;
      int size = oldDeletable.size();
      for(int i=0;i<size;i++) {
        deleteFile((String) oldDeletable.get(i));
      }
    }

    // Incref the files:
    incRef(segmentInfos, isCommit);
    if (docWriter != null)
      incRef(docWriter.files());

    if (isCommit) {
      // Append to our commits list:
      commits.add(new CommitPoint(segmentInfos));

      // Tell policy so it can remove commits:
      policy.onCommit(commits);

      // Decref files for commits that were deleted by the policy:
      deleteCommits();
    }

    // DecRef old files from the last checkpoint, if any:
    int size = lastFiles.size();
    if (size > 0) {
      for(int i=0;i<size;i++)
        decRef((List) lastFiles.get(i));
      lastFiles.clear();
    }

    if (!isCommit) {
      // Save files so we can decr on next checkpoint/commit:
      size = segmentInfos.size();
      for(int i=0;i<size;i++) {
        SegmentInfo segmentInfo = segmentInfos.info(i);
        if (segmentInfo.dir == directory) {
          lastFiles.add(segmentInfo.files());
        }
      }
      if (docWriter != null)
        lastFiles.add(docWriter.files());
    }
  }

  void incRef(SegmentInfos segmentInfos, boolean isCommit) throws IOException {
    int size = segmentInfos.size();
    for(int i=0;i<size;i++) {
      SegmentInfo segmentInfo = segmentInfos.info(i);
      if (segmentInfo.dir == directory) {
        incRef(segmentInfo.files());
      }
    }

    if (isCommit) {
      // Since this is a commit point, also incref its
      // segments_N file:
      getRefCount(segmentInfos.getCurrentSegmentFileName()).IncRef();
    }
  }

  private void incRef(List files) throws IOException {
    int size = files.size();
    for(int i=0;i<size;i++) {
      String fileName = (String) files.get(i);
      RefCount rc = getRefCount(fileName);
      if (infoStream != null) {
        message("  IncRef \"" + fileName + "\": pre-incr count is " + rc.count);
      }
      rc.IncRef();
    }
  }

  private void decRef(List files) throws IOException {
    int size = files.size();
    for(int i=0;i<size;i++) {
      decRef((String) files.get(i));
    }
  }

  private void decRef(String fileName) throws IOException {
    RefCount rc = getRefCount(fileName);
    if (infoStream != null) {
      message("  DecRef \"" + fileName + "\": pre-decr count is " + rc.count);
    }
    if (0 == rc.DecRef()) {
      // This file is no longer referenced by any past
      // commit points nor by the in-memory SegmentInfos:
      deleteFile(fileName);
      refCounts.remove(fileName);
    }
  }

  void decRef(SegmentInfos segmentInfos) throws IOException {
    final int size = segmentInfos.size();
    for(int i=0;i<size;i++) {
      SegmentInfo segmentInfo = segmentInfos.info(i);
      if (segmentInfo.dir == directory) {
        decRef(segmentInfo.files());
      }
    }
  }

  private RefCount getRefCount(String fileName) {
    RefCount rc;
    if (!refCounts.containsKey(fileName)) {
      rc = new RefCount();
      refCounts.put(fileName, rc);
    } else {
      rc = (RefCount) refCounts.get(fileName);
    }
    return rc;
  }

  private void deleteFile(String fileName)
       throws IOException {
    try {
      if (infoStream != null) {
        message("delete \"" + fileName + "\"");
      }
      directory.deleteFile(fileName);
    } catch (IOException e) {			  // if delete fails
      if (directory.fileExists(fileName)) {

        // Some operating systems (e.g. Windows) don't
        // permit a file to be deleted while it is opened
        // for read (e.g. by another process or thread). So
        // we assume that when a delete fails it is because
        // the file is open in another process, and queue
        // the file for subsequent deletion.

        if (infoStream != null) {
          message("IndexFileDeleter: unable to remove file \"" + fileName + "\": " + e.toString() + "; Will re-try later.");
        }
        if (deletable == null) {
          deletable = new ArrayList();
        }
        deletable.add(fileName);                  // add to deletable
      }
    }
  }

  /**
   * Blindly delete the files used by the specific segments,
   * with no reference counting and no retry.  This is only
   * currently used by writer to delete its RAM segments
   * from a RAMDirectory.
   */
  public void deleteDirect(Directory otherDir, List segments) throws IOException {
    int size = segments.size();
    for(int i=0;i<size;i++) {
      List filestoDelete = ((SegmentInfo) segments.get(i)).files();
      int size2 = filestoDelete.size();
      for(int j=0;j<size2;j++) {
        otherDir.deleteFile((String) filestoDelete.get(j));
      }
    }
  }

  /**
   * Tracks the reference count for a single index file:
   */
  final private static class RefCount {

    int count;

    final private int IncRef() {
      return ++count;
    }

    final private int DecRef() {
      return --count;
    }
  }

  /**
   * Holds details for each commit point.  This class is
   * also passed to the deletion policy.  Note: this class
   * has a natural ordering that is inconsistent with
   * equals.
   */

  final private class CommitPoint implements Comparable, IndexCommitPoint {

    long gen;
    List files;
    String segmentsFileName;
    boolean deleted;

    public CommitPoint(SegmentInfos segmentInfos) throws IOException {
      segmentsFileName = segmentInfos.getCurrentSegmentFileName();
      int size = segmentInfos.size();
      files = new ArrayList(size);
      gen = segmentInfos.getGeneration();
      for(int i=0;i<size;i++) {
        SegmentInfo segmentInfo = segmentInfos.info(i);
        if (segmentInfo.dir == directory) {
          files.add(segmentInfo.files());
        }
      }
    }

    /**
     * Get the segments_N file for this commit point.
     */
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    /**
     * Called only be the deletion policy, to remove this
     * commit point from the index.
     */
    public void delete() {
      if (!deleted) {
        deleted = true;
        commitsToDelete.add(this);
      }
    }

    public int compareTo(Object obj) {
      CommitPoint commit = (CommitPoint) obj;
      if (gen < commit.gen) {
        return -1;
      } else if (gen > commit.gen) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}

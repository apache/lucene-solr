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

import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.HashSet;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.FSDirectory;

/**
 * IndexReader implementation that has access to a Directory. 
 * Instances that have a SegmentInfos object (i. e. segmentInfos != null)
 * "own" the directory, which means that they try to acquire a write lock
 * whenever index modifications are performed.
 */
abstract class DirectoryIndexReader extends IndexReader {
  protected Directory directory;
  protected boolean closeDirectory;
  private IndexDeletionPolicy deletionPolicy;

  private SegmentInfos segmentInfos;
  private Lock writeLock;
  private boolean stale;
  private final HashSet synced = new HashSet();

  /** Used by commit() to record pre-commit state in case
   * rollback is necessary */
  private boolean rollbackHasChanges;
  private SegmentInfos rollbackSegmentInfos;

  protected boolean readOnly;

  
  void init(Directory directory, SegmentInfos segmentInfos, boolean closeDirectory, boolean readOnly)
    throws IOException {
    this.directory = directory;
    this.segmentInfos = segmentInfos;
    this.closeDirectory = closeDirectory;
    this.readOnly = readOnly;

    if (!readOnly && segmentInfos != null) {
      // We assume that this segments_N was previously
      // properly sync'd:
      for(int i=0;i<segmentInfos.size();i++) {
        final SegmentInfo info = segmentInfos.info(i);
        List files = info.files();
        for(int j=0;j<files.size();j++)
          synced.add(files.get(j));
      }
    }
  }
  
  protected DirectoryIndexReader() {}
  
  DirectoryIndexReader(Directory directory, SegmentInfos segmentInfos,
                       boolean closeDirectory, boolean readOnly) throws IOException {
    super();
    init(directory, segmentInfos, closeDirectory, readOnly);
  }
  
  static DirectoryIndexReader open(final Directory directory, final boolean closeDirectory, final IndexDeletionPolicy deletionPolicy) throws CorruptIndexException, IOException {
    return open(directory, closeDirectory, deletionPolicy, null, false);
  }

  static DirectoryIndexReader open(final Directory directory, final boolean closeDirectory, final IndexDeletionPolicy deletionPolicy, final IndexCommit commit, final boolean readOnly) throws CorruptIndexException, IOException {

    SegmentInfos.FindSegmentsFile finder = new SegmentInfos.FindSegmentsFile(directory) {

      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {

        SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName);

        DirectoryIndexReader reader;

        if (infos.size() == 1) {          // index is optimized
          reader = SegmentReader.get(readOnly, infos, infos.info(0), false);
        } else if (readOnly) {
          reader = new ReadOnlyMultiSegmentReader(directory, infos, false);
        } else {
          reader = new MultiSegmentReader(directory, infos, false, false);
        }
        reader.setDeletionPolicy(deletionPolicy);
        reader.closeDirectory = closeDirectory;
        return reader;
      }
    };

    DirectoryIndexReader reader = null;
    try {
      if (commit == null)
        reader = (DirectoryIndexReader) finder.run();
      else {
        if (directory != commit.getDirectory())
          throw new IOException("the specified commit does not match the specified Directory");
        // This can & will directly throw IOException if the
        // specified commit point has been deleted:
        reader = (DirectoryIndexReader) finder.doBody(commit.getSegmentsFileName());
      }
    } finally {
      // We passed false above for closeDirectory so that
      // the directory would not be closed before we were
      // done retrying, so at this point if we truly failed
      // to open a reader, which means an exception is being
      // thrown, then close the directory now:
      if (reader == null && closeDirectory) {
        try {
          directory.close();
        } catch (IOException ioe) {
          // suppress, so we keep throwing original failure
          // from opening the reader
        }
      }
    }

    return reader;
  }

  public final synchronized IndexReader reopen() throws CorruptIndexException, IOException {
    ensureOpen();

    if (this.hasChanges || this.isCurrent()) {
      // this has changes, therefore we have the lock and don't need to reopen
      // OR: the index in the directory hasn't changed - nothing to do here
      return this;
    }

    final SegmentInfos.FindSegmentsFile finder = new SegmentInfos.FindSegmentsFile(directory) {

      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName);

        DirectoryIndexReader newReader = doReopen(infos);
        
        if (DirectoryIndexReader.this != newReader) {
          newReader.init(directory, infos, closeDirectory, readOnly);
          newReader.deletionPolicy = deletionPolicy;
        }

        return newReader;
      }
    };

    DirectoryIndexReader reader = null;

    // While trying to reopen, we temporarily mark our
    // closeDirectory as false.  This way any exceptions hit
    // partway while opening the reader, which is expected
    // eg if writer is committing, won't close our
    // directory.  We restore this value below:
    final boolean myCloseDirectory = closeDirectory;
    closeDirectory = false;

    try {
      reader = (DirectoryIndexReader) finder.run();
    } finally {
      if (myCloseDirectory) {
        assert directory instanceof FSDirectory;
        // Restore my closeDirectory
        closeDirectory = true;
        if (reader != null && reader != this) {
          // Success, and a new reader was actually opened
          reader.closeDirectory = true;
          // Clone the directory
          reader.directory = FSDirectory.getDirectory(((FSDirectory) directory).getFile());
        }
      }
    }

    return reader;
  }

  /**
   * Re-opens the index using the passed-in SegmentInfos 
   */
  protected abstract DirectoryIndexReader doReopen(SegmentInfos infos) throws CorruptIndexException, IOException;
  
  public void setDeletionPolicy(IndexDeletionPolicy deletionPolicy) {
    this.deletionPolicy = deletionPolicy;
  }
  
  /** Returns the directory this index resides in.
   */
  public Directory directory() {
    ensureOpen();
    return directory;
  }

  /**
   * Version number when this IndexReader was opened.
   */
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  /**
   * Check whether this IndexReader is still using the
   * current (i.e., most recently committed) version of the
   * index.  If a writer has committed any changes to the
   * index since this reader was opened, this will return
   * <code>false</code>, in which case you must open a new
   * IndexReader in order to see the changes.  See the
   * description of the <a href="IndexWriter.html#autoCommit"><code>autoCommit</code></a>
   * flag which controls when the {@link IndexWriter}
   * actually commits changes to the index.
   * 
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public boolean isCurrent() throws CorruptIndexException, IOException {
    ensureOpen();
    return SegmentInfos.readCurrentVersion(directory) == segmentInfos.getVersion();
  }

  /**
   * Checks is the index is optimized (if it has a single segment and no deletions)
   * @return <code>true</code> if the index is optimized; <code>false</code> otherwise
   */
  public boolean isOptimized() {
    ensureOpen();
    return segmentInfos.size() == 1 && hasDeletions() == false;
  }

  protected void doClose() throws IOException {
    if(closeDirectory)
      directory.close();
  }
  
  /**
   * Commit changes resulting from delete, undeleteAll, or
   * setNorm operations
   *
   * If an exception is hit, then either no changes or all
   * changes will have been committed to the index
   * (transactional semantics).
   * @throws IOException if there is a low-level IO error
   */
  protected void doCommit() throws IOException {
    if (hasChanges) {
      if (segmentInfos != null) {

        // Default deleter (for backwards compatibility) is
        // KeepOnlyLastCommitDeleter:
        IndexFileDeleter deleter =  new IndexFileDeleter(directory,
                                                         deletionPolicy == null ? new KeepOnlyLastCommitDeletionPolicy() : deletionPolicy,
                                                         segmentInfos, null, null);

        // Checkpoint the state we are about to change, in
        // case we have to roll back:
        startCommit();

        boolean success = false;
        try {
          commitChanges();

          // Sync all files we just wrote
          for(int i=0;i<segmentInfos.size();i++) {
            final SegmentInfo info = segmentInfos.info(i);
            final List files = info.files();
            for(int j=0;j<files.size();j++) {
              final String fileName = (String) files.get(j);
              if (!synced.contains(fileName)) {
                assert directory.fileExists(fileName);
                directory.sync(fileName);
                synced.add(fileName);
              }
            }
          }

          segmentInfos.commit(directory);
          success = true;
        } finally {

          if (!success) {

            // Rollback changes that were made to
            // SegmentInfos but failed to get [fully]
            // committed.  This way this reader instance
            // remains consistent (matched to what's
            // actually in the index):
            rollbackCommit();

            // Recompute deletable files & remove them (so
            // partially written .del files, etc, are
            // removed):
            deleter.refresh();
          }
        }

        // Have the deleter remove any now unreferenced
        // files due to this commit:
        deleter.checkpoint(segmentInfos, true);

        if (writeLock != null) {
          writeLock.release();  // release write lock
          writeLock = null;
        }
      }
      else
        commitChanges();
    }
    hasChanges = false;
  }
  
  protected abstract void commitChanges() throws IOException;
  
  /**
   * Tries to acquire the WriteLock on this directory.
   * this method is only valid if this IndexReader is directory owner.
   * 
   * @throws StaleReaderException if the index has changed
   * since this reader was opened
   * @throws CorruptIndexException if the index is corrupt
   * @throws LockObtainFailedException if another writer
   *  has this index open (<code>write.lock</code> could not
   *  be obtained)
   * @throws IOException if there is a low-level IO error
   */
  protected void acquireWriteLock() throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    if (segmentInfos != null) {
      ensureOpen();
      if (stale)
        throw new StaleReaderException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");
  
      if (writeLock == null) {
        Lock writeLock = directory.makeLock(IndexWriter.WRITE_LOCK_NAME);
        if (!writeLock.obtain(IndexWriter.WRITE_LOCK_TIMEOUT)) // obtain write lock
          throw new LockObtainFailedException("Index locked for write: " + writeLock);
        this.writeLock = writeLock;
  
        // we have to check whether index has changed since this reader was opened.
        // if so, this reader is no longer valid for deletion
        if (SegmentInfos.readCurrentVersion(directory) > segmentInfos.getVersion()) {
          stale = true;
          this.writeLock.release();
          this.writeLock = null;
          throw new StaleReaderException("IndexReader out of date and no longer valid for delete, undelete, or setNorm operations");
        }
      }
    }
  }

  /**
   * Should internally checkpoint state that will change
   * during commit so that we can rollback if necessary.
   */
  void startCommit() {
    if (segmentInfos != null) {
      rollbackSegmentInfos = (SegmentInfos) segmentInfos.clone();
    }
    rollbackHasChanges = hasChanges;
  }

  /**
   * Rolls back state to just before the commit (this is
   * called by commit() if there is some exception while
   * committing).
   */
  void rollbackCommit() {
    if (segmentInfos != null) {
      for(int i=0;i<segmentInfos.size();i++) {
        // Rollback each segmentInfo.  Because the
        // SegmentReader holds a reference to the
        // SegmentInfo we can't [easily] just replace
        // segmentInfos, so we reset it in place instead:
        segmentInfos.info(i).reset(rollbackSegmentInfos.info(i));
      }
      rollbackSegmentInfos = null;
    }

    hasChanges = rollbackHasChanges;
  }

  /** Release the write lock, if needed. */
  protected void finalize() throws Throwable {
    try {
      if (writeLock != null) {
        writeLock.release();                        // release write lock
        writeLock = null;
      }
    } finally {
      super.finalize();
    }
  }

  private static class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection files;
    Directory dir;
    long generation;
    long version;
    final boolean isOptimized;

    ReaderCommit(SegmentInfos infos, Directory dir) throws IOException {
      segmentsFileName = infos.getCurrentSegmentFileName();
      this.dir = dir;
      final int size = infos.size();
      files = new ArrayList(size);
      files.add(segmentsFileName);
      for(int i=0;i<size;i++) {
        SegmentInfo info = infos.info(i);
        if (info.dir == dir)
          files.addAll(info.files());
      }
      version = infos.getVersion();
      generation = infos.getGeneration();
      isOptimized = infos.size() == 1 && !infos.info(0).hasDeletions();
    }

    public boolean isOptimized() {
      return isOptimized;
    }
    public String getSegmentsFileName() {
      return segmentsFileName;
    }
    public Collection getFileNames() {
      return files;
    }
    public Directory getDirectory() {
      return dir;
    }
    public long getVersion() {
      return version;
    }
    public long getGeneration() {
      return generation;
    }
    public boolean isDeleted() {
      return false;
    }
  }

  /**
   * Expert: return the IndexCommit that this reader has
   * opened.
   *
   * <p><b>WARNING</b>: this API is new and experimental and
   * may suddenly change.</p>
   */
  public IndexCommit getIndexCommit() throws IOException {
    return new ReaderCommit(segmentInfos, directory);
  }

  /** @see IndexReader#listCommits */
  public static Collection listCommits(Directory dir) throws IOException {

    final String[] files = dir.list();
    if (files == null)
      throw new IOException("cannot read directory " + dir + ": list() returned null");

    Collection commits = new ArrayList();

    SegmentInfos latest = new SegmentInfos();
    latest.read(dir);
    final long currentGen = latest.getGeneration();

    commits.add(new ReaderCommit(latest, dir));
    
    for(int i=0;i<files.length;i++) {

      final String fileName = files[i];

      if (fileName.startsWith(IndexFileNames.SEGMENTS) &&
          !fileName.equals(IndexFileNames.SEGMENTS_GEN) &&
          SegmentInfos.generationFromSegmentsFileName(fileName) < currentGen) {

        SegmentInfos sis = new SegmentInfos();
        try {
          // IOException allowed to throw there, in case
          // segments_N is corrupt
          sis.read(dir, fileName);
        } catch (FileNotFoundException fnfe) {
          // LUCENE-948: on NFS (and maybe others), if
          // you have writers switching back and forth
          // between machines, it's very likely that the
          // dir listing will be stale and will claim a
          // file segments_X exists when in fact it
          // doesn't.  So, we catch this and handle it
          // as if the file does not exist
          sis = null;
        }

        if (sis != null)
          commits.add(new ReaderCommit(sis, dir));
      }
    }

    return commits;
  }
}

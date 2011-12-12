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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MapBackedSet;

/** 
 * An IndexReader which reads indexes with multiple segments.
 */
final class DirectoryReader extends BaseMultiReader<SegmentReader> {
  protected final Directory directory;
  private final IndexWriter writer;
  private final SegmentInfos segmentInfos;
  private final int termInfosIndexDivisor;
  private final boolean applyAllDeletes;
  
  DirectoryReader(SegmentReader[] readers, Directory directory, IndexWriter writer,
    SegmentInfos sis, int termInfosIndexDivisor, boolean applyAllDeletes,
    Collection<ReaderFinishedListener> readerFinishedListeners
  ) throws IOException {
    super(readers);
    this.directory = directory;
    this.writer = writer;
    this.segmentInfos = sis;
    this.termInfosIndexDivisor = termInfosIndexDivisor;
    this.readerFinishedListeners = readerFinishedListeners;
    this.applyAllDeletes = applyAllDeletes;
  }

  static IndexReader open(final Directory directory, final IndexCommit commit,
                          final int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return (IndexReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        final Collection<ReaderFinishedListener> readerFinishedListeners =
          new MapBackedSet<ReaderFinishedListener>(new ConcurrentHashMap<ReaderFinishedListener,Boolean>());
        SegmentInfos sis = new SegmentInfos();
        sis.read(directory, segmentFileName);
        final SegmentReader[] readers = new SegmentReader[sis.size()];
        for (int i = sis.size()-1; i >= 0; i--) {
          IOException prior = null;
          boolean success = false;
          try {
            readers[i] = SegmentReader.get(sis.info(i), termInfosIndexDivisor, IOContext.READ);
            readers[i].readerFinishedListeners = readerFinishedListeners;
            success = true;
          } catch(IOException ex) {
            prior = ex;
          } finally {
            if (!success)
              IOUtils.closeWhileHandlingException(prior, readers);
          }
        }
        return new DirectoryReader(readers, directory, null, sis, termInfosIndexDivisor,
          false, readerFinishedListeners);
      }
    }.run(commit);
  }

  // Used by near real-time search
  static DirectoryReader open(IndexWriter writer, SegmentInfos infos, boolean applyAllDeletes) throws IOException {
    // IndexWriter synchronizes externally before calling
    // us, which ensures infos will not change; so there's
    // no need to process segments in reverse order
    final int numSegments = infos.size();

    List<SegmentReader> readers = new ArrayList<SegmentReader>();
    final Directory dir = writer.getDirectory();

    final SegmentInfos segmentInfos = (SegmentInfos) infos.clone();
    int infosUpto = 0;
    for (int i=0;i<numSegments;i++) {
      IOException prior = null;
      boolean success = false;
      try {
        final SegmentInfo info = infos.info(i);
        assert info.dir == dir;
        final SegmentReader reader = writer.readerPool.getReadOnlyClone(info, IOContext.READ);
        if (reader.numDocs() > 0 || writer.getKeepFullyDeletedSegments()) {
          reader.readerFinishedListeners = writer.getReaderFinishedListeners();
          readers.add(reader);
          infosUpto++;
        } else {
          reader.close();
          segmentInfos.remove(infosUpto);
        }
        success = true;
      } catch(IOException ex) {
        prior = ex;
      } finally {
        if (!success)
          IOUtils.closeWhileHandlingException(prior, readers);
      }
    }
    return new DirectoryReader(readers.toArray(new SegmentReader[readers.size()]),
      dir, writer, segmentInfos, writer.getConfig().getReaderTermsIndexDivisor(),
      applyAllDeletes, writer.getReaderFinishedListeners());
  }

  /** This constructor is only used for {@link #doOpenIfChanged()} */
  static DirectoryReader open(Directory directory, IndexWriter writer, SegmentInfos infos, SegmentReader[] oldReaders,
    boolean doClone, int termInfosIndexDivisor, Collection<ReaderFinishedListener> readerFinishedListeners
  ) throws IOException {
    // we put the old SegmentReaders in a map, that allows us
    // to lookup a reader using its segment name
    final Map<String,Integer> segmentReaders = new HashMap<String,Integer>();

    if (oldReaders != null) {
      // create a Map SegmentName->SegmentReader
      for (int i = 0; i < oldReaders.length; i++) {
        segmentReaders.put(oldReaders[i].getSegmentName(), Integer.valueOf(i));
      }
    }
    
    SegmentReader[] newReaders = new SegmentReader[infos.size()];
    
    // remember which readers are shared between the old and the re-opened
    // DirectoryReader - we have to incRef those readers
    boolean[] readerShared = new boolean[infos.size()];
    
    for (int i = infos.size() - 1; i>=0; i--) {
      // find SegmentReader for this segment
      Integer oldReaderIndex = segmentReaders.get(infos.info(i).name);
      if (oldReaderIndex == null) {
        // this is a new segment, no old SegmentReader can be reused
        newReaders[i] = null;
      } else {
        // there is an old reader for this segment - we'll try to reopen it
        newReaders[i] = oldReaders[oldReaderIndex.intValue()];
      }

      boolean success = false;
      IOException prior = null;
      try {
        SegmentReader newReader;
        if (newReaders[i] == null || infos.info(i).getUseCompoundFile() != newReaders[i].getSegmentInfo().getUseCompoundFile()) {

          // We should never see a totally new segment during cloning
          assert !doClone;

          // this is a new reader; in case we hit an exception we can close it safely
          newReader = SegmentReader.get(infos.info(i), termInfosIndexDivisor, IOContext.READ);
          newReader.readerFinishedListeners = readerFinishedListeners;
          readerShared[i] = false;
          newReaders[i] = newReader;
        } else {
          newReader = newReaders[i].reopenSegment(infos.info(i), doClone);
          if (newReader == null) {
            // this reader will be shared between the old and the new one,
            // so we must incRef it
            readerShared[i] = true;
            newReaders[i].incRef();
          } else {
            assert newReader.readerFinishedListeners == readerFinishedListeners;
            readerShared[i] = false;
            // Steal ref returned to us by reopenSegment:
            newReaders[i] = newReader;
          }
        }
        success = true;
      } catch (IOException ex) {
        prior = ex;
      } finally {
        if (!success) {
          for (i++; i < infos.size(); i++) {
            if (newReaders[i] != null) {
              try {
                if (!readerShared[i]) {
                  // this is a new subReader that is not used by the old one,
                  // we can close it
                  newReaders[i].close();
                } else {
                  // this subReader is also used by the old reader, so instead
                  // closing we must decRef it
                  newReaders[i].decRef();
                }
              } catch (IOException ex) {
                if (prior == null) prior = ex;
              }
            }
          }
        }
        // throw the first exception
        if (prior != null) throw prior;
      }
    }    
    return new DirectoryReader(newReaders,
      directory, writer, infos, termInfosIndexDivisor,
      false, readerFinishedListeners);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final String segmentsFile = segmentInfos.getCurrentSegmentFileName();
    if (segmentsFile != null) {
      buffer.append(segmentsFile).append(":").append(segmentInfos.getVersion());
    }
    if (writer != null) {
      buffer.append(":nrt");
    }
    for(int i=0;i<subReaders.length;i++) {
      buffer.append(' ');
      buffer.append(subReaders[i]);
    }
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public final synchronized Object clone() {
    try {
      DirectoryReader newReader = doOpenIfChanged((SegmentInfos) segmentInfos.clone(), true, writer);
      assert newReader.readerFinishedListeners != null;
      return newReader;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected final IndexReader doOpenIfChanged() throws CorruptIndexException, IOException {
    return doOpenIfChanged(null);
  }

  @Override
  protected final IndexReader doOpenIfChanged(final IndexCommit commit) throws CorruptIndexException, IOException {
    ensureOpen();

    // If we were obtained by writer.getReader(), re-ask the
    // writer to get a new reader.
    if (writer != null) {
      return doOpenFromWriter(commit);
    } else {
      return doOpenNoWriter(commit);
    }
  }

  @Override
  protected final IndexReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
    ensureOpen();
    if (writer == this.writer && applyAllDeletes == this.applyAllDeletes) {
      return doOpenFromWriter(null);
    } else {
      // fail by calling supers impl throwing UOE
      return super.doOpenIfChanged(writer, applyAllDeletes);
    }
  }

  private final IndexReader doOpenFromWriter(IndexCommit commit) throws CorruptIndexException, IOException {
    if (commit != null) {
      throw new IllegalArgumentException("a reader obtained from IndexWriter.getReader() cannot currently accept a commit");
    }

    if (writer.nrtIsCurrent(segmentInfos)) {
      return null;
    }

    IndexReader reader = writer.getReader(applyAllDeletes);

    // If in fact no changes took place, return null:
    if (reader.getVersion() == segmentInfos.getVersion()) {
      reader.decRef();
      return null;
    }

    reader.readerFinishedListeners = readerFinishedListeners;
    return reader;
  }

  private synchronized IndexReader doOpenNoWriter(IndexCommit commit) throws CorruptIndexException, IOException {

    if (commit == null) {
      if (isCurrent()) {
        return null;
      }
    } else {
      if (directory != commit.getDirectory()) {
        throw new IOException("the specified commit does not match the specified Directory");
      }
      if (segmentInfos != null && commit.getSegmentsFileName().equals(segmentInfos.getCurrentSegmentFileName())) {
        return null;
      }
    }

    return (IndexReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        final SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName);
        return doOpenIfChanged(infos, false, null);
      }
    }.run(commit);
  }

  private synchronized DirectoryReader doOpenIfChanged(SegmentInfos infos, boolean doClone, IndexWriter writer) throws CorruptIndexException, IOException {
    return DirectoryReader.open(directory, writer, infos, subReaders, doClone, termInfosIndexDivisor, readerFinishedListeners);
  }

  /** Version number when this IndexReader was opened. */
  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  @Override
  public Map<String,String> getCommitUserData() {
    ensureOpen();
    return segmentInfos.getUserData();
  }

  @Override
  public boolean isCurrent() throws CorruptIndexException, IOException {
    ensureOpen();
    if (writer == null || writer.isClosed()) {
      // we loaded SegmentInfos from the directory
      return SegmentInfos.readCurrentVersion(directory) == segmentInfos.getVersion();
    } else {
      return writer.nrtIsCurrent(segmentInfos);
    }
  }

  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (int i = 0; i < subReaders.length; i++) {
      // try to close each reader, even if an exception is thrown
      try {
        subReaders[i].decRef();
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }

    if (writer != null) {
      // Since we just closed, writer may now be able to
      // delete unused files:
      writer.deletePendingFiles();
    }

    // throw the first exception
    if (ioe != null) throw ioe;
  }

  /** Returns the directory this index resides in. */
  @Override
  public Directory directory() {
    // Don't ensureOpen here -- in certain cases, when a
    // cloned/reopened reader needs to commit, it may call
    // this method on the closed original reader
    return directory;
  }

  @Override
  public int getTermInfosIndexDivisor() {
    ensureOpen();
    return termInfosIndexDivisor;
  }

  /**
   * Expert: return the IndexCommit that this reader has opened.
   * <p/>
   * @lucene.experimental
   */
  @Override
  public IndexCommit getIndexCommit() throws IOException {
    ensureOpen();
    return new ReaderCommit(segmentInfos, directory);
  }

  /** @see org.apache.lucene.index.IndexReader#listCommits */
  public static List<IndexCommit> listCommits(Directory dir) throws IOException {
    final String[] files = dir.listAll();

    List<IndexCommit> commits = new ArrayList<IndexCommit>();

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

    // Ensure that the commit points are sorted in ascending order.
    Collections.sort(commits);

    return commits;
  }  
  
  private static final class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection<String> files;
    Directory dir;
    long generation;
    long version;
    final Map<String,String> userData;
    private final int segmentCount;

    ReaderCommit(SegmentInfos infos, Directory dir) throws IOException {
      segmentsFileName = infos.getCurrentSegmentFileName();
      this.dir = dir;
      userData = infos.getUserData();
      files = Collections.unmodifiableCollection(infos.files(dir, true));
      version = infos.getVersion();
      generation = infos.getGeneration();
      segmentCount = infos.size();
    }

    @Override
    public String toString() {
      return "DirectoryReader.ReaderCommit(" + segmentsFileName + ")";
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
      return dir;
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public boolean isDeleted() {
      return false;
    }

    @Override
    public Map<String,String> getUserData() {
      return userData;
    }

    @Override
    public void delete() {
      throw new UnsupportedOperationException("This IndexCommit does not support deletions");
    }
  }
}

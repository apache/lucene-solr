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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.MapBackedSet;

/** 
 * An IndexReader which reads indexes with multiple segments.
 */
class DirectoryReader extends IndexReader implements Cloneable {
  protected Directory directory;

  IndexWriter writer;

  private final SegmentInfos segmentInfos;
  private final int termInfosIndexDivisor;

  private SegmentReader[] subReaders;
  private ReaderContext topLevelReaderContext;
  private int[] starts;                           // 1st docno for each segment
  private int maxDoc = 0;
  private int numDocs = -1;
  private boolean hasDeletions = false;

  private final boolean applyAllDeletes;

  static IndexReader open(final Directory directory, final IndexCommit commit,
                          final int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return (IndexReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName);
        return new DirectoryReader(directory, infos, termInfosIndexDivisor);
      }
    }.run(commit);
  }
  
  /** Construct reading the named set of readers. */
  DirectoryReader(Directory directory, SegmentInfos sis, int termInfosIndexDivisor) throws IOException {
    this.directory = directory;
    this.segmentInfos = sis;
    this.termInfosIndexDivisor = termInfosIndexDivisor;
    readerFinishedListeners = new MapBackedSet<ReaderFinishedListener>(new ConcurrentHashMap<ReaderFinishedListener,Boolean>());
    applyAllDeletes = false;

    // To reduce the chance of hitting FileNotFound
    // (and having to retry), we open segments in
    // reverse because IndexWriter merges & deletes
    // the newest segments first.

    SegmentReader[] readers = new SegmentReader[sis.size()];
    for (int i = sis.size()-1; i >= 0; i--) {
      boolean success = false;
      try {
        readers[i] = SegmentReader.get(sis.info(i), termInfosIndexDivisor, IOContext.READ);
        readers[i].readerFinishedListeners = readerFinishedListeners;
        success = true;
      } finally {
        if (!success) {
          // Close all readers we had opened:
          for(i++;i<sis.size();i++) {
            try {
              readers[i].close();
            } catch (Throwable ignore) {
              // keep going - we want to clean up as much as possible
            }
          }
        }
      }
    }

    initialize(readers);
  }

  // Used by near real-time search
  DirectoryReader(IndexWriter writer, SegmentInfos infos, boolean applyAllDeletes) throws IOException {
    this.directory = writer.getDirectory();
    this.applyAllDeletes = applyAllDeletes;       // saved for reopen

    this.termInfosIndexDivisor = writer.getConfig().getReaderTermsIndexDivisor();
    readerFinishedListeners = writer.getReaderFinishedListeners();

    // IndexWriter synchronizes externally before calling
    // us, which ensures infos will not change; so there's
    // no need to process segments in reverse order
    final int numSegments = infos.size();

    List<SegmentReader> readers = new ArrayList<SegmentReader>();
    final Directory dir = writer.getDirectory();

    segmentInfos = (SegmentInfos) infos.clone();
    int infosUpto = 0;
    for (int i=0;i<numSegments;i++) {
      boolean success = false;
      try {
        final SegmentInfo info = infos.info(i);
        assert info.dir == dir;
        final SegmentReader reader = writer.readerPool.getReadOnlyClone(info, IOContext.READ);
        if (reader.numDocs() > 0 || writer.getKeepFullyDeletedSegments()) {
          reader.readerFinishedListeners = readerFinishedListeners;
          readers.add(reader);
          infosUpto++;
        } else {
          reader.close();
          segmentInfos.remove(infosUpto);
        }
        success = true;
      } finally {
        if (!success) {
          // Close all readers we had opened:
          for(SegmentReader reader : readers) {
            try {
              reader.close();
            } catch (Throwable ignore) {
              // keep going - we want to clean up as much as possible
            }
          }
        }
      }
    }

    this.writer = writer;

    initialize(readers.toArray(new SegmentReader[readers.size()]));
  }

  /** This constructor is only used for {@link #doOpenIfChanged()} */
  DirectoryReader(Directory directory, SegmentInfos infos, SegmentReader[] oldReaders,
                  boolean doClone, int termInfosIndexDivisor, Collection<ReaderFinishedListener> readerFinishedListeners) throws IOException {
    this.directory = directory;
    this.segmentInfos = infos;
    this.termInfosIndexDivisor = termInfosIndexDivisor;
    this.readerFinishedListeners = readerFinishedListeners;
    applyAllDeletes = false;

    // we put the old SegmentReaders in a map, that allows us
    // to lookup a reader using its segment name
    Map<String,Integer> segmentReaders = new HashMap<String,Integer>();

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
              } catch (IOException ignore) {
                // keep going - we want to clean up as much as possible
              }
            }
          }
        }
      }
    }    
    
    // initialize the readers to calculate maxDoc before we try to reuse the old normsCache
    initialize(newReaders);
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

  private void initialize(SegmentReader[] subReaders) throws IOException {
    this.subReaders = subReaders;
    starts = new int[subReaders.length + 1];    // build starts array
    final AtomicReaderContext[] subReaderCtx = new AtomicReaderContext[subReaders.length];
    topLevelReaderContext = new CompositeReaderContext(this, subReaderCtx, subReaderCtx);
    final List<Fields> subFields = new ArrayList<Fields>();
    
    for (int i = 0; i < subReaders.length; i++) {
      starts[i] = maxDoc;
      subReaderCtx[i] = new AtomicReaderContext(topLevelReaderContext, subReaders[i], i, maxDoc, i, maxDoc);
      maxDoc += subReaders[i].maxDoc();      // compute maxDocs

      if (subReaders[i].hasDeletions()) {
        hasDeletions = true;
      }
      
      final Fields f = subReaders[i].fields();
      if (f != null) {
        subFields.add(f);
      }
    }
    starts[subReaders.length] = maxDoc;
  }

  @Override
  public Bits getLiveDocs() {
    throw new UnsupportedOperationException("please use MultiFields.getLiveDocs, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level Bits liveDocs");
  }

  @Override
  public final synchronized Object clone() {
    try {
      DirectoryReader newReader = doOpenIfChanged((SegmentInfos) segmentInfos.clone(), true);
      newReader.writer = writer;
      newReader.hasDeletions = hasDeletions;
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
        return doOpenIfChanged(infos, false);
      }
    }.run(commit);
  }

  private synchronized DirectoryReader doOpenIfChanged(SegmentInfos infos, boolean doClone) throws CorruptIndexException, IOException {
    return new DirectoryReader(directory, infos, subReaders, doClone, termInfosIndexDivisor, readerFinishedListeners);
  }

  /** Version number when this IndexReader was opened. */
  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    int i = readerIndex(docID);        // find segment num
    return subReaders[i].getTermVectors(docID - starts[i]); // dispatch to segment
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)

    // NOTE: multiple threads may wind up init'ing
    // numDocs... but that's harmless
    if (numDocs == -1) {        // check cache
      int n = 0;                // cache miss--recompute
      for (int i = 0; i < subReaders.length; i++)
        n += subReaders[i].numDocs();      // sum from readers
      numDocs = n;
    }
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    int i = readerIndex(docID);                          // find segment num
    subReaders[i].document(docID - starts[i], visitor);    // dispatch to segment reader
  }

  @Override
  public boolean hasDeletions() {
    ensureOpen();
    return hasDeletions;
  }

  private int readerIndex(int n) {    // find reader for doc n:
    return readerIndex(n, this.starts, this.subReaders.length);
  }
  
  final static int readerIndex(int n, int[] starts, int numSubReaders) {    // find reader for doc n:
    int lo = 0;                                      // search starts array
    int hi = numSubReaders - 1;                  // for first element less

    while (hi >= lo) {
      int mid = (lo + hi) >>> 1;
      int midValue = starts[mid];
      if (n < midValue)
        hi = mid - 1;
      else if (n > midValue)
        lo = mid + 1;
      else {                                      // found a match
        while (mid+1 < numSubReaders && starts[mid+1] == midValue) {
          mid++;                                  // scan to last match
        }
        return mid;
      }
    }
    return hi;
  }

  @Override
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    for (int i = 0; i < subReaders.length; i++) {
      if (subReaders[i].hasNorms(field)) return true;
    }
    return false;
  }

  @Override
  public synchronized byte[] norms(String field) throws IOException {
    ensureOpen();
    throw new UnsupportedOperationException("please use MultiNorms.norms, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level norms");
  }

  @Override
  public int docFreq(String field, BytesRef term) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in segments
    for (int i = 0; i < subReaders.length; i++) {
      total += subReaders[i].docFreq(field, term);
    }
    return total;
  }

  @Override
  public Fields fields() throws IOException {
    throw new UnsupportedOperationException("please use MultiFields.getFields, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level Fields");
  }

  @Override
  public Map<String,String> getCommitUserData() {
    ensureOpen();
    return segmentInfos.getUserData();
  }

  /**
   * Check whether this IndexReader is still using the current (i.e., most recently committed) version of the index.  If
   * a writer has committed any changes to the index since this reader was opened, this will return <code>false</code>,
   * in which case you must open a new IndexReader in order
   * to see the changes.  Use {@link IndexWriter#commit} to
   * commit changes to the index.
   *
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException           if there is a low-level IO error
   */
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

  @Override
  public Collection<String> getFieldNames (IndexReader.FieldOption fieldNames) {
    ensureOpen();
    return getFieldNames(fieldNames, this.subReaders);
  }
  
  static Collection<String> getFieldNames (IndexReader.FieldOption fieldNames, IndexReader[] subReaders) {
    // maintain a unique set of field names
    Set<String> fieldSet = new HashSet<String>();
    for (IndexReader reader : subReaders) {
      Collection<String> names = reader.getFieldNames(fieldNames);
      fieldSet.addAll(names);
    }
    return fieldSet;
  }
  
  @Override
  public ReaderContext getTopReaderContext() {
    ensureOpen();
    return topLevelReaderContext;
  }
  
  @Override
  public IndexReader[] getSequentialSubReaders() {
    return subReaders;
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

  @Override
  public PerDocValues perDocValues() throws IOException {
    throw new UnsupportedOperationException("please use MultiPerDocValues#getPerDocs, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level Fields");
  }
}

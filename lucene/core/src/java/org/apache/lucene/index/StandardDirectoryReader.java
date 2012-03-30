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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

final class StandardDirectoryReader extends DirectoryReader {

  private final IndexWriter writer;
  private final SegmentInfos segmentInfos;
  private final int termInfosIndexDivisor;
  private final boolean applyAllDeletes;
  
  /** called only from static open() methods */
  StandardDirectoryReader(Directory directory, AtomicReader[] readers, IndexWriter writer,
    SegmentInfos sis, int termInfosIndexDivisor, boolean applyAllDeletes) throws IOException {
    super(directory, readers);
    this.writer = writer;
    this.segmentInfos = sis;
    this.termInfosIndexDivisor = termInfosIndexDivisor;
    this.applyAllDeletes = applyAllDeletes;
  }

  /** called from DirectoryReader.open(...) methods */
  static DirectoryReader open(final Directory directory, final IndexCommit commit,
                          final int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    return (DirectoryReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        SegmentInfos sis = new SegmentInfos();
        sis.read(directory, segmentFileName);
        final SegmentReader[] readers = new SegmentReader[sis.size()];
        for (int i = sis.size()-1; i >= 0; i--) {
          IOException prior = null;
          boolean success = false;
          try {
            readers[i] = new SegmentReader(sis.info(i), termInfosIndexDivisor, IOContext.READ);
            success = true;
          } catch(IOException ex) {
            prior = ex;
          } finally {
            if (!success)
              IOUtils.closeWhileHandlingException(prior, readers);
          }
        }
        return new StandardDirectoryReader(directory, readers, null, sis, termInfosIndexDivisor, false);
      }
    }.run(commit);
  }

  /** Used by near real-time search */
  static DirectoryReader open(IndexWriter writer, SegmentInfos infos, boolean applyAllDeletes) throws IOException {
    // IndexWriter synchronizes externally before calling
    // us, which ensures infos will not change; so there's
    // no need to process segments in reverse order
    final int numSegments = infos.size();

    List<SegmentReader> readers = new ArrayList<SegmentReader>();
    final Directory dir = writer.getDirectory();

    final SegmentInfos segmentInfos = infos.clone();
    int infosUpto = 0;
    for (int i=0;i<numSegments;i++) {
      IOException prior = null;
      boolean success = false;
      try {
        final SegmentInfo info = infos.info(i);
        assert info.dir == dir;
        final ReadersAndLiveDocs rld = writer.readerPool.get(info, true);
        try {
          final SegmentReader reader = rld.getReadOnlyClone(IOContext.READ);
          if (reader.numDocs() > 0 || writer.getKeepFullyDeletedSegments()) {
            // Steal the ref:
            readers.add(reader);
            infosUpto++;
          } else {
            reader.close();
            segmentInfos.remove(infosUpto);
          }
        } finally {
          writer.readerPool.release(rld);
        }
        success = true;
      } catch(IOException ex) {
        prior = ex;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(prior, readers);
        }
      }
    }
    return new StandardDirectoryReader(dir, readers.toArray(new SegmentReader[readers.size()]),
      writer, segmentInfos, writer.getConfig().getReaderTermsIndexDivisor(), applyAllDeletes);
  }

  /** This constructor is only used for {@link #doOpenIfChanged()} */
  private static DirectoryReader open(Directory directory, IndexWriter writer, SegmentInfos infos, AtomicReader[] oldReaders,
    int termInfosIndexDivisor) throws IOException {
    // we put the old SegmentReaders in a map, that allows us
    // to lookup a reader using its segment name
    final Map<String,Integer> segmentReaders = new HashMap<String,Integer>();

    if (oldReaders != null) {
      // create a Map SegmentName->SegmentReader
      for (int i = 0; i < oldReaders.length; i++) {
        segmentReaders.put(((SegmentReader) oldReaders[i]).getSegmentName(), Integer.valueOf(i));
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
        newReaders[i] = (SegmentReader) oldReaders[oldReaderIndex.intValue()];
      }

      boolean success = false;
      IOException prior = null;
      try {
        SegmentReader newReader;
        if (newReaders[i] == null || infos.info(i).getUseCompoundFile() != newReaders[i].getSegmentInfo().getUseCompoundFile()) {

          // this is a new reader; in case we hit an exception we can close it safely
          newReader = new SegmentReader(infos.info(i), termInfosIndexDivisor, IOContext.READ);
          readerShared[i] = false;
          newReaders[i] = newReader;
        } else {
          if (newReaders[i].getSegmentInfo().getDelGen() == infos.info(i).getDelGen()) {
            // No change; this reader will be shared between
            // the old and the new one, so we must incRef
            // it:
            readerShared[i] = true;
            newReaders[i].incRef();
          } else {
            readerShared[i] = false;
            // Steal the ref returned by SegmentReader ctor:
            assert infos.info(i).dir == newReaders[i].getSegmentInfo().dir;
            assert infos.info(i).hasDeletions();
            newReaders[i] = new SegmentReader(infos.info(i), newReaders[i].core, IOContext.READ);
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
    return new StandardDirectoryReader(directory, newReaders, writer, infos, termInfosIndexDivisor, false);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final String segmentsFile = segmentInfos.getSegmentsFileName();
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
  protected DirectoryReader doOpenIfChanged() throws CorruptIndexException, IOException {
    return doOpenIfChanged(null);
  }

  @Override
  protected DirectoryReader doOpenIfChanged(final IndexCommit commit) throws CorruptIndexException, IOException {
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
  protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
    ensureOpen();
    if (writer == this.writer && applyAllDeletes == this.applyAllDeletes) {
      return doOpenFromWriter(null);
    } else {
      return writer.getReader(applyAllDeletes);
    }
  }

  private DirectoryReader doOpenFromWriter(IndexCommit commit) throws CorruptIndexException, IOException {
    if (commit != null) {
      throw new IllegalArgumentException("a reader obtained from IndexWriter.getReader() cannot currently accept a commit");
    }

    if (writer.nrtIsCurrent(segmentInfos)) {
      return null;
    }

    DirectoryReader reader = writer.getReader(applyAllDeletes);

    // If in fact no changes took place, return null:
    if (reader.getVersion() == segmentInfos.getVersion()) {
      reader.decRef();
      return null;
    }

    return reader;
  }

  private synchronized DirectoryReader doOpenNoWriter(IndexCommit commit) throws CorruptIndexException, IOException {

    if (commit == null) {
      if (isCurrent()) {
        return null;
      }
    } else {
      if (directory != commit.getDirectory()) {
        throw new IOException("the specified commit does not match the specified Directory");
      }
      if (segmentInfos != null && commit.getSegmentsFileName().equals(segmentInfos.getSegmentsFileName())) {
        return null;
      }
    }

    return (DirectoryReader) new SegmentInfos.FindSegmentsFile(directory) {
      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        final SegmentInfos infos = new SegmentInfos();
        infos.read(directory, segmentFileName);
        return doOpenIfChanged(infos, null);
      }
    }.run(commit);
  }

  synchronized DirectoryReader doOpenIfChanged(SegmentInfos infos, IndexWriter writer) throws CorruptIndexException, IOException {
    return StandardDirectoryReader.open(directory, writer, infos, subReaders, termInfosIndexDivisor);
  }

  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  @Override
  public boolean isCurrent() throws CorruptIndexException, IOException {
    ensureOpen();
    if (writer == null || writer.isClosed()) {
      // Fully read the segments file: this ensures that it's
      // completely written so that if
      // IndexWriter.prepareCommit has been called (but not
      // yet commit), then the reader will still see itself as
      // current:
      SegmentInfos sis = new SegmentInfos();
      sis.read(directory);

      // we loaded SegmentInfos from the directory
      return sis.getVersion() == segmentInfos.getVersion();
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
  public IndexCommit getIndexCommit() throws IOException {
    ensureOpen();
    return new ReaderCommit(segmentInfos, directory);
  }

  static final class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection<String> files;
    Directory dir;
    long generation;
    final Map<String,String> userData;
    private final int segmentCount;

    ReaderCommit(SegmentInfos infos, Directory dir) throws IOException {
      segmentsFileName = infos.getSegmentsFileName();
      this.dir = dir;
      userData = infos.getUserData();
      files = Collections.unmodifiableCollection(infos.files(dir, true));
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

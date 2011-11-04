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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.ChecksumIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.NoSuchDirectoryException;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A collection of segmentInfo objects with methods for operating on
 * those segments in relation to the file system.
 * 
 * @lucene.experimental
 */
public final class SegmentInfos implements Cloneable, Iterable<SegmentInfo> {

  /** The file format version, a negative number. */
  /* Works since counter, the old 1st entry, is always >= 0 */
  public static final int FORMAT = -1;

  /** This format adds details used for lockless commits.  It differs
   * slightly from the previous format in that file names
   * are never re-used (write once).  Instead, each file is
   * written to the next generation.  For example,
   * segments_1, segments_2, etc.  This allows us to not use
   * a commit lock.  See <a
   * href="http://lucene.apache.org/java/docs/fileformats.html">file
   * formats</a> for details.
   */
  public static final int FORMAT_LOCKLESS = -2;

  /** This format adds a "hasSingleNormFile" flag into each segment info.
   * See <a href="http://issues.apache.org/jira/browse/LUCENE-756">LUCENE-756</a>
   * for details.
   */
  public static final int FORMAT_SINGLE_NORM_FILE = -3;

  /** This format allows multiple segments to share a single
   * vectors and stored fields file. */
  public static final int FORMAT_SHARED_DOC_STORE = -4;

  /** This format adds a checksum at the end of the file to
   *  ensure all bytes were successfully written. */
  public static final int FORMAT_CHECKSUM = -5;

  /** This format adds the deletion count for each segment.
   *  This way IndexWriter can efficiently report numDocs(). */
  public static final int FORMAT_DEL_COUNT = -6;

  /** This format adds the boolean hasProx to record if any
   *  fields in the segment store prox information (ie, have
   *  omitTermFreqAndPositions==false) */
  public static final int FORMAT_HAS_PROX = -7;

  /** This format adds optional commit userData (String) storage. */
  public static final int FORMAT_USER_DATA = -8;

  /** This format adds optional per-segment String
   *  diagnostics storage, and switches userData to Map */
  public static final int FORMAT_DIAGNOSTICS = -9;

  /** Each segment records whether it has term vectors */
  public static final int FORMAT_HAS_VECTORS = -10;

  /** Each segment records the Lucene version that created it. */
  public static final int FORMAT_3_1 = -11;
  
  /* This must always point to the most recent file format. */
  public static final int CURRENT_FORMAT = FORMAT_3_1;

  public static final int FORMAT_MINIMUM = FORMAT;
  public static final int FORMAT_MAXIMUM = CURRENT_FORMAT;
  
  public int counter = 0;    // used to name new segments
  /**
   * counts how often the index has been changed by adding or deleting docs.
   * starting with the current time in milliseconds forces to create unique version numbers.
   */
  long version = System.currentTimeMillis();

  private long generation = 0;     // generation of the "segments_N" for the next commit
  private long lastGeneration = 0; // generation of the "segments_N" file we last successfully read
                                   // or wrote; this is normally the same as generation except if
                                   // there was an IOException that had interrupted a commit

  private Map<String,String> userData = Collections.<String,String>emptyMap();       // Opaque Map<String, String> that user can specify during IndexWriter.commit

  private int format;
  
  private List<SegmentInfo> segments = new ArrayList<SegmentInfo>();
  private Set<SegmentInfo> segmentSet = new HashSet<SegmentInfo>();
  private transient List<SegmentInfo> cachedUnmodifiableList;
  private transient Set<SegmentInfo> cachedUnmodifiableSet;  
  
  /**
   * If non-null, information about loading segments_N files
   * will be printed here.  @see #setInfoStream.
   */
  private static PrintStream infoStream = null;

  public void setFormat(int format) {
    this.format = format;
  }

  public int getFormat() {
    return format;
  }

  public SegmentInfo info(int i) {
    return segments.get(i);
  }

  /**
   * Get the generation (N) of the current segments_N file
   * from a list of files.
   *
   * @param files -- array of file names to check
   */
  public static long getCurrentSegmentGeneration(String[] files) {
    if (files == null) {
      return -1;
    }
    long max = -1;
    for (int i = 0; i < files.length; i++) {
      String file = files[i];
      if (file.startsWith(IndexFileNames.SEGMENTS) && !file.equals(IndexFileNames.SEGMENTS_GEN)) {
        long gen = generationFromSegmentsFileName(file);
        if (gen > max) {
          max = gen;
        }
      }
    }
    return max;
  }

  /**
   * Get the generation (N) of the current segments_N file
   * in the directory.
   *
   * @param directory -- directory to search for the latest segments_N file
   */
  public static long getCurrentSegmentGeneration(Directory directory) throws IOException {
    try {
      return getCurrentSegmentGeneration(directory.listAll());
    } catch (NoSuchDirectoryException nsde) {
      return -1;
    }
  }

  /**
   * Get the filename of the current segments_N file
   * from a list of files.
   *
   * @param files -- array of file names to check
   */

  public static String getCurrentSegmentFileName(String[] files) throws IOException {
    return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                 "",
                                                 getCurrentSegmentGeneration(files));
  }

  /**
   * Get the filename of the current segments_N file
   * in the directory.
   *
   * @param directory -- directory to search for the latest segments_N file
   */
  public static String getCurrentSegmentFileName(Directory directory) throws IOException {
    return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                 "",
                                                 getCurrentSegmentGeneration(directory));
  }

  /**
   * Get the segments_N filename in use by this segment infos.
   */
  public String getCurrentSegmentFileName() {
    return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                 "",
                                                 lastGeneration);
  }

  /**
   * Parse the generation off the segments file name and
   * return it.
   */
  public static long generationFromSegmentsFileName(String fileName) {
    if (fileName.equals(IndexFileNames.SEGMENTS)) {
      return 0;
    } else if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
      return Long.parseLong(fileName.substring(1+IndexFileNames.SEGMENTS.length()),
                            Character.MAX_RADIX);
    } else {
      throw new IllegalArgumentException("fileName \"" + fileName + "\" is not a segments file");
    }
  }


  /**
   * Get the next segments_N filename that will be written.
   */
  public String getNextSegmentFileName() {
    long nextGeneration;

    if (generation == -1) {
      nextGeneration = 1;
    } else {
      nextGeneration = generation+1;
    }
    return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                 "",
                                                 nextGeneration);
  }

  /**
   * Read a particular segmentFileName.  Note that this may
   * throw an IOException if a commit is in process.
   *
   * @param directory -- directory containing the segments file
   * @param segmentFileName -- segment file to load
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public final void read(Directory directory, String segmentFileName) throws CorruptIndexException, IOException {
    boolean success = false;

    // Clear any previous segments:
    this.clear();

    ChecksumIndexInput input = new ChecksumIndexInput(directory.openInput(segmentFileName));

    generation = generationFromSegmentsFileName(segmentFileName);

    lastGeneration = generation;

    try {
      int format = input.readInt();
      // check that it is a format we can understand
      if (format > FORMAT_MINIMUM) {
        throw new IndexFormatTooOldException(input, format,
          FORMAT_MINIMUM, FORMAT_MAXIMUM);
      }
      if (format < FORMAT_MAXIMUM) {
        throw new IndexFormatTooNewException(input, format,
          FORMAT_MINIMUM, FORMAT_MAXIMUM);
      }
      version = input.readLong(); // read version
      counter = input.readInt(); // read counter
      
      for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
        SegmentInfo si = new SegmentInfo(directory, format, input);
        if (si.getVersion() == null) {
          // It's a pre-3.1 segment, upgrade its version to either 3.0 or 2.x
          Directory dir = directory;
          if (si.getDocStoreOffset() != -1) {
            if (si.getDocStoreIsCompoundFile()) {
              dir = new CompoundFileReader(dir, IndexFileNames.segmentFileName(
                  si.getDocStoreSegment(),
                  IndexFileNames.COMPOUND_FILE_STORE_EXTENSION), 1024);
            }
          } else if (si.getUseCompoundFile()) {
            dir = new CompoundFileReader(dir, IndexFileNames.segmentFileName(
                si.name, IndexFileNames.COMPOUND_FILE_EXTENSION), 1024);
          }

          try {
            String store = si.getDocStoreOffset() != -1 ? si.getDocStoreSegment() : si.name;
            si.setVersion(FieldsReader.detectCodeVersion(dir, store));
          } finally {
            // If we opened the directory, close it
            if (dir != directory) dir.close();
          }
        }
        add(si);
      }
      
      if(format >= 0){    // in old format the version number may be at the end of the file
        if (input.getFilePointer() >= input.length())
          version = System.currentTimeMillis(); // old file format without version number
        else
          version = input.readLong(); // read version
      }

      if (format <= FORMAT_USER_DATA) {
        if (format <= FORMAT_DIAGNOSTICS) {
          userData = input.readStringStringMap();
        } else if (0 != input.readByte()) {
          userData = Collections.singletonMap("userData", input.readString());
        } else {
          userData = Collections.<String,String>emptyMap();
        }
      } else {
        userData = Collections.<String,String>emptyMap();
      }

      if (format <= FORMAT_CHECKSUM) {
        final long checksumNow = input.getChecksum();
        final long checksumThen = input.readLong();
        if (checksumNow != checksumThen)
          throw new CorruptIndexException("checksum mismatch in segments file (resource: " + input + ")");
      }
      success = true;
    }
    finally {
      input.close();
      if (!success) {
        // Clear any segment infos we had loaded so we
        // have a clean slate on retry:
        this.clear();
      }
    }
  }

  /**
   * This version of read uses the retry logic (for lock-less
   * commits) to find the right segments file to load.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public final void read(Directory directory) throws CorruptIndexException, IOException {

    generation = lastGeneration = -1;

    new FindSegmentsFile(directory) {

      @Override
      protected Object doBody(String segmentFileName) throws CorruptIndexException, IOException {
        read(directory, segmentFileName);
        return null;
      }
    }.run();
  }

  // Only non-null after prepareCommit has been called and
  // before finishCommit is called
  ChecksumIndexOutput pendingSegnOutput;

  private final void write(Directory directory) throws IOException {

    String segmentFileName = getNextSegmentFileName();

    // Always advance the generation on write:
    if (generation == -1) {
      generation = 1;
    } else {
      generation++;
    }

    ChecksumIndexOutput segnOutput = new ChecksumIndexOutput(directory.createOutput(segmentFileName));

    boolean success = false;

    try {
      segnOutput.writeInt(CURRENT_FORMAT); // write FORMAT
      segnOutput.writeLong(version); 
      segnOutput.writeInt(counter); // write counter
      segnOutput.writeInt(size()); // write infos
      for (SegmentInfo si : this) {
        si.write(segnOutput);
      }
      segnOutput.writeStringStringMap(userData);
      segnOutput.prepareCommit();
      pendingSegnOutput = segnOutput;
      success = true;
    } finally {
      if (!success) {
        // We hit an exception above; try to close the file
        // but suppress any exception:
        IOUtils.closeWhileHandlingException(segnOutput);
        try {
          // Try not to leave a truncated segments_N file in
          // the index:
          directory.deleteFile(segmentFileName);
        } catch (Throwable t) {
          // Suppress so we keep throwing the original exception
        }
      }
    }
  }

  /** Prunes any segment whose docs are all deleted. */
  public void pruneDeletedSegments() throws IOException {
    for(final Iterator<SegmentInfo> it = segments.iterator(); it.hasNext();) {
      final SegmentInfo info = it.next();
      if (info.getDelCount() == info.docCount) {
        it.remove();
        segmentSet.remove(info);
      }
    }
    assert segmentSet.size() == segments.size();
  }

  /**
   * Returns a copy of this instance, also copying each
   * SegmentInfo.
   */
  
  @Override
  public Object clone() {
    try {
      final SegmentInfos sis = (SegmentInfos) super.clone();
      // deep clone, first recreate all collections:
      sis.segments = new ArrayList<SegmentInfo>(size());
      sis.segmentSet = new HashSet<SegmentInfo>(size());
      sis.cachedUnmodifiableList = null;
      sis.cachedUnmodifiableSet = null;
      for(final SegmentInfo info : this) {
        // dont directly access segments, use add method!!!
        sis.add((SegmentInfo) info.clone());
      }
      sis.userData = new HashMap<String,String>(userData);
      return sis;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("should not happen", e);
    }
  }

  /**
   * version number when this SegmentInfos was generated.
   */
  public long getVersion() {
    return version;
  }
  public long getGeneration() {
    return generation;
  }
  public long getLastGeneration() {
    return lastGeneration;
  }

  /**
   * Current version number from segments file.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static long readCurrentVersion(Directory directory)
    throws CorruptIndexException, IOException {

    // Fully read the segments file: this ensures that it's
    // completely written so that if
    // IndexWriter.prepareCommit has been called (but not
    // yet commit), then the reader will still see itself as
    // current:
    SegmentInfos sis = new SegmentInfos();
    sis.read(directory);
    return sis.version;
  }

  /**
   * Returns userData from latest segments file
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static Map<String,String> readCurrentUserData(Directory directory)
    throws CorruptIndexException, IOException {
    SegmentInfos sis = new SegmentInfos();
    sis.read(directory);
    return sis.getUserData();
  }

  /** If non-null, information about retries when loading
   * the segments file will be printed to this.
   */
  public static void setInfoStream(PrintStream infoStream) {
    SegmentInfos.infoStream = infoStream;
  }

  /* Advanced configuration of retry logic in loading
     segments_N file */
  private static int defaultGenFileRetryCount = 10;
  private static int defaultGenFileRetryPauseMsec = 50;
  private static int defaultGenLookaheadCount = 10;

  /**
   * Advanced: set how many times to try loading the
   * segments.gen file contents to determine current segment
   * generation.  This file is only referenced when the
   * primary method (listing the directory) fails.
   */
  public static void setDefaultGenFileRetryCount(int count) {
    defaultGenFileRetryCount = count;
  }

  /**
   * @see #setDefaultGenFileRetryCount
   */
  public static int getDefaultGenFileRetryCount() {
    return defaultGenFileRetryCount;
  }

  /**
   * Advanced: set how many milliseconds to pause in between
   * attempts to load the segments.gen file.
   */
  public static void setDefaultGenFileRetryPauseMsec(int msec) {
    defaultGenFileRetryPauseMsec = msec;
  }

  /**
   * @see #setDefaultGenFileRetryPauseMsec
   */
  public static int getDefaultGenFileRetryPauseMsec() {
    return defaultGenFileRetryPauseMsec;
  }

  /**
   * Advanced: set how many times to try incrementing the
   * gen when loading the segments file.  This only runs if
   * the primary (listing directory) and secondary (opening
   * segments.gen file) methods fail to find the segments
   * file.
   */
  public static void setDefaultGenLookaheadCount(int count) {
    defaultGenLookaheadCount = count;
  }
  /**
   * @see #setDefaultGenLookaheadCount
   */
  public static int getDefaultGenLookahedCount() {
    return defaultGenLookaheadCount;
  }

  /**
   * @see #setInfoStream
   */
  public static PrintStream getInfoStream() {
    return infoStream;
  }

  /**
   * Prints the given message to the infoStream. Note, this method does not
   * check for null infoStream. It assumes this check has been performed by the
   * caller, which is recommended to avoid the (usually) expensive message
   * creation.
   */
  private static void message(String message) {
    infoStream.println("SIS [" + Thread.currentThread().getName() + "]: " + message);
  }

  /**
   * Utility class for executing code that needs to do
   * something with the current segments file.  This is
   * necessary with lock-less commits because from the time
   * you locate the current segments file name, until you
   * actually open it, read its contents, or check modified
   * time, etc., it could have been deleted due to a writer
   * commit finishing.
   */
  public abstract static class FindSegmentsFile {
    
    final Directory directory;

    public FindSegmentsFile(Directory directory) {
      this.directory = directory;
    }

    public Object run() throws CorruptIndexException, IOException {
      return run(null);
    }
    
    public Object run(IndexCommit commit) throws CorruptIndexException, IOException {
      if (commit != null) {
        if (directory != commit.getDirectory())
          throw new IOException("the specified commit does not match the specified Directory");
        return doBody(commit.getSegmentsFileName());
      }

      String segmentFileName = null;
      long lastGen = -1;
      long gen = 0;
      int genLookaheadCount = 0;
      IOException exc = null;
      int retryCount = 0;

      boolean useFirstMethod = true;

      // Loop until we succeed in calling doBody() without
      // hitting an IOException.  An IOException most likely
      // means a commit was in process and has finished, in
      // the time it took us to load the now-old infos files
      // (and segments files).  It's also possible it's a
      // true error (corrupt index).  To distinguish these,
      // on each retry we must see "forward progress" on
      // which generation we are trying to load.  If we
      // don't, then the original error is real and we throw
      // it.
      
      // We have three methods for determining the current
      // generation.  We try the first two in parallel (when
      // useFirstMethod is true), and fall back to the third
      // when necessary.

      while(true) {

        if (useFirstMethod) {

          // List the directory and use the highest
          // segments_N file.  This method works well as long
          // as there is no stale caching on the directory
          // contents (NOTE: NFS clients often have such stale
          // caching):
          String[] files = null;

          long genA = -1;

          files = directory.listAll();
          
          if (files != null) {
            genA = getCurrentSegmentGeneration(files);
          }
          
          if (infoStream != null) {
            message("directory listing genA=" + genA);
          }

          // Also open segments.gen and read its
          // contents.  Then we take the larger of the two
          // gens.  This way, if either approach is hitting
          // a stale cache (NFS) we have a better chance of
          // getting the right generation.
          long genB = -1;
          for(int i=0;i<defaultGenFileRetryCount;i++) {
            IndexInput genInput = null;
            try {
              genInput = directory.openInput(IndexFileNames.SEGMENTS_GEN);
            } catch (FileNotFoundException e) {
              if (infoStream != null) {
                message("segments.gen open: FileNotFoundException " + e);
              }
              break;
            } catch (IOException e) {
              if (infoStream != null) {
                message("segments.gen open: IOException " + e);
              }
            }
  
            if (genInput != null) {
              try {
                int version = genInput.readInt();
                if (version == FORMAT_LOCKLESS) {
                  long gen0 = genInput.readLong();
                  long gen1 = genInput.readLong();
                  if (infoStream != null) {
                    message("fallback check: " + gen0 + "; " + gen1);
                  }
                  if (gen0 == gen1) {
                    // The file is consistent.
                    genB = gen0;
                    break;
                  }
                }
              } catch (IOException err2) {
                // will retry
              } finally {
                genInput.close();
              }
            }
            try {
              Thread.sleep(defaultGenFileRetryPauseMsec);
            } catch (InterruptedException ie) {
              throw new ThreadInterruptedException(ie);
            }
          }

          if (infoStream != null) {
            message(IndexFileNames.SEGMENTS_GEN + " check: genB=" + genB);
          }

          // Pick the larger of the two gen's:
          if (genA > genB)
            gen = genA;
          else
            gen = genB;

          if (gen == -1) {
            // Neither approach found a generation
            throw new IndexNotFoundException("no segments* file found in " + directory + ": files: " + Arrays.toString(files));
          }
        }

        if (useFirstMethod && lastGen == gen && retryCount >= 2) {
          // Give up on first method -- this is 3rd cycle on
          // listing directory and checking gen file to
          // attempt to locate the segments file.
          useFirstMethod = false;
        }

        // Second method: since both directory cache and
        // file contents cache seem to be stale, just
        // advance the generation.
        if (!useFirstMethod) {
          if (genLookaheadCount < defaultGenLookaheadCount) {
            gen++;
            genLookaheadCount++;
            if (infoStream != null) {
              message("look ahead increment gen to " + gen);
            }
          } else {
            // All attempts have failed -- throw first exc:
            throw exc;
          }
        } else if (lastGen == gen) {
          // This means we're about to try the same
          // segments_N last tried.
          retryCount++;
        } else {
          // Segment file has advanced since our last loop
          // (we made "progress"), so reset retryCount:
          retryCount = 0;
        }

        lastGen = gen;

        segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                "",
                                                                gen);

        try {
          Object v = doBody(segmentFileName);
          if (infoStream != null) {
            message("success on " + segmentFileName);
          }
          return v;
        } catch (IOException err) {

          // Save the original root cause:
          if (exc == null) {
            exc = err;
          }

          if (infoStream != null) {
            message("primary Exception on '" + segmentFileName + "': " + err + "'; will retry: retryCount=" + retryCount + "; gen = " + gen);
          }

          if (gen > 1 && useFirstMethod && retryCount == 1) {

            // This is our second time trying this same segments
            // file (because retryCount is 1), and, there is
            // possibly a segments_(N-1) (because gen > 1).
            // So, check if the segments_(N-1) exists and
            // try it if so:
            String prevSegmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                               "",
                                                                               gen-1);

            final boolean prevExists;
            prevExists = directory.fileExists(prevSegmentFileName);

            if (prevExists) {
              if (infoStream != null) {
                message("fallback to prior segment file '" + prevSegmentFileName + "'");
              }
              try {
                Object v = doBody(prevSegmentFileName);
                if (infoStream != null) {
                  message("success on fallback " + prevSegmentFileName);
                }
                return v;
              } catch (IOException err2) {
                if (infoStream != null) {
                  message("secondary Exception on '" + prevSegmentFileName + "': " + err2 + "'; will retry");
                }
              }
            }
          }
        }
      }
    }

    /**
     * Subclass must implement this.  The assumption is an
     * IOException will be thrown if something goes wrong
     * during the processing that could have been caused by
     * a writer committing.
     */
    protected abstract Object doBody(String segmentFileName) throws CorruptIndexException, IOException;
  }

  /**
   * Returns a new SegmentInfos containing the SegmentInfo
   * instances in the specified range first (inclusive) to
   * last (exclusive), so total number of segments returned
   * is last-first.
   * @deprecated use {@code asList().subList(first, last)}
   * instead.
   */
  @Deprecated
  public SegmentInfos range(int first, int last) {
    SegmentInfos infos = new SegmentInfos();
    infos.addAll(segments.subList(first, last));
    return infos;
  }

  // Carry over generation numbers from another SegmentInfos
  void updateGeneration(SegmentInfos other) {
    lastGeneration = other.lastGeneration;
    generation = other.generation;
  }

  final void rollbackCommit(Directory dir) throws IOException {
    if (pendingSegnOutput != null) {
      try {
        pendingSegnOutput.close();
      } catch (Throwable t) {
        // Suppress so we keep throwing the original exception
        // in our caller
      }

      // Must carefully compute fileName from "generation"
      // since lastGeneration isn't incremented:
      try {
        final String segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                              "",
                                                                             generation);
        dir.deleteFile(segmentFileName);
      } catch (Throwable t) {
        // Suppress so we keep throwing the original exception
        // in our caller
      }
      pendingSegnOutput = null;
    }
  }

  /** Call this to start a commit.  This writes the new
   *  segments file, but writes an invalid checksum at the
   *  end, so that it is not visible to readers.  Once this
   *  is called you must call {@link #finishCommit} to complete
   *  the commit or {@link #rollbackCommit} to abort it.
   *  <p>
   *  Note: {@link #changed()} should be called prior to this
   *  method if changes have been made to this {@link SegmentInfos} instance
   *  </p>  
   **/
  final void prepareCommit(Directory dir) throws IOException {
    if (pendingSegnOutput != null)
      throw new IllegalStateException("prepareCommit was already called");
    write(dir);
  }

  /** Returns all file names referenced by SegmentInfo
   *  instances matching the provided Directory (ie files
   *  associated with any "external" segments are skipped).
   *  The returned collection is recomputed on each
   *  invocation.  */
  public Collection<String> files(Directory dir, boolean includeSegmentsFile) throws IOException {
    HashSet<String> files = new HashSet<String>();
    if (includeSegmentsFile) {
      files.add(getCurrentSegmentFileName());
    }
    final int size = size();
    for(int i=0;i<size;i++) {
      final SegmentInfo info = info(i);
      if (info.dir == dir) {
        files.addAll(info(i).files());
      }
    }
    return files;
  }

  final void finishCommit(Directory dir) throws IOException {
    if (pendingSegnOutput == null)
      throw new IllegalStateException("prepareCommit was not called");
    boolean success = false;
    try {
      pendingSegnOutput.finishCommit();
      pendingSegnOutput.close();
      pendingSegnOutput = null;
      success = true;
    } finally {
      if (!success)
        rollbackCommit(dir);
    }

    // NOTE: if we crash here, we have left a segments_N
    // file in the directory in a possibly corrupt state (if
    // some bytes made it to stable storage and others
    // didn't).  But, the segments_N file includes checksum
    // at the end, which should catch this case.  So when a
    // reader tries to read it, it will throw a
    // CorruptIndexException, which should cause the retry
    // logic in SegmentInfos to kick in and load the last
    // good (previous) segments_N-1 file.

    final String fileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                  "",
                                                                  generation);
    success = false;
    try {
      dir.sync(Collections.singleton(fileName));
      success = true;
    } finally {
      if (!success) {
        try {
          dir.deleteFile(fileName);
        } catch (Throwable t) {
          // Suppress so we keep throwing the original exception
        }
      }
    }

    lastGeneration = generation;

    try {
      IndexOutput genOutput = dir.createOutput(IndexFileNames.SEGMENTS_GEN);
      try {
        genOutput.writeInt(FORMAT_LOCKLESS);
        genOutput.writeLong(generation);
        genOutput.writeLong(generation);
      } finally {
        genOutput.close();
      }
    } catch (ThreadInterruptedException t) {
      throw t;
    } catch (Throwable t) {
      // It's OK if we fail to write this file since it's
      // used only as one of the retry fallbacks.
    }
  }

  /** Writes & syncs to the Directory dir, taking care to
   *  remove the segments file on exception
   *  <p>
   *  Note: {@link #changed()} should be called prior to this
   *  method if changes have been made to this {@link SegmentInfos} instance
   *  </p>  
   **/
  final void commit(Directory dir) throws IOException {
    prepareCommit(dir);
    finishCommit(dir);
  }

  public String toString(Directory directory) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(getCurrentSegmentFileName()).append(": ");
    final int count = size();
    for(int i = 0; i < count; i++) {
      if (i > 0) {
        buffer.append(' ');
      }
      final SegmentInfo info = info(i);
      buffer.append(info.toString(directory, 0));
    }
    return buffer.toString();
  }

  public Map<String,String> getUserData() {
    return userData;
  }

  void setUserData(Map<String,String> data) {
    if (data == null) {
      userData = Collections.<String,String>emptyMap();
    } else {
      userData = data;
    }
  }

  /** Replaces all segments in this instance, but keeps
   *  generation, version, counter so that future commits
   *  remain write once.
   */
  void replace(SegmentInfos other) {
    rollbackSegmentInfos(other.asList());
    lastGeneration = other.lastGeneration;
  }

  /** Returns sum of all segment's docCounts.  Note that
   *  this does not include deletions */
  public int totalDocCount() {
    int count = 0;
    for(SegmentInfo info : this) {
      count += info.docCount;
    }
    return count;
  }

  /** Call this before committing if changes have been made to the
   *  segments. */
  public void changed() {
    version++;
  }
  
  /** applies all changes caused by committing a merge to this SegmentInfos */
  void applyMergeChanges(MergePolicy.OneMerge merge, boolean dropSegment) {
    final Set<SegmentInfo> mergedAway = new HashSet<SegmentInfo>(merge.segments);
    boolean inserted = false;
    int newSegIdx = 0;
    for (int segIdx = 0, cnt = segments.size(); segIdx < cnt; segIdx++) {
      assert segIdx >= newSegIdx;
      final SegmentInfo info = segments.get(segIdx);
      if (mergedAway.contains(info)) {
        if (!inserted && !dropSegment) {
          segments.set(segIdx, merge.info);
          inserted = true;
          newSegIdx++;
        }
      } else {
        segments.set(newSegIdx, info);
        newSegIdx++;
      }
    }

    // Either we found place to insert segment, or, we did
    // not, but only because all segments we merged became
    // deleted while we are merging, in which case it should
    // be the case that the new segment is also all deleted,
    // we insert it at the beginning if it should not be dropped:
    if (!inserted && !dropSegment) {
      segments.add(0, merge.info);
    }

    // the rest of the segments in list are duplicates, so don't remove from map, only list!
    segments.subList(newSegIdx, segments.size()).clear();
    
    // update the Set
    if (!dropSegment) {
      segmentSet.add(merge.info);
    }
    segmentSet.removeAll(mergedAway);
    
    assert segmentSet.size() == segments.size();
  }

  List<SegmentInfo> createBackupSegmentInfos(boolean cloneChildren) {
    if (cloneChildren) {
      final List<SegmentInfo> list = new ArrayList<SegmentInfo>(size());
      for(final SegmentInfo info : this) {
        list.add((SegmentInfo) info.clone());
      }
      return list;
    } else {
      return new ArrayList<SegmentInfo>(segments);
    }
  }
  
  void rollbackSegmentInfos(List<SegmentInfo> infos) {
    this.clear();
    this.addAll(infos);
  }
  
  /** Returns an <b>unmodifiable</b> {@link Iterator} of contained segments in order. */
  // @Override (comment out until Java 6)
  public Iterator<SegmentInfo> iterator() {
    return asList().iterator();
  }
  
  /** Returns all contained segments as an <b>unmodifiable</b> {@link List} view. */
  public List<SegmentInfo> asList() {
    if (cachedUnmodifiableList == null) {
      cachedUnmodifiableList = Collections.unmodifiableList(segments);
    }
    return cachedUnmodifiableList;
  }
  
  /** Returns all contained segments as an <b>unmodifiable</b> {@link Set} view.
   * The iterator is not sorted, use {@link List} view or {@link #iterator} to get all segments in order. */
  public Set<SegmentInfo> asSet() {
    if (cachedUnmodifiableSet == null) {
      cachedUnmodifiableSet = Collections.unmodifiableSet(segmentSet);
    }
    return cachedUnmodifiableSet;
  }
  
  public int size() {
    return segments.size();
  }

  public void add(SegmentInfo si) {
    if (segmentSet.contains(si)) {
      throw new IllegalStateException("Cannot add the same segment two times to this SegmentInfos instance");
    }
    segments.add(si);
    segmentSet.add(si);
    assert segmentSet.size() == segments.size();
  }
  
  public void addAll(Iterable<SegmentInfo> sis) {
    for (final SegmentInfo si : sis) {
      this.add(si);
    }
  }
  
  public void clear() {
    segments.clear();
    segmentSet.clear();
  }
  
  public void remove(SegmentInfo si) {
    final int index = this.indexOf(si);
    if (index >= 0) {
      this.remove(index);
    }
  }
  
  public void remove(int index) {
    segmentSet.remove(segments.remove(index));
    assert segmentSet.size() == segments.size();
  }
  
  public boolean contains(SegmentInfo si) {
    return segmentSet.contains(si);
  }

  public int indexOf(SegmentInfo si) {
    if (segmentSet.contains(si)) {
      return segments.indexOf(si);
    } else {
      return -1;
    }
  }

}

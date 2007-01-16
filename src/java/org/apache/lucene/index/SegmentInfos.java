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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

public final class SegmentInfos extends Vector {
  
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

  /** This is the current file format written.  It adds a
   * "hasSingleNormFile" flag into each segment info.
   * See <a href="http://issues.apache.org/jira/browse/LUCENE-756">LUCENE-756</a>
   * for details.
   */
  public static final int FORMAT_SINGLE_NORM_FILE = -3;

  public int counter = 0;    // used to name new segments
  /**
   * counts how often the index has been changed by adding or deleting docs.
   * starting with the current time in milliseconds forces to create unique version numbers.
   */
  private long version = System.currentTimeMillis();

  private long generation = 0;     // generation of the "segments_N" for the next commit
  private long lastGeneration = 0; // generation of the "segments_N" file we last successfully read
                                   // or wrote; this is normally the same as generation except if
                                   // there was an IOException that had interrupted a commit

  /**
   * If non-null, information about loading segments_N files
   * will be printed here.  @see #setInfoStream.
   */
  private static PrintStream infoStream;

  public final SegmentInfo info(int i) {
    return (SegmentInfo) elementAt(i);
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
    int prefixLen = IndexFileNames.SEGMENTS.length()+1;
    for (int i = 0; i < files.length; i++) {
      String file = files[i];
      if (file.startsWith(IndexFileNames.SEGMENTS) && !file.equals(IndexFileNames.SEGMENTS_GEN)) {
        if (file.equals(IndexFileNames.SEGMENTS)) {
          // Pre lock-less commits:
          if (max == -1) {
            max = 0;
          }
        } else {
          long v = Long.parseLong(file.substring(prefixLen), Character.MAX_RADIX);
          if (v > max) {
            max = v;
          }
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
    String[] files = directory.list();
    if (files == null)
      throw new IOException("Cannot read directory " + directory);
    return getCurrentSegmentGeneration(files);
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
   */
  public final void read(Directory directory, String segmentFileName) throws IOException {
    boolean success = false;

    IndexInput input = directory.openInput(segmentFileName);

    if (segmentFileName.equals(IndexFileNames.SEGMENTS)) {
      generation = 0;
    } else {
      generation = Long.parseLong(segmentFileName.substring(1+IndexFileNames.SEGMENTS.length()),
                                  Character.MAX_RADIX);
    }
    lastGeneration = generation;

    try {
      int format = input.readInt();
      if(format < 0){     // file contains explicit format info
        // check that it is a format we can understand
        if (format < FORMAT_SINGLE_NORM_FILE)
          throw new IOException("Unknown format version: " + format);
        version = input.readLong(); // read version
        counter = input.readInt(); // read counter
      }
      else{     // file is in old format without explicit format info
        counter = format;
      }
      
      for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
        addElement(new SegmentInfo(directory, format, input));
      }
      
      if(format >= 0){    // in old format the version number may be at the end of the file
        if (input.getFilePointer() >= input.length())
          version = System.currentTimeMillis(); // old file format without version number
        else
          version = input.readLong(); // read version
      }
      success = true;
    }
    finally {
      input.close();
      if (!success) {
        // Clear any segment infos we had loaded so we
        // have a clean slate on retry:
        clear();
      }
    }
  }
  /**
   * This version of read uses the retry logic (for lock-less
   * commits) to find the right segments file to load.
   */
  public final void read(Directory directory) throws IOException {

    generation = lastGeneration = -1;

    new FindSegmentsFile(directory) {

      public Object doBody(String segmentFileName) throws IOException {
        read(directory, segmentFileName);
        return null;
      }
    }.run();
  }

  public final void write(Directory directory) throws IOException {

    String segmentFileName = getNextSegmentFileName();

    // Always advance the generation on write:
    if (generation == -1) {
      generation = 1;
    } else {
      generation++;
    }

    IndexOutput output = directory.createOutput(segmentFileName);

    try {
      output.writeInt(FORMAT_SINGLE_NORM_FILE); // write FORMAT
      output.writeLong(++version); // every write changes
                                   // the index
      output.writeInt(counter); // write counter
      output.writeInt(size()); // write infos
      for (int i = 0; i < size(); i++) {
        info(i).write(output);
      }         
    }
    finally {
      output.close();
    }

    try {
      output = directory.createOutput(IndexFileNames.SEGMENTS_GEN);
      try {
        output.writeInt(FORMAT_LOCKLESS);
        output.writeLong(generation);
        output.writeLong(generation);
      } finally {
        output.close();
      }
    } catch (IOException e) {
      // It's OK if we fail to write this file since it's
      // used only as one of the retry fallbacks.
    }
    
    lastGeneration = generation;
  }

  /**
   * Returns a copy of this instance, also copying each
   * SegmentInfo.
   */
  
  public Object clone() {
    SegmentInfos sis = (SegmentInfos) super.clone();
    for(int i=0;i<sis.size();i++) {
      sis.setElementAt(((SegmentInfo) sis.elementAt(i)).clone(), i);
    }
    return sis;
  }

  /**
   * version number when this SegmentInfos was generated.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Current version number from segments file.
   */
  public static long readCurrentVersion(Directory directory)
    throws IOException {

    return ((Long) new FindSegmentsFile(directory) {
        public Object doBody(String segmentFileName) throws IOException {

          IndexInput input = directory.openInput(segmentFileName);

          int format = 0;
          long version = 0;
          try {
            format = input.readInt();
            if(format < 0){
              if (format < FORMAT_SINGLE_NORM_FILE)
                throw new IOException("Unknown format version: " + format);
              version = input.readLong(); // read version
            }
          }
          finally {
            input.close();
          }
     
          if(format < 0)
            return new Long(version);

          // We cannot be sure about the format of the file.
          // Therefore we have to read the whole file and cannot simply seek to the version entry.
          SegmentInfos sis = new SegmentInfos();
          sis.read(directory, segmentFileName);
          return new Long(sis.getVersion());
        }
      }.run()).longValue();
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

  private static void message(String message) {
    if (infoStream != null) {
      infoStream.println(Thread.currentThread().getName() + ": " + message);
    }
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
    
    File fileDirectory;
    Directory directory;

    public FindSegmentsFile(File directory) {
      this.fileDirectory = directory;
    }

    public FindSegmentsFile(Directory directory) {
      this.directory = directory;
    }

    public Object run() throws IOException {
      String segmentFileName = null;
      long lastGen = -1;
      long gen = 0;
      int genLookaheadCount = 0;
      IOException exc = null;
      boolean retry = false;

      int method = 0;

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
      // generation.  We try each in sequence.

      while(true) {

        // Method 1: list the directory and use the highest
        // segments_N file.  This method works well as long
        // as there is no stale caching on the directory
        // contents:
        String[] files = null;

        if (0 == method) {
          if (directory != null) {
            files = directory.list();
          } else {
            files = fileDirectory.list();
          }

          gen = getCurrentSegmentGeneration(files);

          if (gen == -1) {
            String s = "";
            for(int i=0;i<files.length;i++) {
              s += " " + files[i];
            }
            throw new FileNotFoundException("no segments* file found: files:" + s);
          }
        }

        // Method 2 (fallback if Method 1 isn't reliable):
        // if the directory listing seems to be stale, then
        // try loading the "segments.gen" file.
        if (1 == method || (0 == method && lastGen == gen && retry)) {

          method = 1;
            
          for(int i=0;i<defaultGenFileRetryCount;i++) {
            IndexInput genInput = null;
            try {
              genInput = directory.openInput(IndexFileNames.SEGMENTS_GEN);
            } catch (IOException e) {
              message("segments.gen open: IOException " + e);
            }
            if (genInput != null) {

              try {
                int version = genInput.readInt();
                if (version == FORMAT_LOCKLESS) {
                  long gen0 = genInput.readLong();
                  long gen1 = genInput.readLong();
                  message("fallback check: " + gen0 + "; " + gen1);
                  if (gen0 == gen1) {
                    // The file is consistent.
                    if (gen0 > gen) {
                      message("fallback to '" + IndexFileNames.SEGMENTS_GEN + "' check: now try generation " + gen0 + " > " + gen);
                      gen = gen0;
                    }
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
            } catch (InterruptedException e) {
              // will retry
            }
          }
        }

        // Method 3 (fallback if Methods 2 & 3 are not
        // reliable): since both directory cache and file
        // contents cache seem to be stale, just advance the
        // generation.
        if (2 == method || (1 == method && lastGen == gen && retry)) {

          method = 2;

          if (genLookaheadCount < defaultGenLookaheadCount) {
            gen++;
            genLookaheadCount++;
            message("look ahead increment gen to " + gen);
          }
        }

        if (lastGen == gen) {

          // This means we're about to try the same
          // segments_N last tried.  This is allowed,
          // exactly once, because writer could have been in
          // the process of writing segments_N last time.

          if (retry) {
            // OK, we've tried the same segments_N file
            // twice in a row, so this must be a real
            // error.  We throw the original exception we
            // got.
            throw exc;
          } else {
            retry = true;
          }

        } else {
          // Segment file has advanced since our last loop, so
          // reset retry:
          retry = false;
        }

        lastGen = gen;

        segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                "",
                                                                gen);

        try {
          Object v = doBody(segmentFileName);
          if (exc != null) {
            message("success on " + segmentFileName);
          }
          return v;
        } catch (IOException err) {

          // Save the original root cause:
          if (exc == null) {
            exc = err;
          }

          message("primary Exception on '" + segmentFileName + "': " + err + "'; will retry: retry=" + retry + "; gen = " + gen);

          if (!retry && gen > 1) {

            // This is our first time trying this segments
            // file (because retry is false), and, there is
            // possibly a segments_(N-1) (because gen > 1).
            // So, check if the segments_(N-1) exists and
            // try it if so:
            String prevSegmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                               "",
                                                                               gen-1);
            
            if (directory.fileExists(prevSegmentFileName)) {
              message("fallback to prior segment file '" + prevSegmentFileName + "'");
              try {
                Object v = doBody(prevSegmentFileName);
                if (exc != null) {
                  message("success on fallback " + prevSegmentFileName);
                }
                return v;
              } catch (IOException err2) {
                message("secondary Exception on '" + prevSegmentFileName + "': " + err2 + "'; will retry");
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
    protected abstract Object doBody(String segmentFileName) throws IOException;}
}

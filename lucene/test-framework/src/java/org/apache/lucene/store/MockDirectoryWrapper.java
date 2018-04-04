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
package org.apache.lucene.store;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThrottledIndexOutput;

/**
 * This is a Directory Wrapper that adds methods
 * intended to be used only by unit tests.
 * It also adds a number of features useful for testing:
 * <ul>
 *   <li> Instances created by {@link LuceneTestCase#newDirectory()} are tracked 
 *        to ensure they are closed by the test.
 *   <li> When a MockDirectoryWrapper is closed, it will throw an exception if 
 *        it has any open files against it (with a stacktrace indicating where 
 *        they were opened from).
 *   <li> When a MockDirectoryWrapper is closed, it runs CheckIndex to test if
 *        the index was corrupted.
 *   <li> MockDirectoryWrapper simulates some "features" of Windows, such as
 *        refusing to write/delete to open files.
 * </ul>
 */
public class MockDirectoryWrapper extends BaseDirectoryWrapper {
  long maxSize;

  // Max actual bytes used. This is set by MockRAMOutputStream:
  long maxUsedSize;
  double randomIOExceptionRate;
  double randomIOExceptionRateOnOpen;
  Random randomState;
  boolean assertNoDeleteOpenFile = false;
  boolean trackDiskUsage = false;
  boolean useSlowOpenClosers = LuceneTestCase.TEST_NIGHTLY;
  boolean allowRandomFileNotFoundException = true;
  boolean allowReadingFilesStillOpenForWrite = false;
  private Set<String> unSyncedFiles;
  private Set<String> createdFiles;
  private Set<String> openFilesForWrite = new HashSet<>();
  ConcurrentMap<String,RuntimeException> openLocks = new ConcurrentHashMap<>();
  volatile boolean crashed;
  private ThrottledIndexOutput throttledOutput;
  private Throttling throttling = LuceneTestCase.TEST_NIGHTLY ? Throttling.SOMETIMES : Throttling.NEVER;

  // for testing
  boolean alwaysCorrupt;

  final AtomicInteger inputCloneCount = new AtomicInteger();

  // use this for tracking files for crash.
  // additionally: provides debugging information in case you leave one open
  private Map<Closeable,Exception> openFileHandles = Collections.synchronizedMap(new IdentityHashMap<Closeable,Exception>());

  // NOTE: we cannot initialize the Map here due to the
  // order in which our constructor actually does this
  // member initialization vs when it calls super.  It seems
  // like super is called, then our members are initialized:
  private Map<String,Integer> openFiles;

  // Only tracked if noDeleteOpenFile is true: if an attempt
  // is made to delete an open file, we enroll it here.
  private Set<String> openFilesDeleted;
  
  private synchronized void init() {
    if (openFiles == null) {
      openFiles = new HashMap<>();
      openFilesDeleted = new HashSet<>();
    }

    if (createdFiles == null)
      createdFiles = new HashSet<>();
    if (unSyncedFiles == null)
      unSyncedFiles = new HashSet<>();
  }

  public MockDirectoryWrapper(Random random, Directory delegate) {
    super(delegate);
    // must make a private random since our methods are
    // called from different threads; else test failures may
    // not be reproducible from the original seed
    this.randomState = new Random(random.nextInt());
    this.throttledOutput = new ThrottledIndexOutput(ThrottledIndexOutput
        .mBitsToBytes(40 + randomState.nextInt(10)), 1 + randomState.nextInt(5), null);
    init();
  }

  public int getInputCloneCount() {
    return inputCloneCount.get();
  }
  
  boolean verboseClone;
  
  /** 
   * If set to true, we print a fake exception
   * with filename and stacktrace on every indexinput clone()
   */
  public void setVerboseClone(boolean v) {
    verboseClone = v;
  }

  public void setTrackDiskUsage(boolean v) {
    trackDiskUsage = v;
  }

  /** If set to true (the default), when we throw random
   *  IOException on openInput or createOutput, we may
   *  sometimes throw FileNotFoundException or
   *  NoSuchFileException. */
  public void setAllowRandomFileNotFoundException(boolean value) {
    allowRandomFileNotFoundException = value;
  }
  
  /** If set to true, you can open an inputstream on a file
   *  that is still open for writes. */
  public void setAllowReadingFilesStillOpenForWrite(boolean value) {
    allowReadingFilesStillOpenForWrite = value;
  }
  
  /**
   * Enum for controlling hard disk throttling.
   * Set via {@link MockDirectoryWrapper #setThrottling(Throttling)}
   * <p>
   * WARNING: can make tests very slow.
   */
  public static enum Throttling {
    /** always emulate a slow hard disk. could be very slow! */
    ALWAYS,
    /** sometimes (0.5% of the time) emulate a slow hard disk. */
    SOMETIMES,
    /** never throttle output */
    NEVER
  }
  
  public void setThrottling(Throttling throttling) {
    this.throttling = throttling;
  }
  
  /** 
   * Add a rare small sleep to catch race conditions in open/close
   * <p>
   * You can enable this if you need it.
   */
  public void setUseSlowOpenClosers(boolean v) {
    useSlowOpenClosers = v;
  }

  @Override
  public synchronized void sync(Collection<String> names) throws IOException {
    maybeYield();
    maybeThrowDeterministicException();
    if (crashed) {
      throw new IOException("cannot sync after crash");
    }
    // always pass thru fsync, directories rely on this.
    // 90% of time, we use DisableFsyncFS which omits the real calls.
    for (String name : names) {
      // randomly fail with IOE on any file
      maybeThrowIOException(name);
      in.sync(Collections.singleton(name));
      unSyncedFiles.remove(name);
    }
  }

  @Override
  public synchronized void rename(String source, String dest) throws IOException {
    maybeYield();
    maybeThrowDeterministicException();

    if (crashed) {
      throw new IOException("cannot rename after crash");
    }
    
    if (openFiles.containsKey(source) && assertNoDeleteOpenFile) {
      throw (AssertionError) fillOpenTrace(new AssertionError("MockDirectoryWrapper: source file \"" + source + "\" is still open: cannot rename"), source, true);
    }

    if (openFiles.containsKey(dest) && assertNoDeleteOpenFile) {
      throw (AssertionError) fillOpenTrace(new AssertionError("MockDirectoryWrapper: dest file \"" + dest + "\" is still open: cannot rename"), dest, true);
    }

    boolean success = false;
    try {
      in.rename(source, dest);
      success = true;
    } finally {
      if (success) {
        // we don't do this stuff with lucene's commit, but it's just for completeness
        if (unSyncedFiles.contains(source)) {
          unSyncedFiles.remove(source);
          unSyncedFiles.add(dest);
        }
        openFilesDeleted.remove(source);
        createdFiles.remove(source);
        createdFiles.add(dest);
      }
    }
  }

  @Override
  public synchronized void syncMetaData() throws IOException {
    maybeYield();
    maybeThrowDeterministicException();
    if (crashed) {
      throw new IOException("cannot rename after crash");
    }
    in.syncMetaData();
  }

  public synchronized final long sizeInBytes() throws IOException {
    if (in instanceof RAMDirectory)
      return ((RAMDirectory) in).ramBytesUsed();
    else {
      // hack
      long size = 0;
      for (String file : in.listAll()) {
        // hack 2: see TODO in ExtrasFS (ideally it would always return 0 byte
        // size for extras it creates, even though the size of non-regular files is not defined)
        if (!file.startsWith("extra")) {
          size += in.fileLength(file);
        }
      }
      return size;
    }
  }

  public synchronized void corruptUnknownFiles() throws IOException {

    System.out.println("MDW: corrupt unknown files");
    Set<String> knownFiles = new HashSet<>();
    for(String fileName : listAll()) {
      if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
        System.out.println("MDW: read " + fileName + " to gather files it references");
        SegmentInfos infos;
        try {
          infos = SegmentInfos.readCommit(this, fileName);
        } catch (IOException ioe) {
          System.out.println("MDW: exception reading segment infos " + fileName + "; files: " + Arrays.toString(listAll()));
          throw ioe;
        }
        knownFiles.addAll(infos.files(true));
      }
    }

    Set<String> toCorrupt = new HashSet<>();
    Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
    for(String fileName : listAll()) {
      m.reset(fileName);
      if (knownFiles.contains(fileName) == false &&
          fileName.endsWith("write.lock") == false &&
          (m.matches() || fileName.startsWith(IndexFileNames.PENDING_SEGMENTS))) {
        toCorrupt.add(fileName);
      }
    }

    corruptFiles(toCorrupt);
  }

  public synchronized void corruptFiles(Collection<String> files) throws IOException {
    boolean disabled = TestUtil.disableVirusChecker(in);
    try {
      _corruptFiles(files);
    } finally {
      if (disabled) {
        TestUtil.enableVirusChecker(in);
      }
    }
  }
    
  private synchronized void _corruptFiles(Collection<String> files) throws IOException {

    // TODO: we should also mess with any recent file renames, file deletions, if
    // syncMetaData was not called!!
    
    // Must make a copy because we change the incoming unsyncedFiles
    // when we create temp files, delete, etc., below:
    final List<String> filesToCorrupt = new ArrayList<>(files);
    // sort the files otherwise we have reproducibility issues
    // across JVMs if the incoming collection is a hashSet etc.
    CollectionUtil.timSort(filesToCorrupt);
    for(String name : filesToCorrupt) {
      int damage = randomState.nextInt(6);
      if (alwaysCorrupt && damage == 3) {
        damage = 4;
      }
      String action = null;

      switch(damage) {

      case 0:
        action = "deleted";
        deleteFile(name);
        break;

      case 1:
        action = "zeroed";
        // Zero out file entirely
        long length;
        try {
          length = fileLength(name);
        } catch (IOException ioe) {
          throw new RuntimeException("hit unexpected IOException while trying to corrupt file " + name, ioe);
        }

        // Delete original and write zeros back:
        deleteFile(name);
        
        byte[] zeroes = new byte[256];
        long upto = 0;
        try (IndexOutput out = in.createOutput(name, LuceneTestCase.newIOContext(randomState))) {
          while(upto < length) {
            final int limit = (int) Math.min(length-upto, zeroes.length);
            out.writeBytes(zeroes, 0, limit);
            upto += limit;
          }
        } catch (IOException ioe) {
          throw new RuntimeException("hit unexpected IOException while trying to corrupt file " + name, ioe);
        }
        break;

      case 2:
        {
          action = "partially truncated";
          // Partially Truncate the file:

          // First, make temp file and copy only half this
          // file over:
          String tempFileName = null;
          try (IndexOutput tempOut = in.createTempOutput("name", "mdw_corrupt", LuceneTestCase.newIOContext(randomState));
               IndexInput ii = in.openInput(name, LuceneTestCase.newIOContext(randomState))) {
              tempFileName = tempOut.getName();
              tempOut.copyBytes(ii, ii.length()/2);
          } catch (IOException ioe) {
            throw new RuntimeException("hit unexpected IOException while trying to corrupt file " + name, ioe);
          }

          // Delete original and copy bytes back:
          deleteFile(name);

          try (IndexOutput out = in.createOutput(name, LuceneTestCase.newIOContext(randomState));
               IndexInput ii = in.openInput(tempFileName, LuceneTestCase.newIOContext(randomState))) {
              out.copyBytes(ii, ii.length());
          } catch (IOException ioe) {
            throw new RuntimeException("hit unexpected IOException while trying to corrupt file " + name, ioe);
          }
          deleteFile(tempFileName);
        }
        break;
      
      case 3:
        // The file survived intact:
        action = "didn't change";
        break;

      case 4:
        // Corrupt one bit randomly in the file:

        {

          String tempFileName = null;
          try (IndexOutput tempOut = in.createTempOutput("name", "mdw_corrupt", LuceneTestCase.newIOContext(randomState));
               IndexInput ii = in.openInput(name, LuceneTestCase.newIOContext(randomState))) {
            tempFileName = tempOut.getName();
            if (ii.length() > 0) {
              // Copy first part unchanged:
              long byteToCorrupt = (long) (randomState.nextDouble() * ii.length());
              if (byteToCorrupt > 0) {
                tempOut.copyBytes(ii, byteToCorrupt);
              }

              // Randomly flip one bit from this byte:
              byte b = ii.readByte();
              int bitToFlip = randomState.nextInt(8);
              b = (byte) (b ^ (1 << bitToFlip));
              tempOut.writeByte(b);

              action = "flip bit " + bitToFlip + " of byte " + byteToCorrupt + " out of " + ii.length() + " bytes";

              // Copy last part unchanged:
              long bytesLeft = ii.length() - byteToCorrupt - 1;
              if (bytesLeft > 0) {
                tempOut.copyBytes(ii, bytesLeft);
              }
            } else {
              action = "didn't change";
            }
          } catch (IOException ioe) {
            throw new RuntimeException("hit unexpected IOException while trying to corrupt file " + name, ioe);
          }

          // Delete original and copy bytes back:
          deleteFile(name);

          try (IndexOutput out = in.createOutput(name, LuceneTestCase.newIOContext(randomState));
               IndexInput ii = in.openInput(tempFileName, LuceneTestCase.newIOContext(randomState))) {
            out.copyBytes(ii, ii.length());
          } catch (IOException ioe) {
            throw new RuntimeException("hit unexpected IOException while trying to corrupt file " + name, ioe);
          }

          deleteFile(tempFileName);
        }
        break;
        
      case 5:
        action = "fully truncated";
        // Totally truncate the file to zero bytes
        deleteFile(name);

        try (IndexOutput out = in.createOutput(name, LuceneTestCase.newIOContext(randomState))) {
          out.getFilePointer(); // just fake access to prevent compiler warning
        } catch (IOException ioe) {
          throw new RuntimeException("hit unexpected IOException while trying to corrupt file " + name, ioe);
        }
        break;

      default:
        throw new AssertionError();
      }

      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: " + action + " unsynced file: " + name);
      }
    }
  }

  /** Simulates a crash of OS or machine by overwriting
   *  unsynced files. */
  public synchronized void crash() throws IOException {
    openFiles = new HashMap<>();
    openFilesForWrite = new HashSet<>();
    openFilesDeleted = new HashSet<>();
    // first force-close all files, so we can corrupt on windows etc.
    // clone the file map, as these guys want to remove themselves on close.
    Map<Closeable,Exception> m = new IdentityHashMap<>(openFileHandles);
    for (Closeable f : m.keySet()) {
      try {
        f.close();
      } catch (Exception ignored) {}
    }
    corruptFiles(unSyncedFiles);
    crashed = true;
    unSyncedFiles = new HashSet<>();
  }

  public synchronized void clearCrash() {
    crashed = false;
    openLocks.clear();
  }

  public void setMaxSizeInBytes(long maxSize) {
    this.maxSize = maxSize;
  }
  public long getMaxSizeInBytes() {
    return this.maxSize;
  }

  /**
   * Returns the peek actual storage used (bytes) in this
   * directory.
   */
  public long getMaxUsedSizeInBytes() {
    return this.maxUsedSize;
  }
  public void resetMaxUsedSizeInBytes() throws IOException {
    this.maxUsedSize = getRecomputedActualSizeInBytes();
  }

  /**
   * Trip a test assert if there is an attempt
   * to delete an open file.
  */
  public void setAssertNoDeleteOpenFile(boolean value) {
    this.assertNoDeleteOpenFile = value;
  }
  
  public boolean getAssertNoDeleteOpenFile() {
    return assertNoDeleteOpenFile;
  }

  /**
   * If 0.0, no exceptions will be thrown.  Else this should
   * be a double 0.0 - 1.0.  We will randomly throw an
   * IOException on the first write to an OutputStream based
   * on this probability.
   */
  public void setRandomIOExceptionRate(double rate) {
    randomIOExceptionRate = rate;
  }
  
  public double getRandomIOExceptionRate() {
    return randomIOExceptionRate;
  }

  /**
   * If 0.0, no exceptions will be thrown during openInput
   * and createOutput.  Else this should
   * be a double 0.0 - 1.0 and we will randomly throw an
   * IOException in openInput and createOutput with
   * this probability.
   */
  public void setRandomIOExceptionRateOnOpen(double rate) {
    randomIOExceptionRateOnOpen = rate;
  }
  
  public double getRandomIOExceptionRateOnOpen() {
    return randomIOExceptionRateOnOpen;
  }

  void maybeThrowIOException(String message) throws IOException {
    if (randomState.nextDouble() < randomIOExceptionRate) {
      IOException ioe = new IOException("a random IOException" + (message == null ? "" : " (" + message + ")"));
      if (LuceneTestCase.VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": MockDirectoryWrapper: now throw random exception" + (message == null ? "" : " (" + message + ")"));
        ioe.printStackTrace(System.out);
      }
      throw ioe;
    }
  }

  void maybeThrowIOExceptionOnOpen(String name) throws IOException {
    if (randomState.nextDouble() < randomIOExceptionRateOnOpen) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": MockDirectoryWrapper: now throw random exception during open file=" + name);
        new Throwable().printStackTrace(System.out);
      }
      if (allowRandomFileNotFoundException == false || randomState.nextBoolean()) {
        throw new IOException("a random IOException (" + name + ")");
      } else {
        throw randomState.nextBoolean() ? new FileNotFoundException("a random IOException (" + name + ")") : new NoSuchFileException("a random IOException (" + name + ")");
      }
    }
  }
  
  /** returns current open file handle count */
  public synchronized long getFileHandleCount() {
    return openFileHandles.size();
  }

  @Override
  public synchronized void deleteFile(String name) throws IOException {
    maybeYield();

    maybeThrowDeterministicException();

    if (crashed) {
      throw new IOException("cannot delete after crash");
    }

    if (openFiles.containsKey(name)) {
      openFilesDeleted.add(name);
      if (assertNoDeleteOpenFile) {
        throw (IOException) fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot delete"), name, true);
      }
    } else {
      openFilesDeleted.remove(name);
    }

    unSyncedFiles.remove(name);
    in.deleteFile(name);
    createdFiles.remove(name);
  }

  // sets the cause of the incoming ioe to be the stack
  // trace when the offending file name was opened
  private synchronized Throwable fillOpenTrace(Throwable t, String name, boolean input) {
    for(Map.Entry<Closeable,Exception> ent : openFileHandles.entrySet()) {
      if (input && ent.getKey() instanceof MockIndexInputWrapper && ((MockIndexInputWrapper) ent.getKey()).name.equals(name)) {
        t.initCause(ent.getValue());
        break;
      } else if (!input && ent.getKey() instanceof MockIndexOutputWrapper && ((MockIndexOutputWrapper) ent.getKey()).name.equals(name)) {
        t.initCause(ent.getValue());
        break;
      }
    }
    return t;
  }

  private void maybeYield() {
    if (randomState.nextBoolean()) {
      Thread.yield();
    }
  }

  public synchronized Set<String> getOpenDeletedFiles() {
    return new HashSet<>(openFilesDeleted);
  }

  private boolean failOnCreateOutput = true;

  public void setFailOnCreateOutput(boolean v) {
    failOnCreateOutput = v;
  }
  
  @Override
  public synchronized IndexOutput createOutput(String name, IOContext context) throws IOException {
    maybeThrowDeterministicException();
    maybeThrowIOExceptionOnOpen(name);
    maybeYield();
    if (failOnCreateOutput) {
      maybeThrowDeterministicException();
    }
    if (crashed) {
      throw new IOException("cannot createOutput after crash");
    }
    init();
    synchronized(this) {
      if (createdFiles.contains(name) && !name.equals("segments.gen")) {
        throw new IOException("file \"" + name + "\" was already written to");
      }
    }
    if (assertNoDeleteOpenFile && openFiles.containsKey(name)) {
      throw new AssertionError("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot overwrite");
    }
    
    unSyncedFiles.add(name);
    createdFiles.add(name);
    
    //System.out.println(Thread.currentThread().getName() + ": MDW: create " + name);
    IndexOutput delegateOutput = in.createOutput(name, LuceneTestCase.newIOContext(randomState, context));
    final IndexOutput io = new MockIndexOutputWrapper(this, delegateOutput, name);
    addFileHandle(io, name, Handle.Output);
    openFilesForWrite.add(name);
    
    // throttling REALLY slows down tests, so don't do it very often for SOMETIMES.
    if (throttling == Throttling.ALWAYS || 
        (throttling == Throttling.SOMETIMES && randomState.nextInt(200) == 0)) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: throttling indexOutput (" + name + ")");
      }
      return throttledOutput.newFromDelegate(io);
    } else {
      return io;
    }
  }
  
  @Override
  public synchronized IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    maybeThrowDeterministicException();
    maybeThrowIOExceptionOnOpen("temp: prefix=" + prefix + " suffix=" + suffix);
    maybeYield();
    if (failOnCreateOutput) {
      maybeThrowDeterministicException();
    }
    if (crashed) {
      throw new IOException("cannot createTempOutput after crash");
    }
    init();
    
    IndexOutput delegateOutput = in.createTempOutput(prefix, suffix, LuceneTestCase.newIOContext(randomState, context));
    String name = delegateOutput.getName();
    if (name.toLowerCase(Locale.ROOT).endsWith(".tmp") == false) {
      throw new IllegalStateException("wrapped directory failed to use .tmp extension: got: " + name);
    }

    unSyncedFiles.add(name);
    createdFiles.add(name);
    final IndexOutput io = new MockIndexOutputWrapper(this, delegateOutput, name);
    addFileHandle(io, name, Handle.Output);
    openFilesForWrite.add(name);
    
    // throttling REALLY slows down tests, so don't do it very often for SOMETIMES.
    if (throttling == Throttling.ALWAYS || 
        (throttling == Throttling.SOMETIMES && randomState.nextInt(200) == 0)) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: throttling indexOutput (" + name + ")");
      }
      return throttledOutput.newFromDelegate(io);
    } else {
      return io;
    }
  }

  private static enum Handle {
    Input, Output, Slice
  }

  synchronized void addFileHandle(Closeable c, String name, Handle handle) {
    Integer v = openFiles.get(name);
    if (v != null) {
      v = Integer.valueOf(v.intValue()+1);
      openFiles.put(name, v);
    } else {
      openFiles.put(name, Integer.valueOf(1));
    }
    
    openFileHandles.put(c, new RuntimeException("unclosed Index" + handle.name() + ": " + name));
  }

  private boolean failOnOpenInput = true;

  public void setFailOnOpenInput(boolean v) {
    failOnOpenInput = v;
  }

  @Override
  public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
    maybeThrowDeterministicException();
    maybeThrowIOExceptionOnOpen(name);
    maybeYield();
    if (failOnOpenInput) {
      maybeThrowDeterministicException();
    }
    if (!LuceneTestCase.slowFileExists(in, name)) {
      throw randomState.nextBoolean() ? new FileNotFoundException(name + " in dir=" + in) : new NoSuchFileException(name + " in dir=" + in);
    }

    // cannot open a file for input if it's still open for
    // output, except for segments.gen and segments_N
    if (!allowReadingFilesStillOpenForWrite && openFilesForWrite.contains(name) && !name.startsWith("segments")) {
      throw (IOException) fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open for writing"), name, false);
    }

    IndexInput delegateInput = in.openInput(name, LuceneTestCase.newIOContext(randomState, context));

    final IndexInput ii;
    int randomInt = randomState.nextInt(500);
    if (useSlowOpenClosers && randomInt == 0) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: using SlowClosingMockIndexInputWrapper for file " + name);
      }
      ii = new SlowClosingMockIndexInputWrapper(this, name, delegateInput);
    } else if (useSlowOpenClosers && randomInt  == 1) { 
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: using SlowOpeningMockIndexInputWrapper for file " + name);
      }
      ii = new SlowOpeningMockIndexInputWrapper(this, name, delegateInput);
    } else {
      ii = new MockIndexInputWrapper(this, name, delegateInput, null);
    }
    addFileHandle(ii, name, Handle.Input);
    return ii;
  }
  
  /** Provided for testing purposes.  Use sizeInBytes() instead. */
  public synchronized final long getRecomputedSizeInBytes() throws IOException {
    if (!(in instanceof RAMDirectory))
      return sizeInBytes();
    long size = 0;
    for(final RAMFile file: ((RAMDirectory)in).fileMap.values()) {
      size += file.ramBytesUsed();
    }
    return size;
  }

  /** Like getRecomputedSizeInBytes(), but, uses actual file
   * lengths rather than buffer allocations (which are
   * quantized up to nearest
   * RAMOutputStream.BUFFER_SIZE (now 1024) bytes.
   */

  public final synchronized long getRecomputedActualSizeInBytes() throws IOException {
    if (!(in instanceof RAMDirectory))
      return sizeInBytes();
    long size = 0;
    for (final RAMFile file : ((RAMDirectory)in).fileMap.values())
      size += file.length;
    return size;
  }

  // NOTE: This is off by default; see LUCENE-5574
  private boolean assertNoUnreferencedFilesOnClose;

  public void setAssertNoUnrefencedFilesOnClose(boolean v) {
    assertNoUnreferencedFilesOnClose = v;
  }

  @Override
  public synchronized void close() throws IOException {
    if (isOpen) {
      isOpen = false;
    } else {
      in.close(); // but call it again on our wrapped dir
      return;
    }

    boolean success = false;
    try {
      // files that we tried to delete, but couldn't because readers were open.
      // all that matters is that we tried! (they will eventually go away)
      //   still open when we tried to delete
      maybeYield();
      if (openFiles == null) {
        openFiles = new HashMap<>();
        openFilesDeleted = new HashSet<>();
      }
      if (openFiles.size() > 0) {
        // print the first one as it's very verbose otherwise
        Exception cause = null;
        Iterator<Exception> stacktraces = openFileHandles.values().iterator();
        if (stacktraces.hasNext()) {
          cause = stacktraces.next();
        }
        // RuntimeException instead of IOException because
        // super() does not throw IOException currently:
        throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still " + openFiles.size() + " open files: " + openFiles, cause);
      }
      if (openLocks.size() > 0) {
        Exception cause = null;
        Iterator<RuntimeException> stacktraces = openLocks.values().iterator();
        if (stacktraces.hasNext()) {
          cause = stacktraces.next();
        }
        throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still open locks: " + openLocks, cause);
      }
      randomIOExceptionRate = 0.0;
      randomIOExceptionRateOnOpen = 0.0;

      if ((getCheckIndexOnClose() || assertNoUnreferencedFilesOnClose) && DirectoryReader.indexExists(this)) {
        if (getCheckIndexOnClose()) {

          if (LuceneTestCase.VERBOSE) {
            System.out.println("\nNOTE: MockDirectoryWrapper: now crush");
          }
          crash(); // corrupt any unsynced-files
          if (LuceneTestCase.VERBOSE) {
            System.out.println("\nNOTE: MockDirectoryWrapper: now run CheckIndex");
          } 

          TestUtil.checkIndex(this, getCrossCheckTermVectorsOnClose(), true, null);
        }
          
        // TODO: factor this out / share w/ TestIW.assertNoUnreferencedFiles
        if (assertNoUnreferencedFilesOnClose) {
          System.out.println("MDW: now assert no unref'd files at close");

          // now look for unreferenced files: discount ones that we tried to delete but could not
          Set<String> allFiles = new HashSet<>(Arrays.asList(listAll()));
          String[] startFiles = allFiles.toArray(new String[0]);
          IndexWriterConfig iwc = new IndexWriterConfig(null);
          iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

          // We must do this before opening writer otherwise writer will be angry if there are pending deletions:
          TestUtil.disableVirusChecker(in);

          new IndexWriter(in, iwc).rollback();
          String[] endFiles = in.listAll();
            
          Set<String> startSet = new TreeSet<>(Arrays.asList(startFiles));
          Set<String> endSet = new TreeSet<>(Arrays.asList(endFiles));
            
          startFiles = startSet.toArray(new String[0]);
          endFiles = endSet.toArray(new String[0]);
            
          if (!Arrays.equals(startFiles, endFiles)) {
            List<String> removed = new ArrayList<>();
            for(String fileName : startFiles) {
              if (!endSet.contains(fileName)) {
                removed.add(fileName);
              }
            }
              
            List<String> added = new ArrayList<>();
            for(String fileName : endFiles) {
              if (!startSet.contains(fileName)) {
                added.add(fileName);
              }
            }
              
            String extras;
            if (removed.size() != 0) {
              extras = "\n\nThese files were removed: " + removed;
            } else {
              extras = "";
            }
              
            if (added.size() != 0) {
              extras += "\n\nThese files were added (waaaaaaaaaat!): " + added;
            }
              
            throw new RuntimeException("unreferenced files: before delete:\n    " + Arrays.toString(startFiles) + "\n  after delete:\n    " + Arrays.toString(endFiles) + extras);
          }
            
          DirectoryReader ir1 = DirectoryReader.open(this);
          int numDocs1 = ir1.numDocs();
          ir1.close();
          new IndexWriter(this, new IndexWriterConfig(null)).close();
          DirectoryReader ir2 = DirectoryReader.open(this);
          int numDocs2 = ir2.numDocs();
          ir2.close();
          assert numDocs1 == numDocs2 : "numDocs changed after opening/closing IW: before=" + numDocs1 + " after=" + numDocs2;
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  synchronized void removeOpenFile(Closeable c, String name) {
    Integer v = openFiles.get(name);
    // Could be null when crash() was called
    if (v != null) {
      if (v.intValue() == 1) {
        openFiles.remove(name);
      } else {
        v = Integer.valueOf(v.intValue()-1);
        openFiles.put(name, v);
      }
    }

    openFileHandles.remove(c);
  }
  
  public synchronized void removeIndexOutput(IndexOutput out, String name) {
    openFilesForWrite.remove(name);
    removeOpenFile(out, name);
  }
  
  public synchronized void removeIndexInput(IndexInput in, String name) {
    removeOpenFile(in, name);
  }
  
  /**
   * Objects that represent fail-able conditions. Objects of a derived
   * class are created and registered with the mock directory. After
   * register, each object will be invoked once for each first write
   * of a file, giving the object a chance to throw an IOException.
   */
  public static class Failure {
    /**
     * eval is called on the first write of every new file.
     */
    public void eval(MockDirectoryWrapper dir) throws IOException { }

    /**
     * reset should set the state of the failure to its default
     * (freshly constructed) state. Reset is convenient for tests
     * that want to create one failure object and then reuse it in
     * multiple cases. This, combined with the fact that Failure
     * subclasses are often anonymous classes makes reset difficult to
     * do otherwise.
     *
     * A typical example of use is
     * Failure failure = new Failure() { ... };
     * ...
     * mock.failOn(failure.reset())
     */
    public Failure reset() { return this; }

    protected boolean doFail;

    public void setDoFail() {
      doFail = true;
    }

    public void clearDoFail() {
      doFail = false;
    }
  }

  ArrayList<Failure> failures;

  /**
   * add a Failure object to the list of objects to be evaluated
   * at every potential failure point
   */
  synchronized public void failOn(Failure fail) {
    if (failures == null) {
      failures = new ArrayList<>();
    }
    failures.add(fail);
  }

  /**
   * Iterate through the failures list, giving each object a
   * chance to throw an IOE
   */
  synchronized void maybeThrowDeterministicException() throws IOException {
    if (failures != null) {
      for(int i = 0; i < failures.size(); i++) {
        try {
          failures.get(i).eval(this);
        } catch (Throwable t) {
          if (LuceneTestCase.VERBOSE) {
            System.out.println("MockDirectoryWrapper: throw exc");
            t.printStackTrace(System.out);
          }
          throw IOUtils.rethrowAlways(t);
        }
      }
    }
  }
  
  @Override
  public synchronized String[] listAll() throws IOException {
    maybeYield();
    return in.listAll();
  }

  @Override
  public synchronized long fileLength(String name) throws IOException {
    maybeYield();
    return in.fileLength(name);
  }

  @Override
  public synchronized Lock obtainLock(String name) throws IOException {
    maybeYield();
    return super.obtainLock(name);
    // TODO: consider mocking locks, but not all the time, can hide bugs
  }
  
  /** Use this when throwing fake {@code IOException},
   *  e.g. from {@link MockDirectoryWrapper.Failure}. */
  public static class FakeIOException extends IOException {
  }

  @Override
  public String toString() {
    if (maxSize != 0) {
      return "MockDirectoryWrapper(" + in + ", current=" + maxUsedSize + ",max=" + maxSize + ")";
    } else {
      return super.toString();
    }
  }

  // don't override optional methods like copyFrom: we need the default impl for things like disk 
  // full checks. we randomly exercise "raw" directories anyway. We ensure default impls are used:
  
  @Override
  public final ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
    return super.openChecksumInput(name, context);
  }

  @Override
  public final void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
    super.copyFrom(from, src, dest, context);
  }

  @Override
  protected final void ensureOpen() throws AlreadyClosedException {
    super.ensureOpen();
  }
}

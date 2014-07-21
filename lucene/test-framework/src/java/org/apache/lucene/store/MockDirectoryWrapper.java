package org.apache.lucene.store;

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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
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
  boolean noDeleteOpenFile = true;
  boolean assertNoDeleteOpenFile = false;
  boolean preventDoubleWrite = true;
  boolean trackDiskUsage = false;
  boolean wrapLockFactory = true;
  boolean useSlowOpenClosers = true;
  boolean allowRandomFileNotFoundException = true;
  boolean allowReadingFilesStillOpenForWrite = false;
  private Set<String> unSyncedFiles;
  private Set<String> createdFiles;
  private Set<String> openFilesForWrite = new HashSet<>();
  Map<String,Exception> openLocks = Collections.synchronizedMap(new HashMap<String,Exception>());
  volatile boolean crashed;
  private ThrottledIndexOutput throttledOutput;
  private Throttling throttling = Throttling.SOMETIMES;
  protected LockFactory lockFactory;

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
        .mBitsToBytes(40 + randomState.nextInt(10)), 5 + randomState.nextInt(5), null);
    // force wrapping of lockfactory
    this.lockFactory = new MockLockFactoryWrapper(this, delegate.getLockFactory());
    init();
  }

  public int getInputCloneCount() {
    return inputCloneCount.get();
  }

  public void setTrackDiskUsage(boolean v) {
    trackDiskUsage = v;
  }

  /** If set to true, we throw an IOException if the same
   *  file is opened by createOutput, ever. */
  public void setPreventDoubleWrite(boolean value) {
    preventDoubleWrite = value;
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
    /** sometimes (2% of the time) emulate a slow hard disk. */
    SOMETIMES,
    /** never throttle output */
    NEVER
  }
  
  public void setThrottling(Throttling throttling) {
    this.throttling = throttling;
  }
  
  /** 
   * By default, opening and closing has a rare small sleep to catch race conditions
   * <p>
   * You can disable this if you dont need it
   */
  public void setUseSlowOpenClosers(boolean v) {
    useSlowOpenClosers = v;
  }

  /**
   * Returns true if {@link #in} must sync its files.
   * Currently, only {@link NRTCachingDirectory} requires sync'ing its files
   * because otherwise they are cached in an internal {@link RAMDirectory}. If
   * other directories require that too, they should be added to this method.
   */
  private boolean mustSync() {
    Directory delegate = in;
    while (delegate instanceof FilterDirectory) {
      if (delegate instanceof NRTCachingDirectory) {
        return true;
      }
      delegate = ((FilterDirectory) delegate).getDelegate();
    }
    return delegate instanceof NRTCachingDirectory;
  }
  
  @Override
  public synchronized void sync(Collection<String> names) throws IOException {
    maybeYield();
    maybeThrowDeterministicException();
    if (crashed) {
      throw new IOException("cannot sync after crash");
    }
    // don't wear out our hardware so much in tests.
    if (LuceneTestCase.rarely(randomState) || mustSync()) {
      for (String name : names) {
        // randomly fail with IOE on any file
        maybeThrowIOException(name);
        in.sync(Collections.singleton(name));
        unSyncedFiles.remove(name);
      }
    } else {
      unSyncedFiles.removeAll(names);
    }
  }

  public synchronized final long sizeInBytes() throws IOException {
    if (in instanceof RAMDirectory)
      return ((RAMDirectory) in).ramBytesUsed();
    else {
      // hack
      long size = 0;
      for (String file : in.listAll())
        size += in.fileLength(file);
      return size;
    }
  }

  /** Simulates a crash of OS or machine by overwriting
   *  unsynced files. */
  public synchronized void crash() throws IOException {
    crashed = true;
    openFiles = new HashMap<>();
    openFilesForWrite = new HashSet<>();
    openFilesDeleted = new HashSet<>();
    Iterator<String> it = unSyncedFiles.iterator();
    unSyncedFiles = new HashSet<>();
    // first force-close all files, so we can corrupt on windows etc.
    // clone the file map, as these guys want to remove themselves on close.
    Map<Closeable,Exception> m = new IdentityHashMap<>(openFileHandles);
    for (Closeable f : m.keySet()) {
      try {
        f.close();
      } catch (Exception ignored) {}
    }
    
    while(it.hasNext()) {
      String name = it.next();
      int damage = randomState.nextInt(5);
      String action = null;

      if (damage == 0) {
        action = "deleted";
        deleteFile(name, true);
      } else if (damage == 1) {
        action = "zeroed";
        // Zero out file entirely
        long length = fileLength(name);
        byte[] zeroes = new byte[256];
        long upto = 0;
        IndexOutput out = in.createOutput(name, LuceneTestCase.newIOContext(randomState));
        while(upto < length) {
          final int limit = (int) Math.min(length-upto, zeroes.length);
          out.writeBytes(zeroes, 0, limit);
          upto += limit;
        }
        out.close();
      } else if (damage == 2) {
        action = "partially truncated";
        // Partially Truncate the file:

        // First, make temp file and copy only half this
        // file over:
        String tempFileName;
        while (true) {
          tempFileName = ""+randomState.nextInt();
          if (!LuceneTestCase.slowFileExists(in, tempFileName)) {
            break;
          }
        }
        final IndexOutput tempOut = in.createOutput(tempFileName, LuceneTestCase.newIOContext(randomState));
        IndexInput ii = in.openInput(name, LuceneTestCase.newIOContext(randomState));
        tempOut.copyBytes(ii, ii.length()/2);
        tempOut.close();
        ii.close();

        // Delete original and copy bytes back:
        deleteFile(name, true);
        
        final IndexOutput out = in.createOutput(name, LuceneTestCase.newIOContext(randomState));
        ii = in.openInput(tempFileName, LuceneTestCase.newIOContext(randomState));
        out.copyBytes(ii, ii.length());
        out.close();
        ii.close();
        deleteFile(tempFileName, true);
      } else if (damage == 3) {
        // The file survived intact:
        action = "didn't change";
      } else {
        action = "fully truncated";
        // Totally truncate the file to zero bytes
        deleteFile(name, true);
        IndexOutput out = in.createOutput(name, LuceneTestCase.newIOContext(randomState));
        out.close();
      }
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: " + action + " unsynced file: " + name);
      }
    }
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
   * Emulate windows whereby deleting an open file is not
   * allowed (raise IOException).
  */
  public void setNoDeleteOpenFile(boolean value) {
    this.noDeleteOpenFile = value;
  }
  
  public boolean getNoDeleteOpenFile() {
    return noDeleteOpenFile;
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
      if (LuceneTestCase.VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": MockDirectoryWrapper: now throw random exception" + (message == null ? "" : " (" + message + ")"));
        new Throwable().printStackTrace(System.out);
      }
      throw new IOException("a random IOException" + (message == null ? "" : " (" + message + ")"));
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

  @Override
  public synchronized void deleteFile(String name) throws IOException {
    maybeYield();
    deleteFile(name, false);
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

  private synchronized void deleteFile(String name, boolean forced) throws IOException {
    maybeYield();

    maybeThrowDeterministicException();

    if (crashed && !forced)
      throw new IOException("cannot delete after crash");

    if (unSyncedFiles.contains(name))
      unSyncedFiles.remove(name);
    if (!forced && (noDeleteOpenFile || assertNoDeleteOpenFile)) {
      if (openFiles.containsKey(name)) {
        openFilesDeleted.add(name);

        if (!assertNoDeleteOpenFile) {
          throw (IOException) fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot delete"), name, true);
        } else {
          throw (AssertionError) fillOpenTrace(new AssertionError("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot delete"), name, true);
        }
      } else {
        openFilesDeleted.remove(name);
      }
    }
    in.deleteFile(name);
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
      if (preventDoubleWrite && createdFiles.contains(name) && !name.equals("segments.gen")) {
        throw new IOException("file \"" + name + "\" was already written to");
      }
    }
    if ((noDeleteOpenFile || assertNoDeleteOpenFile) && openFiles.containsKey(name)) {
      if (!assertNoDeleteOpenFile) {
        throw new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot overwrite");
      } else {
        throw new AssertionError("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot overwrite");
      }
    }
    
    if (crashed) {
      throw new IOException("cannot createOutput after crash");
    }
    unSyncedFiles.add(name);
    createdFiles.add(name);
    
    if (in instanceof RAMDirectory) {
      RAMDirectory ramdir = (RAMDirectory) in;
      RAMFile file = new RAMFile(ramdir);
      RAMFile existing = ramdir.fileMap.get(name);
    
      // Enforce write once:
      if (existing!=null && !name.equals("segments.gen") && preventDoubleWrite) {
        throw new IOException("file " + name + " already exists");
      } else {
        if (existing!=null) {
          ramdir.sizeInBytes.getAndAdd(-existing.sizeInBytes);
          existing.directory = null;
        }
        ramdir.fileMap.put(name, file);
      }
    }
    //System.out.println(Thread.currentThread().getName() + ": MDW: create " + name);
    IndexOutput delegateOutput = in.createOutput(name, LuceneTestCase.newIOContext(randomState, context));
    final IndexOutput io = new MockIndexOutputWrapper(this, delegateOutput, name);
    addFileHandle(io, name, Handle.Output);
    openFilesForWrite.add(name);
    
    // throttling REALLY slows down tests, so don't do it very often for SOMETIMES.
    if (throttling == Throttling.ALWAYS || 
        (throttling == Throttling.SOMETIMES && randomState.nextInt(200) == 0) && !(in instanceof RateLimitedDirectoryWrapper)) {
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
      ii = new MockIndexInputWrapper(this, name, delegateInput);
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
  
  /**
   * Set to false if you want to return the pure lockfactory
   * and not wrap it with MockLockFactoryWrapper.
   * <p>
   * Be careful if you turn this off: MockDirectoryWrapper might
   * no longer be able to detect if you forget to close an IndexWriter,
   * and spit out horribly scary confusing exceptions instead of
   * simply telling you that.
   */
  public void setWrapLockFactory(boolean v) {
    this.wrapLockFactory = v;
  }

  @Override
  public synchronized void close() throws IOException {
    // files that we tried to delete, but couldn't because readers were open.
    // all that matters is that we tried! (they will eventually go away)
    Set<String> pendingDeletions = new HashSet<>(openFilesDeleted);
    maybeYield();
    if (openFiles == null) {
      openFiles = new HashMap<>();
      openFilesDeleted = new HashSet<>();
    }
    if (openFiles.size() > 0) {
      // print the first one as its very verbose otherwise
      Exception cause = null;
      Iterator<Exception> stacktraces = openFileHandles.values().iterator();
      if (stacktraces.hasNext()) {
        cause = stacktraces.next();
      }
      // RuntimeException instead of IOException because
      // super() does not throw IOException currently:
      throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still open files: " + openFiles, cause);
    }
    if (openLocks.size() > 0) {
      Exception cause = null;
      Iterator<Exception> stacktraces = openLocks.values().iterator();
      if (stacktraces.hasNext()) {
        cause = stacktraces.next();
      }
      throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still open locks: " + openLocks, cause);
    }

    isOpen = false;
    if (getCheckIndexOnClose()) {
      randomIOExceptionRate = 0.0;
      randomIOExceptionRateOnOpen = 0.0;
      if (DirectoryReader.indexExists(this)) {
        if (LuceneTestCase.VERBOSE) {
          System.out.println("\nNOTE: MockDirectoryWrapper: now crush");
        }
        crash(); // corrupt any unsynced-files
        if (LuceneTestCase.VERBOSE) {
          System.out.println("\nNOTE: MockDirectoryWrapper: now run CheckIndex");
        } 
        TestUtil.checkIndex(this, getCrossCheckTermVectorsOnClose(), true);

        // TODO: factor this out / share w/ TestIW.assertNoUnreferencedFiles
        if (assertNoUnreferencedFilesOnClose) {
          // now look for unreferenced files: discount ones that we tried to delete but could not
          Set<String> allFiles = new HashSet<>(Arrays.asList(listAll()));
          allFiles.removeAll(pendingDeletions);
          String[] startFiles = allFiles.toArray(new String[0]);
          IndexWriterConfig iwc = new IndexWriterConfig(LuceneTestCase.TEST_VERSION_CURRENT, null);
          iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
          new IndexWriter(in, iwc).rollback();
          String[] endFiles = in.listAll();

          Set<String> startSet = new TreeSet<>(Arrays.asList(startFiles));
          Set<String> endSet = new TreeSet<>(Arrays.asList(endFiles));
          
          if (pendingDeletions.contains("segments.gen") && endSet.contains("segments.gen")) {
            // this is possible if we hit an exception while writing segments.gen, we try to delete it
            // and it ends out in pendingDeletions (but IFD wont remove this).
            startSet.add("segments.gen");
            if (LuceneTestCase.VERBOSE) {
              System.out.println("MDW: Unreferenced check: Ignoring segments.gen that we could not delete.");
            }
          }
          
          // its possible we cannot delete the segments_N on windows if someone has it open and
          // maybe other files too, depending on timing. normally someone on windows wouldnt have
          // an issue (IFD would nuke this stuff eventually), but we pass NoDeletionPolicy...
          for (String file : pendingDeletions) {
            if (file.startsWith("segments") && !file.equals("segments.gen") && endSet.contains(file)) {
              startSet.add(file);
              if (LuceneTestCase.VERBOSE) {
                System.out.println("MDW: Unreferenced check: Ignoring segments file: " + file + " that we could not delete.");
              }
              SegmentInfos sis = new SegmentInfos();
              try {
                sis.read(in, file);
              } catch (IOException ioe) {
                // OK: likely some of the .si files were deleted
              }

              try {
                Set<String> ghosts = new HashSet<>(sis.files(in, false));
                for (String s : ghosts) {
                  if (endSet.contains(s) && !startSet.contains(s)) {
                    assert pendingDeletions.contains(s);
                    if (LuceneTestCase.VERBOSE) {
                      System.out.println("MDW: Unreferenced check: Ignoring referenced file: " + s + " " +
                                         "from " + file + " that we could not delete.");
                    }
                    startSet.add(s);
                  }
                }
              } catch (Throwable t) {
                System.err.println("ERROR processing leftover segments file " + file + ":");
                t.printStackTrace();
              }
            }
          }

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

            if (pendingDeletions.size() != 0) {
              extras += "\n\nThese files we had previously tried to delete, but couldn't: " + pendingDeletions;
            }
             
            assert false : "unreferenced files: before delete:\n    " + Arrays.toString(startFiles) + "\n  after delete:\n    " + Arrays.toString(endFiles) + extras;
          }

          DirectoryReader ir1 = DirectoryReader.open(this);
          int numDocs1 = ir1.numDocs();
          ir1.close();
          new IndexWriter(this, new IndexWriterConfig(LuceneTestCase.TEST_VERSION_CURRENT, null)).close();
          DirectoryReader ir2 = DirectoryReader.open(this);
          int numDocs2 = ir2.numDocs();
          ir2.close();
          assert numDocs1 == numDocs2 : "numDocs changed after opening/closing IW: before=" + numDocs1 + " after=" + numDocs2;
        }
      }
    }
    in.close();
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
        failures.get(i).eval(this);
      }
    }
  }
  
  @Override
  public synchronized String[] listAll() throws IOException {
    maybeYield();
    return in.listAll();
  }

  @Override
  public synchronized boolean fileExists(String name) throws IOException {
    maybeYield();
    return in.fileExists(name);
  }

  @Override
  public synchronized long fileLength(String name) throws IOException {
    maybeYield();
    return in.fileLength(name);
  }

  @Override
  public synchronized Lock makeLock(String name) {
    maybeYield();
    return getLockFactory().makeLock(name);
  }

  @Override
  public synchronized void clearLock(String name) throws IOException {
    maybeYield();
    getLockFactory().clearLock(name);
  }

  @Override
  public synchronized void setLockFactory(LockFactory lockFactory) throws IOException {
    maybeYield();
    // sneaky: we must pass the original this way to the dir, because
    // some impls (e.g. FSDir) do instanceof here.
    in.setLockFactory(lockFactory);
    // now set our wrapped factory here
    this.lockFactory = new MockLockFactoryWrapper(this, lockFactory);
  }

  @Override
  public synchronized LockFactory getLockFactory() {
    maybeYield();
    if (wrapLockFactory) {
      return lockFactory;
    } else {
      return in.getLockFactory();
    }
  }

  @Override
  public synchronized String getLockID() {
    maybeYield();
    return in.getLockID();
  }

  @Override
  public synchronized void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    maybeYield();
    // randomize the IOContext here?
    in.copy(to, src, dest, context);
  }
  
  /** Use this when throwing fake {@code IOException},
   *  e.g. from {@link MockDirectoryWrapper.Failure}. */
  public static class FakeIOException extends IOException {
  }

}

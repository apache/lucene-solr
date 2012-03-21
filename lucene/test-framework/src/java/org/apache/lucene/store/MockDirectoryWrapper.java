package org.apache.lucene.store;

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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThrottledIndexOutput;
import org.apache.lucene.util._TestUtil;

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

public class MockDirectoryWrapper extends Directory {
  final Directory delegate;
  long maxSize;

  // Max actual bytes used. This is set by MockRAMOutputStream:
  long maxUsedSize;
  double randomIOExceptionRate;
  Random randomState;
  boolean noDeleteOpenFile = true;
  boolean preventDoubleWrite = true;
  boolean checkIndexOnClose = true;
  boolean crossCheckTermVectorsOnClose = true;
  boolean trackDiskUsage = false;
  private Set<String> unSyncedFiles;
  private Set<String> createdFiles;
  private Set<String> openFilesForWrite = new HashSet<String>();
  Set<String> openLocks = Collections.synchronizedSet(new HashSet<String>());
  volatile boolean crashed;
  private ThrottledIndexOutput throttledOutput;
  private Throttling throttling = Throttling.SOMETIMES;

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
      openFiles = new HashMap<String,Integer>();
      openFilesDeleted = new HashSet<String>();
    }

    if (createdFiles == null)
      createdFiles = new HashSet<String>();
    if (unSyncedFiles == null)
      unSyncedFiles = new HashSet<String>();
  }

  public MockDirectoryWrapper(Random random, Directory delegate) {
    this.delegate = delegate;
    // must make a private random since our methods are
    // called from different threads; else test failures may
    // not be reproducible from the original seed
    this.randomState = new Random(random.nextInt());
    this.throttledOutput = new ThrottledIndexOutput(ThrottledIndexOutput
        .mBitsToBytes(40 + randomState.nextInt(10)), 5 + randomState.nextInt(5), null);
    // force wrapping of lockfactory
    try {
      setLockFactory(new MockLockFactoryWrapper(this, delegate.getLockFactory()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  @Override
  public synchronized void sync(Collection<String> names) throws IOException {
    maybeYield();
    maybeThrowDeterministicException();
    if (crashed)
      throw new IOException("cannot sync after crash");
    unSyncedFiles.removeAll(names);
    if (LuceneTestCase.rarely(randomState) || delegate instanceof NRTCachingDirectory) {
      // don't wear out our hardware so much in tests.
      delegate.sync(names);
    }
  }
  
  @Override
  public String toString() {
    // NOTE: do not maybeYield here, since it consumes
    // randomness and can thus (unexpectedly during
    // debugging) change the behavior of a seed
    // maybeYield();
    return "MockDirWrapper(" + delegate + ")";
  }

  public synchronized final long sizeInBytes() throws IOException {
    if (delegate instanceof RAMDirectory)
      return ((RAMDirectory) delegate).sizeInBytes();
    else {
      // hack
      long size = 0;
      for (String file : delegate.listAll())
        size += delegate.fileLength(file);
      return size;
    }
  }

  /** Simulates a crash of OS or machine by overwriting
   *  unsynced files. */
  public synchronized void crash() throws IOException {
    crashed = true;
    openFiles = new HashMap<String,Integer>();
    openFilesForWrite = new HashSet<String>();
    openFilesDeleted = new HashSet<String>();
    Iterator<String> it = unSyncedFiles.iterator();
    unSyncedFiles = new HashSet<String>();
    // first force-close all files, so we can corrupt on windows etc.
    // clone the file map, as these guys want to remove themselves on close.
    Map<Closeable,Exception> m = new IdentityHashMap<Closeable,Exception>(openFileHandles);
    for (Closeable f : m.keySet())
      try {
        f.close();
      } catch (Exception ignored) {}
    
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
        IndexOutput out = delegate.createOutput(name, LuceneTestCase.newIOContext(randomState));
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
          if (!delegate.fileExists(tempFileName)) {
            break;
          }
        }
        final IndexOutput tempOut = delegate.createOutput(tempFileName, LuceneTestCase.newIOContext(randomState));
        IndexInput in = delegate.openInput(name, LuceneTestCase.newIOContext(randomState));
        tempOut.copyBytes(in, in.length()/2);
        tempOut.close();
        in.close();

        // Delete original and copy bytes back:
        deleteFile(name, true);
        
        final IndexOutput out = delegate.createOutput(name, LuceneTestCase.newIOContext(randomState));
        in = delegate.openInput(tempFileName, LuceneTestCase.newIOContext(randomState));
        out.copyBytes(in, in.length());
        out.close();
        in.close();
        deleteFile(tempFileName, true);
      } else if (damage == 3) {
        // The file survived intact:
        action = "didn't change";
      } else {
        action = "fully truncated";
        // Totally truncate the file to zero bytes
        deleteFile(name, true);
        IndexOutput out = delegate.createOutput(name, LuceneTestCase.newIOContext(randomState));
        out.setLength(0);
        out.close();
      }
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: " + action + " unsynced file: " + name);
      }
    }
  }

  public synchronized void clearCrash() throws IOException {
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
   * Set whether or not checkindex should be run
   * on close
   */
  public void setCheckIndexOnClose(boolean value) {
    this.checkIndexOnClose = value;
  }
  
  public boolean getCheckIndexOnClose() {
    return checkIndexOnClose;
  }

  public void setCrossCheckTermVectorsOnClose(boolean value) {
    this.crossCheckTermVectorsOnClose = value;
  }

  public boolean getCrossCheckTermVectorsOnClose() {
    return crossCheckTermVectorsOnClose;
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

  void maybeThrowIOException() throws IOException {
    maybeThrowIOException(null);
  }

  void maybeThrowIOException(String message) throws IOException {
    if (randomIOExceptionRate > 0.0) {
      int number = Math.abs(randomState.nextInt() % 1000);
      if (number < randomIOExceptionRate*1000) {
        if (LuceneTestCase.VERBOSE) {
          System.out.println(Thread.currentThread().getName() + ": MockDirectoryWrapper: now throw random exception" + (message == null ? "" : " (" + message + ")"));
          new Throwable().printStackTrace(System.out);
        }
        throw new IOException("a random IOException" + (message == null ? "" : "(" + message + ")"));
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
  private synchronized IOException fillOpenTrace(IOException ioe, String name, boolean input) {
    for(Map.Entry<Closeable,Exception> ent : openFileHandles.entrySet()) {
      if (input && ent.getKey() instanceof MockIndexInputWrapper && ((MockIndexInputWrapper) ent.getKey()).name.equals(name)) {
        ioe.initCause(ent.getValue());
        break;
      } else if (!input && ent.getKey() instanceof MockIndexOutputWrapper && ((MockIndexOutputWrapper) ent.getKey()).name.equals(name)) {
        ioe.initCause(ent.getValue());
        break;
      }
    }
    return ioe;
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
    if (!forced && noDeleteOpenFile) {
      if (openFiles.containsKey(name)) {
        openFilesDeleted.add(name);
        throw fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot delete"), name, true);
      } else {
        openFilesDeleted.remove(name);
      }
    }
    delegate.deleteFile(name);
  }

  public synchronized Set<String> getOpenDeletedFiles() {
    return new HashSet<String>(openFilesDeleted);
  }

  private boolean failOnCreateOutput = true;

  public void setFailOnCreateOutput(boolean v) {
    failOnCreateOutput = v;
  }
  
  @Override
  public synchronized IndexOutput createOutput(String name, IOContext context) throws IOException {
    maybeYield();
    if (failOnCreateOutput) {
      maybeThrowDeterministicException();
    }
    if (crashed)
      throw new IOException("cannot createOutput after crash");
    init();
    synchronized(this) {
      if (preventDoubleWrite && createdFiles.contains(name) && !name.equals("segments.gen"))
        throw new IOException("file \"" + name + "\" was already written to");
    }
    if (noDeleteOpenFile && openFiles.containsKey(name))
      throw new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open: cannot overwrite");
    
    if (crashed)
      throw new IOException("cannot createOutput after crash");
    unSyncedFiles.add(name);
    createdFiles.add(name);
    
    if (delegate instanceof RAMDirectory) {
      RAMDirectory ramdir = (RAMDirectory) delegate;
      RAMFile file = new RAMFile(ramdir);
      RAMFile existing = ramdir.fileMap.get(name);
    
      // Enforce write once:
      if (existing!=null && !name.equals("segments.gen") && preventDoubleWrite)
        throw new IOException("file " + name + " already exists");
      else {
        if (existing!=null) {
          ramdir.sizeInBytes.getAndAdd(-existing.sizeInBytes);
          existing.directory = null;
        }
        ramdir.fileMap.put(name, file);
      }
    }
    
    //System.out.println(Thread.currentThread().getName() + ": MDW: create " + name);
    IndexOutput io = new MockIndexOutputWrapper(this, delegate.createOutput(name, LuceneTestCase.newIOContext(randomState)), name);
    addFileHandle(io, name, Handle.Output);
    openFilesForWrite.add(name);
    
    // throttling REALLY slows down tests, so don't do it very often for SOMETIMES.
    if (throttling == Throttling.ALWAYS || 
        (throttling == Throttling.SOMETIMES && randomState.nextInt(50) == 0)) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockDirectoryWrapper: throttling indexOutput");
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
    maybeYield();
    if (failOnOpenInput) {
      maybeThrowDeterministicException();
    }
    if (!delegate.fileExists(name))
      throw new FileNotFoundException(name);

    // cannot open a file for input if it's still open for
    // output, except for segments.gen and segments_N
    if (openFilesForWrite.contains(name) && !name.startsWith("segments")) {
      throw fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open for writing"), name, false);
    }

    IndexInput ii = new MockIndexInputWrapper(this, name, delegate.openInput(name, LuceneTestCase.newIOContext(randomState)));
    addFileHandle(ii, name, Handle.Input);
    return ii;
  }
  
  /** Provided for testing purposes.  Use sizeInBytes() instead. */
  public synchronized final long getRecomputedSizeInBytes() throws IOException {
    if (!(delegate instanceof RAMDirectory))
      return sizeInBytes();
    long size = 0;
    for(final RAMFile file: ((RAMDirectory)delegate).fileMap.values()) {
      size += file.getSizeInBytes();
    }
    return size;
  }

  /** Like getRecomputedSizeInBytes(), but, uses actual file
   * lengths rather than buffer allocations (which are
   * quantized up to nearest
   * RAMOutputStream.BUFFER_SIZE (now 1024) bytes.
   */

  public final synchronized long getRecomputedActualSizeInBytes() throws IOException {
    if (!(delegate instanceof RAMDirectory))
      return sizeInBytes();
    long size = 0;
    for (final RAMFile file : ((RAMDirectory)delegate).fileMap.values())
      size += file.length;
    return size;
  }

  private boolean assertNoUnreferencedFilesOnClose = true;

  public void setAssertNoUnrefencedFilesOnClose(boolean v) {
    assertNoUnreferencedFilesOnClose = v;
  }

  @Override
  public synchronized void close() throws IOException {
    maybeYield();
    if (openFiles == null) {
      openFiles = new HashMap<String,Integer>();
      openFilesDeleted = new HashSet<String>();
    }
    if (noDeleteOpenFile && openFiles.size() > 0) {
      // print the first one as its very verbose otherwise
      Exception cause = null;
      Iterator<Exception> stacktraces = openFileHandles.values().iterator();
      if (stacktraces.hasNext())
        cause = stacktraces.next();
      // RuntimeException instead of IOException because
      // super() does not throw IOException currently:
      throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still open files: " + openFiles, cause);
    }
    if (noDeleteOpenFile && openLocks.size() > 0) {
      throw new RuntimeException("MockDirectoryWrapper: cannot close: there are still open locks: " + openLocks);
    }
    open = false;
    if (checkIndexOnClose) {
      if (indexPossiblyExists(this)) {
        if (LuceneTestCase.VERBOSE) {
          System.out.println("\nNOTE: MockDirectoryWrapper: now crash");
        }
        crash(); // corrumpt any unsynced-files
        if (LuceneTestCase.VERBOSE) {
          System.out.println("\nNOTE: MockDirectoryWrapper: now run CheckIndex");
        } 
        _TestUtil.checkIndex(this, crossCheckTermVectorsOnClose);

        if (assertNoUnreferencedFilesOnClose) {
          // now look for unreferenced files:
          String[] startFiles = listAll();
          new IndexWriter(this, new IndexWriterConfig(LuceneTestCase.TEST_VERSION_CURRENT, null)).rollback();
          String[] endFiles = listAll();

          Arrays.sort(startFiles);
          Arrays.sort(endFiles);

          if (!Arrays.equals(startFiles, endFiles)) {
            assert false : "unreferenced files: before delete:\n    " + Arrays.toString(startFiles) + "\n  after delete:\n    " + Arrays.toString(endFiles);
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
    delegate.close();
  }
  
  /** don't rely upon DirectoryReader.fileExists to determine if we should
   *  checkIndex() or not. It might mask real problems, where we silently
   *  don't checkindex at all. instead we look for a segments file.
   */
  private boolean indexPossiblyExists(Directory d) throws IOException {
    String files[];
    try {
      files = d.listAll();
    } catch (IOException ex) {
      // this means directory doesn't exist, which is ok. return false
      return false;
    }
    for (String f : files) {
      if (f.startsWith("segments_")) {
        return true;
      }
    }
    return false;
  }

  synchronized void removeOpenFile(Closeable c, String name) {
    Integer v = openFiles.get(name);
    // Could be null when crash() was called
    if (v != null) {
      if (v.intValue() == 1) {
        openFiles.remove(name);
        openFilesDeleted.remove(name);
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

  boolean open = true;
  
  public synchronized boolean isOpen() {
    return open;
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
      failures = new ArrayList<Failure>();
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
    return delegate.listAll();
  }

  @Override
  public synchronized boolean fileExists(String name) throws IOException {
    maybeYield();
    return delegate.fileExists(name);
  }

  @Override
  public synchronized long fileLength(String name) throws IOException {
    maybeYield();
    return delegate.fileLength(name);
  }

  @Override
  public synchronized Lock makeLock(String name) {
    maybeYield();
    return delegate.makeLock(name);
  }

  @Override
  public synchronized void clearLock(String name) throws IOException {
    maybeYield();
    delegate.clearLock(name);
  }

  @Override
  public synchronized void setLockFactory(LockFactory lockFactory) throws IOException {
    maybeYield();
    delegate.setLockFactory(lockFactory);
  }

  @Override
  public synchronized LockFactory getLockFactory() {
    maybeYield();
    return delegate.getLockFactory();
  }

  @Override
  public synchronized String getLockID() {
    maybeYield();
    return delegate.getLockID();
  }

  @Override
  public synchronized void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    maybeYield();
    // randomize the IOContext here?
    delegate.copy(to, src, dest, context);
  }

  @Override
  public IndexInputSlicer createSlicer(final String name, IOContext context)
      throws IOException {
    maybeYield();
    if (!delegate.fileExists(name))
      throw new FileNotFoundException(name);
    // cannot open a file for input if it's still open for
    // output, except for segments.gen and segments_N
    if (openFilesForWrite.contains(name) && !name.startsWith("segments")) {
      throw fillOpenTrace(new IOException("MockDirectoryWrapper: file \"" + name + "\" is still open for writing"), name, false);
    }
    
    final IndexInputSlicer delegateHandle = delegate.createSlicer(name, context);
    final IndexInputSlicer handle = new IndexInputSlicer() {
      
      private boolean isClosed;
      @Override
      public void close() throws IOException {
        if (!isClosed) {
          delegateHandle.close();
          MockDirectoryWrapper.this.removeOpenFile(this, name);
          isClosed = true;
        }
      }

      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) throws IOException {
        maybeYield();
        IndexInput ii = new MockIndexInputWrapper(MockDirectoryWrapper.this, name, delegateHandle.openSlice(sliceDescription, offset, length));
        addFileHandle(ii, name, Handle.Input);
        return ii;
      }

      @Override
      public IndexInput openFullSlice() throws IOException {
        maybeYield();
        IndexInput ii = new MockIndexInputWrapper(MockDirectoryWrapper.this, name, delegateHandle.openFullSlice());
        addFileHandle(ii, name, Handle.Input);
        return ii;
      }
      
    };
    addFileHandle(handle, name, Handle.Slice);
    return handle;
  }
}

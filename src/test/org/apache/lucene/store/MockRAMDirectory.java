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

import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This is a subclass of RAMDirectory that adds methods
 * intented to be used only by unit tests.
 * @version $Id: RAMDirectory.java 437897 2006-08-29 01:13:10Z yonik $
 */

public class MockRAMDirectory extends RAMDirectory {
  long maxSize;

  // Max actual bytes used. This is set by MockRAMOutputStream:
  long maxUsedSize;
  double randomIOExceptionRate;
  Random randomState;
  boolean noDeleteOpenFile = true;
  boolean preventDoubleWrite = true;
  private Set unSyncedFiles;
  private Set createdFiles;
  volatile boolean crashed;

  // NOTE: we cannot initialize the Map here due to the
  // order in which our constructor actually does this
  // member initialization vs when it calls super.  It seems
  // like super is called, then our members are initialized:
  Map openFiles;

  private void init() {
    if (openFiles == null)
      openFiles = new HashMap();
    if (createdFiles == null)
      createdFiles = new HashSet();
    if (unSyncedFiles == null)
      unSyncedFiles = new HashSet();
  }

  public MockRAMDirectory() {
    super();
    init();
  }
  public MockRAMDirectory(String dir) throws IOException {
    super(dir);
    init();
  }
  public MockRAMDirectory(Directory dir) throws IOException {
    super(dir);
    init();
  }
  public MockRAMDirectory(File dir) throws IOException {
    super(dir);
    init();
  }

  /** If set to true, we throw an IOException if the same
   *  file is opened by createOutput, ever. */
  public void setPreventDoubleWrite(boolean value) {
    preventDoubleWrite = value;
  }

  public synchronized void sync(String name) throws IOException {
    maybeThrowDeterministicException();
    if (crashed)
      throw new IOException("cannot sync after crash");
    if (unSyncedFiles.contains(name))
      unSyncedFiles.remove(name);
  }

  /** Simulates a crash of OS or machine by overwriting
   *  unsycned files. */
  public void crash() throws IOException {
    synchronized(this) {
      crashed = true;
      openFiles = new HashMap();
    }
    Iterator it = unSyncedFiles.iterator();
    unSyncedFiles = new HashSet();
    int count = 0;
    while(it.hasNext()) {
      String name = (String) it.next();
      RAMFile file = (RAMFile) fileMap.get(name);
      if (count % 3 == 0) {
        deleteFile(name, true);
      } else if (count % 3 == 1) {
        // Zero out file entirely
        final int numBuffers = file.numBuffers();
        for(int i=0;i<numBuffers;i++) {
          byte[] buffer = file.getBuffer(i);
          Arrays.fill(buffer, (byte) 0);
        }
      } else if (count % 3 == 2) {
        // Truncate the file:
        file.setLength(file.getLength()/2);
      }
      count++;
    }
  }

  public synchronized void clearCrash() throws IOException {
    crashed = false;
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
  public void resetMaxUsedSizeInBytes() {
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
   * If 0.0, no exceptions will be thrown.  Else this should
   * be a double 0.0 - 1.0.  We will randomly throw an
   * IOException on the first write to an OutputStream based
   * on this probability.
   */
  public void setRandomIOExceptionRate(double rate, long seed) {
    randomIOExceptionRate = rate;
    // seed so we have deterministic behaviour:
    randomState = new Random(seed);
  }
  public double getRandomIOExceptionRate() {
    return randomIOExceptionRate;
  }

  void maybeThrowIOException() throws IOException {
    if (randomIOExceptionRate > 0.0) {
      int number = Math.abs(randomState.nextInt() % 1000);
      if (number < randomIOExceptionRate*1000) {
        throw new IOException("a random IOException");
      }
    }
  }

  public synchronized void deleteFile(String name) throws IOException {
    deleteFile(name, false);
  }

  private synchronized void deleteFile(String name, boolean forced) throws IOException {

    maybeThrowDeterministicException();

    if (crashed && !forced)
      throw new IOException("cannot delete after crash");

    if (unSyncedFiles.contains(name))
      unSyncedFiles.remove(name);
    if (!forced) {
      synchronized(openFiles) {
        if (noDeleteOpenFile && openFiles.containsKey(name)) {
          throw new IOException("MockRAMDirectory: file \"" + name + "\" is still open: cannot delete");
        }
      }
    }
    super.deleteFile(name);
  }

  public IndexOutput createOutput(String name) throws IOException {
    if (crashed)
      throw new IOException("cannot createOutput after crash");
    init();
    synchronized(openFiles) {
      if (preventDoubleWrite && createdFiles.contains(name) && !name.equals("segments.gen"))
        throw new IOException("file \"" + name + "\" was already written to");
      if (noDeleteOpenFile && openFiles.containsKey(name))
       throw new IOException("MockRAMDirectory: file \"" + name + "\" is still open: cannot overwrite");
    }
    RAMFile file = new RAMFile(this);
    synchronized (this) {
      if (crashed)
        throw new IOException("cannot createOutput after crash");
      unSyncedFiles.add(name);
      createdFiles.add(name);
      RAMFile existing = (RAMFile)fileMap.get(name);
      // Enforce write once:
      if (existing!=null && !name.equals("segments.gen"))
        throw new IOException("file " + name + " already exists");
      else {
        if (existing!=null) {
          sizeInBytes -= existing.sizeInBytes;
          existing.directory = null;
        }

        fileMap.put(name, file);
      }
    }

    return new MockRAMOutputStream(this, file);
  }

  public IndexInput openInput(String name) throws IOException {
    RAMFile file;
    synchronized (this) {
      file = (RAMFile)fileMap.get(name);
    }
    if (file == null)
      throw new FileNotFoundException(name);
    else {
      synchronized(openFiles) {
        if (openFiles.containsKey(name)) {
          Integer v = (Integer) openFiles.get(name);
          v = new Integer(v.intValue()+1);
          openFiles.put(name, v);
        } else {
          openFiles.put(name, new Integer(1));
        }
      }
    }
    return new MockRAMInputStream(this, name, file);
  }

  /** Provided for testing purposes.  Use sizeInBytes() instead. */
  public synchronized final long getRecomputedSizeInBytes() {
    long size = 0;
    Iterator it = fileMap.values().iterator();
    while (it.hasNext())
      size += ((RAMFile) it.next()).getSizeInBytes();
    return size;
  }

  /** Like getRecomputedSizeInBytes(), but, uses actual file
   * lengths rather than buffer allocations (which are
   * quantized up to nearest
   * RAMOutputStream.BUFFER_SIZE (now 1024) bytes.
   */

  public final synchronized long getRecomputedActualSizeInBytes() {
    long size = 0;
    Iterator it = fileMap.values().iterator();
    while (it.hasNext())
      size += ((RAMFile) it.next()).length;
    return size;
  }

  public void close() {
    if (openFiles == null) {
      openFiles = new HashMap();
    }
    synchronized(openFiles) {
      if (noDeleteOpenFile && openFiles.size() > 0) {
        // RuntimeException instead of IOException because
        // super() does not throw IOException currently:
        throw new RuntimeException("MockRAMDirectory: cannot close: there are still open files: " + openFiles);
      }
    }
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
    public void eval(MockRAMDirectory dir) throws IOException { }

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

  ArrayList failures;

  /**
   * add a Failure object to the list of objects to be evaluated
   * at every potential failure point
   */
  synchronized public void failOn(Failure fail) {
    if (failures == null) {
      failures = new ArrayList();
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
        ((Failure)failures.get(i)).eval(this);
      }
    }
  }


}

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

  // NOTE: we cannot initialize the Map here due to the
  // order in which our constructor actually does this
  // member initialization vs when it calls super.  It seems
  // like super is called, then our members are initialized:
  Map openFiles;

  public MockRAMDirectory() throws IOException {
    super();
    if (openFiles == null) {
      openFiles = new HashMap();
    }
  }
  public MockRAMDirectory(String dir) throws IOException {
    super(dir);
    if (openFiles == null) {
      openFiles = new HashMap();
    }
  }
  public MockRAMDirectory(Directory dir) throws IOException {
    super(dir);
    if (openFiles == null) {
      openFiles = new HashMap();
    }
  }
  public MockRAMDirectory(File dir) throws IOException {
    super(dir);
    if (openFiles == null) {
      openFiles = new HashMap();
    }
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
    synchronized(openFiles) {
      if (noDeleteOpenFile && openFiles.containsKey(name)) {
        throw new IOException("MockRAMDirectory: file \"" + name + "\" is still open: cannot delete");
      }
    }
    super.deleteFile(name);
  }

  public IndexOutput createOutput(String name) {
    if (openFiles == null) {
      openFiles = new HashMap();
    }
    synchronized(openFiles) {
      if (noDeleteOpenFile && openFiles.containsKey(name)) {
        // RuntimeException instead of IOException because
        // super() does not throw IOException currently:
        throw new RuntimeException("MockRAMDirectory: file \"" + name + "\" is still open: cannot overwrite");
      }
    }
    RAMFile file = new RAMFile(this);
    synchronized (this) {
      RAMFile existing = (RAMFile)fileMap.get(name);
      if (existing!=null) {
        sizeInBytes -= existing.sizeInBytes;
        existing.directory = null;
      }
      fileMap.put(name, file);
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
   * BufferedIndexOutput.BUFFER_SIZE (now 1024) bytes.
   */

  final long getRecomputedActualSizeInBytes() {
    long size = 0;
    Iterator it = fileMap.values().iterator();
    while (it.hasNext())
      size += ((RAMFile) it.next()).length;
    return size;
  }
}

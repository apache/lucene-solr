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
import java.io.FileNotFoundException;
import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * A memory-resident {@link Directory} implementation.  Locking
 * implementation is by default the {@link SingleInstanceLockFactory}
 * but can be changed with {@link #setLockFactory}.
 *
 * @version $Id$
 */
public class RAMDirectory extends Directory implements Serializable {

  private static final long serialVersionUID = 1l;

  HashMap fileMap = new HashMap();
  long sizeInBytes = 0;
  
  // *****
  // Lock acquisition sequence:  RAMDirectory, then RAMFile
  // *****

  /** Constructs an empty {@link Directory}. */
  public RAMDirectory() {
    setLockFactory(new SingleInstanceLockFactory());
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from a different
   * <code>Directory</code> implementation.  This can be used to load
   * a disk-based index into memory.
   * <P>
   * This should be used only with indices that can fit into memory.
   * <P>
   * Note that the resulting <code>RAMDirectory</code> instance is fully
   * independent from the original <code>Directory</code> (it is a
   * complete copy).  Any subsequent changes to the
   * original <code>Directory</code> will not be visible in the
   * <code>RAMDirectory</code> instance.
   *
   * @param dir a <code>Directory</code> value
   * @exception IOException if an error occurs
   */
  public RAMDirectory(Directory dir) throws IOException {
    this(dir, false);
  }
  
  private RAMDirectory(Directory dir, boolean closeDir) throws IOException {
    this();
    Directory.copy(dir, this, closeDir);
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from the {@link FSDirectory}.
   *
   * @param dir a <code>File</code> specifying the index directory
   *
   * @see #RAMDirectory(Directory)
   */
  public RAMDirectory(File dir) throws IOException {
    this(FSDirectory.getDirectory(dir), true);
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from the {@link FSDirectory}.
   *
   * @param dir a <code>String</code> specifying the full index directory path
   *
   * @see #RAMDirectory(Directory)
   */
  public RAMDirectory(String dir) throws IOException {
    this(FSDirectory.getDirectory(dir), true);
  }

  /** Returns an array of strings, one for each file in the directory. */
  public synchronized final String[] list() {
    Set fileNames = fileMap.keySet();
    String[] result = new String[fileNames.size()];
    int i = 0;
    Iterator it = fileNames.iterator();
    while (it.hasNext())
      result[i++] = (String)it.next();
    return result;
  }

  /** Returns true iff the named file exists in this directory. */
  public final boolean fileExists(String name) {
    RAMFile file;
    synchronized (this) {
      file = (RAMFile)fileMap.get(name);
    }
    return file != null;
  }

  /** Returns the time the named file was last modified.
   * @throws IOException if the file does not exist
   */
  public final long fileModified(String name) throws IOException {
    RAMFile file;
    synchronized (this) {
      file = (RAMFile)fileMap.get(name);
    }
    if (file==null)
      throw new FileNotFoundException(name);
    return file.getLastModified();
  }

  /** Set the modified time of an existing file to now.
   * @throws IOException if the file does not exist
   */
  public void touchFile(String name) throws IOException {
    RAMFile file;
    synchronized (this) {
      file = (RAMFile)fileMap.get(name);
    }
    if (file==null)
      throw new FileNotFoundException(name);
    
    long ts2, ts1 = System.currentTimeMillis();
    do {
      try {
        Thread.sleep(0, 1);
      } catch (InterruptedException e) {}
      ts2 = System.currentTimeMillis();
    } while(ts1 == ts2);
    
    file.setLastModified(ts2);
  }

  /** Returns the length in bytes of a file in the directory.
   * @throws IOException if the file does not exist
   */
  public final long fileLength(String name) throws IOException {
    RAMFile file;
    synchronized (this) {
      file = (RAMFile)fileMap.get(name);
    }
    if (file==null)
      throw new FileNotFoundException(name);
    return file.getLength();
  }
  
  /** Return total size in bytes of all files in this
   * directory.  This is currently quantized to
   * BufferedIndexOutput.BUFFER_SIZE. */
  public synchronized final long sizeInBytes() {
    return sizeInBytes;
  }
  
  /** Removes an existing file in the directory.
   * @throws IOException if the file does not exist
   */
  public synchronized final void deleteFile(String name) throws IOException {
    RAMFile file = (RAMFile)fileMap.get(name);
    if (file!=null) {
        fileMap.remove(name);
        file.directory = null;
        sizeInBytes -= file.sizeInBytes;       // updates to RAMFile.sizeInBytes synchronized on directory
    } else
      throw new FileNotFoundException(name);
  }

  /** Removes an existing file in the directory.
   * @throws IOException if from does not exist
   */
  public synchronized final void renameFile(String from, String to) throws IOException {
    RAMFile fromFile = (RAMFile)fileMap.get(from);
    if (fromFile==null)
      throw new FileNotFoundException(from);
    RAMFile toFile = (RAMFile)fileMap.get(to);
    if (toFile!=null) {
      sizeInBytes -= toFile.sizeInBytes;       // updates to RAMFile.sizeInBytes synchronized on directory
      toFile.directory = null;
    }
    fileMap.remove(from);
    fileMap.put(to, fromFile);
  }

  /** Creates a new, empty file in the directory with the given name. Returns a stream writing this file. */
  public IndexOutput createOutput(String name) {
    RAMFile file = new RAMFile(this);
    synchronized (this) {
      RAMFile existing = (RAMFile)fileMap.get(name);
      if (existing!=null) {
        sizeInBytes -= existing.sizeInBytes;
        existing.directory = null;
      }
      fileMap.put(name, file);
    }
    return new RAMOutputStream(file);
  }

  /** Returns a stream reading an existing file. */
  public final IndexInput openInput(String name) throws IOException {
    RAMFile file;
    synchronized (this) {
      file = (RAMFile)fileMap.get(name);
    }
    if (file == null)
      throw new FileNotFoundException(name);
    return new RAMInputStream(file);
  }

  /** Closes the store to future operations, releasing associated memory. */
  public final void close() {
    fileMap = null;
  }

}

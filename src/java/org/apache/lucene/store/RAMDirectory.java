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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A memory-resident {@link Directory} implementation.  Locking
 * implementation is by default the {@link SingleInstanceLockFactory}
 * but can be changed with {@link #setLockFactory}.
 */
public class RAMDirectory extends Directory implements Serializable {

  private static final long serialVersionUID = 1l;

  HashMap<String,RAMFile> fileMap = new HashMap<String,RAMFile>();
  long sizeInBytes;
  
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

  @Override
  public synchronized final String[] listAll() {
    ensureOpen();
    Set<String> fileNames = fileMap.keySet();
    String[] result = new String[fileNames.size()];
    int i = 0;
    for(final String fileName: fileNames) 
      result[i++] = fileName;
    return result;
  }

  /** Returns true iff the named file exists in this directory. */
  @Override
  public final boolean fileExists(String name) {
    ensureOpen();
    RAMFile file;
    synchronized (this) {
      file = fileMap.get(name);
    }
    return file != null;
  }

  /** Returns the time the named file was last modified.
   * @throws IOException if the file does not exist
   */
  @Override
  public final long fileModified(String name) throws IOException {
    ensureOpen();
    RAMFile file;
    synchronized (this) {
      file = fileMap.get(name);
    }
    if (file==null)
      throw new FileNotFoundException(name);
    return file.getLastModified();
  }

  /** Set the modified time of an existing file to now.
   * @throws IOException if the file does not exist
   */
  @Override
  public void touchFile(String name) throws IOException {
    ensureOpen();
    RAMFile file;
    synchronized (this) {
      file = fileMap.get(name);
    }
    if (file==null)
      throw new FileNotFoundException(name);
    
    long ts2, ts1 = System.currentTimeMillis();
    do {
      try {
        Thread.sleep(0, 1);
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
      ts2 = System.currentTimeMillis();
    } while(ts1 == ts2);
    
    file.setLastModified(ts2);
  }

  /** Returns the length in bytes of a file in the directory.
   * @throws IOException if the file does not exist
   */
  @Override
  public final long fileLength(String name) throws IOException {
    ensureOpen();
    RAMFile file;
    synchronized (this) {
      file = fileMap.get(name);
    }
    if (file==null)
      throw new FileNotFoundException(name);
    return file.getLength();
  }
  
  /** Return total size in bytes of all files in this
   * directory.  This is currently quantized to
   * RAMOutputStream.BUFFER_SIZE. */
  public synchronized final long sizeInBytes() {
    ensureOpen();
    return sizeInBytes;
  }
  
  /** Removes an existing file in the directory.
   * @throws IOException if the file does not exist
   */
  @Override
  public synchronized void deleteFile(String name) throws IOException {
    ensureOpen();
    RAMFile file = fileMap.get(name);
    if (file!=null) {
        fileMap.remove(name);
        file.directory = null;
        sizeInBytes -= file.sizeInBytes;
    } else
      throw new FileNotFoundException(name);
  }

  /** Creates a new, empty file in the directory with the given name. Returns a stream writing this file. */
  @Override
  public IndexOutput createOutput(String name) throws IOException {
    ensureOpen();
    RAMFile file = new RAMFile(this);
    synchronized (this) {
      RAMFile existing = fileMap.get(name);
      if (existing!=null) {
        sizeInBytes -= existing.sizeInBytes;
        existing.directory = null;
      }
      fileMap.put(name, file);
    }
    return new RAMOutputStream(file);
  }

  /** Returns a stream reading an existing file. */
  @Override
  public IndexInput openInput(String name) throws IOException {
    ensureOpen();
    RAMFile file;
    synchronized (this) {
      file = fileMap.get(name);
    }
    if (file == null)
      throw new FileNotFoundException(name);
    return new RAMInputStream(file);
  }

  /** Closes the store to future operations, releasing associated memory. */
  @Override
  public void close() {
    isOpen = false;
    fileMap = null;
  }
}

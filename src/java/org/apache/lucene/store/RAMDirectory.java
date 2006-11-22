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
public final class RAMDirectory extends Directory implements Serializable {

  private static final long serialVersionUID = 1l;

  private HashMap fileMap = new HashMap();
  private Set fileNames = fileMap.keySet();
  private Collection files = fileMap.values();
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
   *
   * @param dir a <code>Directory</code> value
   * @exception IOException if an error occurs
   */
  public RAMDirectory(Directory dir) throws IOException {
    this(dir, false);
  }
  
  private RAMDirectory(Directory dir, boolean closeDir) throws IOException {
    this();
    final String[] files = dir.list();
    byte[] buf = new byte[BufferedIndexOutput.BUFFER_SIZE];
    for (int i = 0; i < files.length; i++) {
      // make place on ram disk
      IndexOutput os = createOutput(files[i]);
      // read current file
      IndexInput is = dir.openInput(files[i]);
      // and copy to ram disk
      long len = is.length();
      long readCount = 0;
      while (readCount < len) {
        int toRead = readCount + BufferedIndexOutput.BUFFER_SIZE > len ? (int)(len - readCount) : BufferedIndexOutput.BUFFER_SIZE;
        is.readBytes(buf, 0, toRead);
        os.writeBytes(buf, toRead);
        readCount += toRead;
      }

      // graceful cleanup
      is.close();
      os.close();
    }
    if(closeDir)
      dir.close();
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from the {@link FSDirectory}.
   *
   * @param dir a <code>File</code> specifying the index directory
   */
  public RAMDirectory(File dir) throws IOException {
    this(FSDirectory.getDirectory(dir, false), true);
  }

  /**
   * Creates a new <code>RAMDirectory</code> instance from the {@link FSDirectory}.
   *
   * @param dir a <code>String</code> specifying the full index directory path
   */
  public RAMDirectory(String dir) throws IOException {
    this(FSDirectory.getDirectory(dir, false), true);
  }

  /** Returns an array of strings, one for each file in the directory. */
  public synchronized final String[] list() {
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
  
  /** Return total size in bytes of all files in this directory */
  public synchronized final long sizeInBytes() {
    return sizeInBytes;
  }
  
  /** Provided for testing purposes.  Use sizeInBytes() instead. */
  public synchronized final long getRecomputedSizeInBytes() {
    long size = 0;
    Iterator it = files.iterator();
    while (it.hasNext())
      size += ((RAMFile) it.next()).getSizeInBytes();
    return size;
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
  public final IndexOutput createOutput(String name) {
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
    fileNames = null;
    files = null;
  }

}

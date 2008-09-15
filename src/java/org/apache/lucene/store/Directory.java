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

/** A Directory is a flat list of files.  Files may be written once, when they
 * are created.  Once a file is created it may only be opened for read, or
 * deleted.  Random access is permitted both when reading and writing.
 *
 * <p> Java's i/o APIs not used directly, but rather all i/o is
 * through this API.  This permits things such as: <ul>
 * <li> implementation of RAM-based indices;
 * <li> implementation indices stored in a database, via JDBC;
 * <li> implementation of an index as a single file;
 * </ul>
 *
 * Directory locking is implemented by an instance of {@link
 * LockFactory}, and can be changed for each Directory
 * instance using {@link #setLockFactory}.
 *
 */
public abstract class Directory {

  volatile boolean isOpen = true;

  /** Holds the LockFactory instance (implements locking for
   * this Directory instance). */
  protected LockFactory lockFactory;

  /** Returns an array of strings, one for each file in the
   * directory.  This method may return null (for example for
   * {@link FSDirectory} if the underlying directory doesn't
   * exist in the filesystem or there are permissions
   * problems).*/
  public abstract String[] list()
       throws IOException;

  /** Returns true iff a file with the given name exists. */
  public abstract boolean fileExists(String name)
       throws IOException;

  /** Returns the time the named file was last modified. */
  public abstract long fileModified(String name)
       throws IOException;

  /** Set the modified time of an existing file to now. */
  public abstract void touchFile(String name)
       throws IOException;

  /** Removes an existing file in the directory. */
  public abstract void deleteFile(String name)
       throws IOException;

  /** Renames an existing file in the directory.
   * If a file already exists with the new name, then it is replaced.
   * This replacement is not guaranteed to be atomic.
   * @deprecated 
   */
  public abstract void renameFile(String from, String to)
       throws IOException;

  /** Returns the length of a file in the directory. */
  public abstract long fileLength(String name)
       throws IOException;


  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public abstract IndexOutput createOutput(String name) throws IOException;

  /** Ensure that any writes to this file are moved to
   *  stable storage.  Lucene uses this to properly commit
   *  changes to the index, to prevent a machine/OS crash
   *  from corrupting the index. */
  public void sync(String name) throws IOException {}

  /** Returns a stream reading an existing file. */
  public abstract IndexInput openInput(String name)
    throws IOException;

  /** Returns a stream reading an existing file, with the
   * specified read buffer size.  The particular Directory
   * implementation may ignore the buffer size.  Currently
   * the only Directory implementations that respect this
   * parameter are {@link FSDirectory} and {@link
   * org.apache.lucene.index.CompoundFileReader}.
  */
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    return openInput(name);
  }

  /** Construct a {@link Lock}.
   * @param name the name of the lock file
   */
  public Lock makeLock(String name) {
      return lockFactory.makeLock(name);
  }
  /**
   * Attempt to clear (forcefully unlock and remove) the
   * specified lock.  Only call this at a time when you are
   * certain this lock is no longer in use.
   * @param name name of the lock to be cleared.
   */
  public void clearLock(String name) throws IOException {
    if (lockFactory != null) {
      lockFactory.clearLock(name);
    }
  }

  /** Closes the store. */
  public abstract void close()
       throws IOException;

  /**
   * Set the LockFactory that this Directory instance should
   * use for its locking implementation.  Each * instance of
   * LockFactory should only be used for one directory (ie,
   * do not share a single instance across multiple
   * Directories).
   *
   * @param lockFactory instance of {@link LockFactory}.
   */
  public void setLockFactory(LockFactory lockFactory) {
      this.lockFactory = lockFactory;
      lockFactory.setLockPrefix(this.getLockID());
  }

  /**
   * Get the LockFactory that this Directory instance is
   * using for its locking implementation.  Note that this
   * may be null for Directory implementations that provide
   * their own locking implementation.
   */
  public LockFactory getLockFactory() {
      return this.lockFactory;
  }

  /**
   * Return a string identifier that uniquely differentiates
   * this Directory instance from other Directory instances.
   * This ID should be the same if two Directory instances
   * (even in different JVMs and/or on different machines)
   * are considered "the same index".  This is how locking
   * "scopes" to the right index.
   */
  public String getLockID() {
      return this.toString();
  }

  /**
   * Copy contents of a directory src to a directory dest.
   * If a file in src already exists in dest then the
   * one in dest will be blindly overwritten.
   *
   * @param src source directory
   * @param dest destination directory
   * @param closeDirSrc if <code>true</code>, call {@link #close()} method on source directory
   * @throws IOException
   */
  public static void copy(Directory src, Directory dest, boolean closeDirSrc) throws IOException {
      final String[] files = src.list();

      if (files == null)
        throw new IOException("cannot read directory " + src + ": list() returned null");

      byte[] buf = new byte[BufferedIndexOutput.BUFFER_SIZE];
      for (int i = 0; i < files.length; i++) {
        IndexOutput os = null;
        IndexInput is = null;
        try {
          // create file in dest directory
          os = dest.createOutput(files[i]);
          // read current file
          is = src.openInput(files[i]);
          // and copy to dest directory
          long len = is.length();
          long readCount = 0;
          while (readCount < len) {
            int toRead = readCount + BufferedIndexOutput.BUFFER_SIZE > len ? (int)(len - readCount) : BufferedIndexOutput.BUFFER_SIZE;
            is.readBytes(buf, 0, toRead);
            os.writeBytes(buf, toRead);
            readCount += toRead;
          }
        } finally {
          // graceful cleanup
          try {
            if (os != null)
              os.close();
          } finally {
            if (is != null)
              is.close();
          }
        }
      }
      if(closeDirSrc)
        src.close();
  }

  /**
   * @throws AlreadyClosedException if this Directory is closed
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (!isOpen)
      throw new AlreadyClosedException("this Directory is closed");
  }
}

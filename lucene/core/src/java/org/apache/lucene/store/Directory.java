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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Closeable;
import java.nio.file.NoSuchFileException;
import java.util.Collection; // for javadocs

import org.apache.lucene.util.IOUtils;

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
public abstract class Directory implements Closeable {

  /**
   * Returns an array of strings, one for each file in the directory.
   * 
   * @throws NoSuchDirectoryException if the directory is not prepared for any
   *         write operations (such as {@link #createOutput(String, IOContext)}).
   * @throws IOException in case of other IO errors
   */
  public abstract String[] listAll() throws IOException;

  /** Returns true iff a file with the given name exists.
   *
   *  @deprecated This method will be removed in 5.0 */
  @Deprecated
  public abstract boolean fileExists(String name)
       throws IOException;

  /** Removes an existing file in the directory. */
  public abstract void deleteFile(String name)
       throws IOException;

  /**
   * Returns the length of a file in the directory. This method follows the
   * following contract:
   * <ul>
   * <li>Throws {@link FileNotFoundException} or {@link NoSuchFileException}
   * if the file does not exist.
   * <li>Returns a value &ge;0 if the file exists, which specifies its length.
   * </ul>
   * 
   * @param name the name of the file for which to return the length.
   * @throws IOException if there was an IO error while retrieving the file's
   *         length.
   */
  public abstract long fileLength(String name) throws IOException;


  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public abstract IndexOutput createOutput(String name, IOContext context)
       throws IOException;

  /**
   * Ensure that any writes to these files are moved to
   * stable storage.  Lucene uses this to properly commit
   * changes to the index, to prevent a machine/OS crash
   * from corrupting the index.<br/>
   * <br/>
   * NOTE: Clients may call this method for same files over
   * and over again, so some impls might optimize for that.
   * For other impls the operation can be a noop, for various
   * reasons.
   */
  public abstract void sync(Collection<String> names) throws IOException;

  /** Returns a stream reading an existing file, with the
   * specified read buffer size.  The particular Directory
   * implementation may ignore the buffer size.  Currently
   * the only Directory implementations that respect this
   * parameter are {@link FSDirectory} and {@link
   * CompoundFileDirectory}.
   * <p>Throws {@link FileNotFoundException} or {@link NoSuchFileException}
   * if the file does not exist.
   */
  public abstract IndexInput openInput(String name, IOContext context) throws IOException;
  
  /** Returns a stream reading an existing file, computing checksum as it reads */
  public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
    return new BufferedChecksumIndexInput(openInput(name, context));
  }
  
  /** Construct a {@link Lock}.
   * @param name the name of the lock file
   */
  public abstract Lock makeLock(String name);

  /**
   * Attempt to clear (forcefully unlock and remove) the
   * specified lock.  Only call this at a time when you are
   * certain this lock is no longer in use.
   * @param name name of the lock to be cleared.
   */
  public abstract void clearLock(String name) throws IOException;

  /** Closes the store. */
  @Override
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
  public abstract void setLockFactory(LockFactory lockFactory) throws IOException;

  /**
   * Get the LockFactory that this Directory instance is
   * using for its locking implementation.  Note that this
   * may be null for Directory implementations that provide
   * their own locking implementation.
   */
  public abstract LockFactory getLockFactory();

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

  @Override
  public String toString() {
    return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode()) + " lockFactory=" + getLockFactory();
  }

  /**
   * Copies the file <i>src</i> to {@link Directory} <i>to</i> under the new
   * file name <i>dest</i>.
   * <p>
   * If you want to copy the entire source directory to the destination one, you
   * can do so like this:
   * 
   * <pre class="prettyprint">
   * Directory to; // the directory to copy to
   * for (String file : dir.listAll()) {
   *   dir.copy(to, file, newFile, IOContext.DEFAULT); // newFile can be either file, or a new name
   * }
   * </pre>
   * <p>
   * <b>NOTE:</b> this method does not check whether <i>dest</i> exist and will
   * overwrite it if it does.
   */
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    IndexOutput os = null;
    IndexInput is = null;
    boolean success = false;
    try {
      os = to.createOutput(dest, context);
      is = openInput(src, context);
      os.copyBytes(is, is.length());
      success = true;
    } finally {
      if (success) {
        IOUtils.close(os, is);
      } else {
        IOUtils.closeWhileHandlingException(os, is);
        try {
          to.deleteFile(dest);
        } catch (Throwable t) {
        }
      }
    }
  }

  /**
   * @throws AlreadyClosedException if this Directory is closed
   */
  protected void ensureOpen() throws AlreadyClosedException {}

}

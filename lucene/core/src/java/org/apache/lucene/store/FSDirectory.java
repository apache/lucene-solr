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

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

import static java.util.Collections.synchronizedSet;

/**
 * Base class for Directory implementations that store index
 * files in the file system.  
 * <a name="subclasses"/>
 * There are currently three core
 * subclasses:
 *
 * <ul>
 *
 *  <li> {@link SimpleFSDirectory} is a straightforward
 *       implementation using java.io.RandomAccessFile.
 *       However, it has poor concurrent performance
 *       (multiple threads will bottleneck) as it
 *       synchronizes when multiple threads read from the
 *       same file.
 *
 *  <li> {@link NIOFSDirectory} uses java.nio's
 *       FileChannel's positional io when reading to avoid
 *       synchronization when reading from the same file.
 *       Unfortunately, due to a Windows-only <a
 *       href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6265734">Sun
 *       JRE bug</a> this is a poor choice for Windows, but
 *       on all other platforms this is the preferred
 *       choice. Applications using {@link Thread#interrupt()} or
 *       {@link Future#cancel(boolean)} should use
 *       {@link SimpleFSDirectory} instead. See {@link NIOFSDirectory} java doc
 *       for details.
 *        
 *        
 *
 *  <li> {@link MMapDirectory} uses memory-mapped IO when
 *       reading. This is a good choice if you have plenty
 *       of virtual memory relative to your index size, eg
 *       if you are running on a 64 bit JRE, or you are
 *       running on a 32 bit JRE but your index sizes are
 *       small enough to fit into the virtual memory space.
 *       Java has currently the limitation of not being able to
 *       unmap files from user code. The files are unmapped, when GC
 *       releases the byte buffers. Due to
 *       <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">
 *       this bug</a> in Sun's JRE, MMapDirectory's {@link IndexInput#close}
 *       is unable to close the underlying OS file handle. Only when
 *       GC finally collects the underlying objects, which could be
 *       quite some time later, will the file handle be closed.
 *       This will consume additional transient disk usage: on Windows,
 *       attempts to delete or overwrite the files will result in an
 *       exception; on other platforms, which typically have a &quot;delete on
 *       last close&quot; semantics, while such operations will succeed, the bytes
 *       are still consuming space on disk.  For many applications this
 *       limitation is not a problem (e.g. if you have plenty of disk space,
 *       and you don't rely on overwriting files on Windows) but it's still
 *       an important limitation to be aware of. This class supplies a
 *       (possibly dangerous) workaround mentioned in the bug report,
 *       which may fail on non-Sun JVMs.
 *       
 *       Applications using {@link Thread#interrupt()} or
 *       {@link Future#cancel(boolean)} should use
 *       {@link SimpleFSDirectory} instead. See {@link MMapDirectory}
 *       java doc for details.
 * </ul>
 *
 * Unfortunately, because of system peculiarities, there is
 * no single overall best implementation.  Therefore, we've
 * added the {@link #open} method, to allow Lucene to choose
 * the best FSDirectory implementation given your
 * environment, and the known limitations of each
 * implementation.  For users who have no reason to prefer a
 * specific implementation, it's best to simply use {@link
 * #open}.  For all others, you should instantiate the
 * desired implementation directly.
 *
 * <p>The locking implementation is by default {@link
 * NativeFSLockFactory}, but can be changed by
 * passing in a custom {@link LockFactory} instance.
 *
 * @see Directory
 */
public abstract class FSDirectory extends BaseDirectory {

  /**
   * Default read chunk size: 8192 bytes (this is the size up to which the JDK
     does not allocate additional arrays while reading/writing)
     @deprecated This constant is no longer used since Lucene 4.5.
   */
  @Deprecated
  public static final int DEFAULT_READ_CHUNK_SIZE = 8192;

  protected final File directory; // The underlying filesystem directory
  protected final Set<String> staleFiles = synchronizedSet(new HashSet<String>()); // Files written, but not yet sync'ed
  private int chunkSize = DEFAULT_READ_CHUNK_SIZE;

  // returns the canonical version of the directory, creating it if it doesn't exist.
  private static File getCanonicalPath(File file) throws IOException {
    return new File(file.getCanonicalPath());
  }

  /** Create a new FSDirectory for the named location (ctor for subclasses).
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException if there is a low-level I/O error
   */
  protected FSDirectory(File path, LockFactory lockFactory) throws IOException {
    // new ctors use always NativeFSLockFactory as default:
    if (lockFactory == null) {
      lockFactory = new NativeFSLockFactory();
    }
    directory = getCanonicalPath(path);

    if (directory.exists() && !directory.isDirectory())
      throw new NoSuchDirectoryException("file '" + directory + "' exists but is not a directory");

    setLockFactory(lockFactory);

  }

  /** Creates an FSDirectory instance, trying to pick the
   *  best implementation given the current environment.
   *  The directory returned uses the {@link NativeFSLockFactory}.
   *
   *  <p>Currently this returns {@link MMapDirectory} for most Solaris
   *  and Windows 64-bit JREs, {@link NIOFSDirectory} for other
   *  non-Windows JREs, and {@link SimpleFSDirectory} for other
   *  JREs on Windows. It is highly recommended that you consult the
   *  implementation's documentation for your platform before
   *  using this method.
   *
   * <p><b>NOTE</b>: this method may suddenly change which
   * implementation is returned from release to release, in
   * the event that higher performance defaults become
   * possible; if the precise implementation is important to
   * your application, please instantiate it directly,
   * instead. For optimal performance you should consider using
   * {@link MMapDirectory} on 64 bit JVMs.
   *
   * <p>See <a href="#subclasses">above</a> */
  public static FSDirectory open(File path) throws IOException {
    return open(path, null);
  }

  /** Just like {@link #open(File)}, but allows you to
   *  also specify a custom {@link LockFactory}. */
  public static FSDirectory open(File path, LockFactory lockFactory) throws IOException {
    if (Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
      return new MMapDirectory(path, lockFactory);
    } else if (Constants.WINDOWS) {
      return new SimpleFSDirectory(path, lockFactory);
    } else {
      return new NIOFSDirectory(path, lockFactory);
    }
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    super.setLockFactory(lockFactory);

    // for filesystem based LockFactory, delete the lockPrefix, if the locks are placed
    // in index dir. If no index dir is given, set ourselves
    if (lockFactory instanceof FSLockFactory) {
      final FSLockFactory lf = (FSLockFactory) lockFactory;
      final File dir = lf.getLockDir();
      // if the lock factory has no lockDir set, use the this directory as lockDir
      if (dir == null) {
        lf.setLockDir(directory);
        lf.setLockPrefix(null);
      } else if (dir.getCanonicalPath().equals(directory.getCanonicalPath())) {
        lf.setLockPrefix(null);
      }
    }

  }
  
  /** Lists all files (not subdirectories) in the
   *  directory.  This method never returns null (throws
   *  {@link IOException} instead).
   *
   *  @throws NoSuchDirectoryException if the directory
   *   does not exist, or does exist but is not a
   *   directory.
   *  @throws IOException if list() returns null */
  public static String[] listAll(File dir) throws IOException {
    if (!dir.exists())
      throw new NoSuchDirectoryException("directory '" + dir + "' does not exist");
    else if (!dir.isDirectory())
      throw new NoSuchDirectoryException("file '" + dir + "' exists but is not a directory");

    // Exclude subdirs
    String[] result = dir.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String file) {
          return !new File(dir, file).isDirectory();
        }
      });

    if (result == null)
      throw new IOException("directory '" + dir + "' exists and is a directory, but cannot be listed: list() returned null");

    return result;
  }

  /** Lists all files (not subdirectories) in the
   * directory.
   * @see #listAll(File) */
  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    return listAll(directory);
  }

  /** Returns true iff a file with the given name exists. */
  @Override
  public boolean fileExists(String name) {
    ensureOpen();
    File file = new File(directory, name);
    return file.exists();
  }

  /** Returns the length in bytes of a file in the directory. */
  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    File file = new File(directory, name);
    final long len = file.length();
    if (len == 0 && !file.exists()) {
      throw new FileNotFoundException(name);
    } else {
      return len;
    }
  }

  /** Removes an existing file in the directory. */
  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    File file = new File(directory, name);
    if (!file.delete())
      throw new IOException("Cannot delete " + file);
    staleFiles.remove(name);
  }

  /** Creates an IndexOutput for the file with the given name. */
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();

    ensureCanWrite(name);
    return new FSIndexOutput(name);
  }

  protected void ensureCanWrite(String name) throws IOException {
    if (!directory.exists())
      if (!directory.mkdirs())
        throw new IOException("Cannot create directory: " + directory);

    File file = new File(directory, name);
    if (file.exists() && !file.delete())          // delete existing, if any
      throw new IOException("Cannot overwrite: " + file);
  }

  /**
   * Sub classes should call this method on closing an open {@link IndexOutput}, reporting the name of the file
   * that was closed. {@code FSDirectory} needs this information to take care of syncing stale files.
   */
  protected void onIndexOutputClosed(String name) {
    staleFiles.add(name);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
    Set<String> toSync = new HashSet<>(names);
    toSync.retainAll(staleFiles);

    for (String name : toSync) {
      fsync(name);
    }
    
    // fsync the directory itsself, but only if there was any file fsynced before
    // (otherwise it can happen that the directory does not yet exist)!
    if (!toSync.isEmpty()) {
      IOUtils.fsync(directory, true);
    }
    
    staleFiles.removeAll(toSync);
  }

  @Override
  public String getLockID() {
    ensureOpen();
    String dirName;                               // name to be hashed
    try {
      dirName = directory.getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException(e.toString(), e);
    }

    int digest = 0;
    for(int charIDX=0;charIDX<dirName.length();charIDX++) {
      final char ch = dirName.charAt(charIDX);
      digest = 31 * digest + ch;
    }
    return "lucene-" + Integer.toHexString(digest);
  }

  /** Closes the store to future operations. */
  @Override
  public synchronized void close() {
    isOpen = false;
  }

  /** @return the underlying filesystem directory */
  public File getDirectory() {
    ensureOpen();
    return directory;
  }

  /** For debug output. */
  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "@" + directory + " lockFactory=" + getLockFactory();
  }

  /**
   * This setting has no effect anymore.
   * @deprecated This is no longer used since Lucene 4.5.
   */
  @Deprecated
  public final void setReadChunkSize(int chunkSize) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be positive");
    }
    this.chunkSize = chunkSize;
  }

  /**
   * This setting has no effect anymore.
   * @deprecated This is no longer used since Lucene 4.5.
   */
  @Deprecated
  public final int getReadChunkSize() {
    return chunkSize;
  }

  final class FSIndexOutput extends OutputStreamIndexOutput {
    /**
     * The maximum chunk size is 8192 bytes, because {@link FileOutputStream} mallocs
     * a native buffer outside of stack if the write buffer size is larger.
     */
    static final int CHUNK_SIZE = 8192;
    
    private final String name;

    public FSIndexOutput(String name) throws IOException {
      super(new FilterOutputStream(new FileOutputStream(new File(directory, name))) {
        // This implementation ensures, that we never write more than CHUNK_SIZE bytes:
        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
          while (length > 0) {
            final int chunk = Math.min(length, CHUNK_SIZE);
            out.write(b, offset, chunk);
            length -= chunk;
            offset += chunk;
          }
        }
      }, CHUNK_SIZE);
      this.name = name;
    }
    
    @Override
    public void close() throws IOException {
      try {
        onIndexOutputClosed(name);
      } finally {
        super.close();
      }
    }
  }

  protected void fsync(String name) throws IOException {
    IOUtils.fsync(new File(directory, name), false);
  }
}

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

import java.io.File;
import java.io.FilenameFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexFileNameFilter;
import org.apache.lucene.util.Constants;

// Used only for WRITE_LOCK_NAME in deprecated create=true case:
import org.apache.lucene.index.IndexWriter;

/**
 * <a name="subclasses"/>
 * Base class for Directory implementations that store index
 * files in the file system.  There are currently three core
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
 *       choice.
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
 * The deprecated <code>getDirectory</code> methods default to use
 * {@link SimpleFSLockFactory} for backwards compatibility.
 * The system properties 
 * <code>org.apache.lucene.store.FSDirectoryLockFactoryClass</code>
 * and <code>org.apache.lucene.FSDirectory.class</code>
 * are deprecated and only used by the deprecated
 * <code>getDirectory</code> methods. The system property
 * <code>org.apache.lucene.lockDir</code> is ignored completely,
 * If you really want to store locks
 * elsewhere, you can create your own {@link
 * SimpleFSLockFactory} (or {@link NativeFSLockFactory},
 * etc.) passing in your preferred lock directory.
 *
 * <p><em>In 3.0 this class will become abstract.</em>
 *
 * @see Directory
 */
// TODO: in 3.0 this will become an abstract base class
public class FSDirectory extends Directory {
    
  /** This cache of directories ensures that there is a unique Directory
   * instance per path, so that synchronization on the Directory can be used to
   * synchronize access between readers and writers.  We use
   * refcounts to ensure when the last use of an FSDirectory
   * instance for a given canonical path is closed, we remove the
   * instance from the cache.  See LUCENE-776
   * for some relevant discussion.
   * @deprecated Not used by any non-deprecated methods anymore
   */
  private static final Map DIRECTORIES = new HashMap();

  private static boolean disableLocks = false;

  // TODO: should this move up to the Directory base class?  Also: should we
  // make a per-instance (in addition to the static "default") version?

  /**
   * Set whether Lucene's use of lock files is disabled. By default, 
   * lock files are enabled. They should only be disabled if the index
   * is on a read-only medium like a CD-ROM.
   * @deprecated Use a {@link #open(File, LockFactory)} or a constructor
   * that takes a {@link LockFactory} and supply
   * {@link NoLockFactory#getNoLockFactory}. This setting does not work
   * with {@link #open(File)} only the deprecated <code>getDirectory</code>
   * respect this setting.   
   */
  public static void setDisableLocks(boolean doDisableLocks) {
    FSDirectory.disableLocks = doDisableLocks;
  }

  /**
   * Returns whether Lucene's use of lock files is disabled.
   * @return true if locks are disabled, false if locks are enabled.
   * @see #setDisableLocks
   * @deprecated Use a constructor that takes a {@link LockFactory} and
   * supply {@link NoLockFactory#getNoLockFactory}.
  */
  public static boolean getDisableLocks() {
    return FSDirectory.disableLocks;
  }

  /**
   * Directory specified by <code>org.apache.lucene.lockDir</code>
   * or <code>java.io.tmpdir</code> system property.

   * @deprecated As of 2.1, <code>LOCK_DIR</code> is unused
   * because the write.lock is now stored by default in the
   * index directory.  If you really want to store locks
   * elsewhere, you can create your own {@link
   * SimpleFSLockFactory} (or {@link NativeFSLockFactory},
   * etc.) passing in your preferred lock directory.  Then,
   * pass this <code>LockFactory</code> instance to one of
   * the <code>open</code> methods that take a
   * <code>lockFactory</code> (for example, {@link #open(File, LockFactory)}).
   */
  public static final String LOCK_DIR = System.getProperty("org.apache.lucene.lockDir",
                                                           System.getProperty("java.io.tmpdir"));

  /** The default class which implements filesystem-based directories. */
  // deprecated
  private static Class IMPL;
  static {
    try {
      String name =
        System.getProperty("org.apache.lucene.FSDirectory.class",
                           SimpleFSDirectory.class.getName());
      if (FSDirectory.class.getName().equals(name)) {
        // FSDirectory will be abstract, so we replace it by the correct class
        IMPL = SimpleFSDirectory.class;
      } else {
        IMPL = Class.forName(name);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("cannot load FSDirectory class: " + e.toString(), e);
    } catch (SecurityException se) {
      IMPL = SimpleFSDirectory.class;
    }
  }

  private static MessageDigest DIGESTER;

  static {
    try {
      DIGESTER = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e.toString(), e);
    }
  }

  /** A buffer optionally used in renameTo method */
  private byte[] buffer = null;


  /** Returns the directory instance for the named location.
   *
   * @deprecated Use {@link #open(File)}
   *
   * @param path the path to the directory.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(String path)
      throws IOException {
    return getDirectory(new File(path), null);
  }

  /** Returns the directory instance for the named location.
   *
   * @deprecated Use {@link #open(File, LockFactory)}
   *
   * @param path the path to the directory.
   * @param lockFactory instance of {@link LockFactory} providing the
   *        locking implementation.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(String path, LockFactory lockFactory)
      throws IOException {
    return getDirectory(new File(path), lockFactory);
  }

  /** Returns the directory instance for the named location.
   *
   * @deprecated Use {@link #open(File)}
   *
   * @param file the path to the directory.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(File file)
    throws IOException {
    return getDirectory(file, null);
  }

  /** Returns the directory instance for the named location.
   *
   * @deprecated Use {@link #open(File, LockFactory)}
   *
   * @param file the path to the directory.
   * @param lockFactory instance of {@link LockFactory} providing the
   *        locking implementation.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(File file, LockFactory lockFactory)
    throws IOException
  {
    file = getCanonicalPath(file);

    FSDirectory dir;
    synchronized (DIRECTORIES) {
      dir = (FSDirectory)DIRECTORIES.get(file);
      if (dir == null) {
        try {
          dir = (FSDirectory)IMPL.newInstance();
        } catch (Exception e) {
          throw new RuntimeException("cannot load FSDirectory class: " + e.toString(), e);
        }
        dir.init(file, lockFactory);
        DIRECTORIES.put(file, dir);
      } else {
        // Catch the case where a Directory is pulled from the cache, but has a
        // different LockFactory instance.
        if (lockFactory != null && lockFactory != dir.getLockFactory()) {
          throw new IOException("Directory was previously created with a different LockFactory instance; please pass null as the lockFactory instance and use setLockFactory to change it");
        }
        dir.checked = false;
      }
    }
    synchronized (dir) {
      dir.refCount++;
    }
    return dir;
  }


  /** Returns the directory instance for the named location.
   *
   * @deprecated Use IndexWriter's create flag, instead, to
   * create a new index.
   *
   * @param path the path to the directory.
   * @param create if true, create, or erase any existing contents.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(String path, boolean create)
      throws IOException {
    return getDirectory(new File(path), create);
  }

  /** Returns the directory instance for the named location.
   *
   * @deprecated Use IndexWriter's create flag, instead, to
   * create a new index.
   *
   * @param file the path to the directory.
   * @param create if true, create, or erase any existing contents.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(File file, boolean create)
    throws IOException
  {
    FSDirectory dir = getDirectory(file, null);

    // This is now deprecated (creation should only be done
    // by IndexWriter):
    if (create) {
      dir.create();
    }

    return dir;
  }

  /** @deprecated */
  private void create() throws IOException {
    if (directory.exists()) {
      String[] files = directory.list(IndexFileNameFilter.getFilter());            // clear old files
      if (files == null)
        throw new IOException("cannot read directory " + directory.getAbsolutePath() + ": list() returned null");
      for (int i = 0; i < files.length; i++) {
        File file = new File(directory, files[i]);
        if (!file.delete())
          throw new IOException("Cannot delete " + file);
      }
    }
    lockFactory.clearLock(IndexWriter.WRITE_LOCK_NAME);
  }

  // returns the canonical version of the directory, creating it if it doesn't exist.
  private static File getCanonicalPath(File file) throws IOException {
    return new File(file.getCanonicalPath());
  }

  private boolean checked;

  final void createDir() throws IOException {
    if (!checked) {
      if (!directory.exists())
        if (!directory.mkdirs())
          throw new IOException("Cannot create directory: " + directory);

      checked = true;
    }
  }

  /** Initializes the directory to create a new file with the given name.
   * This method should be used in {@link #createOutput}. */
  protected final void initOutput(String name) throws IOException {
    ensureOpen();
    createDir();
    File file = new File(directory, name);
    if (file.exists() && !file.delete())          // delete existing, if any
      throw new IOException("Cannot overwrite: " + file);
  }

  /** The underlying filesystem directory */
  protected File directory = null;
  
  /** @deprecated */
  private int refCount = 0;

  /** @deprecated */
  protected FSDirectory() {}; // permit subclassing

  /** Create a new FSDirectory for the named location (ctor for subclasses).
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException
   */
  protected FSDirectory(File path, LockFactory lockFactory) throws IOException {
    path = getCanonicalPath(path);
    // new ctors use always NativeFSLockFactory as default:
    if (lockFactory == null) {
      lockFactory = new NativeFSLockFactory();
    }
    init(path, lockFactory);
    refCount = 1;
  }

  /** Creates an FSDirectory instance, trying to pick the
   *  best implementation given the current environment.
   *  The directory returned uses the {@link NativeFSLockFactory}.
   *
   *  <p>Currently this returns {@link NIOFSDirectory}
   *  on non-Windows JREs and {@link SimpleFSDirectory}
   *  on Windows.
   *
   * <p><b>NOTE</b>: this method may suddenly change which
   * implementation is returned from release to release, in
   * the event that higher performance defaults become
   * possible; if the precise implementation is important to
   * your application, please instantiate it directly,
   * instead. On 64 bit systems, it may also good to
   * return {@link MMapDirectory}, but this is disabled
   * because of officially missing unmap support in Java.
   * For optimal performance you should consider using
   * this implementation on 64 bit JVMs.
   *
   * <p>See <a href="#subclasses">above</a> */
  public static FSDirectory open(File path) throws IOException {
    return open(path, null);
  }

  /** Just like {@link #open(File)}, but allows you to
   *  also specify a custom {@link LockFactory}. */
  public static FSDirectory open(File path, LockFactory lockFactory) throws IOException {
    /* For testing:
    MMapDirectory dir=new MMapDirectory(path, lockFactory);
    dir.setUseUnmap(true);
    return dir;
    */

    if (Constants.WINDOWS) {
      return new SimpleFSDirectory(path, lockFactory);
    } else {
      return new NIOFSDirectory(path, lockFactory);
    }
  }

  /* will move to ctor, when reflection is removed in 3.0 */
  private void init(File path, LockFactory lockFactory) throws IOException {

    // Set up lockFactory with cascaded defaults: if an instance was passed in,
    // use that; else if locks are disabled, use NoLockFactory; else if the
    // system property org.apache.lucene.store.FSDirectoryLockFactoryClass is set,
    // instantiate that; else, use SimpleFSLockFactory:

    directory = path;

    if (directory.exists() && !directory.isDirectory())
      throw new NoSuchDirectoryException("file '" + directory + "' exists but is not a directory");

    if (lockFactory == null) {

      if (disableLocks) {
        // Locks are disabled:
        lockFactory = NoLockFactory.getNoLockFactory();
      } else {
        String lockClassName = System.getProperty("org.apache.lucene.store.FSDirectoryLockFactoryClass");

        if (lockClassName != null && !lockClassName.equals("")) {
          Class c;

          try {
            c = Class.forName(lockClassName);
          } catch (ClassNotFoundException e) {
            throw new IOException("unable to find LockClass " + lockClassName);
          }

          try {
            lockFactory = (LockFactory) c.newInstance();          
          } catch (IllegalAccessException e) {
            throw new IOException("IllegalAccessException when instantiating LockClass " + lockClassName);
          } catch (InstantiationException e) {
            throw new IOException("InstantiationException when instantiating LockClass " + lockClassName);
          } catch (ClassCastException e) {
            throw new IOException("unable to cast LockClass " + lockClassName + " instance to a LockFactory");
          }
        } else {
          // Our default lock is SimpleFSLockFactory;
          // default lockDir is our index directory:
          lockFactory = new SimpleFSLockFactory();
        }
      }
    }

    setLockFactory(lockFactory);
    
    // for filesystem based LockFactory, delete the lockPrefix, if the locks are placed
    // in index dir. If no index dir is given, set ourselves
    if (lockFactory instanceof FSLockFactory) {
      final FSLockFactory lf = (FSLockFactory) lockFactory;
      final File dir = lf.getLockDir();
      // if the lock factory has no lockDir set, use the this directory as lockDir
      if (dir == null) {
        lf.setLockDir(this.directory);
        lf.setLockPrefix(null);
      } else if (dir.getCanonicalPath().equals(this.directory.getCanonicalPath())) {
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
        public boolean accept(File dir, String file) {
          return !new File(dir, file).isDirectory();
        }
      });

    if (result == null)
      throw new IOException("directory '" + dir + "' exists and is a directory, but cannot be listed: list() returned null");

    return result;
  }

  public String[] list() {
    ensureOpen();
    return directory.list(IndexFileNameFilter.getFilter());
  }

  /** Lists all files (not subdirectories) in the
   * directory.
   * @see #listAll(File) */
  public String[] listAll() throws IOException {
    ensureOpen();
    return listAll(directory);
  }

  /** Returns true iff a file with the given name exists. */
  public boolean fileExists(String name) {
    ensureOpen();
    File file = new File(directory, name);
    return file.exists();
  }

  /** Returns the time the named file was last modified. */
  public long fileModified(String name) {
    ensureOpen();
    File file = new File(directory, name);
    return file.lastModified();
  }

  /** Returns the time the named file was last modified. */
  public static long fileModified(File directory, String name) {
    File file = new File(directory, name);
    return file.lastModified();
  }

  /** Set the modified time of an existing file to now. */
  public void touchFile(String name) {
    ensureOpen();
    File file = new File(directory, name);
    file.setLastModified(System.currentTimeMillis());
  }

  /** Returns the length in bytes of a file in the directory. */
  public long fileLength(String name) {
    ensureOpen();
    File file = new File(directory, name);
    return file.length();
  }

  /** Removes an existing file in the directory. */
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    File file = new File(directory, name);
    if (!file.delete())
      throw new IOException("Cannot delete " + file);
  }

  /** Renames an existing file in the directory. 
   * Warning: This is not atomic.
   * @deprecated 
   */
  public synchronized void renameFile(String from, String to)
      throws IOException {
    ensureOpen();
    File old = new File(directory, from);
    File nu = new File(directory, to);

    /* This is not atomic.  If the program crashes between the call to
       delete() and the call to renameTo() then we're screwed, but I've
       been unable to figure out how else to do this... */

    if (nu.exists())
      if (!nu.delete())
        throw new IOException("Cannot delete " + nu);

    // Rename the old file to the new one. Unfortunately, the renameTo()
    // method does not work reliably under some JVMs.  Therefore, if the
    // rename fails, we manually rename by copying the old file to the new one
    if (!old.renameTo(nu)) {
      java.io.InputStream in = null;
      java.io.OutputStream out = null;
      try {
        in = new FileInputStream(old);
        out = new FileOutputStream(nu);
        // see if the buffer needs to be initialized. Initialization is
        // only done on-demand since many VM's will never run into the renameTo
        // bug and hence shouldn't waste 1K of mem for no reason.
        if (buffer == null) {
          buffer = new byte[1024];
        }
        int len;
        while ((len = in.read(buffer)) >= 0) {
          out.write(buffer, 0, len);
        }

        // delete the old file.
        old.delete();
      }
      catch (IOException ioe) {
        IOException newExc = new IOException("Cannot rename " + old + " to " + nu);
        newExc.initCause(ioe);
        throw newExc;
      }
      finally {
        try {
          if (in != null) {
            try {
              in.close();
            } catch (IOException e) {
              throw new RuntimeException("Cannot close input stream: " + e.toString(), e);
            }
          }
        } finally {
          if (out != null) {
            try {
              out.close();
            } catch (IOException e) {
              throw new RuntimeException("Cannot close output stream: " + e.toString(), e);
            }
          }
        }
      }
    }
  }

  /** Creates an IndexOutput for the file with the given name.
   * <em>In 3.0 this method will become abstract.</em> */
  public IndexOutput createOutput(String name) throws IOException {
    initOutput(name);
    return new FSIndexOutput(new File(directory, name));
  }

  public void sync(String name) throws IOException {
    ensureOpen();
    File fullFile = new File(directory, name);
    boolean success = false;
    int retryCount = 0;
    IOException exc = null;
    while(!success && retryCount < 5) {
      retryCount++;
      RandomAccessFile file = null;
      try {
        try {
          file = new RandomAccessFile(fullFile, "rw");
          file.getFD().sync();
          success = true;
        } finally {
          if (file != null)
            file.close();
        }
      } catch (IOException ioe) {
        if (exc == null)
          exc = ioe;
        try {
          // Pause 5 msec
          Thread.sleep(5);
        } catch (InterruptedException ie) {
          // In 3.0 we will change this to throw
          // InterruptedException instead
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }
    if (!success)
      // Throw original exception
      throw exc;
  }

  // Inherit javadoc
  public IndexInput openInput(String name) throws IOException {
    ensureOpen();
    return openInput(name, BufferedIndexInput.BUFFER_SIZE);
  }

  /** Creates an IndexInput for the file with the given name.
   * <em>In 3.0 this method will become abstract.</em> */
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    ensureOpen();
    return new FSIndexInput(new File(directory, name), bufferSize);
  }

  /**
   * So we can do some byte-to-hexchar conversion below
   */
  private static final char[] HEX_DIGITS =
  {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  
  public String getLockID() {
    ensureOpen();
    String dirName;                               // name to be hashed
    try {
      dirName = directory.getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException(e.toString(), e);
    }

    byte digest[];
    synchronized (DIGESTER) {
      digest = DIGESTER.digest(dirName.getBytes());
    }
    StringBuffer buf = new StringBuffer();
    buf.append("lucene-");
    for (int i = 0; i < digest.length; i++) {
      int b = digest[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }

    return buf.toString();
  }

  /** Closes the store to future operations. */
  public synchronized void close() {
    if (isOpen && --refCount <= 0) {
      isOpen = false;
      synchronized (DIRECTORIES) {
        DIRECTORIES.remove(directory);
      }
    }
  }

  public File getFile() {
    ensureOpen();
    return directory;
  }

  /** For debug output. */
  public String toString() {
    return this.getClass().getName() + "@" + directory;
  }

  /**
   * Default read chunk size.  This is a conditional
   * default: on 32bit JVMs, it defaults to 100 MB.  On
   * 64bit JVMs, it's <code>Integer.MAX_VALUE</code>.
   * @see #setReadChunkSize
   */
  public static final int DEFAULT_READ_CHUNK_SIZE = Constants.JRE_IS_64BIT ? Integer.MAX_VALUE: 100 * 1024 * 1024;

  // LUCENE-1566
  private int chunkSize = DEFAULT_READ_CHUNK_SIZE;

  /**
   * Sets the maximum number of bytes read at once from the
   * underlying file during {@link IndexInput#readBytes}.
   * The default value is {@link #DEFAULT_READ_CHUNK_SIZE};
   *
   * <p> This was introduced due to <a
   * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6478546">Sun
   * JVM Bug 6478546</a>, which throws an incorrect
   * OutOfMemoryError when attempting to read too many bytes
   * at once.  It only happens on 32bit JVMs with a large
   * maximum heap size.</p>
   *
   * <p>Changes to this value will not impact any
   * already-opened {@link IndexInput}s.  You should call
   * this before attempting to open an index on the
   * directory.</p>
   *
   * <p> <b>NOTE</b>: This value should be as large as
   * possible to reduce any possible performance impact.  If
   * you still encounter an incorrect OutOfMemoryError,
   * trying lowering the chunk size.</p>
   */
  public final void setReadChunkSize(int chunkSize) {
    // LUCENE-1566
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be positive");
    }
    if (!Constants.JRE_IS_64BIT) {
      this.chunkSize = chunkSize;
    }
  }

  /**
   * The maximum number of bytes to read at once from the
   * underlying file during {@link IndexInput#readBytes}.
   * @see #setReadChunkSize
   */
  public final int getReadChunkSize() {
    // LUCENE-1566
    return chunkSize;
  }


  /** @deprecated Use SimpleFSDirectory.SimpleFSIndexInput instead */
  protected static class FSIndexInput extends SimpleFSDirectory.SimpleFSIndexInput {
  
    /** @deprecated */
    protected static class Descriptor extends SimpleFSDirectory.SimpleFSIndexInput.Descriptor {
      /** @deprecated */
      public Descriptor(File file, String mode) throws IOException {
        super(file, mode);
      }
    }
  
    /** @deprecated */
    public FSIndexInput(File path) throws IOException {
      super(path);
    }
  
    /** @deprecated */
    public FSIndexInput(File path, int bufferSize) throws IOException {
      super(path, bufferSize);
    }
  
  }

  /** @deprecated Use SimpleFSDirectory.SimpleFSIndexOutput instead */
  protected static class FSIndexOutput extends SimpleFSDirectory.SimpleFSIndexOutput {

    /** @deprecated */
    public FSIndexOutput(File path) throws IOException {
      super(path);
    }

  }
}

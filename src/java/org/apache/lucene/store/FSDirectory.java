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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexFileNameFilter;

// Used only for WRITE_LOCK_NAME in deprecated create=true case:
import org.apache.lucene.index.IndexWriter;

/**
 * Straightforward implementation of {@link Directory} as a directory of files.
 * Locking implementation is by default the {@link SimpleFSLockFactory}, but
 * can be changed either by passing in a {@link LockFactory} instance to
 * <code>getDirectory</code>, or specifying the LockFactory class by setting
 * <code>org.apache.lucene.store.FSDirectoryLockFactoryClass</code> Java system
 * property, or by calling {@link #setLockFactory} after creating
 * the Directory.

 * <p>Directories are cached, so that, for a given canonical
 * path, the same FSDirectory instance will always be
 * returned by <code>getDirectory</code>.  This permits
 * synchronization on directories.</p>
 *
 * @see Directory
 */
public class FSDirectory extends Directory {
    
  /** This cache of directories ensures that there is a unique Directory
   * instance per path, so that synchronization on the Directory can be used to
   * synchronize access between readers and writers.  We use
   * refcounts to ensure when the last use of an FSDirectory
   * instance for a given canonical path is closed, we remove the
   * instance from the cache.  See LUCENE-776
   * for some relevant discussion.
   */
  private static final Map DIRECTORIES = new HashMap();

  private static boolean disableLocks = false;

  // TODO: should this move up to the Directory base class?  Also: should we
  // make a per-instance (in addition to the static "default") version?

  /**
   * Set whether Lucene's use of lock files is disabled. By default, 
   * lock files are enabled. They should only be disabled if the index
   * is on a read-only medium like a CD-ROM.
   */
  public static void setDisableLocks(boolean doDisableLocks) {
    FSDirectory.disableLocks = doDisableLocks;
  }

  /**
   * Returns whether Lucene's use of lock files is disabled.
   * @return true if locks are disabled, false if locks are enabled.
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
   * elsewhere you can create your own {@link
   * SimpleFSLockFactory} (or {@link NativeFSLockFactory},
   * etc.) passing in your preferred lock directory.  Then,
   * pass this <code>LockFactory</code> instance to one of
   * the <code>getDirectory</code> methods that take a
   * <code>lockFactory</code> (for example, {@link #getDirectory(String, LockFactory)}).
   */
  public static final String LOCK_DIR = System.getProperty("org.apache.lucene.lockDir",
                                                           System.getProperty("java.io.tmpdir"));

  /** The default class which implements filesystem-based directories. */
  private static Class IMPL;
  static {
    try {
      String name =
        System.getProperty("org.apache.lucene.FSDirectory.class",
                           FSDirectory.class.getName());
      IMPL = Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("cannot load FSDirectory class: " + e.toString(), e);
    } catch (SecurityException se) {
      try {
        IMPL = Class.forName(FSDirectory.class.getName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("cannot load default FSDirectory class: " + e.toString(), e);
      }
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
   * @param path the path to the directory.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(String path)
      throws IOException {
    return getDirectory(new File(path), null);
  }

  /** Returns the directory instance for the named location.
   * @param path the path to the directory.
   * @param lockFactory instance of {@link LockFactory} providing the
   *        locking implementation.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(String path, LockFactory lockFactory)
      throws IOException {
    return getDirectory(new File(path), lockFactory);
  }

  /** Returns the directory instance for the named location.
   * @param file the path to the directory.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(File file)
    throws IOException {
    return getDirectory(file, null);
  }

  /** Returns the directory instance for the named location.
   * @param file the path to the directory.
   * @param lockFactory instance of {@link LockFactory} providing the
   *        locking implementation.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(File file, LockFactory lockFactory)
    throws IOException
  {
    file = new File(file.getCanonicalPath());

    if (file.exists() && !file.isDirectory())
      throw new IOException(file + " not a directory");

    if (!file.exists())
      if (!file.mkdirs())
        throw new IOException("Cannot create directory: " + file);

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

  private File directory = null;
  private int refCount;

  protected FSDirectory() {};                     // permit subclassing

  private void init(File path, LockFactory lockFactory) throws IOException {

    // Set up lockFactory with cascaded defaults: if an instance was passed in,
    // use that; else if locks are disabled, use NoLockFactory; else if the
    // system property org.apache.lucene.store.FSDirectoryLockFactoryClass is set,
    // instantiate that; else, use SimpleFSLockFactory:

    directory = path;

    boolean doClearLockID = false;

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

          if (lockFactory instanceof NativeFSLockFactory) {
            ((NativeFSLockFactory) lockFactory).setLockDir(path);
          } else if (lockFactory instanceof SimpleFSLockFactory) {
            ((SimpleFSLockFactory) lockFactory).setLockDir(path);
          }
        } else {
          // Our default lock is SimpleFSLockFactory;
          // default lockDir is our index directory:
          lockFactory = new SimpleFSLockFactory(path);
          doClearLockID = true;
        }
      }
    }

    setLockFactory(lockFactory);

    if (doClearLockID) {
      // Clear the prefix because write.lock will be
      // stored in our directory:
      lockFactory.setLockPrefix(null);
    }
  }

  /** Returns an array of strings, one for each Lucene index file in the directory. */
  public String[] list() {
    ensureOpen();
    return directory.list(IndexFileNameFilter.getFilter());
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

  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public IndexOutput createOutput(String name) throws IOException {
    ensureOpen();
    File file = new File(directory, name);
    if (file.exists() && !file.delete())          // delete existing, if any
      throw new IOException("Cannot overwrite: " + file);

    return new FSIndexOutput(file);
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
          Thread.currentThread().interrupt();
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

  // Inherit javadoc
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

  protected static class FSIndexInput extends BufferedIndexInput {
  
    protected static class Descriptor extends RandomAccessFile {
      // remember if the file is open, so that we don't try to close it
      // more than once
      protected volatile boolean isOpen;
      long position;
      final long length;
      
      public Descriptor(File file, String mode) throws IOException {
        super(file, mode);
        isOpen=true;
        length=length();
      }
  
      public void close() throws IOException {
        if (isOpen) {
          isOpen=false;
          super.close();
        }
      }
  
      protected void finalize() throws Throwable {
        try {
          close();
        } finally {
          super.finalize();
        }
      }
    }
  
    protected final Descriptor file;
    boolean isClone;
  
    public FSIndexInput(File path) throws IOException {
      this(path, BufferedIndexInput.BUFFER_SIZE);
    }
  
    public FSIndexInput(File path, int bufferSize) throws IOException {
      super(bufferSize);
      file = new Descriptor(path, "r");
    }
  
    /** IndexInput methods */
    protected void readInternal(byte[] b, int offset, int len)
         throws IOException {
      synchronized (file) {
        long position = getFilePointer();
        if (position != file.position) {
          file.seek(position);
          file.position = position;
        }
        int total = 0;
        do {
          int i = file.read(b, offset+total, len-total);
          if (i == -1)
            throw new IOException("read past EOF");
          file.position += i;
          total += i;
        } while (total < len);
      }
    }
  
    public void close() throws IOException {
      // only close the file if this is not a clone
      if (!isClone) file.close();
    }
  
    protected void seekInternal(long position) {
    }
  
    public long length() {
      return file.length;
    }
  
    public Object clone() {
      FSIndexInput clone = (FSIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
  
    /** Method used for testing. Returns true if the underlying
     *  file descriptor is valid.
     */
    boolean isFDValid() throws IOException {
      return file.getFD().valid();
    }
  }

  protected static class FSIndexOutput extends BufferedIndexOutput {
    RandomAccessFile file = null;
  
    // remember if the file is open, so that we don't try to close it
    // more than once
    private volatile boolean isOpen;

    public FSIndexOutput(File path) throws IOException {
      file = new RandomAccessFile(path, "rw");
      isOpen = true;
    }
  
    /** output methods: */
    public void flushBuffer(byte[] b, int offset, int size) throws IOException {
      file.write(b, offset, size);
    }
    public void close() throws IOException {
      // only close the file if it has not been closed yet
      if (isOpen) {
        boolean success = false;
        try {
          super.close();
          success = true;
        } finally {
          isOpen = false;
          if (!success) {
            try {
              file.close();
            } catch (Throwable t) {
              // Suppress so we don't mask original exception
            }
          } else
            file.close();
        }
      }
    }
  
    /** Random-access methods */
    public void seek(long pos) throws IOException {
      super.seek(pos);
      file.seek(pos);
    }
    public long length() throws IOException {
      return file.length();
    }
    public void setLength(long length) throws IOException {
      file.setLength(length);
    }
  }
}

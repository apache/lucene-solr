package org.apache.lucene.store;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.RandomAccessFile;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Hashtable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.lucene.util.Constants;

/**
 * Straightforward implementation of {@link Directory} as a directory of files.
 * <p>If the system property 'disableLuceneLocks' has the String value of
 * "true", lock creation will be disabled.
 *
 * @see Directory
 * @author Doug Cutting
 */
public final class FSDirectory extends Directory {
  /** This cache of directories ensures that there is a unique Directory
   * instance per path, so that synchronization on the Directory can be used to
   * synchronize access between readers and writers.
   *
   * This should be a WeakHashMap, so that entries can be GC'd, but that would
   * require Java 1.2.  Instead we use refcounts...
   */
  private static final Hashtable DIRECTORIES = new Hashtable();

  private static final boolean DISABLE_LOCKS =
      Boolean.getBoolean("disableLuceneLocks") || Constants.JAVA_1_1;

  /**
   * Directory specified by <code>org.apache.lucene.lockdir</code>
   * or <code>java.io.tmpdir</code> system property
   */
  public static final String LOCK_DIR =
    System.getProperty("org.apache.lucene.lockdir",
      System.getProperty("java.io.tmpdir"));

  private static MessageDigest DIGESTER;

  static {
    try {
      DIGESTER = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e.toString());
    }
  }

  /** A buffer optionally used in renameTo method */
  private byte[] buffer = null;

  /** Returns the directory instance for the named location.
   *
   * <p>Directories are cached, so that, for a given canonical path, the same
   * FSDirectory instance will always be returned.  This permits
   * synchronization on directories.
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
   * <p>Directories are cached, so that, for a given canonical path, the same
   * FSDirectory instance will always be returned.  This permits
   * synchronization on directories.
   *
   * @param file the path to the directory.
   * @param create if true, create, or erase any existing contents.
   * @return the FSDirectory for the named file.  */
  public static FSDirectory getDirectory(File file, boolean create)
    throws IOException {
    file = new File(file.getCanonicalPath());
    FSDirectory dir;
    synchronized (DIRECTORIES) {
      dir = (FSDirectory)DIRECTORIES.get(file);
      if (dir == null) {
        dir = new FSDirectory(file, create);
        DIRECTORIES.put(file, dir);
      } else if (create) {
        dir.create();
      }
    }
    synchronized (dir) {
      dir.refCount++;
    }
    return dir;
  }

  private File directory = null;
  private int refCount;
  private File lockDir;

  private FSDirectory(File path, boolean create) throws IOException {
    directory = path;

   lockDir = new File(LOCK_DIR);
   if (!lockDir.isAbsolute()) {
     lockDir = new File(directory, LOCK_DIR);
   }
    if (create)
      create();

    if (!directory.isDirectory())
      throw new IOException(path + " not a directory");
  }

  private synchronized void create() throws IOException {
    if (!directory.exists())
      if (!directory.mkdirs())
        throw new IOException("Cannot create directory: " + directory);

    String[] files = directory.list();            // clear old files
    for (int i = 0; i < files.length; i++) {
      File file = new File(directory, files[i]);
      if (!file.delete())
        throw new IOException("couldn't delete " + files[i]);
    }

    String lockPrefix = getLockPrefix().toString(); // clear old locks
    files = lockDir.list();
    for (int i = 0; i < files.length; i++) {
      if (!files[i].startsWith(lockPrefix))
        continue;
      File lockFile = new File(lockDir, files[i]);
      if (!lockFile.delete())
        throw new IOException("couldn't delete " + files[i]);
    }
  }

  /** Returns an array of strings, one for each file in the directory. */
  public final String[] list() throws IOException {
    return directory.list();
  }

  /** Returns true iff a file with the given name exists. */
  public final boolean fileExists(String name) throws IOException {
    File file = new File(directory, name);
    return file.exists();
  }

  /** Returns the time the named file was last modified. */
  public final long fileModified(String name) throws IOException {
    File file = new File(directory, name);
    return file.lastModified();
  }

  /** Returns the time the named file was last modified. */
  public static final long fileModified(File directory, String name)
       throws IOException {
    File file = new File(directory, name);
    return file.lastModified();
  }

  /** Set the modified time of an existing file to now. */
  public void touchFile(String name) throws IOException {
    File file = new File(directory, name);
    file.setLastModified(System.currentTimeMillis());
  }

  /** Returns the length in bytes of a file in the directory. */
  public final long fileLength(String name) throws IOException {
    File file = new File(directory, name);
    return file.length();
  }

  /** Removes an existing file in the directory. */
  public final void deleteFile(String name) throws IOException {
    File file = new File(directory, name);
    if (!file.delete())
      throw new IOException("couldn't delete " + name);
  }

  /** Renames an existing file in the directory. */
  public final synchronized void renameFile(String from, String to)
      throws IOException {
    File old = new File(directory, from);
    File nu = new File(directory, to);

    /* This is not atomic.  If the program crashes between the call to
       delete() and the call to renameTo() then we're screwed, but I've
       been unable to figure out how else to do this... */

    if (nu.exists())
      if (!nu.delete())
        throw new IOException("couldn't delete " + to);

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
        throw new IOException("couldn't rename " + from + " to " + to);
      }
      finally {
        if (in != null) {
          try {
            in.close();
          } catch (IOException e) {
            throw new RuntimeException("could not close input stream: " + e.getMessage());
          }
        }
        if (out != null) {
          try {
            out.close();
          } catch (IOException e) {
            throw new RuntimeException("could not close output stream: " + e.getMessage());
          }
        }
      }
    }
  }

  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public final OutputStream createFile(String name) throws IOException {
    return new FSOutputStream(new File(directory, name));
  }

  /** Returns a stream reading an existing file. */
  public final InputStream openFile(String name) throws IOException {
    return new FSInputStream(new File(directory, name));
  }

  /**
   * So we can do some byte-to-hexchar conversion below
   */
  private static final char[] HEX_DIGITS =
  {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /** Constructs a {@link Lock} with the specified name.  Locks are implemented
   * with {@link File#createNewFile() }.
   *
   * <p>In JDK 1.1 or if system property <I>disableLuceneLocks</I> is the
   * string "true", locks are disabled.  Assigning this property any other
   * string will <B>not</B> prevent creation of lock files.  This is useful for
   * using Lucene on read-only medium, such as CD-ROM.
   *
   * @param name the name of the lock file
   * @return an instance of <code>Lock</code> holding the lock
   */
  public final Lock makeLock(String name) {
    StringBuffer buf = getLockPrefix();
    buf.append("-");
    buf.append(name);

    // create a lock file
    final File lockFile = new File(lockDir, buf.toString());

    return new Lock() {
      public boolean obtain() throws IOException {
        if (DISABLE_LOCKS)
          return true;
        return lockFile.createNewFile();
      }
      public void release() {
        if (DISABLE_LOCKS)
          return;
        lockFile.delete();
      }
      public boolean isLocked() {
        if (DISABLE_LOCKS)
          return false;
        return lockFile.exists();
      }

      public String toString() {
        return "Lock@" + lockFile;
      }
    };
  }

  private StringBuffer getLockPrefix() {
    String dirName;                               // name to be hashed
    try {
      dirName = directory.getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException(e.toString());
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

    return buf;
  }

  /** Closes the store to future operations. */
  public final synchronized void close() throws IOException {
    if (--refCount <= 0) {
      synchronized (DIRECTORIES) {
        DIRECTORIES.remove(directory);
      }
    }
  }

  public File getFile() {
    return directory;
  }

  /** For debug output. */
  public String toString() {
    return "FSDirectory@" + directory;
  }
}


final class FSInputStream extends InputStream {
  private class Descriptor extends RandomAccessFile {
    /* DEBUG */
    //private String name;
    /* DEBUG */
    public long position;
    public Descriptor(File file, String mode) throws IOException {
      super(file, mode);
      /* DEBUG */
      //name = file.toString();
      //debug_printInfo("OPEN");
      /* DEBUG */
    }

    /* DEBUG */
    //public void close() throws IOException {
    //  debug_printInfo("CLOSE");
    //    super.close();
    //}
    //
    //private void debug_printInfo(String op) {
    //  try { throw new Exception(op + " <" + name + ">");
    //  } catch (Exception e) {
    //    java.io.StringWriter sw = new java.io.StringWriter();
    //    java.io.PrintWriter pw = new java.io.PrintWriter(sw);
    //    e.printStackTrace(pw);
    //    System.out.println(sw.getBuffer().toString());
    //  }
    //}
    /* DEBUG */
  }

  Descriptor file = null;
  boolean isClone;

  public FSInputStream(File path) throws IOException {
    file = new Descriptor(path, "r");
    length = file.length();
  }

  /** InputStream methods */
  protected final void readInternal(byte[] b, int offset, int len)
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

  public final void close() throws IOException {
    if (!isClone)
      file.close();
  }

  /** Random-access methods */
  protected final void seekInternal(long position) throws IOException {
  }

  protected final void finalize() throws IOException {
    close();            // close the file
  }

  public Object clone() {
    FSInputStream clone = (FSInputStream)super.clone();
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


final class FSOutputStream extends OutputStream {
  RandomAccessFile file = null;

  public FSOutputStream(File path) throws IOException {
    file = new RandomAccessFile(path, "rw");
  }

  /** output methods: */
  public final void flushBuffer(byte[] b, int size) throws IOException {
    file.write(b, 0, size);
  }
  public final void close() throws IOException {
    super.close();
    file.close();
  }

  /** Random-access methods */
  public final void seek(long pos) throws IOException {
    super.seek(pos);
    file.seek(pos);
  }
  public final long length() throws IOException {
    return file.length();
  }

  protected final void finalize() throws IOException {
    file.close();          // close the file
  }

}

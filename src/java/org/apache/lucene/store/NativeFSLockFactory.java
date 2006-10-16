package org.apache.lucene.store;

/**
 * Copyright 2006 The Apache Software Foundation
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

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Random;

/**
 * Implements {@link LockFactory} using native OS file locks
 * (available through java.nio.*).  Note that for certain
 * filesystems native locks are possible but must be
 * explicity configured and enabled (and may be disabled by
 * default).  For example, for NFS servers there sometimes
 * must be a separate lockd process running, and other
 * configuration may be required such as running the server
 * in kernel mode.  Other filesystems may not even support
 * native OS locks in which case you must use a different
 * {@link LockFactory} implementation.
 *
 * <p>The advantage of this lock factory over
 * SimpleFSLockFactory is that the locks should be
 * "correct", whereas SimpleFSLockFactory uses
 * java.io.File.createNewFile which has warnings about not
 * using it for locking.  Furthermore, if the JVM crashes,
 * the OS will free any held locks, whereas
 * SimpleFSLockFactory will keep the locks held, requiring
 * manual removal before re-running Lucene.</p>
 *
 * <p>Note that, unlike SimpleFSLockFactory, the existence of
 * leftover lock files in the filesystem on exiting the JVM
 * is fine because the OS will free the locks held against
 * these files even though the files still remain.</p>
 *
 * <p>Native locks file names have the substring "-n-", which
 * you can use to differentiate them from lock files created
 * by SimpleFSLockFactory.</p>
 *
 * @see LockFactory
 */

public class NativeFSLockFactory extends LockFactory {

  /**
   * Directory specified by <code>org.apache.lucene.lockDir</code>
   * system property.  If that is not set, then <code>java.io.tmpdir</code>
   * system property is used.
   */

  public static final String LOCK_DIR =
    System.getProperty("org.apache.lucene.lockDir",
                       System.getProperty("java.io.tmpdir"));

  private File lockDir;

  /*
   * The javadocs for FileChannel state that you should have
   * a single instance of a FileChannel (per JVM) for all
   * locking against a given file.  To do this, we ensure
   * there's a single instance of NativeFSLockFactory per
   * canonical lock directory, and then we always use a
   * single lock instance (per lock name) if it's present.
   */
  private static Hashtable LOCK_FACTORIES = new Hashtable();

  private Hashtable locks = new Hashtable();

  protected NativeFSLockFactory(File lockDir)
    throws IOException {

    this.lockDir = lockDir;

    // Ensure that lockDir exists and is a directory.
    if (!lockDir.exists()) {
      if (!lockDir.mkdirs())
        throw new IOException("Cannot create directory: " +
                              lockDir.getAbsolutePath());
    } else if (!lockDir.isDirectory()) {
      throw new IOException("Found regular file where directory expected: " + 
                            lockDir.getAbsolutePath());
    }
    acquireTestLock();
  }

  // Simple test to verify locking system is "working".  On
  // NFS, if it's misconfigured, you can hit long (35
  // second) timeouts which cause Lock.obtain to take far
  // too long (it assumes the obtain() call takes zero
  // time).  Since it's a configuration problem, we test up
  // front once on creating the LockFactory:
  private void acquireTestLock() throws IOException {
    String randomLockName = "lucene-" + Long.toString(new Random().nextInt(), Character.MAX_RADIX) + "-test.lock";
    
    Lock l = makeLock(randomLockName);
    try {
      l.obtain();
    } catch (IOException e) {
      IOException e2 = new IOException("Failed to acquire random test lock; please verify filesystem for lock directory '" + lockDir + "' supports locking");
      e2.initCause(e);
      throw e2;
    }

    l.release();
  }

  /**
   * Returns a NativeFSLockFactory instance, storing lock
   * files into the default LOCK_DIR:
   * <code>org.apache.lucene.lockDir</code> system property,
   * or (if that is null) then <code>java.io.tmpdir</code>.
   */
  public static NativeFSLockFactory getLockFactory() throws IOException {
    return getLockFactory(new File(LOCK_DIR));
  }

  /**
   * Returns a NativeFSLockFactory instance, storing lock
   * files into the specified lockDirName:
   *
   * @param lockDirName where lock files are created.
   */
  public static NativeFSLockFactory getLockFactory(String lockDirName) throws IOException {
    return getLockFactory(new File(lockDirName));
  }

  /**
   * Returns a NativeFSLockFactory instance, storing lock
   * files into the specified lockDir:
   * 
   * @param lockDir where lock files are created.
   */
  public static NativeFSLockFactory getLockFactory(File lockDir) throws IOException {
    lockDir = new File(lockDir.getCanonicalPath());

    NativeFSLockFactory f;

    synchronized(LOCK_FACTORIES) {
      f = (NativeFSLockFactory) LOCK_FACTORIES.get(lockDir);
      if (f == null) {
        f = new NativeFSLockFactory(lockDir);
        LOCK_FACTORIES.put(lockDir, f);
      }
    }

    return f;
  }

  public synchronized Lock makeLock(String lockName) {
    Lock l = (Lock) locks.get(lockName);
    if (l == null) {
      String fullName;
      if (lockPrefix.equals("")) {
        fullName = lockName;
      } else {
        fullName = lockPrefix + "-n-" + lockName;
      }

      l = new NativeFSLock(lockDir, fullName);
      locks.put(lockName, l);
    }
    return l;
  }

  public void clearAllLocks() throws IOException {
    // Note that this isn't strictly required anymore
    // because the existence of these files does not mean
    // they are locked, but, still do this in case people
    // really want to see the files go away:
    if (lockDir.exists()) {
        String[] files = lockDir.list();
        if (files == null)
          throw new IOException("Cannot read lock directory " +
                                lockDir.getAbsolutePath());
        String prefix = lockPrefix + "-n-";
        for (int i = 0; i < files.length; i++) {
          if (files[i].startsWith(prefix)) {
            File lockFile = new File(lockDir, files[i]);
            if (!lockFile.delete())
              throw new IOException("Cannot delete " + lockFile);
          }
        }
    }
  }
};

class NativeFSLock extends Lock {

  private RandomAccessFile f;
  private FileChannel channel;
  private FileLock lock;
  private File path;
  private File lockDir;

  public NativeFSLock(File lockDir, String lockFileName) {
    this.lockDir = lockDir;
    path = new File(lockDir, lockFileName);
  }

  public synchronized boolean obtain() throws IOException {

    if (isLocked()) {
      // We are already locked:
      return false;
    }

    // Ensure that lockDir exists and is a directory.
    if (!lockDir.exists()) {
      if (!lockDir.mkdirs())
        throw new IOException("Cannot create directory: " +
                              lockDir.getAbsolutePath());
    } else if (!lockDir.isDirectory()) {
      throw new IOException("Found regular file where directory expected: " + 
                            lockDir.getAbsolutePath());
    }

    f = new RandomAccessFile(path, "rw");
    try {
      channel = f.getChannel();
      try {
        try {
          lock = channel.tryLock();
        } catch (IOException e) {
          // At least on OS X, we will sometimes get an
          // intermittant "Permission Denied" IOException,
          // which seems to simply mean "you failed to get
          // the lock".  But other IOExceptions could be
          // "permanent" (eg, locking is not supported via
          // the filesystem).  So, we record the failure
          // reason here; the timeout obtain (usually the
          // one calling us) will use this as "root cause"
          // if it fails to get the lock.
          failureReason = e;
        }
      } finally {
        if (lock == null) {
          try {
            channel.close();
          } finally {
            channel = null;
          }
        }
      }
    } finally {
      if (channel == null) {
        try {
          f.close();
        } finally {
          f = null;
        }
      }
    }
    return lock != null;
  }

  public synchronized void release() {
    try {
      if (isLocked()) {
        try {
          lock.release();
        } finally {
          lock = null;
          try {
            channel.close();
          } finally {
            channel = null;
            try {
              f.close();
            } finally {
              f = null;
            }
          }
        }
        path.delete();
      }
    } catch (IOException e) {
      // Not sure how to better message/handle this without
      // changing API?
      throw new RuntimeException(e);
    }
  }

  public boolean isLocked() {
    return lock != null;
  }

  public String toString() {
    return "NativeFSLock@" + path;
  }

  public void finalize() {
    if (isLocked()) {
      release();
    }
  }
}

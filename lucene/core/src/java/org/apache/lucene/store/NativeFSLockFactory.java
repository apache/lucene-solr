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

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.util.IOUtils;

/**
 * <p>Implements {@link LockFactory} using native OS file
 * locks.  Note that because this LockFactory relies on
 * java.nio.* APIs for locking, any problems with those APIs
 * will cause locking to fail.  Specifically, on certain NFS
 * environments the java.nio.* locks will fail (the lock can
 * incorrectly be double acquired) whereas {@link
 * SimpleFSLockFactory} worked perfectly in those same
 * environments.  For NFS based access to an index, it's
 * recommended that you try {@link SimpleFSLockFactory}
 * first and work around the one limitation that a lock file
 * could be left when the JVM exits abnormally.</p>
 *
 * <p>The primary benefit of {@link NativeFSLockFactory} is
 * that locks (not the lock file itsself) will be properly
 * removed (by the OS) if the JVM has an abnormal exit.</p>
 * 
 * <p>Note that, unlike {@link SimpleFSLockFactory}, the existence of
 * leftover lock files in the filesystem is fine because the OS
 * will free the locks held against these files even though the
 * files still remain. Lucene will never actively remove the lock
 * files, so although you see them, the index may not be locked.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change on all Lucene
 * instances and clean up all leftover lock files before starting
 * the new configuration for the first time. Different implementations
 * can not work together!</p>
 *
 * <p>If you suspect that this or any other LockFactory is
 * not working properly in your environment, you can easily
 * test it by using {@link VerifyingLockFactory}, {@link
 * LockVerifyServer} and {@link LockStressTest}.</p>
 *
 * @see LockFactory
 */

public class NativeFSLockFactory extends FSLockFactory {

  /**
   * Create a NativeFSLockFactory instance, with null (unset)
   * lock directory. When you pass this factory to a {@link FSDirectory}
   * subclass, the lock directory is automatically set to the
   * directory itself. Be sure to create one instance for each directory
   * your create!
   */
  public NativeFSLockFactory() {
    this((File) null);
  }

  /**
   * Create a NativeFSLockFactory instance, storing lock
   * files into the specified lockDirName:
   *
   * @param lockDirName where lock files are created.
   */
  public NativeFSLockFactory(String lockDirName) {
    this(new File(lockDirName));
  }

  /**
   * Create a NativeFSLockFactory instance, storing lock
   * files into the specified lockDir:
   * 
   * @param lockDir where lock files are created.
   */
  public NativeFSLockFactory(File lockDir) {
    setLockDir(lockDir);
  }

  @Override
  public synchronized Lock makeLock(String lockName) {
    if (lockPrefix != null)
      lockName = lockPrefix + "-" + lockName;
    return new NativeFSLock(lockDir, lockName);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    makeLock(lockName).close();
  }
}

class NativeFSLock extends Lock {

  private FileChannel channel;
  private FileLock lock;
  private File path;
  private File lockDir;

  public NativeFSLock(File lockDir, String lockFileName) {
    this.lockDir = lockDir;
    path = new File(lockDir, lockFileName);
  }

  private synchronized boolean lockExists() {
    return lock != null;
  }

  @Override
  public synchronized boolean obtain() throws IOException {

    if (lockExists()) {
      // Our instance is already locked:
      return false;
    }

    // Ensure that lockDir exists and is a directory.
    if (!lockDir.exists()) {
      if (!lockDir.mkdirs())
        throw new IOException("Cannot create directory: " +
            lockDir.getAbsolutePath());
    } else if (!lockDir.isDirectory()) {
      // TODO: NoSuchDirectoryException instead?
      throw new IOException("Found regular file where directory expected: " + 
          lockDir.getAbsolutePath());
    }
    
    channel = FileChannel.open(path.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    boolean success = false;
    try {
      lock = channel.tryLock();
      success = true;
    } catch (IOException | OverlappingFileLockException e) {
      // At least on OS X, we will sometimes get an
      // intermittent "Permission Denied" IOException,
      // which seems to simply mean "you failed to get
      // the lock".  But other IOExceptions could be
      // "permanent" (eg, locking is not supported via
      // the filesystem).  So, we record the failure
      // reason here; the timeout obtain (usually the
      // one calling us) will use this as "root cause"
      // if it fails to get the lock.
      failureReason = e;
    } finally {
      if (!success) {
        try {
          IOUtils.closeWhileHandlingException(channel);
        } finally {
          channel = null;
        }
      }
    }
    return lockExists();
  }

  @Override
  public synchronized void close() throws IOException {
    if (lockExists()) {
      try {
        lock.release();
      } finally {
        lock = null;
        try {
          channel.close();
        } finally {
          channel = null;
        }
      }
    } else {
      // if we don't hold the lock, and somebody still called release(), for
      // example as a result of calling IndexWriter.unlock(), we should attempt
      // to obtain the lock and release it. If the obtain fails, it means the
      // lock cannot be released, and we should throw a proper exception rather
      // than silently failing/not doing anything.
      boolean obtained = false;
      try {
        if (!(obtained = obtain())) {
          throw new LockReleaseFailedException(
              "Cannot forcefully unlock a NativeFSLock which is held by another indexer component: "
                  + path);
        }
      } finally {
        if (obtained) {
          close();
        }
      }
    }
  }

  @Override
  public synchronized boolean isLocked() {
    // The test for is isLocked is not directly possible with native file locks:
    
    // First a shortcut, if a lock reference in this instance is available
    if (lockExists()) return true;
    
    // Look if lock file is present; if not, there can definitely be no lock!
    if (!path.exists()) return false;
    
    // Try to obtain and release (if was locked) the lock
    try {
      boolean obtained = obtain();
      if (obtained) close();
      return !obtained;
    } catch (IOException ioe) {
      return false;
    }    
  }

  @Override
  public String toString() {
    return "NativeFSLock@" + path;
  }
}

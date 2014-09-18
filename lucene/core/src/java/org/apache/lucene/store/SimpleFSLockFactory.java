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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * <p>Implements {@link LockFactory} using {@link
 * Files#createFile}.</p>
 *
 * <p><b>NOTE:</b> the {@linkplain File#createNewFile() javadocs
 * for <code>File.createNewFile()</code>} contain a vague
 * yet spooky warning about not using the API for file
 * locking.  This warning was added due to <a target="_top"
 * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4676183">this
 * bug</a>, and in fact the only known problem with using
 * this API for locking is that the Lucene write lock may
 * not be released when the JVM exits abnormally.</p>

 * <p>When this happens, a {@link LockObtainFailedException}
 * is hit when trying to create a writer, in which case you
 * need to explicitly clear the lock file first.  You can
 * either manually remove the file, or use the {@link
 * org.apache.lucene.index.IndexWriter#unlock(Directory)}
 * API.  But, first be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change all Lucene
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

public class SimpleFSLockFactory extends FSLockFactory {

  /**
   * Create a SimpleFSLockFactory instance, with null (unset)
   * lock directory. When you pass this factory to a {@link FSDirectory}
   * subclass, the lock directory is automatically set to the
   * directory itself. Be sure to create one instance for each directory
   * your create!
   */
  public SimpleFSLockFactory() throws IOException {
    this((Path) null);
  }

  /**
   * Instantiate using the provided directory (as a Path instance).
   * @param lockDir where lock files should be created.
   */
  public SimpleFSLockFactory(Path lockDir) throws IOException {
    setLockDir(lockDir);
  }

  @Override
  public Lock makeLock(String lockName) {
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    return new SimpleFSLock(lockDir, lockName);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    Files.deleteIfExists(lockDir.resolve(lockName));
  }
}

class SimpleFSLock extends Lock {

  Path lockFile;
  Path lockDir;

  public SimpleFSLock(Path lockDir, String lockFileName) {
    this.lockDir = lockDir;
    lockFile = lockDir.resolve(lockFileName);
  }

  @Override
  public boolean obtain() throws IOException {
    try {
      Files.createDirectories(lockDir);
      Files.createFile(lockFile);
      return true;
    } catch (IOException ioe) {
      // On Windows, on concurrent createNewFile, the 2nd process gets "access denied".
      // In that case, the lock was not aquired successfully, so return false.
      // We record the failure reason here; the obtain with timeout (usually the
      // one calling us) will use this as "root cause" if it fails to get the lock.
      failureReason = ioe;
      return false;
    }
  }

  @Override
  public void close() throws LockReleaseFailedException {
    // TODO: wierd that clearLock() throws the raw IOException...
    try {
      Files.deleteIfExists(lockFile);
    } catch (Throwable cause) {
      throw new LockReleaseFailedException("failed to delete " + lockFile, cause);
    }
  }

  @Override
  public boolean isLocked() {
    return Files.exists(lockFile);
  }

  @Override
  public String toString() {
    return "SimpleFSLock@" + lockFile;
  }
}

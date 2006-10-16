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

import java.io.File;
import java.io.IOException;

/**
 * Implements {@link LockFactory} using {@link File#createNewFile()}.  This is
 * currently the default LockFactory used for {@link FSDirectory} if no
 * LockFactory instance is otherwise provided.
 *
 * Note that there are known problems with this locking implementation on NFS.
 *
 * @see LockFactory
 */

public class SimpleFSLockFactory extends LockFactory {

  /**
   * Directory specified by <code>org.apache.lucene.lockDir</code>
   * system property.  If that is not set, then <code>java.io.tmpdir</code>
   * system property is used.
   */

  public static final String LOCK_DIR =
    System.getProperty("org.apache.lucene.lockDir",
                       System.getProperty("java.io.tmpdir"));

  private File lockDir;

  /**
   * Instantiate using default LOCK_DIR: <code>org.apache.lucene.lockDir</code>
   * system property, or (if that is null) then <code>java.io.tmpdir</code>.
   */
  public SimpleFSLockFactory() throws IOException {
    lockDir = new File(LOCK_DIR);
    init(lockDir);
  }

  /**
   * Instantiate using the provided directory (as a File instance).
   * @param lockDir where lock files should be created.
   */
  public SimpleFSLockFactory(File lockDir) throws IOException {
    init(lockDir);
  }

  /**
   * Instantiate using the provided directory name (String).
   * @param lockDirName where lock files should be created.
   */
  public SimpleFSLockFactory(String lockDirName) throws IOException {
    lockDir = new File(lockDirName);
    init(lockDir);
  }

  protected void init(File lockDir) throws IOException {

    this.lockDir = lockDir;

  }

  public Lock makeLock(String lockName) {
    return new SimpleFSLock(lockDir, lockPrefix + "-" + lockName);
  }

  public void clearAllLocks() throws IOException {
    if (lockDir.exists()) {
        String[] files = lockDir.list();
        if (files == null)
          throw new IOException("Cannot read lock directory " +
                                lockDir.getAbsolutePath());
        String prefix = lockPrefix + "-";
        for (int i = 0; i < files.length; i++) {
          if (!files[i].startsWith(prefix))
            continue;
          File lockFile = new File(lockDir, files[i]);
          if (!lockFile.delete())
            throw new IOException("Cannot delete " + lockFile);
        }
    }
  }
};

class SimpleFSLock extends Lock {

  File lockFile;
  File lockDir;

  public SimpleFSLock(File lockDir, String lockFileName) {
    this.lockDir = lockDir;
    lockFile = new File(lockDir, lockFileName);
  }

  public boolean obtain() throws IOException {

    // Ensure that lockDir exists and is a directory:
    if (!lockDir.exists()) {
      if (!lockDir.mkdirs())
        throw new IOException("Cannot create directory: " +
                              lockDir.getAbsolutePath());
    } else if (!lockDir.isDirectory()) {
      throw new IOException("Found regular file where directory expected: " + 
                            lockDir.getAbsolutePath());
    }
    return lockFile.createNewFile();
  }

  public void release() {
    lockFile.delete();
  }

  public boolean isLocked() {
    return lockFile.exists();
  }

  public String toString() {
    return "SimpleFSLock@" + lockFile;
  }
}

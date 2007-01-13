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

  private File lockDir;

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
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    return new SimpleFSLock(lockDir, lockName);
  }

  public void clearLock(String lockName) throws IOException {
    if (lockDir.exists()) {
      if (lockPrefix != null) {
        lockName = lockPrefix + "-" + lockName;
      }
      File lockFile = new File(lockDir, lockName);
      if (lockFile.exists() && !lockFile.delete()) {
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

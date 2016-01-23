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

import java.io.IOException;
import java.util.HashSet;

/**
 * Implements {@link LockFactory} for a single in-process instance,
 * meaning all locking will take place through this one instance.
 * Only use this {@link LockFactory} when you are certain all
 * IndexReaders and IndexWriters for a given index are running
 * against a single shared in-process Directory instance.  This is
 * currently the default locking for RAMDirectory.
 *
 * @see LockFactory
 */

public class SingleInstanceLockFactory extends LockFactory {

  private HashSet<String> locks = new HashSet<String>();

  @Override
  public Lock makeLock(String lockName) {
    // We do not use the LockPrefix at all, because the private
    // HashSet instance effectively scopes the locking to this
    // single Directory instance.
    return new SingleInstanceLock(locks, lockName);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    synchronized(locks) {
      if (locks.contains(lockName)) {
        locks.remove(lockName);
      }
    }
  }
}

class SingleInstanceLock extends Lock {

  String lockName;
  private HashSet<String> locks;

  public SingleInstanceLock(HashSet<String> locks, String lockName) {
    this.locks = locks;
    this.lockName = lockName;
  }

  @Override
  public boolean obtain() throws IOException {
    synchronized(locks) {
      return locks.add(lockName);
    }
  }

  @Override
  public void release() {
    synchronized(locks) {
      locks.remove(lockName);
    }
  }

  @Override
  public boolean isLocked() {
    synchronized(locks) {
      return locks.contains(lockName);
    }
  }

  @Override
  public String toString() {
    return super.toString() + ": " + lockName;
  }
}

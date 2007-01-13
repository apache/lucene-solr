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

/**
 * Use this {@link LockFactory} to disable locking entirely.
 * This LockFactory is used when you call {@link FSDirectory#setDisableLocks}.
 * Only one instance of this lock is created.  You should call {@link
 * #getNoLockFactory()} to get the instance.
 *
 * @see LockFactory
 */

public class NoLockFactory extends LockFactory {

  // Single instance returned whenever makeLock is called.
  private static NoLock singletonLock = new NoLock();
  private static NoLockFactory singleton = new NoLockFactory();

  public static NoLockFactory getNoLockFactory() {
    return singleton;
  }

  public Lock makeLock(String lockName) {
    return singletonLock;
  }

  public void clearLock(String lockName) {};
};

class NoLock extends Lock {
  public boolean obtain() throws IOException {
    return true;
  }

  public void release() {
  }

  public boolean isLocked() {
    return false;
  }

  public String toString() {
    return "NoLock";
  }
}

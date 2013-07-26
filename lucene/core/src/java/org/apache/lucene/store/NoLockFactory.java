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

import java.io.IOException;

/**
 * Use this {@link LockFactory} to disable locking entirely.
 * Only one instance of this lock is created.  You should call {@link
 * #getNoLockFactory()} to get the instance.
 *
 * @see LockFactory
 */

public class NoLockFactory extends LockFactory {

  // Single instance returned whenever makeLock is called.
  private static NoLock singletonLock = new NoLock();
  private static NoLockFactory singleton = new NoLockFactory();
  
  private NoLockFactory() {}

  public static NoLockFactory getNoLockFactory() {
    return singleton;
  }

  @Override
  public Lock makeLock(String lockName) {
    return singletonLock;
  }

  @Override
  public void clearLock(String lockName) {}
}

class NoLock extends Lock {
  @Override
  public boolean obtain() throws IOException {
    return true;
  }

  @Override
  public void release() {
  }

  @Override
  public boolean isLocked() {
    return false;
  }

  @Override
  public String toString() {
    return "NoLock";
  }
}

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
 * Used by MockDirectoryWrapper to wrap another factory
 * and track open locks.
 */
public class MockLockFactoryWrapper extends LockFactory {
  MockDirectoryWrapper dir;
  LockFactory delegate;
  
  public MockLockFactoryWrapper(MockDirectoryWrapper dir, LockFactory delegate) {
    this.dir = dir;
    this.delegate = delegate;
  }
  
  @Override
  public void setLockPrefix(String lockPrefix) {
    delegate.setLockPrefix(lockPrefix);
  }

  @Override
  public String getLockPrefix() {
    return delegate.getLockPrefix();
  }

  @Override
  public Lock makeLock(String lockName) {
    return new MockLock(delegate.makeLock(lockName), lockName);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    delegate.clearLock(lockName);
    dir.openLocks.remove(lockName);
  }
  
  @Override
  public String toString() {
    return "MockLockFactoryWrapper(" + delegate.toString() + ")";
  }

  private class MockLock extends Lock {
    private Lock delegateLock;
    private String name;
    
    MockLock(Lock delegate, String name) {
      this.delegateLock = delegate;
      this.name = name;
    }

    @Override
    public boolean obtain() throws IOException {
      if (delegateLock.obtain()) {
        dir.openLocks.add(name);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void release() throws IOException {
      delegateLock.release();
      dir.openLocks.remove(name);
    }

    @Override
    public boolean isLocked() throws IOException {
      return delegateLock.isLocked();
    }
  }
}

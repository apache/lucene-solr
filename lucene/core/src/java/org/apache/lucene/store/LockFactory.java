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
 * <p>Base class for Locking implementation.  {@link Directory} uses
 * instances of this class to implement locking.</p>
 *
 * <p>Note that there are some useful tools to verify that
 * your LockFactory is working correctly: {@link
 * VerifyingLockFactory}, {@link LockStressTest}, {@link
 * LockVerifyServer}.</p>
 *
 * @see LockVerifyServer
 * @see LockStressTest
 * @see VerifyingLockFactory
 */

public abstract class LockFactory {

  protected String lockPrefix = null;

  /**
   * Set the prefix in use for all locks created in this
   * LockFactory.  This is normally called once, when a
   * Directory gets this LockFactory instance.  However, you
   * can also call this (after this instance is assigned to
   * a Directory) to override the prefix in use.  This
   * is helpful if you're running Lucene on machines that
   * have different mount points for the same shared
   * directory.
   */
  public void setLockPrefix(String lockPrefix) {
    this.lockPrefix = lockPrefix;
  }

  /**
   * Get the prefix in use for all locks created in this LockFactory.
   */
  public String getLockPrefix() {
    return this.lockPrefix;
  }

  /**
   * Return a new Lock instance identified by lockName.
   * @param lockName name of the lock to be created.
   */
  public abstract Lock makeLock(String lockName);

  /**
   * Attempt to clear (forcefully unlock and remove) the
   * specified lock.  Only call this at a time when you are
   * certain this lock is no longer in use.
   * @param lockName name of the lock to be cleared.
   */
  abstract public void clearLock(String lockName) throws IOException;
}

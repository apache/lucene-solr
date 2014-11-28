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
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link LockFactory} that wraps another {@link
 * LockFactory} and verifies that each lock obtain/release
 * is "correct" (never results in two processes holding the
 * lock at the same time).  It does this by contacting an
 * external server ({@link LockVerifyServer}) to assert that
 * at most one process holds the lock at a time.  To use
 * this, you should also run {@link LockVerifyServer} on the
 * host and port matching what you pass to the constructor.
 *
 * @see LockVerifyServer
 * @see LockStressTest
 */

public final class VerifyingLockFactory extends LockFactory {

  final LockFactory lf;
  final InputStream in;
  final OutputStream out;

  private class CheckedLock extends Lock {
    private final Lock lock;

    public CheckedLock(Lock lock) {
      this.lock = lock;
    }

    private void verify(byte message) throws IOException {
      out.write(message);
      out.flush();
      final int ret = in.read();
      if (ret < 0) {
        throw new IllegalStateException("Lock server died because of locking error.");
      }
      if (ret != message) {
        throw new IOException("Protocol violation.");
      }
    }

    @Override
    public synchronized boolean obtain() throws IOException {
      boolean obtained = lock.obtain();
      if (obtained)
        verify((byte) 1);
      return obtained;
    }

    @Override
    public synchronized boolean isLocked() throws IOException {
      return lock.isLocked();
    }

    @Override
    public synchronized void close() throws IOException {
      if (isLocked()) {
        verify((byte) 0);
        lock.close();
      }
    }
  }

  /**
   * @param lf the LockFactory that we are testing
   * @param in the socket's input to {@link LockVerifyServer}
   * @param out the socket's output to {@link LockVerifyServer}
  */
  public VerifyingLockFactory(LockFactory lf, InputStream in, OutputStream out) throws IOException {
    this.lf = lf;
    this.in = in;
    this.out = out;
  }

  @Override
  public Lock makeLock(Directory dir, String lockName) {
    return new CheckedLock(lf.makeLock(dir, lockName));
  }
}

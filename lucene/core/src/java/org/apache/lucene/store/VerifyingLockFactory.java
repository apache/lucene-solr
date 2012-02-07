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

import java.net.Socket;
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
 * host & port matching what you pass to the constructor.
 *
 * @see LockVerifyServer
 * @see LockStressTest
 */

public class VerifyingLockFactory extends LockFactory {

  LockFactory lf;
  byte id;
  String host;
  int port;

  private class CheckedLock extends Lock {
    private Lock lock;

    public CheckedLock(Lock lock) {
      this.lock = lock;
    }

    private void verify(byte message) {
      try {
        Socket s = new Socket(host, port);
        OutputStream out = s.getOutputStream();
        out.write(id);
        out.write(message);
        InputStream in = s.getInputStream();
        int result = in.read();
        in.close();
        out.close();
        s.close();
        if (result != 0)
          throw new RuntimeException("lock was double acquired");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public synchronized boolean obtain(long lockWaitTimeout)
      throws LockObtainFailedException, IOException {
      boolean obtained = lock.obtain(lockWaitTimeout);
      if (obtained)
        verify((byte) 1);
      return obtained;
    }

    @Override
    public synchronized boolean obtain()
      throws LockObtainFailedException, IOException {
      return lock.obtain();
    }

    @Override
    public synchronized boolean isLocked() throws IOException {
      return lock.isLocked();
    }

    @Override
    public synchronized void release() throws IOException {
      if (isLocked()) {
        verify((byte) 0);
        lock.release();
      }
    }
  }

  /**
   * @param id should be a unique id across all clients
   * @param lf the LockFactory that we are testing
   * @param host host or IP where {@link LockVerifyServer}
            is running
   * @param port the port {@link LockVerifyServer} is
            listening on
  */
  public VerifyingLockFactory(byte id, LockFactory lf, String host, int port) throws IOException {
    this.id = id;
    this.lf = lf;
    this.host = host;
    this.port = port;
  }

  @Override
  public synchronized Lock makeLock(String lockName) {
    return new CheckedLock(lf.makeLock(lockName));
  }

  @Override
  public synchronized void clearLock(String lockName)
    throws IOException {
    lf.clearLock(lockName);
  }
}

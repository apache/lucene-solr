package org.apache.lucene.store;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.lucene.index.IndexWriter;

import java.io.IOException;

/** An interprocess mutex lock.
 * <p>Typical use might look like:<pre>
 * new Lock.With(directory.makeLock("my.lock")) {
 *     public Object doBody() {
 *       <i>... code to execute while locked ...</i>
 *     }
 *   }.run();
 * </pre>
 *
 * @author Doug Cutting
 * @version $Id$
 * @see Directory#makeLock(String)
 */
public abstract class Lock {
  public static long LOCK_POLL_INTERVAL = 1000;
    
  private static final String LOCK_DIR =
    System.getProperty("org.apache.lucene.lockdir",
      System.getProperty("java.io.tmpdir"));

  /** Attempts to obtain exclusive access and immediately return
   *  upon success or failure.
   * @return true iff exclusive access is obtained
   */
  public abstract boolean obtain() throws IOException;

  /** Attempts to obtain an exclusive lock within amount
   *  of time given. Currently polls once per second until
   *  lockWaitTimeout is passed.
   * @param lockWaitTimeout length of time to wait in ms
   * @return true if lock was obtained
   * @throws IOException if lock wait times out or obtain() throws an IOException
   */
  public boolean obtain(long lockWaitTimeout) throws IOException {
    boolean locked = obtain();
    int maxSleepCount = (int)(lockWaitTimeout / LOCK_POLL_INTERVAL);
    int sleepCount = 0;
    while (!locked) {
      if (++sleepCount == maxSleepCount) {
        throw new IOException("Lock obtain timed out");
      }
      try {
        Thread.sleep(LOCK_POLL_INTERVAL);
      } catch (InterruptedException e) {
        throw new IOException(e.toString());
      }
      locked = obtain();
    }
    return locked;
  }

  /** Releases exclusive access. */
  public abstract void release();

  /** Returns true if the resource is currently locked.  Note that one must
   * still call {@link #obtain()} before using the resource. */
  public abstract boolean isLocked();


  /** Utility class for executing code with exclusive access. */
  public abstract static class With {
    private Lock lock;
    private long lockWaitTimeout;

    /** Constructs an executor that will grab the named lock.
     *  Defaults lockWaitTimeout to Lock.COMMIT_LOCK_TIMEOUT.
     *  @deprecated Kept only to avoid breaking existing code.
     */
    public With(Lock lock)
    {
      this(lock, IndexWriter.COMMIT_LOCK_TIMEOUT);
    }

    /** Constructs an executor that will grab the named lock. */
    public With(Lock lock, long lockWaitTimeout) {
      this.lock = lock;
      this.lockWaitTimeout = lockWaitTimeout;
    }

    /** Code to execute with exclusive access. */
    protected abstract Object doBody() throws IOException;

    /** Calls {@link #doBody} while <i>lock</i> is obtained.  Blocks if lock
     * cannot be obtained immediately.  Retries to obtain lock once per second
     * until it is obtained, or until it has tried ten times. Lock is released when
     * {@link #doBody} exits. */
    public Object run() throws IOException {
      boolean locked = false;
      try {
         locked = lock.obtain(lockWaitTimeout);
         return doBody();
      } finally {
        if (locked)
	      lock.release();
      }
    }
  }

}

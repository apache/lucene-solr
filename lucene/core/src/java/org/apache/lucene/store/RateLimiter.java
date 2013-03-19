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

import org.apache.lucene.util.ThreadInterruptedException;

/** Abstract base class to rate limit IO.  Typically implementations are
 *  shared across multiple IndexInputs or IndexOutputs (for example
 *  those involved all merging).  Those IndexInputs and
 *  IndexOutputs would call {@link #pause} whenever they
 *  want to read bytes or write bytes. */
public abstract class RateLimiter {

  /**
   * Sets an updated mb per second rate limit.
   */
  public abstract void setMbPerSec(double mbPerSec);
  /**
   * The current mb per second rate limit.
   */
  public abstract double getMbPerSec();
  
  /** Pauses, if necessary, to keep the instantaneous IO
   *  rate at or below the target. 
   *  <p>
   *  Note: the implementation is thread-safe
   *  </p>
   *  @return the pause time in nano seconds 
   * */
  public abstract long pause(long bytes);
  
  /**
   * Simple class to rate limit IO.
   */
  public static class SimpleRateLimiter extends RateLimiter {
    private volatile double mbPerSec;
    private volatile double nsPerByte;
    private volatile long lastNS;

    // TODO: we could also allow eg a sub class to dynamically
    // determine the allowed rate, eg if an app wants to
    // change the allowed rate over time or something

    /** mbPerSec is the MB/sec max IO rate */
    public SimpleRateLimiter(double mbPerSec) {
      setMbPerSec(mbPerSec);
    }

    /**
     * Sets an updated mb per second rate limit.
     */
    @Override
    public void setMbPerSec(double mbPerSec) {
      this.mbPerSec = mbPerSec;
      nsPerByte = 1000000000. / (1024*1024*mbPerSec);
      
    }

    /**
     * The current mb per second rate limit.
     */
    @Override
    public double getMbPerSec() {
      return this.mbPerSec;
    }
    
    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target. NOTE: multiple threads
     *  may safely use this, however the implementation is
     *  not perfectly thread safe but likely in practice this
     *  is harmless (just means in some rare cases the rate
     *  might exceed the target).  It's best to call this
     *  with a biggish count, not one byte at a time.
     *  @return the pause time in nano seconds 
     * */
    @Override
    public long pause(long bytes) {
      if (bytes == 1) {
        return 0;
      }

      // TODO: this is purely instantaneous rate; maybe we
      // should also offer decayed recent history one?
      final long targetNS = lastNS = lastNS + ((long) (bytes * nsPerByte));
      final long startNS;
      long curNS = startNS = System.nanoTime();
      if (lastNS < curNS) {
        lastNS = curNS;
      }

      // While loop because Thread.sleep doesn't always sleep
      // enough:
      while(true) {
        final long pauseNS = targetNS - curNS;
        if (pauseNS > 0) {
          try {
            Thread.sleep((int) (pauseNS/1000000), (int) (pauseNS % 1000000));
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
          curNS = System.nanoTime();
          continue;
        }
        break;
      }
      return curNS - startNS;
    }
  }
}

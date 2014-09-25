package org.apache.solr.search;

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

import org.apache.lucene.index.QueryTimeout;

import java.util.concurrent.TimeUnit;

import static java.lang.System.nanoTime;

/**
 * Implementation of {@link QueryTimeout} that is used by Solr. 
 * It uses a ThreadLocal variable to track the timeoutAt value
 * for each request thread.
 */
public class SolrQueryTimeoutImpl implements QueryTimeout {
  /**
   * The ThreadLocal variable to store the time beyond which, the processing should exit.
   */
  public static ThreadLocal<Long> timeoutAt = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return nanoTime() + Long.MAX_VALUE;
    }
  };

  private SolrQueryTimeoutImpl() { }
  private static SolrQueryTimeoutImpl instance = new SolrQueryTimeoutImpl();

  /** Return singleton instance */
  public static SolrQueryTimeoutImpl getInstance() { 
    return instance; 
  }

  /**
   * Get the current value of timeoutAt.
   */
  public static Long get() {
    return timeoutAt.get();
  }

  /**
   * Return true if a timeoutAt value is set and the current time has exceeded the set timeOut.
   */
  @Override
  public boolean shouldExit() {
    return get() - nanoTime() < 0L;
  }

  /**
   * Method to set the time at which the timeOut should happen.
   * @param timeAllowed set the time at which this thread should timeout.
   */
  public static void set(Long timeAllowed) {
    long time = nanoTime() + TimeUnit.NANOSECONDS.convert(timeAllowed, TimeUnit.MILLISECONDS);
    timeoutAt.set(time);
  }

  /**
   * Cleanup the ThreadLocal timeout value.
   */
  public static void reset() {
    timeoutAt.remove();
  }

  @Override
  public String toString() {
    return "timeoutAt: " + get() + " (System.nanoTime(): " + nanoTime() + ")";
  }
}


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
package org.apache.solr.search;

import static java.lang.System.nanoTime;

import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Implementation of {@link QueryTimeout} that is used by Solr. 
 * It uses a ThreadLocal variable to track the timeoutAt value
 * for each request thread.
 */
public class SolrQueryTimeoutImpl implements QueryTimeout {
  /**
   * The ThreadLocal variable to store the time beyond which, the processing should exit.
   */
  private static final ThreadLocal<Long> timeoutAt = new ThreadLocal<>();

  private static final SolrQueryTimeoutImpl instance = new SolrQueryTimeoutImpl();

  private SolrQueryTimeoutImpl() { }

  /** Return singleton instance */
  public static SolrQueryTimeoutImpl getInstance() { 
    return instance; 
  }

  /**
   * The time (nanoseconds) at which the request should be considered timed out.
   */
  public static Long get() {
    return timeoutAt.get();
  }

  @Override
  public boolean isTimeoutEnabled() {
    return get() != null;
  }

  /**
   * Return true if a timeoutAt value is set and the current time has exceeded the set timeOut.
   */
  @Override
  public boolean shouldExit() {
    Long timeoutAt = get();
    if (timeoutAt == null) {
      // timeout unset
      return false;
    }
    return timeoutAt - nanoTime() < 0L;
  }

  /**
   * Sets or clears the time allowed based on how much time remains from the start of the request plus the configured
   * {@link CommonParams#TIME_ALLOWED}.
   */
  public static void set(SolrQueryRequest req) {
    long timeAllowed = req.getParams().getLong(CommonParams.TIME_ALLOWED, -1L);
    if (timeAllowed >= 0L) {
      set(timeAllowed - (long)req.getRequestTimer().getTime()); // reduce by time already spent
    } else {
      reset();
    }
  }

  /**
   * Sets the time allowed (milliseconds), assuming we start a timer immediately.
   * You should probably invoke {@link #set(SolrQueryRequest)} instead.
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


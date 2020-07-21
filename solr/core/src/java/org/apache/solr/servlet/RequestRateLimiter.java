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

package org.apache.solr.servlet;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles rate limiting for a specific request type.
 *
 * The control flow is as follows:
 * Handle request -- Check if slot is available -- If available, acquire slot and proceed --
 * else asynchronously queue the request
 *
 * When an active request completes, a check is performed to see if there are any pending requests.
 * If there is an available pending request, process the same.
 */
public class RequestRateLimiter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Semaphore allowedConcurrentRequests;
  private RateLimiterConfig rateLimiterConfig;
  private Queue<AsyncContext> waitQueue;
  private Queue<AsyncListener> listenerQueue;

  public RequestRateLimiter(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    this.allowedConcurrentRequests = new Semaphore(rateLimiterConfig.allowedRequests);
    this.waitQueue = new ConcurrentLinkedQueue<>();
    this.listenerQueue = new ConcurrentLinkedQueue<>();
  }

  public boolean handleNewRequest(HttpServletRequest request) throws InterruptedException {
    boolean accepted = allowedConcurrentRequests.tryAcquire(rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS);

    if (!accepted) {
      AsyncContext asyncContext = request.startAsync();
      AsyncListener asyncListener = buildAsyncListener();

      if (rateLimiterConfig.requestSuspendTimeInMS > 0) {
        asyncContext.setTimeout(rateLimiterConfig.requestSuspendTimeInMS);
      }

      asyncContext.addListener(asyncListener);
      listenerQueue.add(asyncListener);
      waitQueue.add(asyncContext);
    }

    return accepted;
  }

  public boolean resumePendingOperation() {
    AsyncContext asyncContext = waitQueue.poll();

    if (asyncContext != null) {
      try {
        asyncContext.dispatch();
        return true;
      }
      catch (IllegalStateException x) {
        log.warn(x.getMessage());
      }
    }

    return false;
  }

  public void decrementConcurrentRequests() {
    allowedConcurrentRequests.release();
  }

  private AsyncListener buildAsyncListener() {
    return new AsyncListener() {
      @Override
      public void onComplete(AsyncEvent asyncEvent) throws IOException {

      }

      @Override
      public void onTimeout(AsyncEvent asyncEvent) throws IOException {
        AsyncContext asyncContext = asyncEvent.getAsyncContext();

        if (!waitQueue.remove(asyncContext)) {
          return;
        }

        HttpServletResponse servletResponse = ((HttpServletResponse)asyncEvent.getSuppliedResponse());

        servletResponse.setContentType("APPLICATION/OCTET-STREAM");
        servletResponse.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);

        String responseMessage = "Too many requests for this request type." +
            "Please try after some time or increase the quota for this request type";

        servletResponse.getWriter().write(responseMessage);
        servletResponse.getWriter().flush();
        servletResponse.getWriter().close();

        asyncContext.complete();
      }

      @Override
      public void onError(AsyncEvent asyncEvent) throws IOException {

      }

      @Override
      public void onStartAsync(AsyncEvent asyncEvent) throws IOException {

      }
    };
  }

  static long getParamAndParseLong(FilterConfig config, String parameterName, long defaultValue) {
    String tempBuffer = config.getInitParameter(parameterName);

    if (tempBuffer != null) {
      return Long.parseLong(tempBuffer);
    }

    return defaultValue;
  }

  static int getParamAndParseInt(FilterConfig config, String parameterName, int defaultValue) {
    String tempBuffer = config.getInitParameter(parameterName);

    if (tempBuffer != null) {
      return Integer.parseInt(tempBuffer);
    }

    return defaultValue;
  }

  static class RateLimiterConfig {
    public long requestSuspendTimeInMS;
    public long waitForSlotAcquisition;
    public int allowedRequests;

    public RateLimiterConfig() { }

    public RateLimiterConfig(long requestSuspendTimeInMS, long waitForSlotAcquisition, int allowedRequests) {
      this.requestSuspendTimeInMS = requestSuspendTimeInMS;
      this.waitForSlotAcquisition = waitForSlotAcquisition;
      this.allowedRequests = allowedRequests;
    }
  }
}

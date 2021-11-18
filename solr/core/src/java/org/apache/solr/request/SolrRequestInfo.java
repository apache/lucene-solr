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
package org.apache.solr.request;

import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Information about the Solr request/response held in a {@link ThreadLocal}. */
public class SolrRequestInfo {

  private static final int MAX_STACK_SIZE = 10;

  private static final ThreadLocal<Deque<SolrRequestInfo>> threadLocal = ThreadLocal.withInitial(LinkedList::new);

  private int refCount = 1; // prevent closing when still used

  private SolrQueryRequest req;
  private SolrQueryResponse rsp;
  private Date now;
  protected HttpServletRequest httpRequest;
  private TimeZone tz;
  private ResponseBuilder rb;
  private List<Closeable> closeHooks;
  private SolrDispatchFilter.Action action;
  private boolean useServerToken = false;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static SolrRequestInfo getRequestInfo() {
    Deque<SolrRequestInfo> stack = threadLocal.get();
    if (stack.isEmpty()) return null;
    return stack.peek();
  }

  /**
   * Adds the SolrRequestInfo onto a stack held in a {@link ThreadLocal}.
   * Remember to call {@link #clearRequestInfo()}!
   */
  public static void setRequestInfo(SolrRequestInfo info) {
    Deque<SolrRequestInfo> stack = threadLocal.get();
    if (info == null) {
      throw new IllegalArgumentException("SolrRequestInfo is null");
    } else if (stack.size() > MAX_STACK_SIZE) {
      assert false : "SolrRequestInfo Stack is full";
      log.error("SolrRequestInfo Stack is full");
    }
    log.trace("{} {}", info, "setRequestInfo()");
    assert !info.isClosed() : "SRI is already closed (odd).";
    stack.push(info);
  }

  /** Removes the most recent SolrRequestInfo from the stack.  Close hooks are called. */
  public static void clearRequestInfo() {
    log.trace("clearRequestInfo()");
    Deque<SolrRequestInfo> stack = threadLocal.get();
    if (stack.isEmpty()) {
      assert false : "clearRequestInfo called too many times";
      log.error("clearRequestInfo called too many times");
    } else {
      SolrRequestInfo info = stack.pop();
      info.close();
    }
  }

  /**
   * This reset method is more of a protection mechanism as
   * we expect it to be empty by now because all "set" calls need to be balanced with a "clear".
   */
  public static void reset() {
    log.trace("reset()");
    Deque<SolrRequestInfo> stack = threadLocal.get();
    assert stack.isEmpty() : "SolrRequestInfo Stack should have been cleared.";
    while (!stack.isEmpty()) {
      SolrRequestInfo info = stack.pop();
      info.close();
    }
  }

  private synchronized void close() {
    log.trace("{} {}", this, "close()");

    if (--refCount > 0) {
      log.trace("{} {}", this, "not closing; still referenced");
      return;
    }

    if (closeHooks != null) {
      for (Closeable hook : closeHooks) {
        try {
          hook.close();
        } catch (Exception e) {
          SolrException.log(log, "Exception during close hook", e);
        }
      }
    }
    closeHooks = null;
  }

  public SolrRequestInfo(SolrQueryRequest req, SolrQueryResponse rsp) {
    this.req = req;
    this.rsp = rsp;    
  }
  public SolrRequestInfo(SolrQueryRequest req, SolrQueryResponse rsp, SolrDispatchFilter.Action action) {
    this(req, rsp);
    this.setAction(action);
  }

  public SolrRequestInfo(HttpServletRequest httpReq, SolrQueryResponse rsp) {
    this.httpRequest = httpReq;
    this.rsp = rsp;
  }

  public SolrRequestInfo(HttpServletRequest httpReq, SolrQueryResponse rsp, SolrDispatchFilter.Action action) {
    this(httpReq, rsp);
    this.action = action;
  }

  public Principal getUserPrincipal() {
    if (req != null) return req.getUserPrincipal();
    if (httpRequest != null) return httpRequest.getUserPrincipal();
    return null;
  }


  public Date getNOW() {    
    if (now != null) return now;

    long ms = 0;
    String nowStr = req.getParams().get(CommonParams.NOW);

    if (nowStr != null) {
      ms = Long.parseLong(nowStr);
    } else {
      ms = req.getStartTime();
    }

    now = new Date(ms);
    return now;
  }

  /** The TimeZone specified by the request, or UTC if none was specified. */
  public TimeZone getClientTimeZone() {
    if (tz == null)  {
      tz = TimeZoneUtils.parseTimezone(req.getParams().get(CommonParams.TZ));
    }
    return tz;
  }

  public SolrQueryRequest getReq() {
    return req;
  }

  public SolrQueryResponse getRsp() {
    return rsp;
  }

  /** May return null if the request handler is not based on SearchHandler */
  public ResponseBuilder getResponseBuilder() {
    return rb;
  }

  public void setResponseBuilder(ResponseBuilder rb) {
    this.rb = rb;
  }

  public void addCloseHook(Closeable hook) {
    // is this better here, or on SolrQueryRequest?
    synchronized (this) {
      if (isClosed()) {
        throw new IllegalStateException("Already closed!");
      }
      if (closeHooks == null) {
        closeHooks = new LinkedList<>();
      }
      closeHooks.add(hook);
    }
  }

  public SolrDispatchFilter.Action getAction() {
    return action;
  }

  public void setAction(SolrDispatchFilter.Action action) {
    this.action = action;
  }

  /**
   * Used when making remote requests to other Solr nodes from the thread associated with this request,
   * true means the server token header should be used instead of the Principal associated with the request.
   */
  public boolean useServerToken() {
    return useServerToken;
  }

  public void setUseServerToken(boolean use) {
    this.useServerToken = use;
  }

  private synchronized boolean isClosed() {
    return refCount <= 0;
  }

  public static ExecutorUtil.InheritableThreadLocalProvider getInheritableThreadLocalProvider() {
    return new ExecutorUtil.InheritableThreadLocalProvider() {
      @Override
      @SuppressWarnings({"unchecked"})
      public void store(@SuppressWarnings({"rawtypes"})AtomicReference ctx) {
        SolrRequestInfo me = SolrRequestInfo.getRequestInfo();
        if (me != null) {
          // increase refCount in store(), while we're still in the thread of the provider to avoid
          //  a race if this thread finishes its work before the pool'ed thread runs
          synchronized (me) {
            me.refCount++;
          }
          ctx.set(me);
        }
      }

      @Override
      @SuppressWarnings({"unchecked"})
      public void set(@SuppressWarnings({"rawtypes"})AtomicReference ctx) {
        SolrRequestInfo me = (SolrRequestInfo) ctx.get();
        if (me != null) {
          SolrRequestInfo.setRequestInfo(me);
        }
      }

      @Override
      public void clean(@SuppressWarnings({"rawtypes"})AtomicReference ctx) {
        if (ctx.get() != null) {
          SolrRequestInfo.clearRequestInfo();
        }
        SolrRequestInfo.reset();
      }
    };
  }
}

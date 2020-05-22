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
import java.util.ArrayDeque;
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


public class SolrRequestInfo {

  protected final static int capacity = 150;

  protected final static ThreadLocal<Deque<SolrRequestInfo>> threadLocal = ThreadLocal.withInitial(() -> {
    Deque<SolrRequestInfo> stack = new ArrayDeque<>(capacity);
    return stack;
  });

  protected SolrQueryRequest req;
  protected SolrQueryResponse rsp;
  protected Date now;
  public HttpServletRequest httpRequest;
  protected TimeZone tz;
  protected ResponseBuilder rb;
  protected List<Closeable> closeHooks;
  protected SolrDispatchFilter.Action action;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static SolrRequestInfo getRequestInfo() {
    if (threadLocal.get().isEmpty()) return null;
    return threadLocal.get().peek();
  }

  public static void setRequestInfo(SolrRequestInfo info) {
    if (info == null) {
      throw new IllegalArgumentException("SolrRequestInfo is null");
    } else {
      if (threadLocal.get().size() <= capacity) {
        threadLocal.get().push(info);
      } else {
        log.error("SolrRequestInfo Stack is full");
      }
    }
  }

  public static void clearRequestInfo() {
    if (threadLocal.get().isEmpty()) {
      log.error("clearRequestInfo called too many times");
    } else {
      SolrRequestInfo info = threadLocal.get().pop();
      closeHooks(info);
    }
  }

  public static void reset() {
    Deque<SolrRequestInfo> stack = threadLocal.get();
    while (!stack.isEmpty()) {
      SolrRequestInfo info = stack.pop();
      closeHooks(info);
    }
  }

  private static void closeHooks(SolrRequestInfo info) {
    if (info != null && info.closeHooks != null) {
      for (Closeable hook : info.closeHooks) {
        try {
          hook.close();
        } catch (Exception e) {
          SolrException.log(log, "Exception during close hook", e);
        }
      }
    }
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

  public static ExecutorUtil.InheritableThreadLocalProvider getInheritableThreadLocalProvider() {
    return new ExecutorUtil.InheritableThreadLocalProvider() {
      @Override
      public void store(AtomicReference ctx) {
        SolrRequestInfo me = SolrRequestInfo.getRequestInfo();
        if (me != null) ctx.set(me);
      }

      @Override
      public void set(AtomicReference ctx) {
        SolrRequestInfo me = (SolrRequestInfo) ctx.get();
        if (me != null) {
          ctx.set(null);
          SolrRequestInfo.setRequestInfo(me);
        }
      }

      @Override
      public void clean(AtomicReference ctx) {
        if (ctx.get() != null) {
          SolrRequestInfo.clearRequestInfo();
        }
      }
    };
  }
}

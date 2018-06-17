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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.solr.common.params.QoSParams;


public class SolrQoSFilter extends QoSFilter {
  static final String MAX_REQUESTS_INIT_PARAM = "maxRequests";
  static final String SUSPEND_INIT_PARAM = "suspendMs";
  static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
  protected int _origMaxRequests;
  
  private AtomicInteger requestCnt = new AtomicInteger();
  
  @Override
  public void init(FilterConfig filterConfig) {
    super.init(filterConfig);
    _origMaxRequests = 100;
    super.setMaxRequests(_origMaxRequests);
    super.setSuspendMs(60000);
    super.setWaitMs(50);
  }
  
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    //requestCnt.incrementAndGet();
    String source = req.getHeader(QoSParams.REQUEST_SOURCE);
    if (source == null || !source.equals(QoSParams.INTERNAL)) {

      if (requestCnt.getAndIncrement() % 5 == 0) {
        // nocommit - deal with not supported? dont call every request?
        double load = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
        double sLoad = load / (double) PROC_COUNT;
        if (sLoad > 0.95D) {
          int cMax = getMaxRequests();
          if (cMax > 3) {
            setMaxRequests(Math.max(3, (int) ((double) cMax * 0.60D)));
          }
        } else if (sLoad < 0.9D && _origMaxRequests != getMaxRequests()) {
          setMaxRequests(_origMaxRequests);
        }
        //System.out.println("external request, load:" + load);
      }

      super.doFilter(req, response, chain);

    } else {
      //System.out.println("internal request");
      chain.doFilter(req, response);
    }
  }
}

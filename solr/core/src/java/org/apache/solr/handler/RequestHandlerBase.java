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

package org.apache.solr.handler;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.stats.Snapshot;
import org.apache.solr.util.stats.Timer;
import org.apache.solr.util.stats.TimerContext;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public abstract class RequestHandlerBase implements SolrRequestHandler, SolrInfoMBean, NestedRequestHandler {

  protected NamedList initArgs = null;
  protected SolrParams defaults;
  protected SolrParams appends;
  protected SolrParams invariants;
  protected boolean httpCaching = true;

  // Statistics
  private final AtomicLong numRequests = new AtomicLong();
  private final AtomicLong numErrors = new AtomicLong();
  private final AtomicLong numTimeouts = new AtomicLong();
  private final Timer requestTimes = new Timer();
  private final long handlerStart = System.currentTimeMillis();

  /**
   * Initializes the {@link org.apache.solr.request.SolrRequestHandler} by creating three {@link org.apache.solr.common.params.SolrParams} named.
   * <table border="1">
   * <tr><th>Name</th><th>Description</th></tr>
   * <tr><td>defaults</td><td>Contains all of the named arguments contained within the list element named "defaults".</td></tr>
   * <tr><td>appends</td><td>Contains all of the named arguments contained within the list element named "appends".</td></tr>
   * <tr><td>invariants</td><td>Contains all of the named arguments contained within the list element named "invariants".</td></tr>
   * </table>
   *
   * Example:
   * <pre>
   * &lt;lst name="defaults"&gt;
   * &lt;str name="echoParams"&gt;explicit&lt;/str&gt;
   * &lt;str name="qf"&gt;text^0.5 features^1.0 name^1.2 sku^1.5 id^10.0&lt;/str&gt;
   * &lt;str name="mm"&gt;2&lt;-1 5&lt;-2 6&lt;90%&lt;/str&gt;
   * &lt;str name="bq"&gt;incubationdate_dt:[* TO NOW/DAY-1MONTH]^2.2&lt;/str&gt;
   * &lt;/lst&gt;
   * &lt;lst name="appends"&gt;
   * &lt;str name="fq"&gt;inStock:true&lt;/str&gt;
   * &lt;/lst&gt;
   *
   * &lt;lst name="invariants"&gt;
   * &lt;str name="facet.field"&gt;cat&lt;/str&gt;
   * &lt;str name="facet.field"&gt;manu_exact&lt;/str&gt;
   * &lt;str name="facet.query"&gt;price:[* TO 500]&lt;/str&gt;
   * &lt;str name="facet.query"&gt;price:[500 TO *]&lt;/str&gt;
   * &lt;/lst&gt;
   * </pre>
   *
   *
   * @param args The {@link org.apache.solr.common.util.NamedList} to initialize from
   *
   * @see #handleRequest(org.apache.solr.request.SolrQueryRequest, org.apache.solr.response.SolrQueryResponse)
   * @see #handleRequestBody(org.apache.solr.request.SolrQueryRequest, org.apache.solr.response.SolrQueryResponse)
   * @see org.apache.solr.util.SolrPluginUtils#setDefaults(org.apache.solr.request.SolrQueryRequest, org.apache.solr.common.params.SolrParams, org.apache.solr.common.params.SolrParams, org.apache.solr.common.params.SolrParams)
   * @see SolrParams#toSolrParams(org.apache.solr.common.util.NamedList)
   *
   * See also the example solrconfig.xml located in the Solr codebase (example/solr/conf).
   */
  @Override
  public void init(NamedList args) {
    initArgs = args;

    // Copied from StandardRequestHandler
    if( args != null ) {
      Object o = args.get("defaults");
      if (o != null && o instanceof NamedList) {
        defaults = SolrParams.toSolrParams((NamedList)o);
      }
      o = args.get("appends");
      if (o != null && o instanceof NamedList) {
        appends = SolrParams.toSolrParams((NamedList)o);
      }
      o = args.get("invariants");
      if (o != null && o instanceof NamedList) {
        invariants = SolrParams.toSolrParams((NamedList)o);
      }
    }
    
    if (initArgs != null) {
      Object caching = initArgs.get("httpCaching");
      httpCaching = caching != null ? Boolean.parseBoolean(caching.toString()) : true;
    }

  }

  public NamedList getInitArgs() {
    return initArgs;
  }
  
  public abstract void handleRequestBody( SolrQueryRequest req, SolrQueryResponse rsp ) throws Exception;

  @Override
  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    numRequests.incrementAndGet();
    TimerContext timer = requestTimes.time();
    try {
      SolrPluginUtils.setDefaults(req,defaults,appends,invariants);
      rsp.setHttpCaching(httpCaching);
      handleRequestBody( req, rsp );
      // count timeouts
      NamedList header = rsp.getResponseHeader();
      if(header != null) {
        Object partialResults = header.get("partialResults");
        boolean timedOut = partialResults == null ? false : (Boolean)partialResults;
        if( timedOut ) {
          numTimeouts.incrementAndGet();
          rsp.setHttpCaching(false);
        }
      }
    } catch (Exception e) {
      if (e instanceof SolrException) {
        SolrException se = (SolrException)e;
        if (se.code() == SolrException.ErrorCode.CONFLICT.code) {
          // TODO: should we allow this to be counted as an error (numErrors++)?

        } else {
          SolrException.log(SolrCore.log,e);
        }
      } else {
        SolrException.log(SolrCore.log,e);
        if (e instanceof SyntaxError) {
          e = new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }
      }

      rsp.setException(e);
      numErrors.incrementAndGet();
    }
    finally {
      timer.stop();
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public abstract String getDescription();
  @Override
  public String getSource() { return null; }
  
  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }
  
  @Override
  public Category getCategory() {
    return Category.QUERYHANDLER;
  }

  @Override
  public URL[] getDocs() {
    return null;  // this can be overridden, but not required
  }


  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    return null;
  }


  /**
   * Get the request handler registered to a given name.
   *
   * This function is thread safe.
   */
  public static SolrRequestHandler getRequestHandler(String handlerName, Map<String, SolrRequestHandler> reqHandlers) {
    if(handlerName == null) return null;
    SolrRequestHandler handler = reqHandlers.get(handlerName);
    int idx = 0;
    if(handler == null) {
      for (; ; ) {
        idx = handlerName.indexOf('/', idx+1);
        if (idx > 0) {
          String firstPart = handlerName.substring(0, idx);
          handler = reqHandlers.get(firstPart);
          if (handler == null) continue;
          if (handler instanceof NestedRequestHandler) {
            return ((NestedRequestHandler) handler).getSubHandler(handlerName.substring(idx));
          }
        } else {
          break;
        }
      }
    }
    return handler;
  }


  @Override
  public NamedList<Object> getStatistics() {
    NamedList<Object> lst = new SimpleOrderedMap<>();
    Snapshot snapshot = requestTimes.getSnapshot();
    lst.add("handlerStart",handlerStart);
    lst.add("requests", numRequests.longValue());
    lst.add("errors", numErrors.longValue());
    lst.add("timeouts", numTimeouts.longValue());
    lst.add("totalTime", requestTimes.getSum());
    lst.add("avgRequestsPerSecond", requestTimes.getMeanRate());
    lst.add("5minRateReqsPerSecond", requestTimes.getFiveMinuteRate());
    lst.add("15minRateReqsPerSecond", requestTimes.getFifteenMinuteRate());
    lst.add("avgTimePerRequest", requestTimes.getMean());
    lst.add("medianRequestTime", snapshot.getMedian());
    lst.add("75thPcRequestTime", snapshot.get75thPercentile());
    lst.add("95thPcRequestTime", snapshot.get95thPercentile());
    lst.add("99thPcRequestTime", snapshot.get99thPercentile());
    lst.add("999thPcRequestTime", snapshot.get999thPercentile());
    return lst;
  }
  
}



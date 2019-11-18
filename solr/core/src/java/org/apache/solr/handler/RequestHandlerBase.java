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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.ApiSupport;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.core.RequestParams.USEPARAM;

/**
 *
 */
public abstract class RequestHandlerBase implements SolrRequestHandler, SolrInfoBean, NestedRequestHandler, ApiSupport {

  protected NamedList initArgs = null;
  protected SolrParams defaults;
  protected SolrParams appends;
  protected SolrParams invariants;
  protected boolean httpCaching = true;

  // Statistics
  private Meter numErrors = new Meter();
  private Meter numServerErrors = new Meter();
  private Meter numClientErrors = new Meter();
  private Meter numTimeouts = new Meter();
  private Counter requests = new Counter();
  private final Map<String, Counter> shardPurposes = new ConcurrentHashMap<>();
  private Timer requestTimes = new Timer();
  private Counter totalTime = new Counter();

  private final long handlerStart;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private PluginInfo pluginInfo;

  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  protected SolrMetricsContext solrMetricsContext;


  @SuppressForbidden(reason = "Need currentTimeMillis, used only for stats output")
  public RequestHandlerBase() {
    handlerStart = System.currentTimeMillis();
  }

  /**
   * Initializes the {@link org.apache.solr.request.SolrRequestHandler} by creating three {@link org.apache.solr.common.params.SolrParams} named.
   * <table border="1" summary="table of parameters">
   * <tr><th>Name</th><th>Description</th></tr>
   * <tr><td>defaults</td><td>Contains all of the named arguments contained within the list element named "defaults".</td></tr>
   * <tr><td>appends</td><td>Contains all of the named arguments contained within the list element named "appends".</td></tr>
   * <tr><td>invariants</td><td>Contains all of the named arguments contained within the list element named "invariants".</td></tr>
   * </table>
   * <p>
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
   * @param args The {@link org.apache.solr.common.util.NamedList} to initialize from
   * @see #handleRequest(org.apache.solr.request.SolrQueryRequest, org.apache.solr.response.SolrQueryResponse)
   * @see #handleRequestBody(org.apache.solr.request.SolrQueryRequest, org.apache.solr.response.SolrQueryResponse)
   * @see org.apache.solr.util.SolrPluginUtils#setDefaults(org.apache.solr.request.SolrQueryRequest, org.apache.solr.common.params.SolrParams, org.apache.solr.common.params.SolrParams, org.apache.solr.common.params.SolrParams)
   * @see NamedList#toSolrParams()
   * <p>
   * See also the example solrconfig.xml located in the Solr codebase (example/solr/conf).
   */
  @Override
  public void init(NamedList args) {
    initArgs = args;

    if (args != null) {
      defaults = getSolrParamsFromNamedList(args, "defaults");
      appends = getSolrParamsFromNamedList(args, "appends");
      invariants = getSolrParamsFromNamedList(args, "invariants");
    }

    if (initArgs != null) {
      Object caching = initArgs.get("httpCaching");
      httpCaching = caching != null ? Boolean.parseBoolean(caching.toString()) : true;
    }

  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    this.solrMetricsContext = parentContext.getChildContext(this);
    numErrors = solrMetricsContext.meter("errors", getCategory().toString(), scope);
    numServerErrors = solrMetricsContext.meter("serverErrors", getCategory().toString(), scope);
    numClientErrors = solrMetricsContext.meter("clientErrors", getCategory().toString(), scope);
    numTimeouts = solrMetricsContext.meter("timeouts", getCategory().toString(), scope);
    requests = solrMetricsContext.counter("requests", getCategory().toString(), scope);
    MetricsMap metricsMap = new MetricsMap((detail, map) ->
        shardPurposes.forEach((k, v) -> map.put(k, v.getCount())));
    solrMetricsContext.gauge(metricsMap, true, "shardRequests", getCategory().toString(), scope);
    requestTimes = solrMetricsContext.timer("requestTimes", getCategory().toString(), scope);
    totalTime = solrMetricsContext.counter("totalTime", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> handlerStart, true, "handlerStart", getCategory().toString(), scope);
  }

  public static SolrParams getSolrParamsFromNamedList(NamedList args, String key) {
    Object o = args.get(key);
    if (o != null && o instanceof NamedList) {
      return ((NamedList) o).toSolrParams();
    }
    return null;
  }

  public NamedList getInitArgs() {
    return initArgs;
  }

  public abstract void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception;

  @Override
  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    requests.inc();
    if (req.getParams().getBool(ShardParams.IS_SHARD, false)) {
      shardPurposes.computeIfAbsent("total", name -> new Counter()).inc();
      int purpose = req.getParams().getInt(ShardParams.SHARDS_PURPOSE, 0);
      if (purpose != 0) {
        String[] names = SolrPluginUtils.getRequestPurposeNames(purpose);
        for (String n : names) {
          shardPurposes.computeIfAbsent(n, name -> new Counter()).inc();
        }
      }
    }
    Timer.Context timer = requestTimes.time();
    try {
      if (pluginInfo != null && pluginInfo.attributes.containsKey(USEPARAM))
        req.getContext().put(USEPARAM, pluginInfo.attributes.get(USEPARAM));
      SolrPluginUtils.setDefaults(this, req, defaults, appends, invariants);
      req.getContext().remove(USEPARAM);
      rsp.setHttpCaching(httpCaching);
      handleRequestBody(req, rsp);
      // count timeouts
      NamedList header = rsp.getResponseHeader();
      if (header != null) {
        if (Boolean.TRUE.equals(header.getBooleanArg(
            SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
          numTimeouts.mark();
          rsp.setHttpCaching(false);
        }
      }
    } catch (Exception e) {
      if (req.getCore() != null) {
        boolean isTragic = req.getCore().getCoreContainer().checkTragicException(req.getCore());
        if (isTragic) {
          if (e instanceof SolrException) {
            // Tragic exceptions should always throw a server error
            assert ((SolrException) e).code() == 500;
          } else {
            // wrap it in a solr exception
            e = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
          }
        }
      }
      boolean incrementErrors = true;
      boolean isServerError = true;
      if (e instanceof SolrException) {
        SolrException se = (SolrException) e;
        if (se.code() == SolrException.ErrorCode.CONFLICT.code) {
          incrementErrors = false;
        } else if (se.code() >= 400 && se.code() < 500) {
          isServerError = false;
        }
      } else {
        if (e instanceof SyntaxError) {
          isServerError = false;
          e = new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }
      }

      rsp.setException(e);

      if (incrementErrors) {
        SolrException.log(log, e);

        numErrors.mark();
        if (isServerError) {
          numServerErrors.mark();
        } else {
          numClientErrors.mark();
        }
      }
    } finally {
      long elapsed = timer.stop();
      totalTime.inc(elapsed);
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
  public Category getCategory() {
    return Category.QUERY;
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    return null;
  }


  /**
   * Get the request handler registered to a given name.
   * <p>
   * This function is thread safe.
   */
  public static SolrRequestHandler getRequestHandler(String handlerName, PluginBag<SolrRequestHandler> reqHandlers) {
    if (handlerName == null) return null;
    SolrRequestHandler handler = reqHandlers.get(handlerName);
    int idx = 0;
    if (handler == null) {
      for (; ; ) {
        idx = handlerName.indexOf('/', idx + 1);
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

  public void setPluginInfo(PluginInfo pluginInfo) {
    if (this.pluginInfo == null) this.pluginInfo = pluginInfo;
  }

  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  @Override
  public Collection<Api> getApis() {
    return ImmutableList.of(new ApiBag.ReqHandlerToApi(this, ApiBag.constructSpec(pluginInfo)));
  }
}



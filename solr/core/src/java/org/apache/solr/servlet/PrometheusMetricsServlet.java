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

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics.
 * This servlet exists for backwards compatibility and will be removed in favor of the native prometheus-exporter.
 */
public final class PrometheusMetricsServlet extends BaseSolrServlet {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // values less than this threshold are considered invalid; mark the invalid values instead of failing the call.
  private static final Integer INVALID_NUMBER = -1;

  private final List<MetricsApiCaller> callers = Collections.unmodifiableList(Arrays.asList(
      new GarbageCollectorMetricsApiCaller(),
      new MemoryMetricsApiCaller(),
      new OsMetricsApiCaller(),
      new ThreadMetricsApiCaller(),
      new StatusCodeMetricsApiCaller(),
      new CoresMetricsApiCaller()
  ));

  private final Map<String, PrometheusMetricType> cacheMetricTypes = ImmutableMap.of(
      "bytesUsed", PrometheusMetricType.GAUGE,
      "lookups", PrometheusMetricType.COUNTER,
      "hits", PrometheusMetricType.COUNTER,
      "puts", PrometheusMetricType.COUNTER,
      "evictions", PrometheusMetricType.COUNTER
  );

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    List<PrometheusMetric> metrics = new ArrayList<>();
    AtomicInteger qTime = new AtomicInteger();
    for(MetricsApiCaller caller : callers) {
      caller.call(qTime, metrics, request);
    }
    getSharedCacheMetrics(metrics, getSolrDispatchFilter(request).getCores(), cacheMetricTypes);
    metrics.add(new PrometheusMetric("metrics_qtime", PrometheusMetricType.GAUGE, "QTime for calling metrics api", qTime));
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    PrintWriter writer = response.getWriter();
    for(PrometheusMetric metric : metrics) {
      metric.write(writer);
    }
    writer.flush();
  }

  static void getSharedCacheMetrics(List<PrometheusMetric> results, CoreContainer cores, Map<String, PrometheusMetricType> types) {
    Object value = Optional.of(cores)
        .map(CoreContainer::getZkController)
        .map(ZkController::getSolrCloudManager)
        .map(SolrCloudManager::getObjectCache)
        .map(cache -> cache.get("fs-shared-caches")) // see AbstractSharedCache.statsSupplier in plugin.
        .filter(Supplier.class::isInstance)
        .map(Supplier.class::cast)
        .map(Supplier::get)
        .orElse(null);
    if (value == null) {
      return;
    }
    Map<String, NamedList<Number>> cacheStats = (Map<String, NamedList<Number>>) value;
    for(Map.Entry<String, NamedList<Number>> cacheStat : cacheStats.entrySet()) {
      String cache = cacheStat.getKey().toLowerCase(Locale.ROOT);
      for(Map.Entry<String, Number> stat : cacheStat.getValue()) {
        String name = stat.getKey();
        PrometheusMetricType type = types.get(name);
        if (type != null) {
          results.add(new PrometheusMetric(String.format(Locale.ROOT, "cache_%s_%s", cache, name), type,
              String.format(Locale.ROOT, "%s %s for cache %s", name, type.getDisplayName(), cache),
              stat.getValue()));
        }
      }
    }
  }

  static class GarbageCollectorMetricsApiCaller extends MetricsApiCaller {

    GarbageCollectorMetricsApiCaller() {
      super("jvm", "gc.G1-,memory.pools.G1-", "");
    }

    /*
  "metrics":{
    "solr.jvm":{
      "gc.G1-Old-Generation.count":0,
      "gc.G1-Old-Generation.time":0,
      "gc.G1-Young-Generation.count":7,
      "gc.G1-Young-Generation.time":75,
      "memory.pools.G1-Eden-Space.committed":374341632,
      "memory.pools.G1-Eden-Space.init":113246208,
      "memory.pools.G1-Eden-Space.max":-1,
      "memory.pools.G1-Eden-Space.usage":0.025210084033613446,
      "memory.pools.G1-Eden-Space.used":9437184,
      "memory.pools.G1-Eden-Space.used-after-gc":0,
      "memory.pools.G1-Old-Gen.committed":1752170496,
      "memory.pools.G1-Old-Gen.init":2034237440,
      "memory.pools.G1-Old-Gen.max":2147483648,
      "memory.pools.G1-Old-Gen.usage":0.010585308074951172,
      "memory.pools.G1-Old-Gen.used":22731776,
      "memory.pools.G1-Old-Gen.used-after-gc":0,
      "memory.pools.G1-Survivor-Space.committed":20971520,
      "memory.pools.G1-Survivor-Space.init":0,
      "memory.pools.G1-Survivor-Space.max":-1,
      "memory.pools.G1-Survivor-Space.usage":1.0,
      "memory.pools.G1-Survivor-Space.used":20971520,
      "memory.pools.G1-Survivor-Space.used-after-gc":20971520}}}
     */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      JsonNode parent = metrics.path("solr.jvm");
      results.add(new PrometheusMetric("collection_count_g1_young_generation", PrometheusMetricType.COUNTER,
          "the number of GC invocations for G1 Young Generation", getNumber(parent, "gc.G1-Young-Generation.count")));
      results.add(new PrometheusMetric("collection_time_g1_young_generation", PrometheusMetricType.COUNTER,
          "the total number of milliseconds of time spent in gc for G1 Young Generation", getNumber(parent, "gc.G1-Young-Generation.time")));
      results.add(new PrometheusMetric("collection_count_g1_old_generation", PrometheusMetricType.COUNTER,
          "the number of GC invocations for G1 Old Generation", getNumber(parent, "gc.G1-Old-Generation.count")));
      results.add(new PrometheusMetric("collection_time_g1_old_generation", PrometheusMetricType.COUNTER,
          "the total number of milliseconds of time spent in gc for G1 Old Generation", getNumber(parent, "gc.G1-Old-Generation.time")));
      results.add(new PrometheusMetric("committed_g1_young_eden", PrometheusMetricType.GAUGE,
          "committed bytes for G1 Young Generation eden space", getNumber(parent, "memory.pools.G1-Eden-Space.committed")));
      results.add(new PrometheusMetric("used_g1_young_eden", PrometheusMetricType.GAUGE,
          "used bytes for G1 Young Generation eden space", getNumber(parent, "memory.pools.G1-Eden-Space.used")));
      results.add(new PrometheusMetric("committed_g1_young_survivor", PrometheusMetricType.GAUGE,
          "committed bytes for G1 Young Generation survivor space", getNumber(parent, "memory.pools.G1-Survivor-Space.committed")));
      results.add(new PrometheusMetric("used_g1_young_survivor", PrometheusMetricType.GAUGE,
          "used bytes for G1 Young Generation survivor space", getNumber(parent, "memory.pools.G1-Survivor-Space.used")));
      results.add(new PrometheusMetric("committed_g1_old", PrometheusMetricType.GAUGE,
          "committed bytes for G1 Old Generation", getNumber(parent, "memory.pools.G1-Old-Gen.committed")));
      results.add(new PrometheusMetric("used_g1_old", PrometheusMetricType.GAUGE,
          "used bytes for G1 Old Generation", getNumber(parent, "memory.pools.G1-Old-Gen.used")));
    }
  }

  static class MemoryMetricsApiCaller extends MetricsApiCaller {

    MemoryMetricsApiCaller() {
      super("jvm", "memory.heap.,memory.non-heap.", "");
    }

    /*
  "metrics":{
    "solr.jvm":{
      "memory.heap.committed":2147483648,
      "memory.heap.init":2147483648,
      "memory.heap.max":2147483648,
      "memory.heap.usage":0.1012108325958252,
      "memory.heap.used":217348608,
      "memory.non-heap.committed":96886784,
      "memory.non-heap.init":7667712,
      "memory.non-heap.max":-1,
      "memory.non-heap.usage":-9.313556E7,
      "memory.non-heap.used":93135560}}}
     */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      JsonNode parent = metrics.path("solr.jvm");
      results.add(new PrometheusMetric("committed_memory_heap", PrometheusMetricType.GAUGE,
          "amount of memory in bytes that is committed for the Java virtual machine to use in the heap",
          getNumber(parent, "memory.heap.committed")));
      results.add(new PrometheusMetric("used_memory_heap", PrometheusMetricType.GAUGE,
          "amount of used memory in bytes in the heap",
          getNumber(parent, "memory.heap.used")));
      results.add(new PrometheusMetric("committed_memory_nonheap", PrometheusMetricType.GAUGE,
          "amount of memory in bytes that is committed for the Java virtual machine to use in the nonheap",
          getNumber(parent, "memory.non-heap.committed")));
      results.add(new PrometheusMetric("used_memory_nonheap", PrometheusMetricType.GAUGE,
          "amount of used memory in bytes in the nonheap",
          getNumber(parent, "memory.non-heap.used")));
    }
  }

  static class OsMetricsApiCaller extends MetricsApiCaller {

    OsMetricsApiCaller() {
      super("jvm", "os.", "");
    }

    /*
  "metrics":{
    "solr.jvm":{
      "os.arch":"x86_64",
      "os.availableProcessors":12,
      "os.committedVirtualMemorySize":10852392960,
      "os.freePhysicalMemorySize":1097367552,
      "os.freeSwapSpaceSize":1423704064,
      "os.maxFileDescriptorCount":10000,
      "os.name":"Mac OS X",
      "os.openFileDescriptorCount":188,
      "os.processCpuLoad":1.391126878589777E-4,
      "os.processCpuTime":141437037000,
      "os.systemCpuLoad":0.09213820553771189,
      "os.systemLoadAverage":1.67724609375,
      "os.totalPhysicalMemorySize":17179869184,
      "os.totalSwapSpaceSize":9663676416,
      "os.version":"10.15.7"}}}
     */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      JsonNode parent = metrics.path("solr.jvm");
      results.add(new PrometheusMetric("open_file_descriptors", PrometheusMetricType.GAUGE, "the number of open file descriptors on the filesystem", getNumber(parent, "os.openFileDescriptorCount")));
      results.add(new PrometheusMetric("max_file_descriptors", PrometheusMetricType.GAUGE, "the number of max file descriptors on the filesystem", getNumber(parent, "os.maxFileDescriptorCount")));
    }
  }

  static class ThreadMetricsApiCaller extends MetricsApiCaller {

    ThreadMetricsApiCaller() {
      super("jvm", "threads.", "");
    }

    /*
  "metrics":{
    "solr.jvm":{
      "threads.blocked.count":0,
      "threads.count":2019,
      "threads.daemon.count":11,
      "threads.deadlock.count":0,
      "threads.deadlocks":[],
      "threads.new.count":0,
      "threads.runnable.count":16,
      "threads.terminated.count":0,
      "threads.timed_waiting.count":247,
      "threads.waiting.count":1756}}}
     */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      JsonNode parent = metrics.path("solr.jvm");
      results.add(new PrometheusMetric("threads_count", PrometheusMetricType.GAUGE, "number of threads", getNumber(parent, "threads.count")));
      results.add(new PrometheusMetric("threads_blocked_count", PrometheusMetricType.GAUGE, "number of blocked threads", getNumber(parent, "threads.blocked.count")));
      results.add(new PrometheusMetric("threads_deadlock_count", PrometheusMetricType.GAUGE, "number of deadlock threads", getNumber(parent, "threads.deadlock.count")));
      results.add(new PrometheusMetric("threads_runnable_count", PrometheusMetricType.GAUGE, "number of runnable threads", getNumber(parent, "threads.runnable.count")));
      results.add(new PrometheusMetric("threads_terminated_count", PrometheusMetricType.GAUGE, "number of terminated threads", getNumber(parent, "threads.terminated.count")));
      results.add(new PrometheusMetric("threads_timed_waiting_count", PrometheusMetricType.GAUGE, "number of timed waiting threads", getNumber(parent, "threads.timed_waiting.count")));
      results.add(new PrometheusMetric("threads_waiting_count", PrometheusMetricType.GAUGE, "number of waiting threads", getNumber(parent, "threads.waiting.count")));
    }
  }

  static class StatusCodeMetricsApiCaller extends MetricsApiCaller {

    StatusCodeMetricsApiCaller() {
      super("jetty", "org.eclipse.jetty.server.handler.DefaultHandler.", "count");
    }

    /*
  "metrics":{
    "solr.jetty":{
      "org.eclipse.jetty.server.handler.DefaultHandler.1xx-responses":{"count":0},
      "org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses":{"count":8816245},
      "org.eclipse.jetty.server.handler.DefaultHandler.3xx-responses":{"count":0},
      "org.eclipse.jetty.server.handler.DefaultHandler.4xx-responses":{"count":1692},
      "org.eclipse.jetty.server.handler.DefaultHandler.5xx-responses":{"count":2066},
      "org.eclipse.jetty.server.handler.DefaultHandler.active-dispatches":0,
      "org.eclipse.jetty.server.handler.DefaultHandler.active-requests":0,
      ...
     */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      JsonNode parent = metrics.path("solr.jetty");
      results.add(new PrometheusMetric("status_codes_2xx", PrometheusMetricType.COUNTER,
          "cumulative number of responses with 2xx status codes",
          getNumber(parent, "org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses", property)));
      results.add(new PrometheusMetric("status_codes_4xx", PrometheusMetricType.COUNTER,
          "cumulative number of responses with 4xx status codes",
          getNumber(parent, "org.eclipse.jetty.server.handler.DefaultHandler.4xx-responses", property)));
      results.add(new PrometheusMetric("status_codes_5xx", PrometheusMetricType.COUNTER,
          "cumulative number of responses with 5xx status codes",
          getNumber(parent, "org.eclipse.jetty.server.handler.DefaultHandler.5xx-responses", property)));
    }
  }

  // Aggregating across all the cores on the node.
  // Report only local requests, excluding forwarded requests to other nodes.
  static class CoresMetricsApiCaller extends MetricsApiCaller {

    CoresMetricsApiCaller() {
      super("core", "INDEX.merge.,QUERY./get.distrib.requestTimes,QUERY./get.local.requestTimes,QUERY./select.distrib.requestTimes,QUERY./select.local.requestTimes,UPDATE./update.distrib.requestTimes,UPDATE./update.local.requestTimes,UPDATE.updateHandler.autoCommits,UPDATE.updateHandler.commits,UPDATE.updateHandler.cumulativeDeletesBy,UPDATE.updateHandler.softAutoCommits", "count");
    }

    /*
  "metrics":{
    "solr.core.loadtest.shard1_1.replica_n8":{
      "INDEX.merge.errors":0,
      "INDEX.merge.major":{"count":0},
      "INDEX.merge.major.running":0,
      "INDEX.merge.major.running.docs":0,
      "INDEX.merge.major.running.segments":0,
      "INDEX.merge.minor":{"count":0},
      "INDEX.merge.minor.running":0,
      "INDEX.merge.minor.running.docs":0,
      "INDEX.merge.minor.running.segments":0,
      "QUERY./get.distrib.requestTimes":{"count":0},
      "QUERY./get.local.requestTimes":{"count":0},
      "QUERY./select.distrib.requestTimes":{"count":2},
      "QUERY./select.local.requestTimes":{"count":0},
      "UPDATE./update.distrib.requestTimes":{"count":0},
      "UPDATE./update.local.requestTimes":{"count":0},
      "UPDATE.updateHandler.autoCommits":0,
      "UPDATE.updateHandler.commits":{"count":14877},
      "UPDATE.updateHandler.cumulativeDeletesById":{"count":0},
      "UPDATE.updateHandler.cumulativeDeletesByQuery":{"count":0},
      "UPDATE.updateHandler.softAutoCommits":0},
    ...
   */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      long mergeMajor = 0;
      long mergeMajorDocs = 0;
      long mergeMinor = 0;
      long mergeMinorDocs = 0;
      long distribGet = 0;
      long localGet = 0;
      long distribSelect = 0;
      long localSelect = 0;
      long distribUpdate = 0;
      long localUpdate = 0;
      long hardAutoCommit = 0;
      long commit = 0;
      long deleteById = 0;
      long deleteByQuery = 0;
      long softAutoCommit = 0;
      for(JsonNode core : metrics) {
        mergeMajor += getNumber(core, "INDEX.merge.major", property).longValue();
        mergeMajorDocs += getNumber(core, "INDEX.merge.major.running.docs").longValue();
        mergeMinor += getNumber(core, "INDEX.merge.minor", property).longValue();
        mergeMinorDocs += getNumber(core, "INDEX.merge.minor.running.docs").longValue();
        distribGet += getNumber(core, "QUERY./get.distrib.requestTimes", property).longValue();
        localGet += getNumber(core, "QUERY./get.local.requestTimes", property).longValue();
        distribSelect += getNumber(core, "QUERY./select.distrib.requestTimes", property).longValue();
        localSelect += getNumber(core, "QUERY./select.local.requestTimes", property).longValue();
        distribUpdate += getNumber(core, "UPDATE./update.distrib.requestTimes", property).longValue();
        localUpdate += getNumber(core, "UPDATE./update.local.requestTimes", property).longValue();
        hardAutoCommit += getNumber(core, "UPDATE.updateHandler.autoCommits").longValue();
        commit += getNumber(core, "UPDATE.updateHandler.commits", property).longValue();
        deleteById += getNumber(core, "UPDATE.updateHandler.cumulativeDeletesById", property).longValue();
        deleteByQuery += getNumber(core, "UPDATE.updateHandler.cumulativeDeletesByQuery", property).longValue();
        softAutoCommit += getNumber(core, "UPDATE.updateHandler.softAutoCommits").longValue();
      }
      results.add(new PrometheusMetric("merges_major", PrometheusMetricType.COUNTER, "cumulative number of major merges across cores", mergeMajor));
      results.add(new PrometheusMetric("merges_major_current_docs", PrometheusMetricType.GAUGE, "current number of docs in major merges across cores", mergeMajorDocs));
      results.add(new PrometheusMetric("merges_minor", PrometheusMetricType.COUNTER, "cumulative number of minor merges across cores", mergeMinor));
      results.add(new PrometheusMetric("merges_minor_current_docs", PrometheusMetricType.GAUGE, "current number of docs in minor merges across cores", mergeMinorDocs));
      results.add(new PrometheusMetric("distributed_requests_get", PrometheusMetricType.COUNTER, "cumulative number of distributed gets across cores", distribGet));
      results.add(new PrometheusMetric("local_requests_get", PrometheusMetricType.COUNTER, "cumulative number of local gets across cores", localGet));
      results.add(new PrometheusMetric("distributed_requests_select", PrometheusMetricType.COUNTER, "cumulative number of distributed selects across cores", distribSelect));
      results.add(new PrometheusMetric("local_requests_select", PrometheusMetricType.COUNTER, "cumulative number of local selects across cores", localSelect));
      results.add(new PrometheusMetric("distributed_requests_update", PrometheusMetricType.COUNTER, "cumulative number of distributed updates across cores", distribUpdate));
      results.add(new PrometheusMetric("local_requests_update", PrometheusMetricType.COUNTER, "cumulative number of local updates across cores", localUpdate));
      results.add(new PrometheusMetric("auto_commits_hard", PrometheusMetricType.COUNTER, "cumulative number of hard auto commits across cores", hardAutoCommit));
      results.add(new PrometheusMetric("auto_commits_soft", PrometheusMetricType.COUNTER, "cumulative number of soft auto commits across cores", softAutoCommit));
      results.add(new PrometheusMetric("commits", PrometheusMetricType.COUNTER, "cumulative number of commits across cores", commit));
      results.add(new PrometheusMetric("deletes_by_id", PrometheusMetricType.COUNTER, "cumulative number of deletes by id across cores", deleteById));
      results.add(new PrometheusMetric("deletes_by_query", PrometheusMetricType.COUNTER, "cumulative number of deletes by query across cores", deleteByQuery));
    }
  }

  enum PrometheusMetricType {

    COUNTER("counter"), GAUGE("gauge");

    private final String displayName;

    PrometheusMetricType(String displayName) {
      this.displayName = displayName;
    }

    String getDisplayName() {
      return displayName;
    }
  }

  static class PrometheusMetric {

    private final String name;
    private final String type;
    private final String description;
    private final Number value;

    PrometheusMetric(String name, PrometheusMetricType type, String description, Number value) {
      this.name = normalize(name);
      this.type = type.getDisplayName();
      this.description = description;
      this.value = value;
    }

    void write(PrintWriter writer) throws IOException {
      writer.append("# HELP ").append(name).append(' ').append(description).println();
      writer.append("# TYPE ").append(name).append(' ').append(type).println();
      writer.append(name).append(' ').append(value.toString()).println();
    }

    static String normalize(String name) {
      StringBuilder builder = new StringBuilder();
      boolean modified = false;
      for(char ch : name.toCharArray()) {
        if (ch == ' ') {
          builder.append('_');
          modified = true;
        } else if (ch == '-') {
          modified = true;
        } else if (Character.isUpperCase(ch)) {
          builder.append('_').append(Character.toLowerCase(ch));
          modified = true;
        } else {
          builder.append(ch);
        }
      }
      return modified ? builder.toString() : name;
    }
  }

  static Number getNumber(JsonNode node, String... names) throws IOException {
    JsonNode originalNode = node;
    for(String name : names) {
      node = node.path(name);
    }
    if (node.isNumber()) {
      return node.numberValue();
    } else {
      LOGGER.warn("node {} does not have a number at the path {}.", originalNode, Arrays.toString(names));
      return INVALID_NUMBER;
    }
  }

  static SolrDispatchFilter getSolrDispatchFilter(HttpServletRequest request) throws IOException {
    Object value = request.getAttribute(HttpSolrCall.class.getName());
    if (!(value instanceof HttpSolrCall)) {
      throw new IOException(String.format(Locale.ROOT, "request attribute %s does not exist.", HttpSolrCall.class.getName()));
    }
    return ((HttpSolrCall) value).solrDispatchFilter;
  }

  static abstract class MetricsApiCaller {

    protected final String group;
    protected final String prefix;
    protected final String property;

    MetricsApiCaller(String group, String prefix, String property) {
      this.group = group;
      this.prefix = prefix;
      this.property = property;
    }

    // use HttpSolrCall to simulate a call to the metrics api.
    void call(AtomicInteger qTime, List<PrometheusMetric> results, HttpServletRequest originalRequest) throws IOException {
      SolrDispatchFilter filter = getSolrDispatchFilter(originalRequest);
      CoreContainer cores = filter.getCores();
      HttpServletRequest request = new MetricsApiRequest(originalRequest, group, prefix, property);
      MetricsApiResponse response = new MetricsApiResponse();
      SolrDispatchFilter.Action action = new HttpSolrCall(filter, cores, request, response, false).call();
      if (action != SolrDispatchFilter.Action.RETURN) {
        throw new IOException(String.format(Locale.ROOT, "metrics api call returns %s; expected %s.", action, SolrDispatchFilter.Action.RETURN));
      }
      handleResponse(qTime, results, response.getJsonNode());
    }

    void handleResponse(AtomicInteger qTime, List<PrometheusMetric> results, JsonNode response) throws IOException {
      JsonNode header = response.path("responseHeader");
      int status = getNumber(header, "status").intValue();
      if (status != 0) {
        throw new IOException(String.format(Locale.ROOT, "metrics api response status is %d; expected 0.", status));
      }
      qTime.addAndGet(getNumber(header, "QTime").intValue());
      handle(results, response.path("metrics"));
    }

    protected abstract void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException;
  }

  // represents a request to e.g., /solr/admin/metrics?wt=json&indent=false&compact=true&group=solr.jvm&prefix=memory.pools.
  // see ServletUtils.getPathAfterContext() for setting getServletPath() and getPathInfo().
  static class MetricsApiRequest extends HttpServletRequestWrapper {

    private final String queryString;
    private final Map<String, Object> attributes = new HashMap<>();

    MetricsApiRequest(HttpServletRequest request, String group, String prefix, String property) throws IOException {
      super(request);
      queryString = String.format(
          "wt=json&indent=false&compact=true&group=%s&prefix=%s&property=%s",
          URLEncoder.encode(group, StandardCharsets.UTF_8.name()),
          URLEncoder.encode(prefix, StandardCharsets.UTF_8.name()),
          URLEncoder.encode(property, StandardCharsets.UTF_8.name()));
    }

    @Override
    public String getServletPath() {
      return CommonParams.METRICS_PATH;
    }

    @Override
    public String getPathInfo() {
      return null;
    }

    @Override
    public String getQueryString() {
      return queryString;
    }

    @Override
    public Object getAttribute(String name) {
      Object value = attributes.get(name);
      if (value == null) {
        value = super.getAttribute(name);
      }
      return value;
    }

    @Override
    public Enumeration<String> getAttributeNames() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String name, Object value) {
      attributes.put(name, value);
    }

    @Override
    public void removeAttribute(String name) {
      throw new UnsupportedOperationException();
    }
  }

  static class ByteArrayServletOutputStream extends ServletOutputStream {

    private ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Override
    public void write(int b) throws IOException {
      output.write(b);
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setWriteListener(WriteListener writeListener) {}

    public byte[] getBytes() {
      return output.toByteArray();
    }
  };

  static class MetricsApiResponse implements HttpServletResponse {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private int statusCode = 0;
    private ByteArrayServletOutputStream body = new ByteArrayServletOutputStream();

    @Override
    public void setStatus(int code) {
      statusCode = code;
    }

    @Override
    public void setStatus(int code, String s) {
      statusCode = code;
    }

    @Override
    public void sendError(int code, String s) throws IOException {
      statusCode = code;
    }

    @Override
    public void sendError(int code) throws IOException {
      statusCode = code;
    }

    @Override
    public int getStatus() {
      return statusCode;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
      return body;
    }

    public JsonNode getJsonNode() throws IOException {
      if (statusCode != 0 && statusCode / 100 != 2) {
        throw new IOException(String.format(Locale.ROOT, "metrics api failed with status code %s.", statusCode));
      }
      return OBJECT_MAPPER.readTree(body.getBytes());
    }

    @Override
    public String encodeURL(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectURL(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeUrl(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectUrl(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendRedirect(String s) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addCookie(Cookie cookie) {}

    @Override
    public void setDateHeader(String s, long l) {}

    @Override
    public void addDateHeader(String s, long l) {}

    @Override
    public void setHeader(String s, String s1) {}

    @Override
    public void addHeader(String s, String s1) {}

    @Override
    public void setIntHeader(String s, int i) {}

    @Override
    public void addIntHeader(String s, int i) {}

    @Override
    public void setCharacterEncoding(String s) {}

    @Override
    public void setContentLength(int i) {}

    @Override
    public void setContentLengthLong(long l) {}

    @Override
    public void setContentType(String s) {}

    @Override
    public void setBufferSize(int i) {}

    @Override
    public void flushBuffer() throws IOException {}

    @Override
    public void resetBuffer() {}

    @Override
    public void reset() {}

    @Override
    public void setLocale(Locale locale) {}

    @Override
    public boolean containsHeader(String s) {
      return false;
    }

    @Override
    public String getHeader(String s) {
      return null;
    }

    @Override
    public Collection<String> getHeaders(String s) {
      return Collections.emptyList();
    }

    @Override
    public Collection<String> getHeaderNames() {
      return Collections.emptyList();
    }

    @Override
    public String getCharacterEncoding() {
      return StandardCharsets.UTF_8.name();
    }

    @Override
    public String getContentType() {
      return null;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getBufferSize() {
      return 0;
    }

    @Override
    public boolean isCommitted() {
      return false;
    }

    @Override
    public Locale getLocale() {
      return Locale.ROOT;
    }
  }
}

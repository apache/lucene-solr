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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class PrometheusMetricsServletTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static void assertMetricsApiCaller(PrometheusMetricsServlet.MetricsApiCaller caller, String json,
                                     int expectedQTime, String expectedOutput) throws Exception {
    AtomicInteger qTime = new AtomicInteger();
    List<PrometheusMetricsServlet.PrometheusMetric> metrics = new ArrayList<>();
    caller.handleResponse(qTime, metrics, OBJECT_MAPPER.readTree(json));
    Assert.assertEquals(expectedQTime, qTime.get());
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    for(PrometheusMetricsServlet.PrometheusMetric metric : metrics) {
      metric.write(printWriter);
    }
    printWriter.flush();
    String output = writer.toString();
    Assert.assertEquals(expectedOutput, output);
  }

  @Test
  public void testGarbageCollectorMetricsApiCaller() throws Exception {
    String json = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":22},\n" +
        "  \"metrics\":{\n" +
        "    \"solr.jvm\":{\n" +
        "      \"gc.G1-Old-Generation.count\":11,\n" +
        "      \"gc.G1-Old-Generation.time\":33,\n" +
        "      \"gc.G1-Young-Generation.count\":7,\n" +
        "      \"gc.G1-Young-Generation.time\":76,\n" +
        "      \"memory.pools.G1-Eden-Space.committed\":611319808,\n" +
        "      \"memory.pools.G1-Eden-Space.init\":113246208,\n" +
        "      \"memory.pools.G1-Eden-Space.max\":-1,\n" +
        "      \"memory.pools.G1-Eden-Space.usage\":0.07032590051457976,\n" +
        "      \"memory.pools.G1-Eden-Space.used\":42991616,\n" +
        "      \"memory.pools.G1-Eden-Space.used-after-gc\":0,\n" +
        "      \"memory.pools.G1-Old-Gen.committed\":1516240896,\n" +
        "      \"memory.pools.G1-Old-Gen.init\":2034237440,\n" +
        "      \"memory.pools.G1-Old-Gen.max\":2147483648,\n" +
        "      \"memory.pools.G1-Old-Gen.usage\":0.010740622878074646,\n" +
        "      \"memory.pools.G1-Old-Gen.used\":23065312,\n" +
        "      \"memory.pools.G1-Old-Gen.used-after-gc\":0,\n" +
        "      \"memory.pools.G1-Survivor-Space.committed\":19922977,\n" +
        "      \"memory.pools.G1-Survivor-Space.init\":0,\n" +
        "      \"memory.pools.G1-Survivor-Space.max\":-1,\n" +
        "      \"memory.pools.G1-Survivor-Space.usage\":1.0,\n" +
        "      \"memory.pools.G1-Survivor-Space.used\":19922944,\n" +
        "      \"memory.pools.G1-Survivor-Space.used-after-gc\":19922922}}}";
    String output = "# HELP collection_count_g1_young_generation the number of GC invocations for G1 Young Generation\n" +
        "# TYPE collection_count_g1_young_generation counter\n" +
        "collection_count_g1_young_generation 7\n" +
        "# HELP collection_time_g1_young_generation the total number of milliseconds of time spent in gc for G1 Young Generation\n" +
        "# TYPE collection_time_g1_young_generation counter\n" +
        "collection_time_g1_young_generation 76\n" +
        "# HELP collection_count_g1_old_generation the number of GC invocations for G1 Old Generation\n" +
        "# TYPE collection_count_g1_old_generation counter\n" +
        "collection_count_g1_old_generation 11\n" +
        "# HELP collection_time_g1_old_generation the total number of milliseconds of time spent in gc for G1 Old Generation\n" +
        "# TYPE collection_time_g1_old_generation counter\n" +
        "collection_time_g1_old_generation 33\n" +
        "# HELP committed_g1_young_eden committed bytes for G1 Young Generation eden space\n" +
        "# TYPE committed_g1_young_eden gauge\n" +
        "committed_g1_young_eden 611319808\n" +
        "# HELP used_g1_young_eden used bytes for G1 Young Generation eden space\n" +
        "# TYPE used_g1_young_eden gauge\n" +
        "used_g1_young_eden 42991616\n" +
        "# HELP committed_g1_young_survivor committed bytes for G1 Young Generation survivor space\n" +
        "# TYPE committed_g1_young_survivor gauge\n" +
        "committed_g1_young_survivor 19922977\n" +
        "# HELP used_g1_young_survivor used bytes for G1 Young Generation survivor space\n" +
        "# TYPE used_g1_young_survivor gauge\n" +
        "used_g1_young_survivor 19922944\n" +
        "# HELP committed_g1_old committed bytes for G1 Old Generation\n" +
        "# TYPE committed_g1_old gauge\n" +
        "committed_g1_old 1516240896\n" +
        "# HELP used_g1_old used bytes for G1 Old Generation\n" +
        "# TYPE used_g1_old gauge\n" +
        "used_g1_old 23065312\n";
    assertMetricsApiCaller(new PrometheusMetricsServlet.GarbageCollectorMetricsApiCaller(), json, 22, output);
  }

  @Test
  public void testMemoryMetricsApiCaller() throws Exception {
    String json = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":25},\n" +
        "  \"metrics\":{\n" +
        "    \"solr.jvm\":{\n" +
        "      \"memory.heap.committed\":2147483648,\n" +
        "      \"memory.heap.init\":2147483639,\n" +
        "      \"memory.heap.max\":2147483658,\n" +
        "      \"memory.heap.usage\":0.050779685378074646,\n" +
        "      \"memory.heap.used\":109048544,\n" +
        "      \"memory.non-heap.committed\":93962240,\n" +
        "      \"memory.non-heap.init\":7667712,\n" +
        "      \"memory.non-heap.max\":-1,\n" +
        "      \"memory.non-heap.usage\":-9.0404528E7,\n" +
        "      \"memory.non-heap.used\":90404528}}}";
    String output = "# HELP committed_memory_heap amount of memory in bytes that is committed for the Java virtual machine to use in the heap\n" +
        "# TYPE committed_memory_heap gauge\n" +
        "committed_memory_heap 2147483648\n" +
        "# HELP used_memory_heap amount of used memory in bytes in the heap\n" +
        "# TYPE used_memory_heap gauge\n" +
        "used_memory_heap 109048544\n" +
        "# HELP committed_memory_nonheap amount of memory in bytes that is committed for the Java virtual machine to use in the nonheap\n" +
        "# TYPE committed_memory_nonheap gauge\n" +
        "committed_memory_nonheap 93962240\n" +
        "# HELP used_memory_nonheap amount of used memory in bytes in the nonheap\n" +
        "# TYPE used_memory_nonheap gauge\n" +
        "used_memory_nonheap 90404528\n";
    assertMetricsApiCaller(new PrometheusMetricsServlet.MemoryMetricsApiCaller(), json, 25, output);
  }

  @Test
  public void testOsMetricsApiCaller() throws Exception {
    String json = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":4},\n" +
        "  \"metrics\":{\n" +
        "    \"solr.jvm\":{\n" +
        "      \"os.arch\":\"x86_64\",\n" +
        "      \"os.availableProcessors\":12,\n" +
        "      \"os.committedVirtualMemorySize\":10869137408,\n" +
        "      \"os.freePhysicalMemorySize\":1393836032,\n" +
        "      \"os.freeSwapSpaceSize\":1432354816,\n" +
        "      \"os.maxFileDescriptorCount\":10000,\n" +
        "      \"os.name\":\"Mac OS X\",\n" +
        "      \"os.openFileDescriptorCount\":188,\n" +
        "      \"os.processCpuLoad\":1.4754393799119972E-4,\n" +
        "      \"os.processCpuTime\":32529609000,\n" +
        "      \"os.systemCpuLoad\":0.4031793590008635,\n" +
        "      \"os.systemLoadAverage\":2.4921875,\n" +
        "      \"os.totalPhysicalMemorySize\":17179869184,\n" +
        "      \"os.totalSwapSpaceSize\":10737418240,\n" +
        "      \"os.version\":\"10.15.7\"}}}";
    String output = "# HELP open_file_descriptors the number of open file descriptors on the filesystem\n" +
        "# TYPE open_file_descriptors gauge\n" +
        "open_file_descriptors 188\n" +
        "# HELP max_file_descriptors the number of max file descriptors on the filesystem\n" +
        "# TYPE max_file_descriptors gauge\n" +
        "max_file_descriptors 10000\n";
    assertMetricsApiCaller(new PrometheusMetricsServlet.OsMetricsApiCaller(), json, 4, output);
  }

  @Test
  public void testThreadMetricsApiCaller() throws Exception {
    String json = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":14},\n" +
        "  \"metrics\":{\n" +
        "    \"solr.jvm\":{\n" +
        "      \"threads.blocked.count\":1,\n" +
        "      \"threads.count\":41,\n" +
        "      \"threads.daemon.count\":11,\n" +
        "      \"threads.deadlock.count\":2,\n" +
        "      \"threads.deadlocks\":[],\n" +
        "      \"threads.new.count\":3,\n" +
        "      \"threads.runnable.count\":15,\n" +
        "      \"threads.terminated.count\":4,\n" +
        "      \"threads.timed_waiting.count\":13,\n" +
        "      \"threads.waiting.count\":17}}}";
    String output = "# HELP threads_count number of threads\n" +
        "# TYPE threads_count gauge\n" +
        "threads_count 41\n" +
        "# HELP threads_blocked_count number of blocked threads\n" +
        "# TYPE threads_blocked_count gauge\n" +
        "threads_blocked_count 1\n" +
        "# HELP threads_deadlock_count number of deadlock threads\n" +
        "# TYPE threads_deadlock_count gauge\n" +
        "threads_deadlock_count 2\n" +
        "# HELP threads_runnable_count number of runnable threads\n" +
        "# TYPE threads_runnable_count gauge\n" +
        "threads_runnable_count 15\n" +
        "# HELP threads_terminated_count number of terminated threads\n" +
        "# TYPE threads_terminated_count gauge\n" +
        "threads_terminated_count 4\n" +
        "# HELP threads_timed_waiting_count number of timed waiting threads\n" +
        "# TYPE threads_timed_waiting_count gauge\n" +
        "threads_timed_waiting_count 13\n" +
        "# HELP threads_waiting_count number of waiting threads\n" +
        "# TYPE threads_waiting_count gauge\n" +
        "threads_waiting_count 17\n";
    assertMetricsApiCaller(new PrometheusMetricsServlet.ThreadMetricsApiCaller(), json, 14, output);
  }

  @Test
  public void testStatusCodeMetricsApiCaller() throws Exception {
    String json = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":9},\n" +
        "  \"metrics\":{\n" +
        "    \"solr.jetty\":{\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.1xx-responses\":{\"count\":22},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses\":{\"count\":166},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.3xx-responses\":{\"count\":33},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.4xx-responses\":{\"count\":99},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.5xx-responses\":{\"count\":77},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.active-dispatches\":0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.active-requests\":0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.active-suspended\":0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.async-dispatches\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.async-timeouts\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.connect-requests\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.delete-requests\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.dispatches\":{\"count\":166},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.get-requests\":{\"count\":8},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.head-requests\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.move-requests\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.options-requests\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.other-requests\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.percent-4xx-15m\":0.0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.percent-4xx-1m\":0.0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.percent-4xx-5m\":0.0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.percent-5xx-15m\":0.0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.percent-5xx-1m\":0.0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.percent-5xx-5m\":0.0,\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.post-requests\":{\"count\":158},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.put-requests\":{\"count\":0},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.requests\":{\"count\":166},\n" +
        "      \"org.eclipse.jetty.server.handler.DefaultHandler.trace-requests\":{\"count\":0}}}}";
    String output = "# HELP status_codes_2xx cumulative number of responses with 2xx status codes\n" +
        "# TYPE status_codes_2xx counter\n" +
        "status_codes_2xx 166\n" +
        "# HELP status_codes_4xx cumulative number of responses with 4xx status codes\n" +
        "# TYPE status_codes_4xx counter\n" +
        "status_codes_4xx 99\n" +
        "# HELP status_codes_5xx cumulative number of responses with 5xx status codes\n" +
        "# TYPE status_codes_5xx counter\n" +
        "status_codes_5xx 77\n";
    assertMetricsApiCaller(new PrometheusMetricsServlet.StatusCodeMetricsApiCaller(), json, 9, output);
  }

  @Test
  public void testCoresMetricsApiCaller() throws Exception {
    String json = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":14},\n" +
        "  \"metrics\":{\n" +
        "    \"solr.core.loadtest.shard1_1.replica_n8\":{\n" +
        "      \"INDEX.merge.errors\":1,\n" +
        "      \"INDEX.merge.major\":{\"count\":2},\n" +
        "      \"INDEX.merge.major.running\":3,\n" +
        "      \"INDEX.merge.major.running.docs\":4,\n" +
        "      \"INDEX.merge.major.running.segments\":5,\n" +
        "      \"INDEX.merge.minor\":{\"count\":6},\n" +
        "      \"INDEX.merge.minor.running\":7,\n" +
        "      \"INDEX.merge.minor.running.docs\":8,\n" +
        "      \"INDEX.merge.minor.running.segments\":9,\n" +
        "      \"QUERY./get.distrib.requestTimes\":{\"count\":35},\n" +
        "      \"QUERY./get.local.requestTimes\":{\"count\":10},\n" +
        "      \"QUERY./select.distrib.requestTimes\":{\"count\":36},\n" +
        "      \"QUERY./select.local.requestTimes\":{\"count\":11},\n" +
        "      \"UPDATE./update.distrib.requestTimes\":{\"count\":37},\n" +
        "      \"UPDATE./update.local.requestTimes\":{\"count\":12},\n" +
        "      \"UPDATE.updateHandler.autoCommits\":13,\n" +
        "      \"UPDATE.updateHandler.commits\":{\"count\":33}," +
        "      \"UPDATE.updateHandler.cumulativeDeletesById\":{\"count\":14},\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesByQuery\":{\"count\":15},\n" +
        "      \"UPDATE.updateHandler.softAutoCommits\":16},\n" +
        "    \"solr.core.testdrive.shard1.replica_n1\":{\n" +
        "      \"INDEX.merge.errors\":17,\n" +
        "      \"INDEX.merge.major\":{\"count\":18},\n" +
        "      \"INDEX.merge.major.running\":19,\n" +
        "      \"INDEX.merge.major.running.docs\":20,\n" +
        "      \"INDEX.merge.major.running.segments\":21,\n" +
        "      \"INDEX.merge.minor\":{\"count\":22},\n" +
        "      \"INDEX.merge.minor.running\":23,\n" +
        "      \"INDEX.merge.minor.running.docs\":24,\n" +
        "      \"INDEX.merge.minor.running.segments\":25,\n" +
        "      \"QUERY./get.distrib.requestTimes\":{\"count\":38},\n" +
        "      \"QUERY./get.local.requestTimes\":{\"count\":26},\n" +
        "      \"QUERY./select.distrib.requestTimes\":{\"count\":39},\n" +
        "      \"QUERY./select.local.requestTimes\":{\"count\":27},\n" +
        "      \"UPDATE./update.distrib.requestTimes\":{\"count\":40},\n" +
        "      \"UPDATE./update.local.requestTimes\":{\"count\":28},\n" +
        "      \"UPDATE.updateHandler.autoCommits\":29,\n" +
        "      \"UPDATE.updateHandler.commits\":{\"count\":34}," +
        "      \"UPDATE.updateHandler.cumulativeDeletesById\":{\"count\":30},\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesByQuery\":{\"count\":31},\n" +
        "      \"UPDATE.updateHandler.softAutoCommits\":32}}}";
    String output = "# HELP merges_major cumulative number of major merges across cores\n" +
        "# TYPE merges_major counter\n" +
        "merges_major 20\n" +
        "# HELP merges_major_current_docs current number of docs in major merges across cores\n" +
        "# TYPE merges_major_current_docs gauge\n" +
        "merges_major_current_docs 24\n" +
        "# HELP merges_minor cumulative number of minor merges across cores\n" +
        "# TYPE merges_minor counter\n" +
        "merges_minor 28\n" +
        "# HELP merges_minor_current_docs current number of docs in minor merges across cores\n" +
        "# TYPE merges_minor_current_docs gauge\n" +
        "merges_minor_current_docs 32\n" +
        "# HELP distributed_requests_get cumulative number of distributed gets across cores\n" +
        "# TYPE distributed_requests_get counter\n" +
        "distributed_requests_get 73\n" +
        "# HELP local_requests_get cumulative number of local gets across cores\n" +
        "# TYPE local_requests_get counter\n" +
        "local_requests_get 36\n" +
        "# HELP distributed_requests_select cumulative number of distributed selects across cores\n" +
        "# TYPE distributed_requests_select counter\n" +
        "distributed_requests_select 75\n" +
        "# HELP local_requests_select cumulative number of local selects across cores\n" +
        "# TYPE local_requests_select counter\n" +
        "local_requests_select 38\n" +
        "# HELP distributed_requests_update cumulative number of distributed updates across cores\n" +
        "# TYPE distributed_requests_update counter\n" +
        "distributed_requests_update 77\n" +
        "# HELP local_requests_update cumulative number of local updates across cores\n" +
        "# TYPE local_requests_update counter\n" +
        "local_requests_update 40\n" +
        "# HELP auto_commits_hard cumulative number of hard auto commits across cores\n" +
        "# TYPE auto_commits_hard counter\n" +
        "auto_commits_hard 42\n" +
        "# HELP auto_commits_soft cumulative number of soft auto commits across cores\n" +
        "# TYPE auto_commits_soft counter\n" +
        "auto_commits_soft 48\n" +
        "# HELP commits cumulative number of commits across cores\n" +
        "# TYPE commits counter\n" +
        "commits 67\n" +
        "# HELP deletes_by_id cumulative number of deletes by id across cores\n" +
        "# TYPE deletes_by_id counter\n" +
        "deletes_by_id 44\n" +
        "# HELP deletes_by_query cumulative number of deletes by query across cores\n" +
        "# TYPE deletes_by_query counter\n" +
        "deletes_by_query 46\n";
    assertMetricsApiCaller(new PrometheusMetricsServlet.CoresMetricsApiCaller(), json, 14, output);
  }

  @Test
  public void testCoresMetricsApiCallerMissingIndex() throws Exception {
    String json = "{\n" +
        "  \"responseHeader\":{\n" +
        "    \"status\":0,\n" +
        "    \"QTime\":25},\n" +
        "  \"metrics\":{\n" +
        "    \"solr.core.loadtest.shard1_1.replica_n8\":{\n" +
        "      \"QUERY./get.distrib.requestTimes\":{\"count\":29},\n" +
        "      \"QUERY./get.local.requestTimes\":{\"count\":1},\n" +
        "      \"QUERY./select.distrib.requestTimes\":{\"count\":30},\n" +
        "      \"QUERY./select.local.requestTimes\":{\"count\":2},\n" +
        "      \"UPDATE./update.distrib.requestTimes\":{\"count\":31},\n" +
        "      \"UPDATE./update.local.requestTimes\":{\"count\":3},\n" +
        "      \"UPDATE.updateHandler.autoCommits\":4,\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesById\":{\"count\":5},\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesByQuery\":{\"count\":6},\n" +
        "      \"UPDATE.updateHandler.softAutoCommits\":7},\n" +
        "    \"solr.core.testdrive.shard1.replica_n1\":{\n" +
        "      \"QUERY./get.distrib.requestTimes\":{\"count\":32},\n" +
        "      \"QUERY./get.local.requestTimes\":{\"count\":8},\n" +
        "      \"QUERY./select.distrib.requestTimes\":{\"count\":33},\n" +
        "      \"QUERY./select.local.requestTimes\":{\"count\":9},\n" +
        "      \"UPDATE./update.distrib.requestTimes\":{\"count\":34},\n" +
        "      \"UPDATE./update.local.requestTimes\":{\"count\":10},\n" +
        "      \"UPDATE.updateHandler.autoCommits\":11,\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesById\":{\"count\":12},\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesByQuery\":{\"count\":13},\n" +
        "      \"UPDATE.updateHandler.softAutoCommits\":14},\n" +
        "    \"solr.core.loadtest.shard1_0.replica_n7\":{\n" +
        "      \"QUERY./get.distrib.requestTimes\":{\"count\":35},\n" +
        "      \"QUERY./get.local.requestTimes\":{\"count\":15},\n" +
        "      \"QUERY./select.distrib.requestTimes\":{\"count\":36},\n" +
        "      \"QUERY./select.local.requestTimes\":{\"count\":16},\n" +
        "      \"UPDATE./update.distrib.requestTimes\":{\"count\":37},\n" +
        "      \"UPDATE./update.local.requestTimes\":{\"count\":17},\n" +
        "      \"UPDATE.updateHandler.autoCommits\":18,\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesById\":{\"count\":19},\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesByQuery\":{\"count\":20},\n" +
        "      \"UPDATE.updateHandler.softAutoCommits\":21},\n" +
        "    \"solr.core.local.shard1.replica_n1\":{\n" +
        "      \"QUERY./get.distrib.requestTimes\":{\"count\":38},\n" +
        "      \"QUERY./get.local.requestTimes\":{\"count\":22},\n" +
        "      \"QUERY./select.distrib.requestTimes\":{\"count\":39},\n" +
        "      \"QUERY./select.local.requestTimes\":{\"count\":23},\n" +
        "      \"UPDATE./update.distrib.requestTimes\":{\"count\":40},\n" +
        "      \"UPDATE./update.local.requestTimes\":{\"count\":24},\n" +
        "      \"UPDATE.updateHandler.autoCommits\":25,\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesById\":{\"count\":26},\n" +
        "      \"UPDATE.updateHandler.cumulativeDeletesByQuery\":{\"count\":27},\n" +
        "      \"UPDATE.updateHandler.softAutoCommits\":28}}}";
    String output = "# HELP merges_major cumulative number of major merges across cores\n" +
        "# TYPE merges_major counter\n" +
        "merges_major -4\n" +
        "# HELP merges_major_current_docs current number of docs in major merges across cores\n" +
        "# TYPE merges_major_current_docs gauge\n" +
        "merges_major_current_docs -4\n" +
        "# HELP merges_minor cumulative number of minor merges across cores\n" +
        "# TYPE merges_minor counter\n" +
        "merges_minor -4\n" +
        "# HELP merges_minor_current_docs current number of docs in minor merges across cores\n" +
        "# TYPE merges_minor_current_docs gauge\n" +
        "merges_minor_current_docs -4\n" +
        "# HELP distributed_requests_get cumulative number of distributed gets across cores\n" +
        "# TYPE distributed_requests_get counter\n" +
        "distributed_requests_get 134\n" +
        "# HELP local_requests_get cumulative number of local gets across cores\n" +
        "# TYPE local_requests_get counter\n" +
        "local_requests_get 46\n" +
        "# HELP distributed_requests_select cumulative number of distributed selects across cores\n" +
        "# TYPE distributed_requests_select counter\n" +
        "distributed_requests_select 138\n" +
        "# HELP local_requests_select cumulative number of local selects across cores\n" +
        "# TYPE local_requests_select counter\n" +
        "local_requests_select 50\n" +
        "# HELP distributed_requests_update cumulative number of distributed updates across cores\n" +
        "# TYPE distributed_requests_update counter\n" +
        "distributed_requests_update 142\n" +
        "# HELP local_requests_update cumulative number of local updates across cores\n" +
        "# TYPE local_requests_update counter\n" +
        "local_requests_update 54\n" +
        "# HELP auto_commits_hard cumulative number of hard auto commits across cores\n" +
        "# TYPE auto_commits_hard counter\n" +
        "auto_commits_hard 58\n" +
        "# HELP auto_commits_soft cumulative number of soft auto commits across cores\n" +
        "# TYPE auto_commits_soft counter\n" +
        "auto_commits_soft 70\n" +
        "# HELP commits cumulative number of commits across cores\n" +
        "# TYPE commits counter\n" +
        "commits -4\n" +
        "# HELP deletes_by_id cumulative number of deletes by id across cores\n" +
        "# TYPE deletes_by_id counter\n" +
        "deletes_by_id 62\n" +
        "# HELP deletes_by_query cumulative number of deletes by query across cores\n" +
        "# TYPE deletes_by_query counter\n" +
        "deletes_by_query 66\n";
    assertMetricsApiCaller(new PrometheusMetricsServlet.CoresMetricsApiCaller(), json, 25, output);
  }
}

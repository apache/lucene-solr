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

import com.sun.management.UnixOperatingSystemMXBean;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics.
 * This servlet exists for backwards compatibility and will be removed in favor of the native prometheus-exporter.
 */
public final class PrometheusMetricsServlet extends BaseSolrServlet {

  private enum PromType {
    counter,
    gauge
  }

  private static void writeProm(PrintWriter writer, String inName, PromType type, String desc, long value) {
    String name = inName.toLowerCase().replace(" ", "_");
    writer.printf("# HELP %s %s", name, desc);
    writer.println();
    writer.printf("# TYPE %s %s", name, type);
    writer.println();
    writer.printf("%s %d", name, value);
    writer.println();
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    Writer out = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");
    PrintWriter pw = new PrintWriter(out);
    writeStats(pw);
  }

  static void writeStats(PrintWriter writer) {
    // GC stats
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      writeProm(writer, "collection_count_" + gcBean.getName(), PromType.counter, "the number of GC invocations for " + gcBean.getName(), gcBean.getCollectionCount());
      writeProm(writer, "colleciton_time_" + gcBean.getName(), PromType.counter, "the total number of milliseconds of time spent in gc for " + gcBean.getName(), gcBean.getCollectionTime());
    }

    // Write heap memory stats
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    MemoryUsage heap = memoryBean.getHeapMemoryUsage();
    writeProm(writer, "committed_memory_heap", PromType.gauge, "amount of memory in bytes that is committed for the Java virtual machine to use in the heap", heap.getCommitted());
    writeProm(writer, "used_memory_heap", PromType.gauge, "amount of used memory in bytes in the heap", heap.getUsed());


    MemoryUsage nonHeap = memoryBean.getNonHeapMemoryUsage();
    writeProm(writer, "committed_memory_nonheap", PromType.gauge, "amount of memory in bytes that is committed for the Java virtual machine to use in the heap", nonHeap.getCommitted());
    writeProm(writer, "used_memory_nonheap", PromType.gauge, "amount of used memory in bytes in the heap", nonHeap.getUsed());

    // Write OS stats
    UnixOperatingSystemMXBean osBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    writeProm(writer, "open_file_descriptors", PromType.gauge, "the number of open file descriptors on the filesystem", osBean.getOpenFileDescriptorCount());

    writer.flush();
  }
}
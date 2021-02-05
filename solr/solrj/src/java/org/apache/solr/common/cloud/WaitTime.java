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

package org.apache.solr.common.cloud;

import java.io.IOException;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class WaitTime {
  static Map<String, Count> counts = new ConcurrentHashMap<>();
  private final String name;
  private long start;

  WaitTime(String name) {
    this.name = name;
    start = System.currentTimeMillis();

  }

  static final ThreadLocal<Stack<WaitTime>> current = ThreadLocal.withInitial(Stack::new);

  public static void start(String name) {
    WaitTime value = new WaitTime(name);
    current.get().push(value);

  }

  public static void end() {
    WaitTime val = null;
    try {
      val = current.get().pop();
      if (val == null) return;
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    Count c = counts.get(val.name);
    if (c == null) counts.put(val.name, c = new Count());
    c.count.incrementAndGet();
    long delta = System.currentTimeMillis() - val.start;
    c.cumulativeTime.addAndGet(delta);
    if (delta > c.max.get()) c.max.set(delta);
    if (delta < c.min.get()) c.min.set(delta);
  }


  public static MapWriter getCounts() {
    return ew -> counts.forEach(ew.getBiConsumer());
  }

  public static class Count implements ReflectMapWriter {
    @JsonProperty
    public final AtomicInteger count = new AtomicInteger();

    @JsonProperty
    public final AtomicLong cumulativeTime = new AtomicLong();

    @JsonProperty
    public AtomicLong max = new AtomicLong(0);

    @JsonProperty
    public AtomicLong min = new AtomicLong(Long.MAX_VALUE);

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ReflectMapWriter.super.writeMap(ew);
      int count = this.count.get();
      if (count > 0) ew.put("avg", cumulativeTime.get() / count);

    }
  }

}

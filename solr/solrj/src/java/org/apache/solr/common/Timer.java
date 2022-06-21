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

package org.apache.solr.common;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class Timer implements ReflectMapWriter {
  public String name;
  @JsonProperty
  public String currentStart;
  @JsonProperty
  public long lastTimeTaken;

  @JsonProperty
  public AtomicLong totalTimeTaken = new AtomicLong(0);

  long startL;

  @JsonProperty
  public AtomicInteger times = new AtomicInteger();

  void end() {
    lastTimeTaken = System.currentTimeMillis() - startL;
    totalTimeTaken.addAndGet(lastTimeTaken);
    startL = 0;
    this.currentStart = null;
  }



  public static class Inst implements MapWriter{
    public Map<String, Timer> timers;

    public void start(String name) {
      init();
      Timer t = timers.get(name);
      if (t == null) {
        t = new Timer();
        t.name = name;
        timers.put(t.name, t);
      }
      t.times.incrementAndGet();
      t.startL = System.currentTimeMillis();
      t.currentStart = new Date(t.startL).toString();
    }

    public Inst init() {
      if (timers == null) {
       timers = new ConcurrentHashMap<>();
      }
      return this;
    }

    public void end(String name) {
      init();
      Timer c = timers.get(name);
      if (c != null) c.end();
    }

    public void add(Inst inst) {
      Map<String, Timer> t = inst.timers;
      if(t !=null ) {
        if(timers == null) timers = new ConcurrentHashMap<>();
        t.forEach((name, timer) -> {
          Timer old = timers.computeIfAbsent(name, s -> new Timer());
          old.times.incrementAndGet();
          old.totalTimeTaken.addAndGet(timer.lastTimeTaken);
        });
      }
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("timers", timers);
    }
  }
  public static ThreadLocal<Inst> INST = new ThreadLocal<>();

  public static class TLInst implements MapWriter{
    private final List<Inst> inflight = new CopyOnWriteArrayList<>();
    private final Inst cumulative = new Inst().init();
    public static void start(String name) {
      Inst inst = INST.get();
      if(inst == null) return;
      inst.start(name);
    }

    public static void end(String name) {
      Inst inst = INST.get();
      if(inst == null) return;
      inst.end(name);
    }

    public Inst init() {
      Inst inst = INST.get();
      if(inst == null) {
        inst =  new Inst().init();
        INST.set(inst);
      }
      inflight.add(INST.get());
      return inst;
    }

    public void destroy() {
      Inst inst = INST.get();
      if(inst == null) return;
      cumulative.add(inst);
      inflight.remove(inst);
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("cumulative", cumulative);
      ew.put("inflight", inflight);
    }
  }
}

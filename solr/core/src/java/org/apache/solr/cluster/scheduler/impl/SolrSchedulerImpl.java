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
package org.apache.solr.cluster.scheduler.impl;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cluster.events.ClusterSingleton;
import org.apache.solr.cluster.scheduler.Schedule;
import org.apache.solr.cluster.scheduler.Schedulable;
import org.apache.solr.cluster.scheduler.SolrScheduler;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Scheduled executions are triggered at most with {@link #SCHEDULE_INTERVAL_SEC} interval.
 * Each registered {@link Schedulable} is processed sequentially and if its next execution time
 * is in the past its {@link Schedulable#run()} method will be invoked.
 * <p>NOTE: If the total time of execution of all registered Schedulable-s exceeds any schedule
 * interval then exact execution times will be silently missed.</p>
 */
public class SolrSchedulerImpl implements SolrScheduler, ClusterSingleton {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SCHEDULE_INTERVAL_SEC = 10;

  private final Set<Schedulable> schedulables = ConcurrentHashMap.newKeySet();
  private final Map<String, CompiledSchedule> compiledSchedules = new ConcurrentHashMap<>();

  private ScheduledExecutorService scheduler;
  private boolean running = false;

  @Override
  public void registerSchedulable(Schedulable schedulable) {
    try {
      CompiledSchedule compiledSchedule = new CompiledSchedule(schedulable.getSchedule());
      if (compiledSchedules.containsKey(compiledSchedule.name)) {
        throw new Exception("Schedule defined by " + schedulable +
            " already exists under name " + compiledSchedule.name);
      }
      compiledSchedules.put(compiledSchedule.name, compiledSchedule);
      schedulables.add(schedulable);
    } catch (Exception e) {
      log.warn("Invalid schedule for {}, skipping: {}", schedulable, e);
    }
  }

  @Override
  public void unregisterSchedulable(Schedulable schedulable) {
    compiledSchedules.remove(schedulable.getSchedule().getName());
    schedulables.remove(schedulable);
  }

  @Override
  public void start() {
    // create scheduler
    scheduler = Executors.newSingleThreadScheduledExecutor(new SolrNamedThreadFactory("cluster-event-scheduler"));
    scheduler.schedule(() -> maybeFireScheduledEvent(), SCHEDULE_INTERVAL_SEC, TimeUnit.SECONDS);
    running = true;
  }

  private void maybeFireScheduledEvent() {
    if (!running) {
      return;
    }
    if (compiledSchedules.isEmpty()) {
      return;
    }
    schedulables.forEach(schedulable -> {
      Schedule schedule = schedulable.getSchedule();
      CompiledSchedule compiledSchedule = compiledSchedules.get(schedule.getName());
      if (compiledSchedule == null) { // ???
        return;
      }
      if (compiledSchedule.shouldRun()) {
        Instant now = Instant.now();
        schedulable.run();
        compiledSchedule.setLastRunAt(now);
      }
    });
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void stop() {
    running = false;
    if (scheduler != null) {
      scheduler.shutdownNow();
      try {
        scheduler.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for the scheduler to shut down, ignoring");
      }
    }
  }
}

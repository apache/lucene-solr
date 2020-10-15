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

package org.apache.solr.cloud.autoscaling;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link ScheduledTrigger}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class ScheduledTriggerTest extends SolrCloudTestCase {

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the listener to fire on first run!");
    return true;
  };

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  // this does not appear to be a good way to test this
  public void testTrigger() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();

    Map<String, Object> properties = createTriggerProperties(new Date().toInstant().toString(), TimeZone.getDefault().getID());

    scheduledTriggerTest(container, properties);

    TimeZone timeZone = TimeZone.getDefault();
    DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE).appendPattern("['T'[HH[:mm[:ss]]]]") //brackets mean optional
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter(Locale.ROOT).withZone(timeZone.toZoneId());
    properties = createTriggerProperties(dateTimeFormatter.format(Instant.now()), timeZone.getID());
    scheduledTriggerTest(container, properties);
  }

  @Test
  public void testIgnoredEvent() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    long threeDaysAgo = new Date().getTime() - TimeUnit.DAYS.toMillis(3);
    Map<String, Object> properties = createTriggerProperties(new Date(threeDaysAgo).toInstant().toString(),
        TimeZone.getDefault().getID(),
        "+2DAYS", "+1HOUR");
    try (ScheduledTrigger scheduledTrigger = new ScheduledTrigger("sched1")) {
      scheduledTrigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), properties);
      scheduledTrigger.init();
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      scheduledTrigger.setProcessor(event -> {
        eventRef.set(event);
        return true;
      });
      scheduledTrigger.run();
      assertTrue(eventRef.get().isIgnored());
    }
  }

  private void scheduledTriggerTest(CoreContainer container, Map<String, Object> properties) throws Exception {
    try (ScheduledTrigger scheduledTrigger = new ScheduledTrigger("sched1")) {
      scheduledTrigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), properties);
      scheduledTrigger.init();
      scheduledTrigger.setProcessor(noFirstRunProcessor);
      scheduledTrigger.run();
      final List<Long> eventTimes = Collections.synchronizedList(new ArrayList<>());
      scheduledTrigger.setProcessor(event -> {
        eventTimes.add(event.getEventTime());
        return true;
      });
      for (int i = 0; i < 3; i++) {
        Thread.sleep(3000);
        scheduledTrigger.run();
      }
      assertEquals(3, eventTimes.size());
    }
  }

  private Map<String, Object> createTriggerProperties(String startTime, String timeZone) {
    return createTriggerProperties(startTime, timeZone, "+3SECOND", "+2SECOND");
  }

  private Map<String, Object> createTriggerProperties(String startTime, String timeZone, String every, String graceTime) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("graceDuration", graceTime);
    properties.put("startTime", startTime);
    properties.put("timeZone", timeZone);
    properties.put("every", every);
    List<Map<String, String>> actions = new ArrayList<>(3);
    Map<String, String> map = new HashMap<>(2);
    map.put("name", "compute_plan");
    map.put("class", "solr.ComputePlanAction");
    actions.add(map);
    map = new HashMap<>(2);
    map.put("name", "execute_plan");
    map.put("class", "solr.ExecutePlanAction");
    actions.add(map);
    properties.put("actions", actions);
    return properties;
  }
}

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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;
import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.timeSource;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class TriggerSetPropertiesIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    // disable .scheduled_maintenance
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {'name' : '.scheduled_maintenance'}" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    SolrClient solrClient = cluster.getSolrClient();
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
  }

  private static CountDownLatch getTriggerFiredLatch() {
    return triggerFiredLatch;
  }

  public void testSetProperties() throws Exception {
    JettySolrRunner runner = cluster.getJettySolrRunner(0);
    SolrResourceLoader resourceLoader = runner.getCoreContainer().getResourceLoader();
    SolrCloudManager solrCloudManager = runner.getCoreContainer().getZkController().getSolrCloudManager();
    AtomicLong diff = new AtomicLong(0);
    triggerFiredLatch = new CountDownLatch(2); // have the trigger run twice to capture time difference
    try (ScheduledTriggers scheduledTriggers = new ScheduledTriggers(resourceLoader, solrCloudManager)) {
      AutoScalingConfig config = new AutoScalingConfig(Collections.emptyMap());
      scheduledTriggers.setAutoScalingConfig(config);
      AutoScaling.Trigger t = new TriggerBase(TriggerEventType.NODELOST, "x") {
        @Override
        protected Map<String, Object> getState() {
          return Collections.singletonMap("x", "y");
        }

        @Override
        protected void setState(Map<String, Object> state) {

        }

        @Override
        public void restoreState(AutoScaling.Trigger old) {

        }

        @Override
        public void run() {
          if (getTriggerFiredLatch().getCount() == 0) return;
          long l = diff.get();
          diff.set(timeSource.getTimeNs() - l);
          getTriggerFiredLatch().countDown();
        }
      };
      t.configure(runner.getCoreContainer().getResourceLoader(), runner.getCoreContainer().getZkController().getSolrCloudManager(), Collections.emptyMap());
      scheduledTriggers.add(t);

      assertTrue(getTriggerFiredLatch().await(4, TimeUnit.SECONDS));
      assertTrue(diff.get() - TimeUnit.SECONDS.toNanos(ScheduledTriggers.DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS) >= 0);

      // change schedule delay
      config = config.withProperties(Collections.singletonMap(AutoScalingParams.TRIGGER_SCHEDULE_DELAY_SECONDS, 4));
      scheduledTriggers.setAutoScalingConfig(config);
      triggerFiredLatch = new CountDownLatch(2);
      assertTrue("Timed out waiting for latch to fire", getTriggerFiredLatch().await(10, TimeUnit.SECONDS));
      assertTrue(diff.get() - TimeUnit.SECONDS.toNanos(4) >= 0);

      // reset with default properties
      scheduledTriggers.remove("x"); // remove the old trigger
      config = config.withProperties(ScheduledTriggers.DEFAULT_PROPERTIES);
      scheduledTriggers.setAutoScalingConfig(config);

      // test core thread count
      List<AutoScaling.Trigger> triggerList = new ArrayList<>();
      final Set<String> threadNames = Collections.synchronizedSet(new HashSet<>());
      final Set<String> triggerNames = Collections.synchronizedSet(new HashSet<>());
      triggerFiredLatch = new CountDownLatch(8);
      for (int i = 0; i < 8; i++) {
        AutoScaling.Trigger trigger = new MockTrigger(TriggerEventType.NODELOST, "x" + i)  {
          @Override
          public void run() {
            try {
              // If core pool size is increased then new threads won't be started if existing threads
              // aren't busy with tasks. So we make this thread wait longer than necessary
              // so that the pool is forced to start threads for other triggers
              Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
            if (triggerNames.add(getName())) {
              getTriggerFiredLatch().countDown();
              threadNames.add(Thread.currentThread().getName());
            }
          }
        };
        trigger.configure(resourceLoader, solrCloudManager, Collections.emptyMap());
        triggerList.add(trigger);
        scheduledTriggers.add(trigger);
      }
      assertTrue("Timed out waiting for latch to fire", getTriggerFiredLatch().await(20, TimeUnit.SECONDS));
      assertEquals("Expected 8 triggers but found: " + triggerNames, 8, triggerNames.size());
      assertEquals("Expected " + ScheduledTriggers.DEFAULT_TRIGGER_CORE_POOL_SIZE
              + " threads but found: " + threadNames,
          ScheduledTriggers.DEFAULT_TRIGGER_CORE_POOL_SIZE, threadNames.size());

      // change core pool size
      config = config.withProperties(Collections.singletonMap(AutoScalingParams.TRIGGER_CORE_POOL_SIZE, 6));
      scheduledTriggers.setAutoScalingConfig(config);
      triggerFiredLatch = new CountDownLatch(8);
      threadNames.clear();
      triggerNames.clear();
      assertTrue(getTriggerFiredLatch().await(20, TimeUnit.SECONDS));
      assertEquals("Expected 8 triggers but found: " + triggerNames, 8, triggerNames.size());
      assertEquals("Expected 6 threads but found: " + threadNames, 6, threadNames.size());

      // reset
      for (int i = 0; i < 8; i++) {
        scheduledTriggers.remove(triggerList.get(i).getName());
      }
    }
  }

  public static class MockTrigger extends TriggerBase {

    public MockTrigger(TriggerEventType eventType, String name) {
      super(eventType, name);
    }

    @Override
    protected Map<String, Object> getState() {
      return Collections.emptyMap();
    }

    @Override
    protected void setState(Map<String, Object> state) {

    }

    @Override
    public void restoreState(AutoScaling.Trigger old) {

    }

    @Override
    public void run() {

    }
  }
}

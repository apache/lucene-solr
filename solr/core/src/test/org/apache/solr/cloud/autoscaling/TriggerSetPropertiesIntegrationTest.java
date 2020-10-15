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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class TriggerSetPropertiesIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    
    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
  }

  /** 
   * Test that we can add/remove triggers to a scheduler, and change the config on the fly, and still get
   * expected behavior 
   */
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void testSetProperties() throws Exception {
    final JettySolrRunner runner = cluster.getJettySolrRunner(0);
    final SolrResourceLoader resourceLoader = runner.getCoreContainer().getResourceLoader();
    final SolrCloudManager solrCloudManager = runner.getCoreContainer().getZkController().getSolrCloudManager();
    
    try (ScheduledTriggers scheduledTriggers = new ScheduledTriggers(resourceLoader, solrCloudManager)) {
      AutoScalingConfig config = new AutoScalingConfig(Collections.emptyMap());
      scheduledTriggers.setAutoScalingConfig(config);

      // Setup a trigger that records the timestamp of each time it was run
      // we only need 2 timestamps for the test, so limit the queue and make the trigger a No-Op if full
      final BlockingQueue<Long> timestamps = new ArrayBlockingQueue<Long>(2);
      final AutoScaling.Trigger t1 = new MockTrigger(TriggerEventType.NODELOST, "mock-timestamper") {
        @Override
        public void run() {
          if (log.isInfoEnabled()) {
            log.info("Running {} in {}", this.getName(), Thread.currentThread().getName());
          }
          timestamps.offer(solrCloudManager.getTimeSource().getTimeNs());
        }
      };

      if (log.isInfoEnabled()) {
        log.info("Configuring simple scheduler and adding trigger: {}", t1.getName());
      }
      t1.configure(resourceLoader, solrCloudManager, Collections.emptyMap());
      scheduledTriggers.add(t1);

      waitForAndDiffTimestamps("conf(default delay)",
                               ScheduledTriggers.DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS, TimeUnit.SECONDS,
                               timestamps);

      if (log.isInfoEnabled()) {
        log.info("Reconfiguing scheduler to use 4s delay and clearing queue for trigger: {}", t1.getName());
      }
      config = config.withProperties(Collections.singletonMap
                                     (AutoScalingParams.TRIGGER_SCHEDULE_DELAY_SECONDS, 4));
      scheduledTriggers.setAutoScalingConfig(config);
      timestamps.clear();

      waitForAndDiffTimestamps("conf(four sec delay)", 
                               4, TimeUnit.SECONDS, 
                               timestamps);

      if (log.isInfoEnabled()) {
        log.info("Removing trigger: {}", t1.getName());
      }
      scheduledTriggers.remove(t1.getName());
      
      log.info("Reconfiguing scheduler to use default props");
      config = config.withProperties(ScheduledTriggers.DEFAULT_PROPERTIES);
      scheduledTriggers.setAutoScalingConfig(config);

                 
      assertTrue("Test sanity check, need default thread pool to be at least 3 so we can" +
                 "test lowering it by 2", ScheduledTriggers.DEFAULT_TRIGGER_CORE_POOL_SIZE >= 3);
      final int numTriggers = ScheduledTriggers.DEFAULT_TRIGGER_CORE_POOL_SIZE;
      final int reducedThreadPoolSize = numTriggers - 2;
      
      // Setup X instances of a trigger that:
      //  - records it's name as being run
      //    - skipping all remaining execution if it's name has already been recorded
      //  - records the name of the thread that ran it
      //  - blocks on a cyclic barrier untill at Y instances have run (to hog a thread)
      // ...to test that the scheduler will add new threads as needed, up to the configured limit
      //
      // NOTE: the reason we need X unique instances is because the scheduler won't "re-run" a single
      // trigger while a previouss "run" is still in process
      final List<AutoScaling.Trigger> triggerList = new ArrayList<>(numTriggers);
      
      // Use a cyclic barrier gated by an atomic ref so we can swap it out later
      final AtomicReference<CyclicBarrier> latch = new AtomicReference<>(new CyclicBarrier(numTriggers));
      
      // variables for tracking state as we go
      // NOTE: all read/write must be gated by synchronizing on the barrier (ref),
      //       so we we can ensure we are reading a consistent view
      final Set<String> threadNames = Collections.synchronizedSet(new LinkedHashSet<>());
      final Set<String> triggerNames = Collections.synchronizedSet(new LinkedHashSet<>());
      final AtomicLong fails = new AtomicLong(0);

      // Use a semaphore to track when each trigger *finishes* so our test thread
      // can know when to check & clear the tracking state
      final Semaphore completionSemaphore = new Semaphore(numTriggers);
      
      for (int i = 0; i < numTriggers; i++) {
        AutoScaling.Trigger trigger = new MockTrigger(TriggerEventType.NODELOST,
                                                      "mock-blocking-trigger-" + i)  {
          @Override
          public void run() {
            if (log.isInfoEnabled()) {
              log.info("Running {} in {}", this.getName(), Thread.currentThread().getName());
            }
            CyclicBarrier barrier = null;
            synchronized (latch) {
              if (triggerNames.add(this.getName())) {
                if (log.isInfoEnabled()) {
                  log.info("{}: No-Op since we've already recorded a run", this.getName());
                }
                return;
              }
              threadNames.add(Thread.currentThread().getName());
              barrier = latch.get();
            }
            
            try {
              if (log.isInfoEnabled()) {
                log.info("{}: waiting on barrier to hog a thread", this.getName());
              }
              barrier.await(30, TimeUnit.SECONDS);
              completionSemaphore.release();
            } catch (Exception e) {
              fails.incrementAndGet();
              log.error("{} : failure waiting on cyclic barrier: {}", this.getName(), e, e);
            }
          }
        };

        trigger.configure(resourceLoader, solrCloudManager, Collections.emptyMap());
        triggerList.add(trigger);
        completionSemaphore.acquire();
        if (log.isInfoEnabled()) {
          log.info("Adding trigger {} to scheduler", trigger.getName());
        }
        scheduledTriggers.add(trigger);
      }
      
      log.info("Waiting on semaphore for all triggers to signal completion...");
      assertTrue("Timed out waiting for semaphore count to be released",
                 completionSemaphore.tryAcquire(numTriggers, 60, TimeUnit.SECONDS));
                                                
      synchronized (latch) {
        assertEquals("Unexpected number of trigger names found: " + triggerNames.toString(),
                     numTriggers, triggerNames.size());
        assertEquals("Unexpected number of thread ames found: " + threadNames.toString(),
                     numTriggers, threadNames.size());
        assertEquals("Unexpected number of trigger fails recorded, check logs?",
                     0, fails.get());

        // before releasing the latch, clear the state and update our config to use a lower number of threads
        log.info("Updating scheduler config to use {} threads", reducedThreadPoolSize);
        config = config.withProperties(Collections.singletonMap(AutoScalingParams.TRIGGER_CORE_POOL_SIZE,
                                                                reducedThreadPoolSize));
        scheduledTriggers.setAutoScalingConfig(config);

        log.info("Updating cyclic barrier and clearing test state so triggers will 'run' again");
        latch.set(new CyclicBarrier(reducedThreadPoolSize));
        threadNames.clear();
        triggerNames.clear();
      }
      
      log.info("Waiting on semaphore for all triggers to signal completion...");
      assertTrue("Timed out waiting for semaphore count to be released",
                 completionSemaphore.tryAcquire(numTriggers, 60, TimeUnit.SECONDS));
      
      synchronized (latch) {
        assertEquals("Unexpected number of trigger names found: " + triggerNames.toString(),
                     numTriggers, triggerNames.size());
        assertEquals("Unexpected number of thread names found: " + threadNames.toString(),
                    reducedThreadPoolSize, threadNames.size());
        assertEquals("Unexpected number of trigger fails recorded, check logs?",
                     0, fails.get());
      }
    }
  }



      
  private static final void waitForAndDiffTimestamps(final String label,
                                                     final long minExpectedDelta,
                                                     final TimeUnit minExpectedDeltaUnit,
                                                     final BlockingQueue<Long> timestamps) {
    try {
      log.info("{}: Waiting for 2 timestamps to be recorded", label);
      Long firstTs = timestamps.poll(minExpectedDelta * 3, minExpectedDeltaUnit);
      assertNotNull(label + ": Couldn't get first timestampe after max allowed polling", firstTs);
      Long secondTs = timestamps.poll(minExpectedDelta * 3, minExpectedDeltaUnit);
      assertNotNull(label + ": Couldn't get second timestampe after max allowed polling", secondTs);
      
      final long deltaInNanos = secondTs - firstTs;
      final long minExpectedDeltaInNanos = minExpectedDeltaUnit.toNanos(minExpectedDelta);
      assertTrue(label + ": Delta between timestamps ("+secondTs+"ns - "+firstTs+"ns = "+deltaInNanos+"ns) is not " +
                 "at least as much as min expected delay: " + minExpectedDeltaInNanos + "ns",
                 deltaInNanos >= minExpectedDeltaInNanos);
    } catch (InterruptedException e) {
      log.error("{}: interupted", label, e);
      fail(label + ": interupted:" + e.toString());
    }
  }
  
  private static abstract class MockTrigger extends TriggerBase {

    public MockTrigger(TriggerEventType eventType, String name) {
      super(eventType, name);
    }

    @Override
    protected Map<String, Object> getState() { 
      return Collections.emptyMap();
    }

    @Override
    protected void setState(Map<String, Object> state) {  /* No-Op */ }

    @Override
    public void restoreState(AutoScaling.Trigger old) { /* No-Op */ }
  }
}

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
package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.SolrTestCaseJ4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.BeforeClass;

/**
 * This test is currently flawed because it only ensures the 'test-*' threads don't exit before the asserts,
 * it doesn't adequately ensure they 'start' before the asserts.
 * Fixing the ownershipt should be possible using latches, but fixing the '*-blocked' threads may not be possible
 * w/o polling
 */
public class ThreadDumpHandlerTest extends SolrTestCaseJ4 {
   private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
 
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public void testMonitor() throws Exception {
    doTestMonitor(false);
  }

  /* checking for the BLOCKED thread requires some polling, so only do it nightly */
  @Nightly
  public void testMonitorBlocked() throws Exception {
    doTestMonitor(true);
  }
  
  public void doTestMonitor(final boolean checkBlockedThreadViaPolling) throws Exception {
    assumeTrue("monitor checking not supported on this JVM",
               ManagementFactory.getThreadMXBean().isObjectMonitorUsageSupported());
    
    /** unique class name to show up as a lock class name in output */
    final class TestMonitorStruct { /* empty */ }
    
    final List<String> failures = new ArrayList<>();
    final CountDownLatch lockIsHeldLatch = new CountDownLatch(1);
    final CountDownLatch doneWithTestLatch = new CountDownLatch(1);
    final Object monitor = new TestMonitorStruct();
    final Thread ownerT = new Thread(() -> {
        synchronized (monitor) {
          lockIsHeldLatch.countDown();
          log.info("monitor ownerT waiting for doneWithTestLatch to release me...");
          try {
            if ( ! doneWithTestLatch.await(30, TimeUnit.SECONDS ) ){
              failures.add("ownerT: never saw doneWithTestLatch released");
            }
          } catch (InterruptedException ie) {
            failures.add("ownerT: " + ie.toString());
          }
        }
      }, "test-thread-monitor-owner");

    // only used if checkBlockedThreadViaPolling
    // don't start until after lockIsHeldLatch fires    
    final Thread blockedT = new Thread(() -> {
        log.info("blockedT waiting for monitor...");
        synchronized (monitor) {
          log.info("monitor now unblocked");
        }
      }, "test-thread-monitor-blocked");
    
    try {
      ownerT.start();
      if ( ! lockIsHeldLatch.await(30, TimeUnit.SECONDS ) ){
        failures.add("never saw lockIsHeldLatch released");
        return;
      }
      assertQ(req("qt", "/admin/threads", "indent", "true")
              // monitor owner 'ownerT'
              // (which *MAY* also be waiting on doneWithTestLatch, but may not have reached that line yet)
              , "//lst[@name='thread'][str[@name='name'][.='test-thread-monitor-owner']]"
              + "                     [arr[@name='monitors-locked']/str[contains(.,'TestMonitorStruct')]]"
              );

      if (checkBlockedThreadViaPolling) {
        log.info("Also checking with blockedT thread setup via polling...");
        try {
          blockedT.setPriority(Thread.MAX_PRIORITY);
        } catch (Exception e) {
          log.warn("Couldn't set blockedT priority", e);
        }
        blockedT.start();
        // there is no way to "await" on the situation of the 'blockedT' thread actually reaching the
        // "synchronized" block and becoming BLOCKED ... we just have to Poll for it...
        for (int i = 0; i < 500 && (! Thread.State.BLOCKED.equals(blockedT.getState())); i++) {
          Thread.sleep(10); // 10ms at a time, at most 5 sec total
        }
        if (Thread.State.BLOCKED.equals(blockedT.getState())) {
          assertQ(req("qt", "/admin/threads", "indent", "true")
                  // same monitor owner 'ownerT'
                  , "//lst[@name='thread'][str[@name='name'][.='test-thread-monitor-owner']]"
                  + "                     [arr[@name='monitors-locked']/str[contains(.,'TestMonitorStruct')]]"
                  // blocked thread 'blockedT', waiting on the monitor
                  , "//lst[@name='thread'][str[@name='name'][.='test-thread-monitor-blocked']]"
                  + "                     [str[@name='state'][.='BLOCKED']]"
                  + "                     [lst[@name='lock-waiting'][lst[@name='owner']/str[.='test-thread-monitor-owner']]]"
                  );
        }
      }
    } finally {
      lockIsHeldLatch.countDown();
      doneWithTestLatch.countDown();
      ownerT.join(1000);
      assertFalse("ownerT is still alive", ownerT.isAlive());
      blockedT.join(1000);
      assertFalse("blockedT is still alive", blockedT.isAlive());
    }
  }

  
  public void testOwnableSync() throws Exception {
    doTestOwnableSync(false);
  }
  
  /* checking for the WAITING thread requires some polling, so only do it nightly */
  @Nightly
  public void testOwnableSyncWaiting() throws Exception {
    doTestOwnableSync(true);
  }
  
  public void doTestOwnableSync(final boolean checkWaitingThreadViaPolling) throws Exception {
    assumeTrue("ownable sync checking not supported on this JVM",
               ManagementFactory.getThreadMXBean().isSynchronizerUsageSupported());

    /** unique class name to show up as a lock class name in output */
    final class TestReentrantLockStruct extends ReentrantLock { /* empty */ }
    
    final List<String> failures = new ArrayList<>();
    final CountDownLatch lockIsHeldLatch = new CountDownLatch(1);
    final CountDownLatch doneWithTestLatch = new CountDownLatch(1);
    final ReentrantLock lock = new ReentrantLock();
    final Thread ownerT = new Thread(() -> {
        lock.lock();
        try {
          lockIsHeldLatch.countDown();
          log.info("lock ownerT waiting for doneWithTestLatch to release me...");
          try {
            if ( ! doneWithTestLatch.await(5, TimeUnit.SECONDS ) ){
              failures.add("ownerT: never saw doneWithTestLatch release");
            }
          } catch (InterruptedException ie) {
            failures.add("ownerT: " + ie.toString());
          }
        } finally {
          lock.unlock();
        }
      }, "test-thread-sync-lock-owner");

    // only used if checkWaitingThreadViaPolling
    // don't start until after lockIsHeldLatch fires
    final Thread blockedT = new Thread(() -> { 
        log.info("blockedT waiting for lock...");
        lock.lock();
        try {
          log.info("lock now unblocked");
        } finally {
          lock.unlock();
        }
      }, "test-thread-sync-lock-blocked");
    try {
      ownerT.start();
      if ( ! lockIsHeldLatch.await(30, TimeUnit.SECONDS ) ){
        failures.add("never saw lockIsHeldLatch released");
        return;
      }
      assertQ(req("qt", "/admin/threads", "indent", "true")
              // lock owner 'ownerT'
              // (which *MAY* also be waiting on doneWithTestLatch, but may not have reached that line yet)
              , "//lst[@name='thread'][str[@name='name'][.='test-thread-sync-lock-owner']]"
              + "                     [arr[@name='synchronizers-locked']/str[contains(.,'ReentrantLock')]]"
              );
      
      if (checkWaitingThreadViaPolling) {
        log.info("Also checking with blockedT thread setup via polling...");
        try {
          blockedT.setPriority(Thread.MAX_PRIORITY);
        } catch (Exception e) {
          log.warn("Couldn't set blockedT priority", e);
        }
        blockedT.start();
        // there is no way to "await" on the situation of the 'blockedT' thread actually reaches the lock()
        // call and WAITING in the queue ... we just have to Poll for it...
        for (int i = 0; i < 500 && (! lock.hasQueuedThread(blockedT)); i++) {
          Thread.sleep(10); // 10ms at a time, at most 5 sec total
        }
        if (lock.hasQueuedThread(blockedT)) {
          assertQ(req("qt", "/admin/threads", "indent", "true")
                  // same lock owner 'ownerT'
                  , "//lst[@name='thread'][str[@name='name'][.='test-thread-sync-lock-owner']]"
                  + "                     [arr[@name='synchronizers-locked']/str[contains(.,'ReentrantLock')]]"
                  // blocked thread 'blockedT', waiting on the lock
                  , "//lst[@name='thread'][str[@name='name'][.='test-thread-sync-lock-blocked']]"
                  + "                     [str[@name='state'][.='WAITING']]"
                  + "                     [lst[@name='lock-waiting'][lst[@name='owner']/str[.='test-thread-sync-lock-owner']]]"
                  );
          
        }
      }
    } finally {
      lockIsHeldLatch.countDown();
      doneWithTestLatch.countDown();
      ownerT.join(1000);
      assertFalse("ownerT is still alive", ownerT.isAlive());
      blockedT.join(1000);
      assertFalse("blockedT is still alive", blockedT.isAlive());
    }
  }
  
}

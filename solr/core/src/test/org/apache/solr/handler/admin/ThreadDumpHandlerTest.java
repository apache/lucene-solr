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
@SolrTestCaseJ4.AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-14635")
public class ThreadDumpHandlerTest extends SolrTestCaseJ4 {
   private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
 
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public void testMonitor() throws Exception {
    assumeTrue("monitor checking not supported on this JVM",
               ManagementFactory.getThreadMXBean().isObjectMonitorUsageSupported());
    
    /** unique class name to show up as a lock class name in output */
    final class TestMonitorStruct { /* empty */ }
    
    final List<String> failures = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(1);
    final Object monitor = new TestMonitorStruct();
    final Thread owner = new Thread(() -> {
        synchronized (monitor) {
          log.info("monitor owner waiting for latch to release me...");
          try {
            if ( ! latch.await(5, TimeUnit.SECONDS ) ){
              failures.add("owner: never saw latch release");
            }
          } catch (InterruptedException ie) {
            failures.add("owner: " + ie.toString());
          }
        }
      }, "test-thread-monitor-owner");
    final Thread blocked = new Thread(() -> {
        log.info("blocked waiting for monitor...");
        synchronized (monitor) {
          log.info("monitor now unblocked");
        }
      }, "test-thread-monitor-blocked");
    try {
      owner.start();
      blocked.start();
      
      assertQ(req("qt", "/admin/threads", "indent", "true")
              // monitor owner thread (which is also currently waiting on CountDownLatch)
              , "//lst[@name='thread'][str[@name='name'][.='test-thread-monitor-owner']]"
              + "                     [lst[@name='lock-waiting'][null[@name='owner']]]" // latch
              + "                     [arr[@name='monitors-locked']/str[contains(.,'TestMonitorStruct')]]"
              // blocked thread, waiting on the monitor
              , "//lst[@name='thread'][str[@name='name'][.='test-thread-monitor-blocked']]"
              + "                     [lst[@name='lock-waiting'][lst[@name='owner']/str[.='test-thread-monitor-owner']]]"
              );
      
    } finally {
      latch.countDown();
      owner.join(1000);
      assertFalse("owner is still alive", owner.isAlive());
      blocked.join(1000);
      assertFalse("blocked is still alive", blocked.isAlive());
    }
  }

  
  public void testOwnableSync() throws Exception {
    assumeTrue("ownable sync checking not supported on this JVM",
               ManagementFactory.getThreadMXBean().isSynchronizerUsageSupported());

    /** unique class name to show up as a lock class name in output */
    final class TestReentrantLockStruct extends ReentrantLock { /* empty */ }
    
    final List<String> failures = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(1);
    final ReentrantLock lock = new ReentrantLock();
    final Thread owner = new Thread(() -> {
        lock.lock();
        try {
          log.info("lock owner waiting for latch to release me...");
          try {
            if ( ! latch.await(5, TimeUnit.SECONDS ) ){
              failures.add("owner: never saw latch release");
            }
          } catch (InterruptedException ie) {
            failures.add("owner: " + ie.toString());
          }
        } finally {
          lock.unlock();
        }
      }, "test-thread-sync-lock-owner");
    final Thread blocked = new Thread(() -> {
        log.info("blocked waiting for lock...");
        lock.lock();
        try {
          log.info("lock now unblocked");
        } finally {
          lock.unlock();
        }
      }, "test-thread-sync-lock-blocked");
    try {
      owner.start();
      blocked.start();
      
      assertQ(req("qt", "/admin/threads", "indent", "true")
              // lock owner thread (which is also currently waiting on CountDownLatch)
              , "//lst[@name='thread'][str[@name='name'][.='test-thread-sync-lock-owner']]"
              + "                     [lst[@name='lock-waiting'][null[@name='owner']]]" // latch
              + "                     [arr[@name='synchronizers-locked']/str[contains(.,'ReentrantLock')]]"
              // blocked thread, waiting on the lock
              , "//lst[@name='thread'][str[@name='name'][.='test-thread-sync-lock-blocked']]"
              + "                     [lst[@name='lock-waiting'][lst[@name='owner']/str[.='test-thread-sync-lock-owner']]]"
              );
      
    } finally {
      latch.countDown();
      owner.join(1000);
      assertFalse("owner is still alive", owner.isAlive());
      blocked.join(1000);
      assertFalse("blocked is still alive", blocked.isAlive());
    }
  }
  
}

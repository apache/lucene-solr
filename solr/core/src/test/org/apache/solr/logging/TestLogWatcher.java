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
package org.apache.solr.logging;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestLogWatcher extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private LogWatcherConfig config;

  @Before
  public void before() {
    config = new LogWatcherConfig(true, null, "INFO", 1);
  }

  // Create several log watchers and ensure that new messages go to the new watcher.
  @Test
  public void testLog4jWatcher() {
    LogWatcher watcher;
    int lim = random().nextInt(3) + 2;
    for (int idx = 0; idx < lim; ++idx) {
      String msg = "This is a test message: " + idx;
      watcher = LogWatcher.newRegisteredLogWatcher(config, null);

      // First ensure there's nothing in the new watcher.

      // Every time you put a message in the queue, you wait for it to come out _before_ creating
      // a new watcher so it should be fine.
      if (looper(watcher, null) == false) {
        fail("There should be no messages when a new watcher finally gets registered! In loop: " + idx);
      }

      // Now log a message and ensure that the new watcher sees it.
      log.warn(msg);

      // Loop to give the logger time to process the async message and notify the new watcher.
      if (looper(watcher, msg) == false) {
        fail("Should have found message " + msg + ". In loop: " + idx);
      }
    }
  }
  private boolean looper(LogWatcher watcher, String msg) {
    // In local testing this loop usually succeeds 1-2 tries.
    boolean success = false;
    boolean testingNew = msg == null;
    for (int msgIdx = 0; msgIdx < 100 && success == false; ++msgIdx) {
      if (testingNew) { // check that there are no entries registered for the watcher
        success = watcher.getLastEvent() == -1;
      } else { // check that the expected message is there.
        // Returns an empty (but non-null) list even if there are no messages yet.
        SolrDocumentList events = watcher.getHistory(-1, new AtomicBoolean());
        for (SolrDocument doc : events) {
          if (doc.get("message").equals(msg)) {
            success = true;
          }
        }
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        ;
      }
    }
    return success;
  }
}

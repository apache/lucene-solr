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
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestLogWatcher extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private LogWatcherConfig config;

  @Before
  public void before() {
    config = new LogWatcherConfig(true, null, "INFO", 1);
  }

  // Create several log watchers and ensure that new messages go to the new watcher.
  // NOTE: Since the superclass logs messages, it's possible that there are one or more
  //       messages in the queue at the start, especially with asynch logging.
  //       All we really care about is that new watchers get the new messages, so test for that
  //       explicitly. See SOLR-12732.
  @Test
  public void testLog4jWatcher() throws InterruptedException {
    @SuppressWarnings({"rawtypes"})
    LogWatcher watcher = null;
    int lim = random().nextInt(3) + 2;
    // Every time through this loop, insure that, of all the test messages that have been logged, only the current
    // test message is present. NOTE: there may be log messages from the superclass the first time around.
    List<String> oldMessages = new ArrayList<>(lim);
    for (int idx = 0; idx < lim; ++idx) {

      watcher = LogWatcher.newRegisteredLogWatcher(config, null);

      // Now log a message and ensure that the new watcher sees it.
      String msg = "This is a test message: " + idx;
      log.warn(msg);

      // Loop to give the logger time to process the async message and notify the new watcher.
      TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      boolean foundNewMsg = false;
      boolean foundOldMessage = false;
      // In local testing this loop usually succeeds 1-2 tries, so it's not very expensive to loop.
      do {
        // Returns an empty (but non-null) list even if there are no messages yet.
        SolrDocumentList events = watcher.getHistory(-1, null);
        for (SolrDocument doc : events) {
          String oneMsg = (String) doc.get("message");
          if (oneMsg.equals(msg)) {
            foundNewMsg = true;
          }
          // Check that no old messages bled over into this watcher.
          for (String oldMsg : oldMessages) {
            if (oneMsg.equals(oldMsg)) {
              foundOldMessage = true;
            }
          }
        }
        if (foundNewMsg == false) {
          Thread.sleep(10);
        }
      } while (foundNewMsg == false && timeOut.hasTimedOut() == false);

      if (foundNewMsg == false || foundOldMessage) {
        System.out.println("Dumping all events in failed watcher:");
        SolrDocumentList events = watcher.getHistory(-1, null);
        for (SolrDocument doc : events) {
          System.out.println("   Event:'" + doc.toString() + "'");
        }
        System.out.println("Recorded old messages");
        for (String oldMsg : oldMessages) {
          System.out.println("    " + oldMsg);
        }

        fail("Did not find expected message state, dumped current watcher's messages above, last message added: '" + msg + "'");
      }
      oldMessages.add(msg);
    }
  }
}

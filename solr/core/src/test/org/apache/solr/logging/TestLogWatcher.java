package org.apache.solr.logging;

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

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLogWatcher {

  private LogWatcherConfig config;

  @Before
  public void setUp() {
    config = new LogWatcherConfig(true, null, null, 50);
  }

  @Test
  public void testLog4jWatcher() {

    Logger log = LoggerFactory.getLogger("testlogger");
    LogWatcher watcher = LogWatcher.newRegisteredLogWatcher(config, null);

    assertEquals(watcher.getLastEvent(), -1);

    log.warn("This is a test message");

    assertTrue(watcher.getLastEvent() > -1);

    SolrDocumentList events = watcher.getHistory(-1, new AtomicBoolean());
    assertEquals(events.size(), 1);

    SolrDocument event = events.get(0);
    assertEquals(event.get("logger"), "testlogger");
    assertEquals(event.get("message"), "This is a test message");

  }

}

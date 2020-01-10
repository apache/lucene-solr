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

package org.apache.solr.security;

import java.io.IOException;
import java.util.HashMap;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.security.AuditLoggerPluginTest.EVENT_ANONYMOUS;
import static org.apache.solr.security.AuditLoggerPluginTest.EVENT_AUTHENTICATED;

public class SolrLogAuditLoggerPluginTest extends SolrTestCaseJ4 {
  private SolrLogAuditLoggerPlugin plugin;
  private HashMap<String, Object> config;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    plugin = new SolrLogAuditLoggerPlugin();
    config = new HashMap<>();
    config.put("async", false);
  }

  @Test(expected = SolrException.class)
  public void badConfig() throws IOException {
    config.put("invalid", "parameter");
    plugin.init(config);
  }
  
  @Test
  public void audit() {
    plugin.init(config);
    plugin.doAudit(EVENT_ANONYMOUS);
  }

  @Test
  public void eventFormatter() {
    plugin.init(config);
    assertEquals("type=\"ANONYMOUS\" message=\"Anonymous\" method=\"GET\" status=\"-1\" requestType=\"null\" username=\"null\" resource=\"/collection1\" queryString=\"null\" collections=null", 
        plugin.formatter.formatEvent(EVENT_ANONYMOUS));
    assertEquals("type=\"AUTHENTICATED\" message=\"Authenticated\" method=\"GET\" status=\"-1\" requestType=\"null\" username=\"Jan\" resource=\"/collection1\" queryString=\"null\" collections=null", 
        plugin.formatter.formatEvent(EVENT_AUTHENTICATED));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (null != plugin) {
      plugin.close();
      plugin = null;
    }
  }
}

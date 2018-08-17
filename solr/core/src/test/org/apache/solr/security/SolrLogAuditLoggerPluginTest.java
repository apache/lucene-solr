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

import java.util.HashMap;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.SolrException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.security.AuditLoggerPluginTest.EVENT_ANONYMOUS;
import static org.apache.solr.security.AuditLoggerPluginTest.EVENT_AUTHENTICATED;

public class SolrLogAuditLoggerPluginTest extends LuceneTestCase {
  private SolrLogAuditLoggerPlugin plugin;
  private HashMap<String, Object> config;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    plugin = new SolrLogAuditLoggerPlugin();
    config = new HashMap<>();
    plugin.init(config);
  }

  @Test(expected = SolrException.class)
  public void badConfig() {
    config = new HashMap<>();
    config.put("invalid", "parameter");
    plugin.init(config);
  }
  
  @Test
  public void audit() {
    plugin.audit(EVENT_ANONYMOUS);
  }

  @Test
  public void eventFormatter() {
    assertEquals("type=\"ANONYMOUS\" message=\"Anonymous\" method=\"GET\" username=\"null\" resource=\"/collection1\" collections=null", 
        plugin.formatter.formatEvent(EVENT_ANONYMOUS));
    assertEquals("type=\"AUTHENTICATED\" message=\"Authenticated\" method=\"GET\" username=\"Jan\" resource=\"/collection1\" collections=null", 
        plugin.formatter.formatEvent(EVENT_AUTHENTICATED));
  } 
}
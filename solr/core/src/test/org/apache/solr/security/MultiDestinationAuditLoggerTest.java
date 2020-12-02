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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

public class MultiDestinationAuditLoggerTest extends SolrTestCaseJ4 {
  @Test
  public void init() throws IOException {
    MultiDestinationAuditLogger al = new MultiDestinationAuditLogger();
    Map<String,Object> config = new HashMap<>();
    config.put("class", "solr.MultiDestinationAuditLogger");
    config.put("async", false);
    config.put("eventTypes", Arrays.asList(AuditEvent.EventType.COMPLETED.name()));
    ArrayList<Map<String, Object>> plugins = new ArrayList<Map<String, Object>>();

    Map<String,Object> conf1 = new HashMap<>();
    conf1.put("class", "solr.SolrLogAuditLoggerPlugin");
    conf1.put("async", false);
    conf1.put("eventTypes", Arrays.asList(AuditEvent.EventType.ANONYMOUS.name()));
    plugins.add(conf1);
    Map<String,Object> conf2 = new HashMap<>();
    conf2.put("class", "solr.MockAuditLoggerPlugin");
    conf2.put("async", false);
    conf2.put("eventTypes", Arrays.asList(AuditEvent.EventType.AUTHENTICATED.name()));
    plugins.add(conf2);
    config.put("plugins", plugins);

    SolrResourceLoader loader = new SolrResourceLoader(Paths.get(""));
    al.inform(loader);
    al.init(config);

    al.doAudit(new AuditEvent(AuditEvent.EventType.ANONYMOUS).setUsername("me"));
    assertEquals(0, ((MockAuditLoggerPlugin)al.plugins.get(1)).events.size()); // not configured for ANONYMOUS
    al.doAudit(new AuditEvent(AuditEvent.EventType.AUTHENTICATED).setUsername("me"));
    assertEquals(1, ((MockAuditLoggerPlugin)al.plugins.get(1)).events.size()); // configured for authenticated
    
    assertFalse(al.shouldLog(AuditEvent.EventType.ERROR));
    assertFalse(al.shouldLog(AuditEvent.EventType.UNAUTHORIZED));
    assertTrue(al.shouldLog(AuditEvent.EventType.COMPLETED));
    assertTrue(al.shouldLog(AuditEvent.EventType.ANONYMOUS));
    assertTrue(al.shouldLog(AuditEvent.EventType.AUTHENTICATED));

    assertEquals(0, config.size());
    
    al.close();
    loader.close();
  }

  @Test
  public void wrongConfigParam() throws IOException {
    MultiDestinationAuditLogger al = new MultiDestinationAuditLogger();
    Map<String,Object> config = new HashMap<>();
    config.put("class", "solr.MultiDestinationAuditLogger");
    config.put("foo", "Should complain");
    al.init(config);
    assertEquals(1, config.size());
    al.close();
  }
}

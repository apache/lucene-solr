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
import java.util.ArrayList;
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
    ArrayList<Map<String, Object>> plugins = new ArrayList<Map<String, Object>>();

    Map<String,Object> conf1 = new HashMap<>();
    conf1.put("class", "solr.SolrLogAuditLoggerPlugin");
    conf1.put("async", false);
    plugins.add(conf1);
    Map<String,Object> conf2 = new HashMap<>();
    conf2.put("class", "solr.MockAuditLoggerPlugin");
    conf2.put("async", false);
    plugins.add(conf2);
    config.put("plugins", plugins);

    al.inform(new SolrResourceLoader());
    al.init(config);

    al.doAudit(new AuditEvent(AuditEvent.EventType.ANONYMOUS).setUsername("me"));
    assertEquals(1, ((MockAuditLoggerPlugin)al.plugins.get(1)).events.size());

    assertEquals(0, config.size());
    al.close();
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
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

import static org.junit.Assert.*;

public class MultiDestinationAuditLoggerTest {
  @Test
  public void init() throws Exception {
    MultiDestinationAuditLogger al = new MultiDestinationAuditLogger();
    Map<String,Object> config = new HashMap<>();
    config.put("class", "solr.MultiDestinationAuditLogger");
    ArrayList<Map<String, Object>> plugins = new ArrayList<Map<String, Object>>();

    Map<String, Object> myPlugin = new HashMap<>();
    myPlugin.put("class", "solr.SolrLogAuditLoggerPlugin");
    plugins.add(myPlugin);
    config.put("plugins", plugins);

    al.inform(new SolrResourceLoader());
    al.init(config);

    al.audit(new AuditEvent(AuditEvent.EventType.ANONYMOUS).setUsername("me"));
  }

}
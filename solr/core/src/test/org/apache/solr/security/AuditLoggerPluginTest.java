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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AuditLoggerPluginTest extends SolrTestCaseJ4 {
  protected static final Date SAMPLE_DATE = new Date(1234567890);
  protected static final AuditEvent EVENT_ANONYMOUS = new AuditEvent(AuditEvent.EventType.ANONYMOUS)
      .setHttpMethod("GET")
      .setMessage("Anonymous")
      .setResource("/collection1")
      .setDate(SAMPLE_DATE);
  protected static final AuditEvent EVENT_WITH_URL = new AuditEvent(AuditEvent.EventType.ANONYMOUS)
      .setHttpMethod("GET")
      .setMessage("Anonymous")
      .setResource("/collection1")
      .setBaseUrl("http://myserver/mypath")
      .setHttpQueryString("a=b&c=d")
      .setDate(SAMPLE_DATE);
  protected static final AuditEvent EVENT_ANONYMOUS_REJECTED = new AuditEvent(AuditEvent.EventType.ANONYMOUS_REJECTED)
      .setHttpMethod("GET")
      .setMessage("Anonymous rejected")
      .setResource("/collection1");
  protected static final AuditEvent EVENT_AUTHENTICATED = new AuditEvent(AuditEvent.EventType.AUTHENTICATED)
      .setUsername("Jan")
      .setHttpMethod("GET")
      .setMessage("Authenticated")
      .setDate(SAMPLE_DATE)
      .setResource("/collection1");
  protected static final AuditEvent EVENT_REJECTED = new AuditEvent(AuditEvent.EventType.REJECTED)
      .setUsername("Jan")
      .setHttpMethod("POST")
      .setMessage("Wrong password")
      .setDate(SAMPLE_DATE)
      .setResource("/collection1");
  protected static final AuditEvent EVENT_AUTHORIZED = new AuditEvent(AuditEvent.EventType.AUTHORIZED)
      .setUsername("Per")
      .setClientIp("192.168.0.10")
      .setHttpMethod("GET")
      .setMessage("Async")
      .setDate(SAMPLE_DATE)
      .setResource("/collection1");
  protected static final AuditEvent EVENT_UNAUTHORIZED = new AuditEvent(AuditEvent.EventType.UNAUTHORIZED)
      .setUsername("Jan")
      .setHttpMethod("POST")
      .setMessage("No access to collection1")
      .setDate(SAMPLE_DATE)
      .setResource("/collection1");
  protected static final AuditEvent EVENT_ERROR = new AuditEvent(AuditEvent.EventType.ERROR)
      .setUsername("Jan")
      .setHttpMethod("POST")
      .setMessage("Error occurred")
      .setDate(SAMPLE_DATE)
      .setSolrParams(Collections.singletonMap("action", Collections.singletonList("DELETE")))
      .setResource("/admin/collections");
  protected static final AuditEvent EVENT_UPDATE = new AuditEvent(AuditEvent.EventType.COMPLETED)
      .setUsername("updateuser")
      .setHttpMethod("POST")
      .setRequestType(AuditEvent.RequestType.UPDATE)
      .setMessage("Success")
      .setDate(SAMPLE_DATE)
      .setCollections(Collections.singletonList("updatecoll"))
      .setRequestType(AuditEvent.RequestType.UPDATE)
      .setResource("/update");
  protected static final AuditEvent EVENT_STREAMING = new AuditEvent(AuditEvent.EventType.COMPLETED)
      .setUsername("streaminguser")
      .setHttpMethod("POST")
      .setRequestType(AuditEvent.RequestType.STREAMING)
      .setMessage("Success")
      .setDate(SAMPLE_DATE)
      .setCollections(Collections.singletonList("streamcoll"))
      .setResource("/stream");
  protected static final AuditEvent EVENT_HEALTH_API = new AuditEvent(AuditEvent.EventType.COMPLETED)
      .setUsername("Jan")
      .setHttpMethod("GET")
      .setMessage("Healthy")
      .setDate(SAMPLE_DATE)
      .setResource("/api/node/health");
  protected static final AuditEvent EVENT_HEALTH_V2 = new AuditEvent(AuditEvent.EventType.COMPLETED)
      .setUsername("Jan")
      .setHttpMethod("GET")
      .setMessage("Healthy")
      .setDate(SAMPLE_DATE)
      .setResource("/____v2/node/health");

  private MockAuditLoggerPlugin plugin;
  private HashMap<String, Object> config;
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    plugin = new MockAuditLoggerPlugin();
    config = new HashMap<>();
    config.put("async", false);
    plugin.init(config);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (null != plugin) {
      plugin.close();
      plugin = null;
    }
    super.tearDown();
  }

  @Test
  public void init() {
    config = new HashMap<>();
    config.put("eventTypes", Collections.singletonList("REJECTED"));
    config.put("async", false);
    plugin.init(config);
    assertTrue(plugin.shouldLog(EVENT_REJECTED.getEventType()));
    assertFalse(plugin.shouldLog(EVENT_UNAUTHORIZED.getEventType()));
  }

  @Test
  public void shouldLog() {
    // Default types
    assertTrue(plugin.shouldLog(EVENT_ANONYMOUS_REJECTED.getEventType()));
    assertTrue(plugin.shouldLog(EVENT_REJECTED.getEventType()));
    assertTrue(plugin.shouldLog(EVENT_UNAUTHORIZED.getEventType()));
    assertTrue(plugin.shouldLog(EVENT_ERROR.getEventType()));
    assertFalse(plugin.shouldLog(EVENT_ANONYMOUS.getEventType()));    
    assertFalse(plugin.shouldLog(EVENT_AUTHENTICATED.getEventType()));    
    assertFalse(plugin.shouldLog(EVENT_AUTHORIZED.getEventType()));
    assertFalse(plugin.shouldLog(EVENT_AUTHORIZED.getEventType()));
  }

  @Test(expected = SolrException.class)
  public void invalidMuteRule() {
    config.put("muteRules", Collections.singletonList("foo:bar"));
    plugin.init(config);
  }
  
  @Test
  public void shouldMute() {
    List<Object> rules = new ArrayList<>();
    rules.add("type:STREAMING");
    rules.add(Arrays.asList("user:updateuser", "collection:updatecoll"));
    rules.add(Arrays.asList("path:/admin/collection", "param:action=DELETE"));
    rules.add("ip:192.168.0.10");
    config.put("muteRules",rules); 
    plugin.init(config);
    assertFalse(plugin.shouldMute(EVENT_ANONYMOUS));
    assertFalse(plugin.shouldMute(EVENT_AUTHENTICATED));
    assertTrue(plugin.shouldMute(EVENT_STREAMING));  // type:STREAMING
    assertTrue(plugin.shouldMute(EVENT_UPDATE));     // updateuser, updatecoll
    assertTrue(plugin.shouldMute(EVENT_ERROR));      // admin/collection action=DELETE
    assertTrue(plugin.shouldMute(EVENT_AUTHORIZED)); // ip
  }

  @Test
  public void audit() {
    plugin.doAudit(EVENT_ANONYMOUS_REJECTED);
    plugin.doAudit(EVENT_REJECTED);
    assertEquals(1, plugin.typeCounts.getOrDefault("ANONYMOUS_REJECTED", new AtomicInteger()).get());
    assertEquals(1, plugin.typeCounts.getOrDefault("REJECTED", new AtomicInteger()).get());
    assertEquals(2, plugin.events.size());
  }

  @Test
  public void v2ApiPath() {
    assertEquals("/api/node/health", EVENT_HEALTH_API.getResource());
    // /____v2/ is mapped to /api/
    assertEquals("/api/node/health", EVENT_HEALTH_V2.getResource());
  }

  @Test
  public void jsonEventFormatter() {
    assertEquals("{\"message\":\"Anonymous\",\"level\":\"INFO\",\"date\":" + SAMPLE_DATE.getTime() + ",\"solrParams\":{},\"solrPort\":0,\"resource\":\"/collection1\",\"httpMethod\":\"GET\",\"eventType\":\"ANONYMOUS\",\"status\":-1,\"qtime\":-1.0}", 
        plugin.formatter.formatEvent(EVENT_ANONYMOUS));
    assertEquals("{\"message\":\"Authenticated\",\"level\":\"INFO\",\"date\":" + SAMPLE_DATE.getTime() + ",\"username\":\"Jan\",\"solrParams\":{},\"solrPort\":0,\"resource\":\"/collection1\",\"httpMethod\":\"GET\",\"eventType\":\"AUTHENTICATED\",\"status\":-1,\"qtime\":-1.0}", 
        plugin.formatter.formatEvent(EVENT_AUTHENTICATED));
  }

  @Test
  public void getBaseUrl() {
    assertEquals("http://myserver/mypath", EVENT_WITH_URL.getBaseUrl());
    // Deprecated
    assertEquals("http://myserver/mypath", EVENT_WITH_URL.getRequestUrl().toString());
  }

  @Test
  public void getUrl() {
    assertEquals("http://myserver/mypath?a=b&c=d",
        EVENT_WITH_URL.getUrl());
  }
}

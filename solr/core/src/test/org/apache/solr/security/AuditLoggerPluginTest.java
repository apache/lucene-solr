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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuditLoggerPluginTest {
  protected static final Date SAMPLE_DATE = new Date(1234567890);
  protected static final AuditEvent EVENT_ANONYMOUS = new AuditEvent(AuditEvent.EventType.ANONYMOUS)
      .setHttpMethod("GET")
      .setMessage("Anonymous")
      .setResource("/collection1")
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
      .setResource("/collection1");

  private MockAuditLoggerPlugin plugin;
  private HashMap<String, Object> config;
  
  @Before
  public void setUp() throws Exception {
    plugin = new MockAuditLoggerPlugin();
    config = new HashMap<>();
    plugin.init(config);
  }
  
  @Test
  public void init() {
    config = new HashMap<>();
    config.put("eventTypes", Arrays.asList("REJECTED"));
    plugin.init(config);
    assertTrue(AuditLoggerPlugin.shouldLog(EVENT_REJECTED.getEventType()));
    assertFalse(AuditLoggerPlugin.shouldLog(EVENT_UNAUTHORIZED.getEventType()));
  }

  @Test
  public void shouldLog() {
    // Default types
    assertTrue(AuditLoggerPlugin.shouldLog(EVENT_ANONYMOUS_REJECTED.getEventType()));
    assertTrue(AuditLoggerPlugin.shouldLog(EVENT_REJECTED.getEventType()));
    assertTrue(AuditLoggerPlugin.shouldLog(EVENT_UNAUTHORIZED.getEventType()));
    assertTrue(AuditLoggerPlugin.shouldLog(EVENT_ERROR.getEventType()));
    assertFalse(AuditLoggerPlugin.shouldLog(EVENT_ANONYMOUS.getEventType()));    
    assertFalse(AuditLoggerPlugin.shouldLog(EVENT_AUTHENTICATED.getEventType()));    
    assertFalse(AuditLoggerPlugin.shouldLog(EVENT_AUTHORIZED.getEventType()));
  }
  
  @Test
  public void audit() {
    plugin.audit(EVENT_ANONYMOUS_REJECTED);
    plugin.audit(EVENT_REJECTED);
    assertEquals(1, plugin.typeCounts.get("ANONYMOUS_REJECTED").get());
    assertEquals(1, plugin.typeCounts.get("REJECTED").get());
    assertEquals(2, plugin.events.size());
  }
  
  @Test
  public void jsonEventFormatter() {
    assertEquals("{\"message\":\"Anonymous\",\"level\":\"INFO\",\"date\":" + SAMPLE_DATE.getTime() + ",\"solrPort\":0,\"resource\":\"/collection1\",\"httpMethod\":\"GET\",\"eventType\":\"ANONYMOUS\",\"status\":-1,\"qtime\":-1.0}", 
        plugin.formatter.formatEvent(EVENT_ANONYMOUS));
    assertEquals("{\"message\":\"Authenticated\",\"level\":\"INFO\",\"date\":1234567890,\"username\":\"Jan\",\"solrPort\":0,\"resource\":\"/collection1\",\"httpMethod\":\"GET\",\"eventType\":\"AUTHENTICATED\",\"status\":-1,\"qtime\":-1.0}", 
        plugin.formatter.formatEvent(EVENT_AUTHENTICATED));
  } 
  
}
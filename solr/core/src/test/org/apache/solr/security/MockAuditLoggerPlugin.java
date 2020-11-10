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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockAuditLoggerPlugin extends AuditLoggerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public List<AuditEvent> events = new ArrayList<>();
  public Map<String,AtomicInteger> typeCounts = new HashMap<>();

  /**
   * Audits an event to an internal list that can be inspected later by the test code
   * @param event the audit event
   */
  @Override
  public void audit(AuditEvent event) {
    events.add(event);
    incrementType(event.getEventType().name());
    if (log.isInfoEnabled()) {
      log.info("#{} - {}", events.size(), typeCounts);
    }
  }

  private void incrementType(String type) {
    if (!typeCounts.containsKey(type))
      typeCounts.put(type, new AtomicInteger(0));
    typeCounts.get(type).incrementAndGet();
  }

  public void reset() {
    events.clear();
    typeCounts.clear();
  }
}

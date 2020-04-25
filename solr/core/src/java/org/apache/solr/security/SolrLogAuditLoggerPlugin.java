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
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Audit logger that writes to the Solr log.
 * This interface may change in next release and is marked experimental
 * @since 8.1.0
 * @lucene.experimental
 */
public class SolrLogAuditLoggerPlugin extends AuditLoggerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Initialize the plugin from security.json
   * @param pluginConfig the config for the plugin
   */
  @Override
  public void init(Map<String, Object> pluginConfig) {
    super.init(pluginConfig);
    setFormatter(event ->
        new StringBuilder()
            .append("type=\"").append(event.getEventType().name()).append("\"")
            .append(" message=\"").append(event.getMessage()).append("\"")
            .append(" method=\"").append(event.getHttpMethod()).append("\"")
            .append(" status=\"").append(event.getStatus()).append("\"")
            .append(" requestType=\"").append(event.getRequestType()).append("\"")
            .append(" username=\"").append(event.getUsername()).append("\"")
            .append(" resource=\"").append(event.getResource()).append("\"")
            .append(" queryString=\"").append(event.getHttpQueryString()).append("\"")
            .append(" collections=").append(event.getCollections()).toString());
    if (pluginConfig.size() > 0) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "Plugin config was not fully consumed. Remaining parameters are " + pluginConfig);
    }
    log.debug("Initialized SolrLogAuditLoggerPlugin");  
  }
  
  /**
   * Audit logs an event to Solr log. The event should be a {@link AuditEvent} to be able to pull context info
   * @param event the event to log
   */
  @Override
  public void audit(AuditEvent event) {
    switch (event.getLevel()) {
      case INFO:
        if (log.isInfoEnabled()) {
          log.info(formatter.formatEvent(event));
        }
        break;

      case WARN:
        log.warn(formatter.formatEvent(event));
        break;

      case ERROR:
        log.error(formatter.formatEvent(event));
        break;
    }
  }
}

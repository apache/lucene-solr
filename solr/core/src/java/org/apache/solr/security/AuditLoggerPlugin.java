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

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Audit logger plugins
 */
public abstract class AuditLoggerPlugin implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected AuditEventFormatter formatter;

  /**
   * Audits an event. The event should be a {@link AuditEvent} to be able to pull context info.
   * @param event the audit event
   */
  public abstract void audit(AuditEvent event);

  /**
   * Initialize the plugin from security.json.
   * This method removes parameters from config object after consuming, so subclasses can check for config errors.
   * @param pluginConfig the config for the plugin
   */
  public void init(Map<String, Object> pluginConfig) {
    formatter = new JSONAuditEventFormatter();
  }

  public void setFormatter(AuditEventFormatter formatter) {
    this.formatter = formatter;
  }
  
  /**
   * Interface for formatting the event
   */
  public interface AuditEventFormatter {
    String formatEvent(AuditEvent event);
  }

  /**
   * Event formatter that returns event as JSON string
   */
  public static class JSONAuditEventFormatter implements AuditEventFormatter {
    /**
     * Formats an audit event as a JSON string
     */
    @Override
    public String formatEvent(AuditEvent event) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
      try {
        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, event);
        return sw.toString();
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error converting Event to JSON", e);
      }
    }
  }
}

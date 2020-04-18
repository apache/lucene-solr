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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

/**
 * Audit logger that chains other loggers. Lets you configure logging to multiple destinations.
 * The config is simply a list of configs for the sub plugins:
 * <pre>
 *   "class" : "solr.MultiDestinationAuditLogger",
 *   "plugins" : [
 *     { "class" : "solr.SolrLogAuditLoggerPlugin" },
 *     { "class" : "solr.MyOtherAuditPlugin"}
 *   ]
 * </pre>
 *
 * This interface may change in next release and is marked experimental
 * @since 8.1.0
 * @lucene.experimental
 */
public class MultiDestinationAuditLogger extends AuditLoggerPlugin implements ResourceLoaderAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_PLUGINS = "plugins";
  private ResourceLoader loader;
  List<AuditLoggerPlugin> plugins = new ArrayList<>();
  List<String> pluginNames = new ArrayList<>();

  /**
   * Passes the AuditEvent to all sub plugins in parallel. The event should be a {@link AuditEvent} to be able to pull context info.
   * @param event the audit event
   */
  @Override
  public void audit(AuditEvent event) {
    log.debug("Passing auditEvent to plugins {}", pluginNames);
    plugins.parallelStream().forEach(plugin -> {
      if (plugin.shouldLog(event.getEventType()))
        plugin.doAudit(event);
    });
  }

  /**
   * Initialize the plugin from security.json
   * @param pluginConfig the config for the plugin
   */
  @Override
  public void init(Map<String, Object> pluginConfig) {
    pluginConfig.put(PARAM_ASYNC, false); // Force the multi plugin to synchronous operation
    super.init(pluginConfig);
    if (!pluginConfig.containsKey(PARAM_PLUGINS)) {
      log.warn("No plugins configured");
    } else {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> pluginList = (List<Map<String, Object>>) pluginConfig.get(PARAM_PLUGINS);
      pluginList.forEach(pluginConf -> plugins.add(createPlugin(pluginConf)));
      pluginConfig.remove(PARAM_PLUGINS);
      pluginNames = plugins.stream().map(AuditLoggerPlugin::getName).collect(Collectors.toList());
    }
    if (pluginConfig.size() > 0) {
      log.error("Plugin config was not fully consumed. Remaining parameters are {}", pluginConfig);
    }
    if (log.isInfoEnabled()) {
      log.info("Initialized {} audit plugins: {}", plugins.size(), pluginNames);
    }
  }

  @Override
  public boolean shouldLog(AuditEvent.EventType eventType) {
    return super.shouldLog(eventType) || plugins.stream().anyMatch(p -> p.shouldLog(eventType));
  }

  private AuditLoggerPlugin createPlugin(Map<String, Object> auditConf) {
    if (auditConf != null) {
      String klas = (String) auditConf.get("class");
      if (klas == null) {
        throw new SolrException(SERVER_ERROR, "class is required for auditlogger plugin");
      }
      log.info("Initializing auditlogger plugin: {}", klas);
      AuditLoggerPlugin p = loader.newInstance(klas, AuditLoggerPlugin.class);
      if (p.getClass().equals(this.getClass())) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot nest MultiDestinationAuditLogger");
      }
      p.init(auditConf);
      return p;
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Empty config when creating audit plugin");
    }
  }

  @Override
  public void inform(ResourceLoader loader) {
    this.loader = loader;
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    super.initializeMetrics(manager, registryName, tag, scope);
    plugins.forEach(p -> p.initializeMetrics(manager, registryName, tag, scope));
  }

  @Override
  public void close() throws IOException {
    super.close(); // Waiting for queue to drain before shutting down the loggers
    plugins.forEach(p -> {
      try {
        p.close();
      } catch (IOException e) {
        log.error("Exception trying to close {}", p.getName());
      }
    });
  }
}

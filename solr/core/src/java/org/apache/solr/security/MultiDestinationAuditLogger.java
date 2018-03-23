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

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
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
 */
public class MultiDestinationAuditLogger extends AuditLoggerPlugin implements ResourceLoaderAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_PLUGINS = "plugins";
  private List<AuditLoggerPlugin> plugins = new ArrayList<>();;
  private ResourceLoader loader;

  /**
   * Audits an event. The event should be a {@link AuditEvent} to be able to pull context info.
   * @param event
   */
  @Override
  public void audit(AuditEvent event) {
    plugins.forEach(plugin -> {
      log.debug("Passing auditEvent to plugin {}", plugin.getClass().getName());
      plugin.audit(event);
    });
  }

  /**
   * Initialize the plugin from security.json
   * @param pluginConfig the config for the plugin
   */
  @Override
  public void init(Map<String, Object> pluginConfig) {
    super.init(pluginConfig);
    if (!pluginConfig.containsKey(PARAM_PLUGINS)) {
      log.warn("No plugins configured");
    } else {
      List<Map<String, Object>> pluginList = (List<Map<String, Object>>) pluginConfig.get(PARAM_PLUGINS);
      pluginList.forEach(pluginConf -> {
        plugins.add(createPlugin(pluginConf));
      });
      pluginConfig.remove(PARAM_PLUGINS);
    }
    pluginConfig.remove("class");
    if (pluginConfig.size() > 0) {
      log.error("Plugin config was not fully consumed. Remaining parameters are {}", pluginConfig);
    }
    log.info("Initialized {} audit plugins", plugins.size());
  }

  private AuditLoggerPlugin createPlugin(Map<String, Object> auditConf) {
    if (auditConf != null) {
      String klas = (String) auditConf.get("class");
      if (klas == null) {
        throw new SolrException(SERVER_ERROR, "class is required for auditlogger plugin");
      }
      log.info("Initializing auditlogger plugin: " + klas);
      AuditLoggerPlugin p = loader.newInstance(klas, AuditLoggerPlugin.class);
      p.init(auditConf);
      return p;
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Empty config when creating audit plugin");
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    this.loader = loader;
  }
}

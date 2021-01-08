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
package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.common.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple HTTP callback that POSTs event data to a URL.
 * URL, payload and headers may contain property substitution patterns, with the following properties available:
 * <ul>
 *   <li>config.* - listener configuration</li>
 *   <li>event.* - event properties</li>
 *   <li>stage - current stage of event processing</li>
 *   <li>actionName - optional current action name</li>
 *   <li>context.* - optional {@link ActionContext} properties</li>
 *   <li>error - optional error string (from {@link Throwable#toString()})</li>
 *   <li>message - optional message</li>
 * </ul>
 * The following listener configuration is supported:
 * <ul>
 *   <li>url - a URL template</li>
 *   <li>payload - string, optional payload template. If absent a JSON map of all properties listed above will be used.</li>
 *   <li>contentType - string, optional payload content type. If absent then <code>application/json</code> will be used.</li>
 *   <li>header.* - string, optional header template(s). The name of the property without "header." prefix defines the literal header name.</li>
 *   <li>timeout - int, optional connection and socket timeout in milliseconds. Default is 60 seconds.</li>
 *   <li>followRedirects - boolean, optional setting to follow redirects. Default is false.</li>
 * </ul>
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class HttpTriggerListener extends TriggerListenerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String urlTemplate;
  private String payloadTemplate;
  private String contentType;
  private Map<String, String> headerTemplates = new HashMap<>();
  private int timeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
  private boolean followRedirects;

  public HttpTriggerListener() {
    super();
    TriggerUtils.requiredProperties(requiredProperties, validProperties, "url");
    TriggerUtils.validProperties(validProperties, "payload", "contentType", "timeout", "followRedirects");
    validPropertyPrefixes.add("header.");
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
    super.configure(loader, cloudManager, config);
    urlTemplate = (String)config.properties.get("url");
    payloadTemplate = (String)config.properties.get("payload");
    contentType = (String)config.properties.get("contentType");
    config.properties.forEach((k, v) -> {
      if (k.startsWith("header.")) {
        headerTemplates.put(k.substring(7), String.valueOf(v));
      }
    });
    timeout = PropertiesUtil.toInteger(String.valueOf(config.properties.get("timeout")), HttpClientUtil.DEFAULT_CONNECT_TIMEOUT);
    followRedirects = PropertiesUtil.toBoolean(String.valueOf(config.properties.get("followRedirects")));
  }

  @Override
  public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context, Throwable error, String message) {
    Properties properties = new Properties();
    properties.setProperty("stage", stage.toString());
    // if configuration used "actionName" but we're in a non-action related stage then PropertiesUtil will
    // throws an exception on missing value - so replace it with an empty string
    if (actionName == null) {
      actionName = "";
    }
    properties.setProperty("actionName", actionName);
    if (context != null) {
      context.getProperties().forEach((k, v) -> {
        properties.setProperty("context." + k, String.valueOf(v));
      });
    }
    if (error != null) {
      properties.setProperty("error", error.toString());
    } else {
      properties.setProperty("error", "");
    }
    if (message != null) {
      properties.setProperty("message", message);
    } else {
      properties.setProperty("message", "");
    }
    // add event properties
    properties.setProperty("event.id", event.getId());
    properties.setProperty("event.source", event.getSource());
    properties.setProperty("event.eventTime", String.valueOf(event.eventTime));
    properties.setProperty("event.eventType", event.getEventType().toString());
    event.getProperties().forEach((k, v) -> {
      properties.setProperty("event.properties." + k, String.valueOf(v));
    });
    // add config properties
    properties.setProperty("config.name", config.name);
    properties.setProperty("config.trigger", config.trigger);
    properties.setProperty("config.listenerClass", config.listenerClass);
    properties.setProperty("config.beforeActions", String.join(",", config.beforeActions));
    properties.setProperty("config.afterActions", String.join(",", config.afterActions));
    StringJoiner joiner = new StringJoiner(",");
    config.stages.forEach(s -> joiner.add(s.toString()));
    properties.setProperty("config.stages", joiner.toString());
    config.properties.forEach((k, v) -> {
      properties.setProperty("config.properties." + k, String.valueOf(v));
    });
    String url = PropertiesUtil.substituteProperty(urlTemplate, properties);
    String payload;
    String type;
    if (payloadTemplate != null) {
      payload = PropertiesUtil.substituteProperty(payloadTemplate, properties);
      if (contentType != null) {
        type = contentType;
      } else {
        type = "application/json";
      }
    } else {
      payload = Utils.toJSONString(properties);
      type = "application/json";
    }
    Map<String, String> headers = new HashMap<>();
    headerTemplates.forEach((k, v) -> {
      String headerVal = PropertiesUtil.substituteProperty(v, properties);
      if (!headerVal.isEmpty()) {
        headers.put(k, headerVal);
      }
    });
    headers.put("Content-Type", type);
    try {
      cloudManager.httpRequest(url, SolrRequest.METHOD.POST, headers, payload, timeout, followRedirects);
    } catch (IOException e) {
      log.warn("Exception sending request for event {}", event, e);
    }
  }
}

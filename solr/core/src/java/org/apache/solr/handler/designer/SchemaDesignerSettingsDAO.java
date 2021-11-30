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

package org.apache.solr.handler.designer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.designer.SchemaDesignerAPI.getConfigSetZkPath;

class SchemaDesignerSettingsDAO implements SchemaDesignerConstants {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer cc;

  SchemaDesignerSettingsDAO(CoreContainer cc) {
    this.cc = cc;
  }

  SchemaDesignerSettings getSettings(String configSet) {
    SolrConfig solrConfig =
        SolrConfig.readFromResourceLoader(zkLoaderForConfigSet(configSet), SOLR_CONFIG_XML, true, null);
    return getSettings(solrConfig);
  }

  SchemaDesignerSettings getSettings(SolrConfig solrConfig) {
    Map<String, Object> map = new HashMap<>();

    boolean isFieldGuessingEnabled = true;
    if (solrConfig != null) {
      ConfigOverlay overlay = solrConfig.getOverlay();
      Map<String, Object> userProps = overlay != null ? overlay.getUserProps() : null;
      if (userProps != null) {
        map.putAll(userProps);
      }
      isFieldGuessingEnabled = isFieldGuessingEnabled(solrConfig);
    }

    map.putIfAbsent(AUTO_CREATE_FIELDS, isFieldGuessingEnabled);
    map.putIfAbsent(DESIGNER_KEY + LANGUAGES_PARAM, Collections.emptyList());
    map.putIfAbsent(DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, true);
    map.putIfAbsent(DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, false);

    return new SchemaDesignerSettings(map);
  }

  boolean persistIfChanged(String configSet, SchemaDesignerSettings settings) throws IOException {
    boolean changed = false;

    ConfigOverlay overlay = getConfigOverlay(configSet);
    Map<String, Object> storedUserProps = overlay != null ? overlay.getUserProps() : Collections.emptyMap();
    for (Map.Entry<String, Object> e : settings.toMap().entrySet()) {
      String key = e.getKey();
      Object propValue = e.getValue();
      if (propValue != null && !propValue.equals(storedUserProps.get(key))) {
        if (overlay == null) overlay = new ConfigOverlay(null, -1);
        overlay = overlay.setUserProperty(key, propValue);
        changed = true;
      }
    }

    if (changed) {
      ZkController.persistConfigResourceToZooKeeper(zkLoaderForConfigSet(configSet), overlay.getZnodeVersion(),
          ConfigOverlay.RESOURCE_NAME, overlay.toByteArray(), true);
    }

    return changed;
  }

  boolean isDesignerDisabled(String configSet) {
    // filter out any configs that don't want to be edited by the Schema Designer
    // this allows users to lock down specific configs from being edited by the designer
    ConfigOverlay overlay = null;
    try {
      overlay = getConfigOverlay(configSet);
    } catch (IOException exc) {
      log.error("Failed to get config overlay for {}", configSet, exc);
    }
    Map<String, Object> userProps = overlay != null ? overlay.getUserProps() : Collections.emptyMap();
    return SchemaDesignerSettings.getSettingAsBool(userProps, DESIGNER_KEY + DISABLED, false);
  }

  @SuppressWarnings("unchecked")
  private ConfigOverlay getConfigOverlay(String config) throws IOException {
    ConfigOverlay overlay = null;
    String path = getConfigSetZkPath(config, CONFIGOVERLAY_JSON);
    byte[] data = null;
    Stat stat = new Stat();
    try {
      data = cc.getZkController().getZkStateReader().getZkClient().getData(path, null, stat, true);
    } catch (KeeperException.NoNodeException nne) {
      // ignore
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading path: " + path, SolrZkClient.checkInterrupted(e));
    }
    if (data != null && data.length > 0) {
      Map<String, Object> json =
          (Map<String, Object>) ObjectBuilder.getVal(new JSONParser(new String(data, StandardCharsets.UTF_8)));
      overlay = new ConfigOverlay(json, stat.getVersion());
    }
    return overlay;
  }

  private boolean isFieldGuessingEnabled(final SolrConfig solrConfig) {
    if (!hasFieldGuessingURPChain(solrConfig)) {
      return false; // no URP chain, so can't be enabled
    }

    ConfigOverlay overlay = solrConfig.getOverlay();
    return overlay == null || SchemaDesignerSettings.getSettingAsBool(overlay.getUserProps(), AUTO_CREATE_FIELDS, true);
  }

  private boolean hasFieldGuessingURPChain(final SolrConfig solrConfig) {
    boolean hasPlugin = false;
    List<PluginInfo> plugins = solrConfig.getPluginInfos(UpdateRequestProcessorChain.class.getName());
    if (plugins != null) {
      for (PluginInfo next : plugins) {
        if ("add-unknown-fields-to-the-schema".equals(next.name)) {
          hasPlugin = true;
          break;
        }
      }
    }
    return hasPlugin;
  }

  private ZkSolrResourceLoader zkLoaderForConfigSet(final String configSet) {
    SolrResourceLoader loader = cc.getResourceLoader();
    return new ZkSolrResourceLoader(loader.getInstancePath(), configSet, loader.getClassLoader(), new Properties(), cc.getZkController());
  }
}

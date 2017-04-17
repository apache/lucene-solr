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

package org.apache.solr.util.modules;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.update.DefaultUpdateRepository;
import ro.fortsoft.pf4j.update.PluginInfo;

/**
 * Change file name
 */
public class SolrUpdateRepository extends DefaultUpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String modulesUrl;
  private boolean modulesUrlResolved = false;
  protected String modulesJson;
  protected Map<String, PluginInfo> plugins;

  public SolrUpdateRepository(String id) {
    this(id, "");
  }

  public SolrUpdateRepository(String id, String location) {
    this(id, location, "modules.json");
  }

  public SolrUpdateRepository(String id, String location, String modulesJson) {
    super(id, location);
    this.modulesJson = modulesJson;
  }

  @Override
  public Map<String, PluginInfo> getPlugins() {
    return super.getPlugins();
  }

  @Override
  public String getLocation() {
    if (modulesUrl == null && !modulesUrlResolved) {
      modulesUrl = resolveModulesUrl();
      modulesUrlResolved = true;
    }
    return modulesUrl;
  }

  protected String resolveModulesUrl() {
    return getLocation();
  }

  protected void initPlugins() {
    Reader pluginsJsonReader;
    URL pluginsUrl = null;
    try {
        pluginsUrl = new URL(new URL(getLocation()), modulesJson);
        log.debug("Read plugins of '{}' repository from '{}'", getId(), pluginsUrl);
        pluginsJsonReader = new InputStreamReader(pluginsUrl.openStream());
    } catch (Exception e) {
        log.error("Failed to find {}", pluginsUrl);
        plugins = Collections.emptyMap();
        return;
    }

    Gson gson = new GsonBuilder().create();
    PluginInfo[] items = gson.fromJson(pluginsJsonReader, PluginInfo[].class);
    plugins = new HashMap<>(items.length);
    for (PluginInfo p : items) {
        for (PluginInfo.PluginRelease r : p.releases) {
            try {
                r.url = new URL(new URL(getLocation()), r.url).toString();
            } catch (MalformedURLException e) {
                log.warn("Skipping release {} of plugin {} due to failure to build valid absolute URL. Url was {}{}", r.version, p.id, getLocation(), r.url);
            }
        }
        plugins.put(p.id, p);
    }
    log.debug("Found {} plugins in repository '{}'", plugins.size(), getId());
  }

}

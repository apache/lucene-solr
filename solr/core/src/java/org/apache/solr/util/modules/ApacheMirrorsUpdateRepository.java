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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.update.PluginInfo;
import ro.fortsoft.pf4j.update.UpdateRepository;

/**
 * Update Repository that resolves Apache Mirros
 */
public class ApacheMirrorsUpdateRepository implements UpdateRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String APACHE_DIST_URL = "https://www.apache.org/dist/";
  private static final String APACHE_ARCHIVE_URL = "https://archive.apache.org/dist/";
  private static final String CLOSER_URL = "https://www.apache.org/dyn/closer.lua?action=download&filename=";
  private final String id;
  private final String modulesPath;
  private final String modulesJson;
  private Map<String, PluginInfo> plugins;
  private String modulesUrl;

  public ApacheMirrorsUpdateRepository(String id, String modulesPath, String modulesJson) {
    this.id = id;
    this.modulesPath = modulesPath;
    this.modulesJson = modulesJson;
  }

  public ApacheMirrorsUpdateRepository(String id, String modulesPath) {
    this(id, modulesPath, "modules.json");
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getLocation() {
    return getModulesUrl() != null ? getModulesUrl() : null;
  }

  public String getModulesUrl() {
    if (modulesUrl == null) {
      modulesUrl = resolveModulesUrl();
    }
    return modulesUrl;
  }

  private String resolveModulesUrl() {
    String mirrorUrl = CLOSER_URL + modulesPath;
    try {
      mirrorUrl = getFinalURL(mirrorUrl);
      return mirrorUrl;
    } catch (IOException e) {
      log.debug("Url {} not found in mirrors, response={}",
          mirrorUrl, e.getMessage());
      try {
        mirrorUrl = getFinalURL(APACHE_DIST_URL + modulesPath);
        log.debug("Resolved URL: {}", mirrorUrl);
        return mirrorUrl;
      } catch (IOException e1) {
        log.debug("Url {} not found in main repo, response={}",
            mirrorUrl, e1.getMessage());
        try {
          mirrorUrl = getFinalURL(APACHE_ARCHIVE_URL + modulesPath);
          log.debug("Resolved URL: {}", mirrorUrl);
          return mirrorUrl;
        } catch (IOException e2) {
          log.debug("Url {} not found in archive repo, response={}",
              mirrorUrl, e2.getMessage());
          return null;
        }
      }
    }
  }

  @Override
  public Map<String, PluginInfo> getPlugins() {
    if (plugins == null) {
      initPlugins();
    }
    return plugins;
  }

  @Override
  public PluginInfo getPlugin(String id) {
    return getPlugins().get(id);
  }

  @Override
  public void refresh() {
    plugins = null;
  }

  private void initPlugins() {
    Reader pluginsJsonReader;
    URL pluginsUrl = null;
    try {
        pluginsUrl = new URL(new URL(getModulesUrl()), modulesJson);
        log.debug("Read plugins of '{}' repository from '{}'", id, pluginsUrl);
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
                r.url = new URL(new URL(getModulesUrl()), r.url).toString();
            } catch (MalformedURLException e) {
                log.warn("Skipping release {} of plugin {} due to failure to build valid absolute URL. Url was {}{}", r.version, p.id, getLocation(), r.url);
            }
        }
        plugins.put(p.id, p);
    }
    log.debug("Found {} plugins in repository '{}'", plugins.size(), id);
  }

  public static String getFinalURL(String url) throws IOException {
      HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
      con.setInstanceFollowRedirects(false);
      con.setRequestMethod("GET");
      con.connect();
      con.getInputStream();

      if (con.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM || con.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP) {
          String redirectUrl = con.getHeaderField("Location");
          return getFinalURL(redirectUrl);
      }
      return url;
  }
}

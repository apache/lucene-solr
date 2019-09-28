/*
 * Copyright (C) 2012-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.packagemanager.pf4j;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.solr.packagemanager.pf4j.PluginInfo.PluginRelease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * The default implementation of {@link UpdateRepository}.
 *
 * @author Decebal Suiu
 */
public class DefaultUpdateRepository implements UpdateRepository {

    private static final String DEFAULT_PLUGINS_JSON_FILENAME = "plugins.json";
    private static final Logger log = LoggerFactory.getLogger(DefaultUpdateRepository.class);

    private String id;
    private URL url;
    private String pluginsJsonFileName;

    private Map<String, PluginInfo> plugins;

    /**
     * Instantiates a new default update repository. The default plugins JSON file
     * name {@code plugins.json} will be used. Please use
     * {@link #DefaultUpdateRepository(String, URL, String)} if you want to choose
     * another file name than {@code plugins.json}}.
     *
     * @param id  the repository id
     * @param url the repository url
     */
    public DefaultUpdateRepository(String id, URL url) {
        this(id, url, DEFAULT_PLUGINS_JSON_FILENAME);
    }

    public DefaultUpdateRepository(String id, URL url, String pluginsJsonFileName) {
	this.id = id;
	this.url = url;
	this.pluginsJsonFileName = pluginsJsonFileName;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public URL getUrl() {
        return url;
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

    private void initPlugins() {
        Reader pluginsJsonReader;
        try {
            URL pluginsUrl = new URL(getUrl(), getPluginsJsonFileName());
            log.debug("Read plugins of '{}' repository from '{}'", id, pluginsUrl);
            pluginsJsonReader = new InputStreamReader(pluginsUrl.openStream());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            plugins = Collections.emptyMap();
            return;
        }

        Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, new LenientDateTypeAdapter()).create();
        PluginInfo[] items = gson.fromJson(pluginsJsonReader, PluginInfo[].class);
        plugins = new HashMap<>(items.length);
        for (PluginInfo p : items) {
            for (PluginRelease r : p.releases) {
                try {
                    r.url = new URL(getUrl(), r.url).toString();
                    if (r.date.getTime() == 0) {
                        log.warn("Illegal release date when parsing {}@{}, setting to epoch", p.id, r.version);
                    }
                } catch (MalformedURLException e) {
                    log.warn("Skipping release {} of plugin {} due to failure to build valid absolute URL. Url was {}{}", r.version, p.id, getUrl(), r.url);
                }
            }
            p.setRepositoryId(getId());
            plugins.put(p.id, p);
        }
        log.debug("Found {} plugins in repository '{}'", plugins.size(), id);
    }

    /**
     * Causes {@code plugins.json} to be read again to look for new updates from repositories.
     */
    @Override
    public void refresh() {
        plugins = null;
    }

    @Override
    public FileDownloader getFileDownloader() {
        return new SimpleFileDownloader();
    }

    /**
     * Gets a file verifier to execute on the downloaded file for it to be claimed valid.
     * May be a CompoundVerifier in order to chain several verifiers.
     * @return list of {@link FileVerifier}s
     */
    @Override
    public FileVerifier getFileVerfier() {
        return new CompoundVerifier();
    }

    /**
     * Gets the plugins json file name. Returns {@code plugins.json} if null.
     *
     * @return the plugins json file name
     */
    public String getPluginsJsonFileName() {
        if (pluginsJsonFileName == null) {
            pluginsJsonFileName = DEFAULT_PLUGINS_JSON_FILENAME;
        }

        return pluginsJsonFileName;
    }

    /**
     * Choose another file name than {@code plugins.json}.
     *
     * @param pluginsJsonFileName the name (relative) of plugins.json file. 
	 * If null, will default to {@code plugins.json}
     */
    public void setPluginsJsonFileName(String pluginsJsonFileName) {
        this.pluginsJsonFileName = pluginsJsonFileName;
    }

}

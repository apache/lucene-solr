/*
 * Copyright 2017 Decebal Suiu
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Decebal Suiu
 */
public class CompoundPluginLoader implements PluginLoader {

    private static final Logger log = LoggerFactory.getLogger(CompoundPluginLoader.class);

    private List<PluginLoader> loaders = new ArrayList<>();

    public CompoundPluginLoader add(PluginLoader loader) {
        if (loader == null) {
            throw new IllegalArgumentException("null not allowed");
        }

        loaders.add(loader);

        return this;
    }

    public int size() {
        return loaders.size();
    }

    @Override
    public boolean isApplicable(Path pluginPath) {
        for (PluginLoader loader : loaders) {
            if (loader.isApplicable(pluginPath)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public ClassLoader loadPlugin(Path pluginPath, PluginDescriptor pluginDescriptor) {
        for (PluginLoader loader : loaders) {
            if (loader.isApplicable(pluginPath)) {
                log.debug("'{}' is applicable for plugin '{}'", loader, pluginPath);
                try {
                    ClassLoader classLoader = loader.loadPlugin(pluginPath, pluginDescriptor);
                    if (classLoader != null) {
                        return classLoader;
                    }
                } catch (Exception e) {
                    // log the exception and continue with the next loader
                    log.error(e.getMessage()); // ?!
                }
            } else {
                log.debug("'{}' is not applicable for plugin '{}'", loader, pluginPath);
            }
        }

        throw new RuntimeException("No PluginLoader for plugin '" + pluginPath + "' and descriptor '" + pluginDescriptor + "'");
    }

}

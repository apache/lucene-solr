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
package org.apache.solr.packagemanager.pf4j;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * Provides the functionality for plugin management such as load,
 * start and stop the plugins.
 *
 * @author Decebal Suiu
 */
public interface PluginManager {

    /**
     * Retrieves all plugins.
     */
    List<PluginWrapper> getPlugins();

    /**
     * Retrieves all plugins with this state.
     */
    List<PluginWrapper> getPlugins(PluginState pluginState);

    /**
     * Retrieves all resolved plugins (with resolved dependency).
     */
    List<PluginWrapper> getResolvedPlugins();

  /**
   * Retrieves all unresolved plugins (with unresolved dependency).
   */
    List<PluginWrapper> getUnresolvedPlugins();

    /**
     * Retrieves all started plugins.
     */
    List<PluginWrapper> getStartedPlugins();

    /**
     * Retrieves the plugin with this id, or null if the plugin does not exist.
     *
     * @param pluginId The pluginId for the plugin you are trying to get.
     * @return A PluginWrapper object for this plugin, or null if it does not exist.
     */
    PluginWrapper getPlugin(String pluginId);

    /**
     * Load plugins.
     */
    void loadPlugins();

    /**
     * Load a plugin.
     *
     * @return the pluginId of the installed plugin or null
     */
  String loadPlugin(Path pluginPath);

    /**
     * Start all active plugins.
     */
    void startPlugins();

    /**
     * Start the specified plugin and it's dependencies.
     *
     * @return the plugin state
     */
    PluginState startPlugin(String pluginId);

    /**
     * Stop all active plugins.
     */
    void stopPlugins();

    /**
     * Stop the specified plugin and it's dependencies.
     *
     * @return the plugin state
     */
    PluginState stopPlugin(String pluginId);

    /**
     * Unload a plugin.
     *
     * @return true if the plugin was unloaded
     */
    boolean unloadPlugin(String pluginId);

    /**
     * Disables a plugin from being loaded.
     *
     * @return true if plugin is disabled
     */
    boolean disablePlugin(String pluginId);

    /**
     * Enables a plugin that has previously been disabled.
     *
     * @return true if plugin is enabled
     */
    boolean enablePlugin(String pluginId);

    /**
     * Deletes a plugin.
     *
     * @return true if the plugin was deleted
     */
    boolean deletePlugin(String pluginId);

  ClassLoader getPluginClassLoader(String pluginId);

    <T> List<Class<T>> getExtensionClasses(Class<T> type);

    <T> List<Class<T>> getExtensionClasses(Class<T> type, String pluginId);

  <T> List<T> getExtensions(Class<T> type);

    <T> List<T> getExtensions(Class<T> type, String pluginId);

    List getExtensions(String pluginId);

    Set<String> getExtensionClassNames(String pluginId);

    ExtensionFactory getExtensionFactory();

    /**
   * The runtime mode. Must currently be either DEVELOPMENT or DEPLOYMENT.
   */
  RuntimeMode getRuntimeMode();

    /**
     * Retrieves the {@link PluginWrapper} that loaded the given class 'clazz'.
     */
    PluginWrapper whichPlugin(Class<?> clazz);

    void addPluginStateListener(PluginStateListener listener);

    void removePluginStateListener(PluginStateListener listener);

    /**
     * Set the system version.  This is used to compare against the plugin
     * requires attribute.  The default system version is 0.0.0 which
     * disables all version checking.
     *
     * @default 0.0.0
     */
    void setSystemVersion(String version);

    /**
     * Returns the system version.
     *
     * @return the system version
     */
    String getSystemVersion();

    /**
     * Gets the path of the folder where plugins are installed
     * @return Path of plugins root
     */
    Path getPluginsRoot();

    VersionManager getVersionManager();

}

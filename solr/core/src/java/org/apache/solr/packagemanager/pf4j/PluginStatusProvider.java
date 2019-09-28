/*
 * Copyright 2012 Decebal Suiu
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

/**
 * @author Decebal Suiu
 * @author Mário Franco
 */
public interface PluginStatusProvider {

    /**
     * Checks if the plugin is disabled or not
     *
     * @param pluginId the plugin id
     * @return if the plugin is disabled or not
     */
    boolean isPluginDisabled(String pluginId);

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

}

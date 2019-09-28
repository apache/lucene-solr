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

import java.nio.file.Path;
import java.util.List;

/**
 * Directory that contains plugins. A plugin could be a zip file.
 *
 * @author Decebal Suiu
 * @author Mário Franco
 */
public interface PluginRepository {

    /**
     * List all plugin paths.
     *
     * @return a list of files
     */
    List<Path> getPluginPaths();

    /**
     * Removes a plugin from the repository.
     *
     * @param pluginPath the plugin path
     * @return true if deleted
     */
    boolean deletePluginPath(Path pluginPath);

}

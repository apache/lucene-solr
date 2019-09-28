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

import java.util.List;
import java.util.Set;

/**
 * @author Decebal Suiu
 */
public interface ExtensionFinder {

    /**
     * Retrieves a list with all extensions found for an extension point.
     */
    <T> List<ExtensionWrapper<T>> find(Class<T> type);

    /**
     * Retrieves a list with all extensions found for an extension point and a plugin.
     */
    <T> List<ExtensionWrapper<T>> find(Class<T> type, String pluginId);

    /**
     * Retrieves a list with all extensions found for a plugin
     */
    List<ExtensionWrapper> find(String pluginId);

    /**
     * Retrieves a list with all extension class names found for a plugin.
     */
    Set<String> findClassNames(String pluginId);

}

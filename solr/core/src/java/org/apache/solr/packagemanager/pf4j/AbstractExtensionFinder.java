/*
 * Copyright 2015 Decebal Suiu
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

import org.pf4j.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Decebal Suiu
 */
public abstract class AbstractExtensionFinder implements ExtensionFinder, PluginStateListener {

    private static final Logger log = LoggerFactory.getLogger(AbstractExtensionFinder.class);

    protected PluginManager pluginManager;
    protected volatile Map<String, Set<String>> entries; // cache by pluginId

    public AbstractExtensionFinder(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    public abstract Map<String, Set<String>> readPluginsStorages();

    public abstract Map<String, Set<String>> readClasspathStorages();

    @Override
    @SuppressWarnings("unchecked")
	public <T> List<ExtensionWrapper<T>> find(Class<T> type) {
		log.debug("Finding extensions of extension point '{}'", type.getName());
        Map<String, Set<String>> entries = getEntries();
        List<ExtensionWrapper<T>> result = new ArrayList<>();

        // add extensions found in classpath and plugins
        for (String pluginId : entries.keySet()) {
            // classpath's extensions <=> pluginId = null
            List<ExtensionWrapper<T>> pluginExtensions = find(type, pluginId);
            result.addAll(pluginExtensions);
        }

        if (entries.isEmpty()) {
        	log.debug("No extensions found for extension point '{}'", type.getName());
        } else {
        	log.debug("Found {} extensions for extension point '{}'", result.size(), type.getName());
        }

        // sort by "ordinal" property
        Collections.sort(result);

		return result;
	}

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<ExtensionWrapper<T>> find(Class<T> type, String pluginId) {
        log.debug("Finding extensions of extension point '{}' for plugin '{}'", type.getName(), pluginId);
        List<ExtensionWrapper<T>> result = new ArrayList<>();

        // classpath's extensions <=> pluginId = null
        Set<String> classNames = findClassNames(pluginId);
        if (classNames == null || classNames.isEmpty()) {
            return result;
        }

        if (pluginId != null) {
            PluginWrapper pluginWrapper = pluginManager.getPlugin(pluginId);
            if (PluginState.STARTED != pluginWrapper.getPluginState()) {
                return result;
            }

            log.trace("Checking extensions from plugin '{}'", pluginId);
        } else {
            log.trace("Checking extensions from classpath");
        }

        ClassLoader classLoader = (pluginId != null) ? pluginManager.getPluginClassLoader(pluginId) : getClass().getClassLoader();

        for (String className : classNames) {
            try {
                log.debug("Loading class '{}' using class loader '{}'", className, classLoader);
                Class<?> extensionClass = classLoader.loadClass(className);

                log.debug("Checking extension type '{}'", className);
                if (type.isAssignableFrom(extensionClass)) {
                    ExtensionWrapper extensionWrapper = createExtensionWrapper(extensionClass);
                    result.add(extensionWrapper);
                    log.debug("Added extension '{}' with ordinal {}", className, extensionWrapper.getOrdinal());
                } else {
                    log.trace("'{}' is not an extension for extension point '{}'", className, type.getName());
                    if (RuntimeMode.DEVELOPMENT.equals(pluginManager.getRuntimeMode())) {
                        checkDifferentClassLoaders(type, extensionClass);
                    }
                }
            } catch (ClassNotFoundException e) {
                log.error(e.getMessage(), e);
            }
        }

        if (result.isEmpty()) {
            log.debug("No extensions found for extension point '{}'", type.getName());
        } else {
            log.debug("Found {} extensions for extension point '{}'", result.size(), type.getName());
        }

        // sort by "ordinal" property
        Collections.sort(result);

        return result;
    }

    @Override
	public List<ExtensionWrapper> find(String pluginId) {
        log.debug("Finding extensions from plugin '{}'", pluginId);
        List<ExtensionWrapper> result = new ArrayList<>();

	    Set<String> classNames = findClassNames(pluginId);
        if (classNames.isEmpty()) {
            return result;
        }

        if (pluginId != null) {
            PluginWrapper pluginWrapper = pluginManager.getPlugin(pluginId);
            if (PluginState.STARTED != pluginWrapper.getPluginState()) {
                return result;
            }

            log.trace("Checking extensions from plugin '{}'", pluginId);
        } else {
            log.trace("Checking extensions from classpath");
        }

        ClassLoader classLoader = (pluginId != null) ? pluginManager.getPluginClassLoader(pluginId) : getClass().getClassLoader();

        for (String className : classNames) {
            try {
                log.debug("Loading class '{}' using class loader '{}'", className, classLoader);
                Class<?> extensionClass = classLoader.loadClass(className);

                ExtensionWrapper extensionWrapper = createExtensionWrapper(extensionClass);
                result.add(extensionWrapper);
                log.debug("Added extension '{}' with ordinal {}", className, extensionWrapper.getOrdinal());
            } catch (ClassNotFoundException e) {
                log.error(e.getMessage(), e);
            }
        }

        if (result.isEmpty()) {
            log.debug("No extensions found for plugin '{}'", pluginId);
        } else {
            log.debug("Found {} extensions for plugin '{}'", result.size(), pluginId);
        }

        // sort by "ordinal" property
        Collections.sort(result);

        return result;
    }

    @Override
    public Set<String> findClassNames(String pluginId) {
        return getEntries().get(pluginId);
    }

    @Override
	public void pluginStateChanged(PluginStateEvent event) {
        // TODO optimize (do only for some transitions)
        // clear cache
        entries = null;
    }

    protected void debugExtensions(Set<String> extensions) {
        if (log.isDebugEnabled()) {
            if (extensions.isEmpty()) {
                log.debug("No extensions found");
            } else {
                log.debug("Found possible {} extensions:", extensions.size());
                for (String extension : extensions) {
                    log.debug("   " + extension);
                }
            }
        }
    }

    private Map<String, Set<String>> readStorages() {
        Map<String, Set<String>> result = new LinkedHashMap<>();

        result.putAll(readClasspathStorages());
        result.putAll(readPluginsStorages());

        return result;
    }

    private Map<String, Set<String>> getEntries() {
        if (entries == null) {
            entries = readStorages();
        }

        return entries;
    }

    private ExtensionWrapper createExtensionWrapper(Class<?> extensionClass) {
        int ordinal = 0;
        if (extensionClass.isAnnotationPresent(Extension.class)) {
            ordinal = extensionClass.getAnnotation(Extension.class).ordinal();
        }
        ExtensionDescriptor descriptor = new ExtensionDescriptor(ordinal, extensionClass);

        return new ExtensionWrapper<>(descriptor, pluginManager.getExtensionFactory());
    }

    private void checkDifferentClassLoaders(Class<?> type, Class<?> extensionClass) {
        ClassLoader typeClassLoader = type.getClassLoader(); // class loader of extension point
        ClassLoader extensionClassLoader = extensionClass.getClassLoader();
        boolean match = ClassUtils.getAllInterfacesNames(extensionClass).contains(type.getSimpleName());
        if (match && !extensionClassLoader.equals(typeClassLoader)) {
            // in this scenario the method 'isAssignableFrom' returns only FALSE
            // see http://www.coderanch.com/t/557846/java/java/FWIW-FYI-isAssignableFrom-isInstance-differing
            log.error("Different class loaders: '{}' (E) and '{}' (EP)", extensionClassLoader, typeClassLoader);
        }
    }

}

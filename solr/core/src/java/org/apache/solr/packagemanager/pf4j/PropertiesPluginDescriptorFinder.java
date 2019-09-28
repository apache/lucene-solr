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

import org.pf4j.util.FileUtils;
import org.pf4j.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Find a plugin descriptor in a properties file (in plugin repository).
 *
 * @author Decebal Suiu
 */
public class PropertiesPluginDescriptorFinder implements PluginDescriptorFinder {

	private static final Logger log = LoggerFactory.getLogger(PropertiesPluginDescriptorFinder.class);

	private static final String DEFAULT_PROPERTIES_FILE_NAME = "plugin.properties";

	protected String propertiesFileName;

	public PropertiesPluginDescriptorFinder() {
		this(DEFAULT_PROPERTIES_FILE_NAME);
	}

	public PropertiesPluginDescriptorFinder(String propertiesFileName) {
        this.propertiesFileName = propertiesFileName;
	}

    @Override
    public boolean isApplicable(Path pluginPath) {
        return Files.exists(pluginPath) && (Files.isDirectory(pluginPath) || FileUtils.isJarFile(pluginPath));
    }

    @Override
	public PluginDescriptor find(Path pluginPath) throws PluginException {
        Properties properties = readProperties(pluginPath);

        return createPluginDescriptor(properties);
	}

    protected Properties readProperties(Path pluginPath) throws PluginException {
        Path propertiesPath = getPropertiesPath(pluginPath, propertiesFileName);
        if (propertiesPath == null) {
            throw new PluginException("Cannot find the properties path");
        }

        log.debug("Lookup plugin descriptor in '{}'", propertiesPath);
        if (Files.notExists(propertiesPath)) {
            throw new PluginException("Cannot find '{}' path", propertiesPath);
        }

        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(propertiesPath)) {
            properties.load(input);
        } catch (IOException e) {
            throw new PluginException(e);
        }

        return properties;
    }

    protected Path getPropertiesPath(Path pluginPath, String propertiesFileName) throws PluginException {
	    if (Files.isDirectory(pluginPath)) {
            return pluginPath.resolve(Paths.get(propertiesFileName));
        } else {
	        // it's a jar file
            try {
                return FileUtils.getPath(pluginPath, propertiesFileName);
            } catch (IOException e) {
                throw new PluginException(e);
            }
        }
    }

    protected PluginDescriptor createPluginDescriptor(Properties properties) {
        DefaultPluginDescriptor pluginDescriptor = createPluginDescriptorInstance();

        // TODO validate !!!
        String id = properties.getProperty("plugin.id");
        pluginDescriptor.setPluginId(id);

        String description = properties.getProperty("plugin.description");
        if (StringUtils.isNullOrEmpty(description)) {
            pluginDescriptor.setPluginDescription("");
        } else {
            pluginDescriptor.setPluginDescription(description);
        }

        String clazz = properties.getProperty("plugin.class");
        pluginDescriptor.setPluginClass(clazz);

        String version = properties.getProperty("plugin.version");
        if (StringUtils.isNotNullOrEmpty(version)) {
            pluginDescriptor.setPluginVersion(version);
        }

        String provider = properties.getProperty("plugin.provider");
        pluginDescriptor.setProvider(provider);

        String dependencies = properties.getProperty("plugin.dependencies");
        pluginDescriptor.setDependencies(dependencies);

        String requires = properties.getProperty("plugin.requires");
        if (StringUtils.isNotNullOrEmpty(requires)) {
            pluginDescriptor.setRequires(requires);
        }

        pluginDescriptor.setLicense(properties.getProperty("plugin.license"));

        return pluginDescriptor;
    }

    protected DefaultPluginDescriptor createPluginDescriptorInstance() {
        return new DefaultPluginDescriptor();
    }

}

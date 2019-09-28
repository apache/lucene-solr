/*
 * Copyright 2016 Decebal Suiu
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
import org.pf4j.util.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements the boilerplate plugin code that any {@link PluginManager}
 * implementation would have to support.
 * It helps cut the noise out of the subclass that handles plugin management.
 *
 * @author Decebal Suiu
 */
public abstract class AbstractPluginManager implements PluginManager {

    private static final Logger log = LoggerFactory.getLogger(AbstractPluginManager.class);

    private Path pluginsRoot;

    protected ExtensionFinder extensionFinder;

    private PluginDescriptorFinder pluginDescriptorFinder;

    /*
     * A map of plugins this manager is responsible for (the key is the 'pluginId').
     */
    protected Map<String, PluginWrapper> plugins;

    /*
     * A map of plugin class loaders (the key is the 'pluginId').
     */
    private Map<String, ClassLoader> pluginClassLoaders;

    /*
     * A list with unresolved plugins (unresolved dependency).
     */
    private List<PluginWrapper> unresolvedPlugins;

    /**
     * A list with all resolved plugins (resolved dependency).
     */
    private List<PluginWrapper> resolvedPlugins;

    /*
     * A list with started plugins.
     */
    private List<PluginWrapper> startedPlugins;

    /*
     * The registered {@link PluginStateListener}s.
     */
    private List<PluginStateListener> pluginStateListeners;

    /*
     * Cache value for the runtime mode.
     * No need to re-read it because it wont change at runtime.
     */
    private RuntimeMode runtimeMode;

    /*
     * The system version used for comparisons to the plugin requires attribute.
     */
    private String systemVersion = "0.0.0";

    private PluginRepository pluginRepository;
    private PluginFactory pluginFactory;
    private ExtensionFactory extensionFactory;
    private PluginStatusProvider pluginStatusProvider;
    private DependencyResolver dependencyResolver;
    private PluginLoader pluginLoader;
    private boolean exactVersionAllowed = false;

    private VersionManager versionManager;

    /**
     * The plugins root is supplied by {@code System.getProperty("pf4j.pluginsDir", "plugins")}.
     */
    public AbstractPluginManager() {
        initialize();
    }

    /**
     * Constructs {@code AbstractPluginManager} with the given plugins root.
     *
     * @param pluginsRoot the root to search for plugins
     */
    public AbstractPluginManager(Path pluginsRoot) {
        this.pluginsRoot = pluginsRoot;

        initialize();
    }

    @Override
    public void setSystemVersion(String version) {
        systemVersion = version;
    }

    @Override
    public String getSystemVersion() {
        return systemVersion;
    }

    @Override
    public List<PluginWrapper> getPlugins() {
        return new ArrayList<>(plugins.values());
    }

    @Override
    public List<PluginWrapper> getPlugins(PluginState pluginState) {
        List<PluginWrapper> plugins = new ArrayList<>();
        for (PluginWrapper plugin : getPlugins()) {
            if (pluginState.equals(plugin.getPluginState())) {
                plugins.add(plugin);
            }
        }

        return plugins;
    }

    @Override
    public List<PluginWrapper> getResolvedPlugins() {
        return resolvedPlugins;
    }

    @Override
    public List<PluginWrapper> getUnresolvedPlugins() {
        return unresolvedPlugins;
    }

    @Override
    public List<PluginWrapper> getStartedPlugins() {
        return startedPlugins;
    }

    @Override
    public PluginWrapper getPlugin(String pluginId) {
        return plugins.get(pluginId);
    }

    @Override
    public String loadPlugin(Path pluginPath) {
        if ((pluginPath == null) || Files.notExists(pluginPath)) {
            throw new IllegalArgumentException(String.format("Specified plugin %s does not exist!", pluginPath));
        }

        log.debug("Loading plugin from '{}'", pluginPath);

        try {
            PluginWrapper pluginWrapper = loadPluginFromPath(pluginPath);

            // try to resolve  the loaded plugin together with other possible plugins that depend on this plugin
            resolvePlugins();

            return pluginWrapper.getDescriptor().getPluginId();
        } catch (PluginException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    /**
     * Load plugins.
     */
    @Override
    public void loadPlugins() {
        log.debug("Lookup plugins in '{}'", pluginsRoot);
        // check for plugins root
        if (Files.notExists(pluginsRoot) || !Files.isDirectory(pluginsRoot)) {
            log.warn("No '{}' root", pluginsRoot);
            return;
        }

        // get all plugin paths from repository
        List<Path> pluginPaths = pluginRepository.getPluginPaths();

        // check for no plugins
        if (pluginPaths.isEmpty()) {
            log.info("No plugins");
            return;
        }

        log.debug("Found {} possible plugins: {}", pluginPaths.size(), pluginPaths);

        // load plugins from plugin paths
        for (Path pluginPath : pluginPaths) {
            try {
                loadPluginFromPath(pluginPath);
            } catch (PluginException e) {
                log.error(e.getMessage(), e);
            }
        }

        // resolve plugins
        try {
            resolvePlugins();
        } catch (PluginException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Unload the specified plugin and it's dependents.
     */
    @Override
    public boolean unloadPlugin(String pluginId) {
        return unloadPlugin(pluginId, true);
    }

    private boolean unloadPlugin(String pluginId, boolean unloadDependents) {
        try {
            if (unloadDependents) {
                List<String> dependents = dependencyResolver.getDependents(pluginId);
                while (!dependents.isEmpty()) {
                    String dependent = dependents.remove(0);
                    unloadPlugin(dependent, false);
                    dependents.addAll(0, dependencyResolver.getDependents(dependent));
                }
            }

            PluginState pluginState = stopPlugin(pluginId, false);
            if (PluginState.STARTED == pluginState) {
                return false;
            }

            PluginWrapper pluginWrapper = getPlugin(pluginId);
            log.info("Unload plugin '{}'", getPluginLabel(pluginWrapper.getDescriptor()));

            // remove the plugin
            plugins.remove(pluginId);
            getResolvedPlugins().remove(pluginWrapper);

            firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));

            // remove the classloader
            Map<String, ClassLoader> pluginClassLoaders = getPluginClassLoaders();
            if (pluginClassLoaders.containsKey(pluginId)) {
                ClassLoader classLoader = pluginClassLoaders.remove(pluginId);
                if (classLoader instanceof Closeable) {
                    try {
                        ((Closeable) classLoader).close();
                    } catch (IOException e) {
                        log.error("Cannot close classloader", e);
                    }
                }
            }

            return true;
        } catch (IllegalArgumentException e) {
            // ignore not found exceptions because this method is recursive
        }

        return false;
    }

    @Override
    public boolean deletePlugin(String pluginId) {
        checkPluginId(pluginId);

        PluginWrapper pluginWrapper = getPlugin(pluginId);
        PluginState pluginState = stopPlugin(pluginId);
        if (PluginState.STARTED == pluginState) {
            log.error("Failed to stop plugin '{}' on delete", pluginId);
            return false;
        }

        if (!unloadPlugin(pluginId)) {
            log.error("Failed to unload plugin '{}' on delete", pluginId);
            return false;
        }

        Path pluginPath = pluginWrapper.getPluginPath();

        return pluginRepository.deletePluginPath(pluginPath);
    }

    /**
     * Start all active plugins.
     */
    @Override
    public void startPlugins() {
        for (PluginWrapper pluginWrapper : resolvedPlugins) {
            PluginState pluginState = pluginWrapper.getPluginState();
            if ((PluginState.DISABLED != pluginState) && (PluginState.STARTED != pluginState)) {
                try {
                    log.info("Start plugin '{}'", getPluginLabel(pluginWrapper.getDescriptor()));
                    pluginWrapper.getPlugin().start();
                    pluginWrapper.setPluginState(PluginState.STARTED);
                    startedPlugins.add(pluginWrapper);

                    firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));
                } catch (PluginException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Start the specified plugin and it's dependencies.
     */
    @Override
    public PluginState startPlugin(String pluginId) {
        checkPluginId(pluginId);

        PluginWrapper pluginWrapper = getPlugin(pluginId);
        PluginDescriptor pluginDescriptor = pluginWrapper.getDescriptor();
        PluginState pluginState = pluginWrapper.getPluginState();
        if (PluginState.STARTED == pluginState) {
            log.debug("Already started plugin '{}'", getPluginLabel(pluginDescriptor));
            return PluginState.STARTED;
        }

        if (!resolvedPlugins.contains(pluginWrapper)) {
            log.warn("Cannot start an unresolved plugin '{}'", getPluginLabel(pluginDescriptor));
            return pluginState;
        }

        if (PluginState.DISABLED == pluginState) {
            // automatically enable plugin on manual plugin start
            if (!enablePlugin(pluginId)) {
                return pluginState;
            }
        }

        for (PluginDependency dependency : pluginDescriptor.getDependencies()) {
            startPlugin(dependency.getPluginId());
        }

        try {
            log.info("Start plugin '{}'", getPluginLabel(pluginDescriptor));
            pluginWrapper.getPlugin().start();
            pluginWrapper.setPluginState(PluginState.STARTED);
            startedPlugins.add(pluginWrapper);

            firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));
        } catch (PluginException e) {
            log.error(e.getMessage(), e);
        }

        return pluginWrapper.getPluginState();
    }

    /**
     * Stop all active plugins.
     */
    @Override
    public void stopPlugins() {
        // stop started plugins in reverse order
        Collections.reverse(startedPlugins);
        Iterator<PluginWrapper> itr = startedPlugins.iterator();
        while (itr.hasNext()) {
            PluginWrapper pluginWrapper = itr.next();
            PluginState pluginState = pluginWrapper.getPluginState();
            if (PluginState.STARTED == pluginState) {
                try {
                    log.info("Stop plugin '{}'", getPluginLabel(pluginWrapper.getDescriptor()));
                    pluginWrapper.getPlugin().stop();
                    pluginWrapper.setPluginState(PluginState.STOPPED);
                    itr.remove();

                    firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));
                } catch (PluginException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Stop the specified plugin and it's dependents.
     */
    @Override
    public PluginState stopPlugin(String pluginId) {
        return stopPlugin(pluginId, true);
    }

    private PluginState stopPlugin(String pluginId, boolean stopDependents) {
        checkPluginId(pluginId);

        PluginWrapper pluginWrapper = getPlugin(pluginId);
        PluginDescriptor pluginDescriptor = pluginWrapper.getDescriptor();
        PluginState pluginState = pluginWrapper.getPluginState();
        if (PluginState.STOPPED == pluginState) {
            log.debug("Already stopped plugin '{}'", getPluginLabel(pluginDescriptor));
            return PluginState.STOPPED;
        }

        // test for disabled plugin
        if (PluginState.DISABLED == pluginState) {
            // do nothing
            return pluginState;
        }

        if (stopDependents) {
            List<String> dependents = dependencyResolver.getDependents(pluginId);
            while (!dependents.isEmpty()) {
                String dependent = dependents.remove(0);
                stopPlugin(dependent, false);
                dependents.addAll(0, dependencyResolver.getDependents(dependent));
            }
        }

        try {
            log.info("Stop plugin '{}'", getPluginLabel(pluginDescriptor));
            pluginWrapper.getPlugin().stop();
            pluginWrapper.setPluginState(PluginState.STOPPED);
            startedPlugins.remove(pluginWrapper);

            firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));
        } catch (PluginException e) {
            log.error(e.getMessage(), e);
        }

        return pluginWrapper.getPluginState();
    }

    private void checkPluginId(String pluginId) {
        if (!plugins.containsKey(pluginId)) {
            throw new IllegalArgumentException(String.format("Unknown pluginId %s", pluginId));
        }
    }

    @Override
    public boolean disablePlugin(String pluginId) {
        checkPluginId(pluginId);

        PluginWrapper pluginWrapper = getPlugin(pluginId);
        PluginDescriptor pluginDescriptor = pluginWrapper.getDescriptor();
        PluginState pluginState = pluginWrapper.getPluginState();
        if (PluginState.DISABLED == pluginState) {
            log.debug("Already disabled plugin '{}'", getPluginLabel(pluginDescriptor));
            return true;
        }

        if (PluginState.STOPPED == stopPlugin(pluginId)) {
            pluginWrapper.setPluginState(PluginState.DISABLED);

            firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, PluginState.STOPPED));

            if (!pluginStatusProvider.disablePlugin(pluginId)) {
                return false;
            }

            log.info("Disabled plugin '{}'", getPluginLabel(pluginDescriptor));

            return true;
        }

        return false;
    }

    @Override
    public boolean enablePlugin(String pluginId) {
        checkPluginId(pluginId);

        PluginWrapper pluginWrapper = getPlugin(pluginId);
        if (!isPluginValid(pluginWrapper)) {
            log.warn("Plugin '{}' can not be enabled", getPluginLabel(pluginWrapper.getDescriptor()));
            return false;
        }

        PluginDescriptor pluginDescriptor = pluginWrapper.getDescriptor();
        PluginState pluginState = pluginWrapper.getPluginState();
        if (PluginState.DISABLED != pluginState) {
            log.debug("Plugin '{}' is not disabled", getPluginLabel(pluginDescriptor));
            return true;
        }

        if (!pluginStatusProvider.enablePlugin(pluginId)) {
            return false;
        }

        pluginWrapper.setPluginState(PluginState.CREATED);

        firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));

        log.info("Enabled plugin '{}'", getPluginLabel(pluginDescriptor));

        return true;
    }

    /**
     * Get the {@link ClassLoader} for plugin.
     */
    @Override
    public ClassLoader getPluginClassLoader(String pluginId) {
        return pluginClassLoaders.get(pluginId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<Class<T>> getExtensionClasses(Class<T> type) {
        List<ExtensionWrapper<T>> extensionsWrapper = extensionFinder.find(type);
        List<Class<T>> extensionClasses = new ArrayList<>(extensionsWrapper.size());
        for (ExtensionWrapper<T> extensionWrapper : extensionsWrapper) {
            Class<T> c = (Class<T>) extensionWrapper.getDescriptor().extensionClass;
            extensionClasses.add(c);
        }

        return extensionClasses;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<Class<T>> getExtensionClasses(Class<T> type, String pluginId) {
        List<ExtensionWrapper<T>> extensionsWrapper = extensionFinder.find(type, pluginId);
        List<Class<T>> extensionClasses = new ArrayList<>(extensionsWrapper.size());
        for (ExtensionWrapper<T> extensionWrapper : extensionsWrapper) {
            Class<T> c = (Class<T>) extensionWrapper.getDescriptor().extensionClass;
            extensionClasses.add(c);
        }

        return extensionClasses;
    }

    @Override
    public <T> List<T> getExtensions(Class<T> type) {
        List<ExtensionWrapper<T>> extensionsWrapper = extensionFinder.find(type);
        List<T> extensions = new ArrayList<>(extensionsWrapper.size());
        for (ExtensionWrapper<T> extensionWrapper : extensionsWrapper) {
            extensions.add(extensionWrapper.getExtension());
        }

        return extensions;
    }

    @Override
    public <T> List<T> getExtensions(Class<T> type, String pluginId) {
        List<ExtensionWrapper<T>> extensionsWrapper = extensionFinder.find(type, pluginId);
        List<T> extensions = new ArrayList<>(extensionsWrapper.size());
        for (ExtensionWrapper<T> extensionWrapper : extensionsWrapper) {
            extensions.add(extensionWrapper.getExtension());
        }

        return extensions;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List getExtensions(String pluginId) {
        List<ExtensionWrapper> extensionsWrapper = extensionFinder.find(pluginId);
        List extensions = new ArrayList<>(extensionsWrapper.size());
        for (ExtensionWrapper extensionWrapper : extensionsWrapper) {
            extensions.add(extensionWrapper.getExtension());
        }

        return extensions;
    }

    @Override
    public Set<String> getExtensionClassNames(String pluginId) {
        return extensionFinder.findClassNames(pluginId);
    }

    @Override
    public ExtensionFactory getExtensionFactory() {
        return extensionFactory;
    }

    // TODO remove
    public PluginLoader getPluginLoader() {
        return pluginLoader;
    }

    public Path getPluginsRoot() {
        return pluginsRoot;
    }

    @Override
    public RuntimeMode getRuntimeMode() {
        if (runtimeMode == null) {
            // retrieves the runtime mode from system
            String modeAsString = System.getProperty("pf4j.mode", RuntimeMode.DEPLOYMENT.toString());
            runtimeMode = RuntimeMode.byName(modeAsString);
        }

        return runtimeMode;
    }

    @Override
    public PluginWrapper whichPlugin(Class<?> clazz) {
        ClassLoader classLoader = clazz.getClassLoader();
        for (PluginWrapper plugin : resolvedPlugins) {
            if (plugin.getPluginClassLoader() == classLoader) {
                return plugin;
            }
        }

        return null;
    }

    @Override
    public synchronized void addPluginStateListener(PluginStateListener listener) {
        pluginStateListeners.add(listener);
    }

    @Override
    public synchronized void removePluginStateListener(PluginStateListener listener) {
        pluginStateListeners.remove(listener);
    }

    public String getVersion() {
        String version = null;

        Package pf4jPackage = PluginManager.class.getPackage();
        if (pf4jPackage != null) {
            version = pf4jPackage.getImplementationVersion();
            if (version == null) {
                version = pf4jPackage.getSpecificationVersion();
            }
        }

        return (version != null) ? version : "0.0.0";
    }

    protected abstract PluginRepository createPluginRepository();

    protected abstract PluginFactory createPluginFactory();

    protected abstract ExtensionFactory createExtensionFactory();

    protected abstract PluginDescriptorFinder createPluginDescriptorFinder();

    protected abstract ExtensionFinder createExtensionFinder();

    protected abstract PluginStatusProvider createPluginStatusProvider();

    protected abstract PluginLoader createPluginLoader();

    protected abstract VersionManager createVersionManager();

    protected PluginDescriptorFinder getPluginDescriptorFinder() {
        return pluginDescriptorFinder;
    }

    protected PluginFactory getPluginFactory() {
        return pluginFactory;
    }

    protected Map<String, ClassLoader> getPluginClassLoaders() {
        return pluginClassLoaders;
    }

    protected void initialize() {
        plugins = new HashMap<>();
        pluginClassLoaders = new HashMap<>();
        unresolvedPlugins = new ArrayList<>();
        resolvedPlugins = new ArrayList<>();
        startedPlugins = new ArrayList<>();

        pluginStateListeners = new ArrayList<>();

        if (pluginsRoot == null) {
            pluginsRoot = createPluginsRoot();
        }

        System.setProperty("pf4j.pluginsDir", pluginsRoot.toString());

        pluginRepository = createPluginRepository();
        pluginFactory = createPluginFactory();
        extensionFactory = createExtensionFactory();
        pluginDescriptorFinder = createPluginDescriptorFinder();
        extensionFinder = createExtensionFinder();
        pluginStatusProvider = createPluginStatusProvider();
        pluginLoader = createPluginLoader();

        versionManager = createVersionManager();
        dependencyResolver = new DependencyResolver(versionManager);
    }

    /**
     * Add the possibility to override the plugins root.
     * If a {@code pf4j.pluginsDir} system property is defined than this method returns that root.
     * If {@link #getRuntimeMode()} returns {@link RuntimeMode#DEVELOPMENT} than {@code ../plugins}
     * is returned else this method returns {@code plugins}.
     *
     * @return the plugins root
     */
    protected Path createPluginsRoot() {
        String pluginsDir = System.getProperty("pf4j.pluginsDir");
        if (pluginsDir == null) {
            if (isDevelopment()) {
                pluginsDir = "../plugins";
            } else {
                pluginsDir = "plugins";
            }
        }

        return Paths.get(pluginsDir);
    }

    /**
     * Check if this plugin is valid (satisfies "requires" param) for a given system version.
     *
     * @param pluginWrapper the plugin to check
     * @return true if plugin satisfies the "requires" or if requires was left blank
     */
    protected boolean isPluginValid(PluginWrapper pluginWrapper) {
        String requires = pluginWrapper.getDescriptor().getRequires().trim();
        if (!isExactVersionAllowed() && requires.matches("^\\d+\\.\\d+\\.\\d+$")) {
            // If exact versions are not allowed in requires, rewrite to >= expression
            requires = ">=" + requires;
        }
        if (systemVersion.equals("0.0.0") || versionManager.checkVersionConstraint(systemVersion, requires)) {
            return true;
        }

        PluginDescriptor pluginDescriptor = pluginWrapper.getDescriptor();
        log.warn("Plugin '{}' requires a minimum system version of {}, and you have {}",
            getPluginLabel(pluginDescriptor),
            pluginWrapper.getDescriptor().getRequires(),
            getSystemVersion());

        return false;
    }

    protected boolean isPluginDisabled(String pluginId) {
        return pluginStatusProvider.isPluginDisabled(pluginId);
    }

    protected void resolvePlugins() throws PluginException {
        // retrieves the  plugins descriptors
        List<PluginDescriptor> descriptors = new ArrayList<>();
        for (PluginWrapper plugin : plugins.values()) {
            descriptors.add(plugin.getDescriptor());
        }

        DependencyResolver.Result result = dependencyResolver.resolve(descriptors);

        if (result.hasCyclicDependency()) {
            throw new DependencyResolver.CyclicDependencyException();
        }

        List<String> notFoundDependencies = result.getNotFoundDependencies();
        if (!notFoundDependencies.isEmpty()) {
            throw new DependencyResolver.DependenciesNotFoundException(notFoundDependencies);
        }

        List<DependencyResolver.WrongDependencyVersion> wrongVersionDependencies = result.getWrongVersionDependencies();
        if (!wrongVersionDependencies.isEmpty()) {
            throw new DependencyResolver.DependenciesWrongVersionException(wrongVersionDependencies);
        }

        List<String> sortedPlugins = result.getSortedPlugins();

        // move plugins from "unresolved" to "resolved"
        for (String pluginId : sortedPlugins) {
            PluginWrapper pluginWrapper = plugins.get(pluginId);
            if (unresolvedPlugins.remove(pluginWrapper)) {
                PluginState pluginState = pluginWrapper.getPluginState();
                pluginWrapper.setPluginState(PluginState.RESOLVED);

                resolvedPlugins.add(pluginWrapper);
                log.info("Plugin '{}' resolved", getPluginLabel(pluginWrapper.getDescriptor()));

                firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));
            }
        }
    }

    protected synchronized void firePluginStateEvent(PluginStateEvent event) {
        for (PluginStateListener listener : pluginStateListeners) {
            log.trace("Fire '{}' to '{}'", event, listener);
            listener.pluginStateChanged(event);
        }
    }

    protected PluginWrapper loadPluginFromPath(Path pluginPath) throws PluginException {
        // test for plugin duplication
        String pluginId = idForPath(pluginPath);
        if (pluginId != null) {
            log.warn("Plugin '{}' already loaded with id '{}'", pluginPath, pluginId);
            return null;
        }

        // retrieves the plugin descriptor
        PluginDescriptorFinder pluginDescriptorFinder = getPluginDescriptorFinder();
        log.debug("Use '{}' to find plugins descriptors", pluginDescriptorFinder);
        log.debug("Finding plugin descriptor for plugin '{}'", pluginPath);
        PluginDescriptor pluginDescriptor = pluginDescriptorFinder.find(pluginPath);
        validatePluginDescriptor(pluginDescriptor);
        log.debug("Found descriptor {}", pluginDescriptor);
        String pluginClassName = pluginDescriptor.getPluginClass();
        log.debug("Class '{}' for plugin '{}'",  pluginClassName, pluginPath);

        // load plugin
        log.debug("Loading plugin '{}'", pluginPath);
        ClassLoader pluginClassLoader = getPluginLoader().loadPlugin(pluginPath, pluginDescriptor);
        log.debug("Loaded plugin '{}' with class loader '{}'", pluginPath, pluginClassLoader);

        // create the plugin wrapper
        log.debug("Creating wrapper for plugin '{}'", pluginPath);
        PluginWrapper pluginWrapper = new PluginWrapper(this, pluginDescriptor, pluginPath, pluginClassLoader);
        pluginWrapper.setPluginFactory(getPluginFactory());
        pluginWrapper.setRuntimeMode(getRuntimeMode());

        // test for disabled plugin
        if (isPluginDisabled(pluginDescriptor.getPluginId())) {
            log.info("Plugin '{}' is disabled", pluginPath);
            pluginWrapper.setPluginState(PluginState.DISABLED);
        }

        // validate the plugin
        if (!isPluginValid(pluginWrapper)) {
            log.info("Plugin '{}' is disabled", pluginPath);
            pluginWrapper.setPluginState(PluginState.DISABLED);
        }

        log.debug("Created wrapper '{}' for plugin '{}'", pluginWrapper, pluginPath);

        pluginId = pluginDescriptor.getPluginId();

        // add plugin to the list with plugins
        plugins.put(pluginId, pluginWrapper);
        getUnresolvedPlugins().add(pluginWrapper);

        // add plugin class loader to the list with class loaders
        getPluginClassLoaders().put(pluginId, pluginClassLoader);

        return pluginWrapper;
    }

    /**
     * Tests for already loaded plugins on given path.
     *
     * @param pluginPath the path to investigate
     * @return id of plugin or null if not loaded
     */
    protected String idForPath(Path pluginPath) {
        for (PluginWrapper plugin : plugins.values()) {
            if (plugin.getPluginPath().equals(pluginPath)) {
                return plugin.getPluginId();
            }
        }

        return null;
    }

    /**
     * Override this to change the validation criteria.
     *
     * @param descriptor the plugin descriptor to validate
     * @throws PluginException if validation fails
     */
    protected void validatePluginDescriptor(PluginDescriptor descriptor) throws PluginException {
        if (StringUtils.isNullOrEmpty(descriptor.getPluginId())) {
            throw new PluginException("Field 'id' cannot be empty");
        }

        if (StringUtils.isNullOrEmpty(descriptor.getPluginClass())) {
            throw new PluginException("Field 'class' cannot be empty");
        }

        if (descriptor.getVersion() == null) {
            throw new PluginException("Field 'version' cannot be empty");
        }
    }

    // TODO add this method in PluginManager as default method for Java 8.
    protected boolean isDevelopment() {
        return RuntimeMode.DEVELOPMENT.equals(getRuntimeMode());
    }

    /**
     * @return true if exact versions in requires is allowed
     */
    public boolean isExactVersionAllowed() {
        return exactVersionAllowed;
    }

    /**
     * Set to true to allow requires expression to be exactly x.y.z.
     * The default is false, meaning that using an exact version x.y.z will
     *
     * @param exactVersionAllowed set to true or false
     */
    public void setExactVersionAllowed(boolean exactVersionAllowed) {
        this.exactVersionAllowed = exactVersionAllowed;
    }

    @Override
    public VersionManager getVersionManager() {
        return versionManager;
    }

    protected String getPluginLabel(PluginDescriptor pluginDescriptor) {
        return pluginDescriptor.getPluginId() + "@" + pluginDescriptor.getVersion();
    }

}

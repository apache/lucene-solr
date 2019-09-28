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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pf4j.util.DirectedGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class builds a dependency graph for a list of plugins (descriptors).
 * The entry point is the {@link #resolve(List)} method, method that returns a {@link Result} object.
 * The {@code Result} class contains nice information about the result of resolve operation (if it's a cyclic dependency,
 * they are not found dependencies, they are dependencies with wrong version).
 * This class is very useful for if-else scenarios.
 *
 * Only some attributes (pluginId, dependencies and pluginVersion) from {@link PluginDescriptor} are used in
 * the process of {@code resolve} operation.
 *
 * @author Decebal Suiu
 */
public class DependencyResolver {

	private static final Logger log = LoggerFactory.getLogger(DependencyResolver.class);

	private VersionManager versionManager;

    private DirectedGraph<String> dependenciesGraph; // the value is 'pluginId'
    private DirectedGraph<String> dependentsGraph; // the value is 'pluginId'
    private boolean resolved;

    public DependencyResolver(VersionManager versionManager) {
        this.versionManager = versionManager;
    }

    public Result resolve(List<PluginDescriptor> plugins) {
        // create graphs
        dependenciesGraph = new DirectedGraph<>();
        dependentsGraph = new DirectedGraph<>();

        // populate graphs
        Map<String, PluginDescriptor> pluginByIds = new HashMap<>();
        for (PluginDescriptor plugin : plugins) {
            addPlugin(plugin);
            pluginByIds.put(plugin.getPluginId(), plugin);
        }

        log.debug("Graph: {}", dependenciesGraph);

        // get a sorted list of dependencies
        List<String> sortedPlugins = dependenciesGraph.reverseTopologicalSort();
        log.debug("Plugins order: {}", sortedPlugins);

        // create the result object
        Result result = new Result(sortedPlugins);

        resolved = true;

        if (sortedPlugins != null) { // no cyclic dependency
            // detect not found dependencies
            for (String pluginId : sortedPlugins) {
                if (!pluginByIds.containsKey(pluginId)) {
                    result.addNotFoundDependency(pluginId);
                }
            }
        }

        // check dependencies versions
        for (PluginDescriptor plugin : plugins) {
            String pluginId = plugin.getPluginId();
            String existingVersion = plugin.getVersion();

            List<String> dependents = getDependents(pluginId);
            while (!dependents.isEmpty()) {
                String dependentId = dependents.remove(0);
                PluginDescriptor dependent = pluginByIds.get(dependentId);
                String requiredVersion = getDependencyVersionSupport(dependent, pluginId);
                boolean ok = checkDependencyVersion(requiredVersion, existingVersion);
                if (!ok) {
                    result.addWrongDependencyVersion(new WrongDependencyVersion(pluginId, dependentId, existingVersion, requiredVersion));
                }
            }
        }

        return result;
    }

    /**
     * Retrieves the plugins ids that the given plugin id directly depends on.
     *
     */
    public List<String> getDependencies(String pluginId) {
        checkResolved();
        return dependenciesGraph.getNeighbors(pluginId);
    }

    /**
     * Retrieves the plugins ids that the given content is a direct dependency of.
     *
     */
    public List<String> getDependents(String pluginId) {
        checkResolved();
        return dependentsGraph.getNeighbors(pluginId);
    }

    /**
     * Check if an existing version of dependency is compatible with the required version (from plugin descriptor).
     *
     */
    protected boolean checkDependencyVersion(String requiredVersion, String existingVersion) {
        return versionManager.checkVersionConstraint(existingVersion, requiredVersion);
    }

    private void addPlugin(PluginDescriptor descriptor) {
        String pluginId = descriptor.getPluginId();
        List<PluginDependency> dependencies = descriptor.getDependencies();
        if (dependencies.isEmpty()) {
            dependenciesGraph.addVertex(pluginId);
            dependentsGraph.addVertex(pluginId);
        } else {
            for (PluginDependency dependency : dependencies) {
                dependenciesGraph.addEdge(pluginId, dependency.getPluginId());
                dependentsGraph.addEdge(dependency.getPluginId(), pluginId);
            }
        }
    }

    private void checkResolved() {
        if (!resolved) {
            throw new IllegalStateException("Call 'resolve' method first");
        }
    }

    private String getDependencyVersionSupport(PluginDescriptor dependent, String dependencyId) {
        List<PluginDependency> dependencies = dependent.getDependencies();
        for (PluginDependency dependency : dependencies) {
            if (dependencyId.equals(dependency.getPluginId())) {
                return dependency.getPluginVersionSupport();
            }
        }

        throw new IllegalStateException("Cannot find a dependency with id '" + dependencyId +
            "' for plugin '" + dependent.getPluginId() + "'");
    }

	public static class Result {

	    private boolean cyclicDependency;
	    private List<String> notFoundDependencies; // value is "pluginId"
        private List<String> sortedPlugins; // value is "pluginId"
        private List<WrongDependencyVersion> wrongVersionDependencies;

        Result(List<String> sortedPlugins) {
            if (sortedPlugins == null) {
                cyclicDependency = true;
                this.sortedPlugins = Collections.emptyList();
            } else {
                this.sortedPlugins = new ArrayList<>(sortedPlugins);
            }

            notFoundDependencies = new ArrayList<>();
            wrongVersionDependencies = new ArrayList<>();
        }

        /**
         * Returns true is a cyclic dependency was detected.
         */
        public boolean hasCyclicDependency() {
            return cyclicDependency;
        }

	    /**
	     * Returns a list with dependencies required that were not found.
	     */
        public List<String> getNotFoundDependencies() {
            return notFoundDependencies;
        }

        /**
         * Returns a list with dependencies with wrong version.
         */
        public List<WrongDependencyVersion> getWrongVersionDependencies() {
            return wrongVersionDependencies;
        }

        /**
         * Get the list of plugins in dependency sorted order.
         */
        public List<String> getSortedPlugins() {
            return sortedPlugins;
        }

        void addNotFoundDependency(String pluginId) {
            notFoundDependencies.add(pluginId);
        }

        void addWrongDependencyVersion(WrongDependencyVersion wrongDependencyVersion) {
            wrongVersionDependencies.add(wrongDependencyVersion);
        }

    }

    public static class WrongDependencyVersion {

        private String dependencyId; // value is "pluginId"
        private String dependentId; // value is "pluginId"
        private String existingVersion;
        private String requiredVersion;

        WrongDependencyVersion(String dependencyId, String dependentId, String existingVersion, String requiredVersion) {
            this.dependencyId = dependencyId;
            this.dependentId = dependentId;
            this.existingVersion = existingVersion;
            this.requiredVersion = requiredVersion;
        }

        public String getDependencyId() {
            return dependencyId;
        }

        public String getDependentId() {
            return dependentId;
        }

        public String getExistingVersion() {
            return existingVersion;
        }

        public String getRequiredVersion() {
            return requiredVersion;
        }

    }

    /**
     * It will be thrown if a cyclic dependency is detected.
     */
    public static class CyclicDependencyException extends PluginException {

        public CyclicDependencyException() {
            super("Cyclic dependencies");
        }

    }

    /**
     * Indicates that the dependencies required were not found.
     */
    public static class DependenciesNotFoundException extends PluginException {

        private List<String> dependencies;

        public DependenciesNotFoundException(List<String> dependencies) {
            super("Dependencies '{}' not found", dependencies);

            this.dependencies = dependencies;
        }

        public List<String> getDependencies() {
            return dependencies;
        }

    }

    /**
     * Indicates that some dependencies have wrong version.
     */
    public static class DependenciesWrongVersionException extends PluginException {

        private List<WrongDependencyVersion> dependencies;

        public DependenciesWrongVersionException(List<WrongDependencyVersion> dependencies) {
            super("Dependencies '{}' have wrong version", dependencies);

            this.dependencies = dependencies;
        }

        public List<WrongDependencyVersion> getDependencies() {
            return dependencies;
        }

    }

}

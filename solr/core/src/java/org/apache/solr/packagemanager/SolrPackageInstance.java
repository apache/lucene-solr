package org.apache.solr.packagemanager;

import java.util.List;

import org.apache.solr.packagemanager.SolrPackage.Plugin;

/**
 * Describes one instance of a package as it exists in Solr when installed.
 */
public class SolrPackageInstance {
	final public String id;
	final public String description;
	final public String version;
	final public List<Plugin> plugins;
	
	public SolrPackageInstance(String id, String description, String version,
	    List<Plugin> plugins) {
		this.id = id;
		this.description = description;
		this.version = version;
		this.plugins = plugins;
	}

	public String getPluginId() {
		return id;
	}

	public String getPluginDescription() {
		return description;
	}

	public String getVersion() {
		return version;
	}

	public List<Plugin> getPlugins() {
    return plugins;
  }
}

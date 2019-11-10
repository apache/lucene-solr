package org.apache.solr.packagemanager;

import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.packagemanager.SolrPackage.Manifest;
import org.apache.solr.packagemanager.SolrPackage.Plugin;

/**
 * Describes one instance of a package as it exists in Solr when installed.
 */
public class SolrPackageInstance implements ReflectMapWriter {
	final public String name;
	final public String description;
	final public String version;
	final public Manifest manifest;
	final public List<Plugin> plugins;
	final Map<String, String> parameterDefaults;
	
	public SolrPackageInstance(String id, String description, String version, Manifest manifest,
	    List<Plugin> plugins, Map<String, String> params) {
		this.name = id;
		this.description = description;
		this.version = version;
		this.manifest = manifest;
		this.plugins = plugins;
		this.parameterDefaults = params;
	}

	public String getPackageName() {
		return name;
	}

	public String getPackageDescription() {
		return description;
	}

	public String getVersion() {
		return version;
	}

	public List<Plugin> getPlugins() {
    return plugins;
  }
}

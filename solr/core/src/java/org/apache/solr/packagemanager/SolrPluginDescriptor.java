package org.apache.solr.packagemanager;

import java.util.List;

import org.pf4j.DefaultPluginDescriptor;

public class SolrPluginDescriptor extends DefaultPluginDescriptor {
	final private String id;
	final private String description;
	final private String version;
	final private List<String> setupCommands;
  final private List<String> updateCommands;
	
	public SolrPluginDescriptor(String id, String description, String version,
	    List<String> setupCommands, List<String> updateCommands) {
		this.id = id;
		this.description = description;
		this.version = version;
		this.setupCommands = setupCommands;
    this.updateCommands = updateCommands;
	}

	@Override
	public String getPluginId() {
		return id;
	}

	@Override
	public String getPluginDescription() {
		return description;
	}

	@Override
	public String getVersion() {
		return version;
	}

	public List<String> getSetupCommands() {
		return setupCommands;
	}
  public List<String> getUpdateCommands() {
    return updateCommands;
  }

}

package org.apache.solr.packagemanager;

import java.util.List;

public class SolrPackage {
	public String name;
	public String version;
	public String repository;
	public String sha256;
	public List<String> setupCommands;
}
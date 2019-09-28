package org.apache.solr.packagemanager;


import java.util.List;

import org.apache.solr.packagemanager.pf4j.PluginInfo;

import com.google.gson.annotations.SerializedName;

/**
 * {@code PluginInfo} describing a plugin from a repository.
 */
public class SolrPluginInfo extends PluginInfo {

  public List<SolrPluginRelease> versions;

  public static class SolrPluginRelease extends PluginRelease {

    public String sig;

    Metadata metadata;
    @Override
    public String toString() {
      return "SolrPluginRelease{" +
          "version='" + version + '\'' +
          ", date=" + date +
          ", requires='" + requires + '\'' +
          ", url='" + url + '\'' +
          ", sig='" + sig + '\'' +
          ", min='" + metadata.minSolrVersion + '\'' +
          ", max='" + metadata.maxSolrVersion + '\'' +
          ", dependencies='" + metadata.dependencies + '\'' +
          ", plugins='" + metadata.plugins + '\'' +
          ", sha512sum='" + sha512sum + '\'' +
          '}';
    }
    
  }

  public static class Metadata {
    @SerializedName("min-solr-version")
    String minSolrVersion;
    @SerializedName("max-solr-version")
    String maxSolrVersion;

    List<String> dependencies;
    List<Plugin> plugins;
  }

  public static class Plugin {
    String id;
    @SerializedName("setup-command")
    public String setupCommand;

    @SerializedName("update-command")
    public String updateCommand;

    @SerializedName("uninstall-command")
    public String uninstallCommands;

    @SerializedName("verify-command")
    public String verifyCommand;

    @Override
    public String toString() {
      return id + ": {setup: "+setupCommand+", update: "+updateCommand+", uninstall: "+uninstallCommands+", verify: "+verifyCommand+"}";
    }
  }

}


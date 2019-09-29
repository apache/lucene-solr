package org.apache.solr.packagemanager;


import java.io.Serializable;
import java.util.Date;
import java.util.List;

import com.google.gson.annotations.SerializedName;

/**
 * {@code PluginInfo} describing a plugin from a repository.
 */
public class SolrPluginInfo implements Serializable, Comparable<SolrPluginInfo> {

  public String id;
  public String description;
  public List<SolrPluginRelease> versions;

  private String repositoryId;
  
  @Override
  public int compareTo(SolrPluginInfo o) {
      return id.compareTo(o.id);
  }

  public String getRepositoryId() {
      return repositoryId;
  }

  public void setRepositoryId(String repositoryId) {
      this.repositoryId = repositoryId;
  }


  public static class SolrPluginRelease {

    public String version;
    public Date date;
    public String requires;
    public String url;

    public String sha512sum;
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


package org.apache.solr.packagemanager;


import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a package (along with all released versions) as it appears in a repository.
 */
public class SolrPackage implements Serializable, Comparable<SolrPackage> {

  public String id;
  public String description;
  public List<SolrPackageRelease> versions;

  private String repositoryId;

  public static class SolrPackageRelease {

    public String version;
    public Date date;
    public String requires;
    public String url;

    public String sha512sum;
    public String sig;

    public Metadata metadata;
    @Override
    public String toString() {
      return "SolrPackageRelease{" +
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
    @JsonProperty("min-solr-version")
    public String minSolrVersion;
    @JsonProperty("max-solr-version")
    public String maxSolrVersion;

    public List<String> dependencies;
    public List<Plugin> plugins;
  }

  public static class Plugin {
    public String id;
    @JsonProperty("setup-command")
    public String setupCommand;

    @JsonProperty("update-command")
    public String updateCommand;

    @JsonProperty("uninstall-command")
    public String uninstallCommand;

    @JsonProperty("verify-command")
    public Command verifyCommand;

    @Override
    public String toString() {
      return id + ": {setup: "+setupCommand+", update: "+updateCommand+", uninstall: "+uninstallCommand+", verify: "+verifyCommand+"}";
    }
  }

  @Override
  public int compareTo(SolrPackage o) {
    return id.compareTo(o.id);
  }

  public String getRepositoryId() {
    return repositoryId;
  }

  public void setRepositoryId(String repositoryId) {
    this.repositoryId = repositoryId;
  }

  public static class Command {
    public String path;
    public String method;
    public Map payload;
    public String condition;
    public String expected;
    
    @Override
      public String toString() {
        return method + " " + path + ", Payload: "+ payload+", Condition: "+condition+", expected: "+expected;
      }
  }
}

